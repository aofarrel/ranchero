import sys
from src.statics import host_species, sample_sources, kolumns, countries, regions
from .config import RancheroConfig
import polars as pl
from tqdm import tqdm
from collections import OrderedDict # dictionaries are ordered in Python 3.7+, but OrderedDict has a better popitem() function we need
from . import _NeighLib as NeighLib

globals().update({f"_cfg_{name}": object() for name in [
	"rm_phages"
]})
_SENTINEL_TO_CONFIG = {
	_cfg_rm_phages: "rm_phages"
}

class ProfessionalsHaveStandards():
	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger
			self.taxoncore_ruleset = self.cfg.taxoncore_ruleset

	def _sentinal_handler(self, arg):
		"""Handles "allow overriding config" variables in function calls"""
		if arg in _SENTINEL_TO_CONFIG:
			config_attr = _SENTINEL_TO_CONFIG[arg]
			check_me = getattr(self.cfg, config_attr)
			assert check_me != arg, f"Configuration for '{config_attr}' is invalid or uninitialized"
			return check_me
		else:
			return arg

	def standardize_everything(self, polars_df, add_expected_nulls=True, assume_organism="Mycobacterium tuberculosis", assume_clade="tuberculosis"):
		if any(column in polars_df.columns for column in ['geoloc_info', 'country', 'region']):
			self.logging.info("Standardizing countries...")
			polars_df = self.standardize_countries(polars_df)
		
		if 'date_collected' in polars_df.columns:
			self.logging.info("Cleaning up dates...")
			polars_df = self.cleanup_dates(polars_df)

		if ['date_collected_year', 'date_collected_month'] in polars_df.columns:
			NeighLib.print_only_where_col_not_null(polars_df, 'date_collected_year', cols_of_interest=kolumns.id_columns+'date_collected'+'date_collected_year')
			NeighLib.print_only_where_col_not_null(polars_df, 'date_collected_month', cols_of_interest=kolumns.id_columns+'date_collected'+'date_collected_year')
			exit(1)
		
		if 'isolation_source' in polars_df.columns:
			self.logging.info("Standardizing isolation sources...")
			polars_df = self.standardize_sample_source(polars_df) # must be before taxoncore
		
		if 'host' in polars_df.columns:
			self.logging.info("Standardizing host organisms...")
			polars_df = self.standarize_hosts(polars_df)
		
		if 'host_disease' in polars_df.columns:
			self.logging.info("Standardizing host diseases...")
			polars_df = self.standardize_host_disease(polars_df)
		
		if any(column in polars_df.columns for column in ['genotype', 'lineage', 'strain', 'organism']):
			self.logging.info("Standardizing lineage, strain, and mycobacterial scientific names... (this may take a while)")
			polars_df = self.sort_out_taxoncore_columns(polars_df)
		elif add_expected_nulls:
			if 'organism' not in polars_df.columns:
				polars_df = NeighLib.add_column_of_just_this_value(polars_df, 'organism', assume_organism)
			if 'clade' not in polars_df.columns:
				polars_df = NeighLib.add_column_of_just_this_value(polars_df, 'clade', assume_clade)

		polars_df = self.drop_no_longer_useful_columns(polars_df)
		polars_df = NeighLib.check_index(polars_df)
		return polars_df

	def standardize_sample_source(self, polars_df):
		if polars_df.schema['isolation_source'] == pl.List:
			return self.standardize_sample_source_as_list(polars_df)
		else:
			return self.standardize_sample_source_as_string(polars_df)

	def inject_metadata(self, polars_df: pl.DataFrame, metadata_dictlist, dataset=None, overwrite=False):
		"""
		Modify a Rancheroized polars_df with BioProject-level metadata as controlled by a dictionary. For example:
		metadata_dictlist=[{"BioProject": "PRJEB15463", "country": "DRC", "region": "Kinshasa"}]

		Will create these polars expressions if overwrite is False:
		pl.when(pl.col("BioProject") == "PRJEB15463").and_(pl.col("country").is_null()).then(pl.lit("DRC")).otherwise(pl.col("country")).alias("country"), 
		pl.when(pl.col("BioProject") == "PRJEB15463").and_(pl.col("region").is_null()).then(pl.lit("Kinshasa")).otherwise(pl.col("region")).alias("region")

		If overwrite=True:
		pl.when(pl.col("BioProject") == "PRJEB15463").then(pl.lit("DRC")).otherwise(pl.col("country")).alias("country"), 
		pl.when(pl.col("BioProject") == "PRJEB15463").then(pl.lit("Kinshasa")).otherwise(pl.col("region")).alias("region")
		"""
		indicators = []
		drop_me = []
		for ordered_dictionary in metadata_dictlist:
			for key in ordered_dictionary:
				if key not in polars_df.columns:
					self.logging.warning(f"Attempted to inject {key} metadata, but existing column with that name doesn't exist")
					drop_me.append(key)
		assert type(metadata_dictlist[0]) == OrderedDict

		for ordered_dictionary in metadata_dictlist:
			# {"BioProject": "PRJEB15463", "country": "DRC", "region": "Kinshasa"}
			match = ordered_dictionary.popitem(last=False) # FIFO
			match_key, match_value = match[0], match[1] # "BioProject", "PRJEB15463"
			if overwrite:
				polars_expressions = [
					pl.when(pl.col(match_key) == match_value)
					.then(pl.lit(value))
					.otherwise(pl.col(key))
					.alias(key)
					for key, value in ordered_dictionary.items()
				]
			else:
				polars_expressions = [
					pl.when((pl.col(match_key) == match_value).and_(pl.col(key).is_null()))
					.then(pl.lit(value))
					.otherwise(pl.col(key))
					.alias(key)
					for key, value in ordered_dictionary.items()
				]
			polars_df = polars_df.with_columns(polars_expressions)

		# ['BioProject', 'PRJEB15463', 'FZB_DRC']
		if len(indicators) > 0:
			self.logging.debug("Processing indicators...")
			if self.cfg.indicator_column not in polars_df.columns:
				polars_df = polars_df.with_columns(pl.lit(None).alias(self.cfg.indicator_column))
			all_indicator_expressions = []
			for indicator_list in indicators:
				match_col, match_value, indicator_column, indicator_value = indicator_list[0], indicator_list[1], self.cfg.indicator_column, indicator_list[2]
				self.logging.debug(f"When {match_col} is {match_value}, then concatenate {indicator_value} to {indicator_column}")
				this_expression = [
						pl.when(pl.col(match_col) == match_value)
						.then(pl.concat_list([pl.lit(indicator_value), self.cfg.indicator_column]))
						.otherwise(pl.col(self.cfg.indicator_column))
						.alias(self.cfg.indicator_column)
					]
			polars_df = polars_df.with_columns(all_indicator_expressions)

		return polars_df

	def drop_no_longer_useful_columns(self, polars_df):
		"""ONLY RUN THIS AFTER ALL METADATA PROCESSING"""
		return polars_df.drop(kolumn for kolumn in kolumns.columns_to_drop_after_rancheroize if kolumn in polars_df.columns)

	def dictionary_match(self, polars_df, match_col: str, write_col: str, key: str, value, 
		substrings=False, 
		overwrite=False, 
		status_cols=False, 
		remove_match_from_list=False):
		"""
		Replace a pl.Utf8 or pl.List(pl.Utf8) column's values with the values in a dictionary per its key-value pairs.
		Case-insensitive. If substrings, will match substrings (ex: "US Virgin Islands" matches "US")

		Matched and Written columns are not started over if already existed in case this is being called in a for loop
		"""
		#self.logging.debug(f"Where {key} is in {match_col}, write {value} in {write_col} (substrings {substrings}, overwrite {overwrite}, status_cols {status_cols}, remove_match_from_list {remove_match_from_list})")
		if status_cols:
			polars_df = polars_df.with_columns(pl.lit(False).alias('matched')) if 'matched' not in polars_df.columns else polars_df.with_columns(pl.col('matched').fill_null(False))
			polars_df = polars_df.with_columns(pl.lit(False).alias('written')) if 'written' not in polars_df.columns else polars_df.with_columns(pl.col('written').fill_null(False))
		if write_col not in polars_df.columns:
			self.logging.debug(f"Write column {write_col} not in polars_df yet so we'll add it")
			polars_df = polars_df.with_columns(pl.lit(None).alias(write_col)) if write_col not in polars_df.columns else polars_df
		
		# matching expression
		if substrings and polars_df.schema[match_col] == pl.Utf8:
			found_a_match = pl.col(match_col).str.contains(f"(?i){key}")
		elif substrings and polars_df.schema[match_col] == pl.List(pl.Utf8):
			found_a_match = pl.col(match_col).list.contains(f"(?i){key}").any()
		elif not substrings and polars_df.schema[match_col] == pl.Utf8:
			found_a_match = pl.col(match_col).str.to_lowercase() == key.lower()
		elif not substrings and polars_df.schema[match_col] == pl.List(pl.Utf8):
			found_a_match = pl.col(match_col).list.eval(pl.element().str.to_lowercase() == key.lower()).list.any()
		else:
			self.logging.warning(f"Invalid type {polars[match_col].schema} for match_col named {match_col}")
			return polars_df
	
		# write_col expression -- true if write column is empty list, empty string, or pl.Null
		if polars_df.schema[write_col] == pl.List:
			write_col_is_empty = (pl.col(write_col).is_null()).or_(pl.col(write_col).list.len() < 1)
		elif polars_df.schema[write_col] == pl.Utf8:
			write_col_is_empty = (pl.col(write_col).is_null()).or_(pl.col(write_col).str.len_bytes() == 0)
		else:
			write_col_is_empty = pl.col(write_col).is_null()

		if status_cols:
			polars_df = polars_df.with_columns([
				# match status
				pl.when(found_a_match).then(True).otherwise(pl.col('matched')).alias('matched'),

				# write status
				pl.when((found_a_match).and_(
					(pl.lit(overwrite) == True).or_(write_col_is_empty)
				))
				.then(True)
				.otherwise(pl.col('written'))
				.alias('written'),

				# actual writing
				pl.when((found_a_match)
				.and_(
					(pl.lit(overwrite) == True).or_(write_col_is_empty)
				))
				.then(pl.lit(value))
				.otherwise(pl.col(write_col))
				.alias(write_col)
			])
		else:
			polars_df = polars_df.with_columns([
				pl.when((found_a_match)
				.and_(
					(pl.lit(overwrite) == True).or_(write_col_is_empty)
				))
				.then(pl.lit(value))
				.otherwise(pl.col(write_col))
				.alias(write_col)
			])

		if remove_match_from_list and polars_df.schema[match_col] == pl.List(pl.Utf8):
			if substrings:
				filter_exp = pl.element.str.contains(key)
			else:
				filter_exp = pl.element().str.to_lowercase() != key.lower()
			polars_df = polars_df.with_columns([
				pl.when(found_a_match)
				.then(pl.col(match_col).list.eval(pl.element().filter(filter_exp)))
				.otherwise(pl.col(match_col))
				.alias(match_col)
			])
		if self.logging.getEffectiveLevel() == 10:
			if status_cols:
				pass
				#print(polars_df.select(['run_index', write_col, 'geoloc_info', 'matched', 'written']))
			else:
				pass
				#print(polars_df.select(['run_index', write_col, 'geoloc_info']))
		return polars_df

	def standardize_host_disease(self, polars_df):
		assert 'host_disease' in polars_df.columns
		for host_disease, simplified_host_disease in sample_sources.host_disease_exact_match.items():
			polars_df = self.dictionary_match(polars_df, match_col='host_disease', write_col='host_disease', key=host_disease, value=simplified_host_disease, substrings=True)
		for host_disease, simplified_host_disease in sample_sources.host_disease.items():
			polars_df = self.dictionary_match(polars_df, match_col='host_disease', write_col='host_disease', key=host_disease, value=simplified_host_disease, substrings=False)
		return polars_df

	def standardize_sample_source_as_list(self, polars_df, write_hosts=True, write_lineages=True):
		assert 'isolation_source' in polars_df.columns
		assert polars_df.schema['isolation_source'] == pl.List

		if write_lineages:
			if 'lineage_sam' in polars_df.columns and polars_df.schema['lineage_sam'] == pl.Utf8:
				lineage, skip_lineage = 'lineage_sam', False
			elif 'lineage' in polars_df.columns and polars_df.schema['lineage'] == pl.Utf8:
				lineage, skip_lineage = 'lineage', False
			else:
				self.logging.warning("write_lineages is True, but can't find a lineage column!")
				skip_lineage = True
			if 'strain_sam_ss_dpl139' in polars_df.columns and polars_df.schema['strain_sam_ss_dpl139'] == pl.Utf8:
				strain, skip_strain = 'strain_sam_ss_dpl139', False
			elif 'strain' in polars_df.columns and polars_df.schema['strain'] == pl.Utf8:
				strain, skip_strain = 'strain', False
			else:
				self.logging.warning("write_lineages is True, but can't find a strain column!")
				skip_strain = True
			
			if not skip_lineage:
				polars_df = polars_df.with_columns([
					pl.when(
						pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)lineage4.6.2.2')).list.any()
					)
					.then(pl.lit('lineage4.6.2.2'))
					.otherwise(pl.col('lineage_sam'))
					.alias('lineage_sam')
				])
			if not skip_strain:
				polars_df = polars_df.with_columns([
					pl.when(
						pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)H37Rv')).list.any()
					)
					.then(pl.lit('H37Rv'))
					.otherwise(pl.col('strain_sam_ss_dpl139'))
					.alias('strain_sam_ss_dpl139')
				])

		if write_hosts:
			human = pl.lit(['Homo sapiens']) if polars_df.schema['host'] == pl.List else pl.lit('Homo sapiens') 
			mouse = pl.lit(['Mus musculus']) if polars_df.schema['host'] == pl.List else pl.lit('Mus musculus') 
			cow = pl.lit(['Bos tarus']) if polars_df.schema['host'] == pl.List else pl.lit('Bos tarus') 
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)human|sapiens')).list.any()
				)
				.then(human)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)mouse|musculus')).list.any()
				)
				.then(mouse)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)cow|taurus')).list.any() # do not match "bovine" as that can be taxoncore
				)
				.then(cow)
				.otherwise(pl.col('host'))
				.alias('host')
			])


		# and now, the stuff in the actual sample source column 
		for unhelpful_value in sample_sources.sample_sources_nonspecific:
			polars_df = polars_df.with_columns(
				pl.col('isolation_source').list.eval(pl.element().filter(pl.element() != unhelpful_value)).alias('isolation_source')
			)

		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)laboratory strain')).list.any()
				.or_(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)lab strain')).list.any()
				)
			)
			.then(pl.lit(['laboratory strain']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)culture')).list.any()
				.and_(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)sputum')).list.any()
				)
			)
			.then(pl.lit(['culture from sputum']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)culture')).list.any()
				.and_(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)feces')).list.any()
				)
			)
			.then(pl.lit(['culture from feces']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)scrapate')).list.any()
				.and_(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)granuloma')).list.any()
				)
			)
			.then(pl.lit(['scrapate of gramuloma']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])

		for sample_source, simplified_sample_source in sample_sources.sample_source.items():
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='isolation_source', key=sample_source, value=simplified_sample_source, substrings=True)
		for sample_source, simplified_sample_source in sample_sources.sample_source_exact_match.items():
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='isolation_source', key=sample_source, value=simplified_sample_source, substrings=False)
		
		self.logging.info(f"The isolation_source column has type list. We will be .join()ing them into strings.") # done AFTER most standardization
		polars_df = polars_df.with_columns(
			pl.col("isolation_source").list.join(", ").alias("isolation_source")
		)

		return polars_df

	def standardize_sample_source_as_string(self, polars_df):
		assert 'isolation_source' in polars_df.columns
		assert polars_df.schema['isolation_source'] == pl.Utf8
		for sample_source, simplified_sample_source in sample_sources.sample_source_exact_match.items():
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='isolation_source', key=sample_source, value=simplified_sample_source, substrings=False)
		for sample_source, simplified_sample_source in sample_sources.sample_source.items():
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='isolation_source', key=sample_source, value=simplified_sample_source, substrings=True)
		return polars_df
	
	def standarize_hosts(self, polars_df, eager=True):
		if polars_df.schema['host'] == pl.List:
			#self.logging.info(f"The host column has type list. We will take the first value as the source of truth.") # done BEFORE most standardization
			#polars_df = polars_df.with_columns(pl.col('host').list.first().alias('host'))
			polars_df = polars_df.with_columns(pl.col('host').list.join(", ").alias('host'))
		assert polars_df.schema['host'] == pl.Utf8
		if eager:
			polars_df = self.standardize_hosts_eager(polars_df)
		else:
			polars_df = self.standardize_hosts_lazy(polars_df)
		polars_df = polars_df.drop('host')
		return polars_df

	def standardize_hosts_eager(self, polars_df):
		"""
		Checks for string matches in hosts column. This is "eager" in the sense that matches are checked even
		though non-nulls are not filled in, so you could use this to overwrite. Except that isn't implemented yet.

		Assumes polars_df has column 'host' but not 'host_scienname', 'host_confidence', nor 'host_commonname'

		We have to use "host_scienname" instead of "host_sciname" as there is already an sra column with that name.
		"""
		polars_df = polars_df.with_columns(host_scienname=None, host_confidence=None, host_commonname=None)
		assert polars_df.schema['host'] == pl.Utf8
		
		for host, (sciname, confidence, streetname) in host_species.species.items():
			polars_df = polars_df.with_columns([
				pl.when(pl.col('host').str.contains(host))
				.then(pl.lit(sciname))
				.otherwise(
					pl.when(pl.col('host_scienname').is_not_null())
					.then(pl.col('host_scienname'))
					.otherwise(None))
				.alias("host_scienname"),
				
				pl.when(pl.col('host').str.contains(host))
				.then(pl.lit(confidence))
				.otherwise(
					pl.when(pl.col('host_confidence').is_not_null())
					.then(pl.col('host_confidence'))
					.otherwise(None))
				.alias("host_confidence"),
				
				pl.when(pl.col('host').str.contains(host))
				.then(pl.lit(streetname))
				.otherwise(
					pl.when(pl.col('host_commonname').is_not_null())
					.then(pl.col('host_commonname'))
					.otherwise(None))
				.alias("host_commonname"),
			])

		for host, (sciname, confidence, streetname) in host_species.exact_match_only.items():
			polars_df = polars_df.with_columns([
				pl.when(pl.col('host') == host)
				.then(pl.lit(sciname))
				.otherwise(
					pl.when(pl.col('host_scienname').is_not_null())
					.then(pl.col('host_scienname'))
					.otherwise(None))
				.alias("host_scienname"),
				
				pl.when(pl.col('host') == host)
				.then(pl.lit(confidence))
				.otherwise(
					pl.when(pl.col('host_confidence').is_not_null())
					.then(pl.col('host_confidence'))
					.otherwise(None))
				.alias("host_confidence"),
				
				pl.when(pl.col('host') == host)
				.then(pl.lit(streetname))
				.otherwise(
					pl.when(pl.col('host_commonname').is_not_null())
					.then(pl.col('host_commonname'))
					.otherwise(None))
				.alias("host_commonname"),
			])
		polars_df = self.unmask_mice(self.unmask_badgers(polars_df))
		return polars_df

	def cleanup_dates(self, polars_df, keep_only_bad_examples=False, err_on_list=True, force_strings=True):
		"""
		Notes:
		* keep_only_bad_examples is for debugging; it effectively hides dates that are probably good
		* len_bytes() is way faster than len_chars()
		* yeah you can have mutliple expressions in one with_columns() but that'd require tons of alias columns plus nullify so I'm not doing that
		"""

		if polars_df.schema['date_collected'] == pl.List:
			polars_df = NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=['date_collected'])
			if polars_df.schema['date_collected'] == pl.List:
				if err_on_list:
					self.logging.error("Tried to flatten date_collected, but there seems to be some rows with unique values.")
					print(NeighLib.get_rows_where_list_col_more_than_one_value(polars_df, 'date_collected').select([NeighLib.get_index_column(polars_df), 'date_collected']))
					exit(1)
				else:
					self.logging.warning("Tried to flatten date_collected, but there seems to be some rows with unique values. Will convert to string. This may be less accurate.")
					polars_df = NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, force_strings=True, just_these_columns=['date_collected'])

		if polars_df.schema['date_collected'] != pl.Utf8 and force_strings:
			self.logging.warning("date_collected column is not of type string. Will attempt to cast it as string.")
			polars_df = polars_df.with_columns(
				pl.col("date_collected").cast(pl.Utf8).alias("date_collected")
			)

		# YYYY/YYYY
		polars_df = polars_df.with_columns(
			pl.when((pl.col('date_collected').str.len_bytes() == 9)
			.and_(pl.col('date_collected').str.count_matches("/") == 1))
			.then(None).otherwise(pl.col('date_collected')).alias("date_collected")
		)

		# YYYY/YYYY/YYYY
		polars_df = polars_df.with_columns(
			pl.when((pl.col('date_collected').str.len_bytes() == 14)
			.and_(pl.col('date_collected').str.count_matches("/") == 2))
			.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
		)

		# YYYY/YYYY/YYYY/YYYY
		polars_df = polars_df.with_columns(
			pl.when((pl.col('date_collected').str.len_bytes() == 19)
			.and_(pl.col('date_collected').str.count_matches("/") == 3))
			.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
		)
		
		# YYYY/MM or MM/YYYY
		polars_df = polars_df.with_columns(
			pl.when((pl.col('date_collected').str.len_bytes() == 7)
			.and_(pl.col('date_collected').str.count_matches("/") == 1))
			.then(pl.col('date_collected').str.extract(r'[0-9][0-9][0-9][0-9]', 0)).otherwise(pl.col('date_collected')).alias("date_collected")
		)

		# MM/DD/YYYY or DD/MM/YYYY (ambigious, so just get year)
		polars_df = polars_df.with_columns(
			pl.when((pl.col('date_collected').str.len_bytes() == 10)
				.and_(
					(pl.col('date_collected').str.count_matches("/") == 2)
					.or_(pl.col('date_collected').str.count_matches("-") == 2)
				)
			)
			.then(pl.col('date_collected').str.extract(r'[0-9][0-9][0-9][0-9]', 0)).otherwise(pl.col('date_collected')).alias("date_collected")
		)

		# YYYY-MM/YYYY-MM
		polars_df = polars_df.with_columns([
			pl.when((pl.col('date_collected').str.len_bytes() == 15)
			.and_(pl.col('date_collected').str.count_matches("/") == 1)
			.and_(pl.col('date_collected').str.count_matches("-") == 2))
			.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
		])

		# YYYY-MM-DDT00:00:00Z
		polars_df = polars_df.with_columns([
			pl.when((pl.col('date_collected').str.len_bytes() == 20)
			.and_(pl.col('date_collected').str.count_matches("Z") == 1)
			.and_(pl.col('date_collected').str.count_matches(":") == 2))
			.then(pl.col('date_collected').str.extract(r'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]', 0)).otherwise(pl.col('date_collected')).alias("date_collected"),
		])

		# YYYY-MM-DD/YYYY-MM-DD
		polars_df = polars_df.with_columns(
			pl.when((pl.col('date_collected').str.len_bytes() == 21))
			.then(None)
			.otherwise(pl.col('date_collected'))
			.alias("date_collected"),
		)

		polars_df = polars_df.with_columns(
			pl.when((pl.col('date_collected') == '0')
				.or_(pl.col('date_collected') == '0000')
				.or_(pl.col('date_collected') == '1970-01-01')
				.or_(pl.col('date_collected') == '1900')
			)
			.then(None)
			.otherwise(pl.col('date_collected'))
			.alias("date_collected"),
		)

		if 'sample_index' in polars_df.columns:
			polars_df = polars_df.with_columns(
				pl.when((pl.col('date_collected') == '0')
					.or_(pl.col('sample_index') == 'SAMEA5977381') # 2025/2026
					.or_(pl.col('sample_index') == 'SAMEA5977380') # 2025/2026
				)
				.then(None)
				.otherwise(pl.col('date_collected'))
				.alias("date_collected"),
			)

		if 'date_collected_year' in polars_df.columns:
			polars_df = NeighLib.try_nullfill(polars_df, 'date_collected', 'date_collected_year')[0]
			polars_df.drop('date_collected_year')

		# this is going to be annoying to handle properly and might not ever be helpful -- low priority TODO
		if 'date_collected_month' in polars_df.columns:
			polars_df.drop('date_collected_month')

		if keep_only_bad_examples:
			polars_df = polars_df.with_columns(
				pl.when(pl.col('date_collected').str.len_bytes() == 4)
				.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
			)
			polars_df = polars_df.with_columns(
				pl.when(pl.col('date_collected').str.len_bytes() == 10)
				.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
			)
			polars_df = polars_df.with_columns(
				pl.when(pl.col('date_collected').str.len_bytes() == 7)
				.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
			)
			polars_df = polars_df.with_columns(
				pl.when((pl.col('date_collected').str.len_bytes() == 14))
				.then(pl.lit('foo')).otherwise(pl.col('date_collected')).alias("date_collected"),
			)

		return polars_df

	def unmask_badgers(self, polars_df):
		"""
		TODO: this doesn't add a confidence score
		"""
		if 'anonymised_badger_id_sam' in polars_df.columns:
			polars_df = polars_df.with_columns([
				pl.when((pl.col('anonymised_badger_id_sam').is_not_null()) & (pl.col('host_commonname').is_null()))
				.then(pl.lit('badger'))
				.otherwise(pl.col('host_commonname'))
				.alias('host_commonname')
			])
		return polars_df.drop('anonymised_badger_id_sam')

	def unmask_mice(self, polars_df):
		"""
		TODO: this doesn't add a confidence score
		"""
		if 'mouse_strain_sam' in polars_df.columns:
			polars_df = polars_df.with_columns([
				pl.when((pl.col('mouse_strain_sam').is_not_null()) & (pl.col('host_commonname').is_null()))
				.then(pl.lit('mouse'))
				.otherwise(pl.col('host_commonname'))
				.alias('host_commonname'),

				pl.when((pl.col('mouse_strain_sam').is_not_null()) & (pl.col('host_scienname').is_null()))
				.then(pl.lit('Mus musculus'))
				.otherwise(pl.col('host_scienname'))
				.alias('host_scienname')
			])
		return polars_df.drop('mouse_strain_sam')

	# because polars runs with_columns() matches in parallel, this is probably the most effecient way to do this. but having four functions for it is ugly.
	def taxoncore_GO(self, polars_df, match_string, i_group, i_organism, exact=False):
		if exact:
			polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element() == match_string).list.any())
				.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element() == match_string).list.any())
				.then(pl.lit(i_organism) if i_organism is not pl.Null else pl.Null).otherwise(pl.col('i_organism')).alias('i_organism')
			])
		else:
			polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
				.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
				.then(pl.lit(i_organism) if i_organism is not pl.Null else pl.Null).otherwise(pl.col('i_organism')).alias('i_organism')
			])
		return polars_df
	
	def taxoncore_GOS(self, polars_df, match_string, i_group, i_organism, i_strain):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism) if i_organism is not pl.Null else pl.Null).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_strain) if i_strain is not pl.Null else pl.Null).otherwise(pl.col('i_strain')).alias('i_strain')
		])
		return polars_df
	
	def taxoncore_GOL(self, polars_df, match_string, i_group, i_organism, i_lineage):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism) if i_organism is not pl.Null else pl.Null).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_lineage) if i_lineage is not pl.Null else pl.Null).otherwise(pl.col('i_lineage')).alias('i_lineage')
		])
		return polars_df
	
	def taxoncore_GOLS(self, polars_df, match_string, i_group, i_organism, i_lineage, i_strain):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism) if i_organism is not pl.Null else pl.Null).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_lineage) if i_lineage is not pl.Null else pl.Null).otherwise(pl.col('i_lineage')).alias('i_lineage'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_strain) if i_strain is not pl.Null else pl.Null).otherwise(pl.col('i_strain')).alias('i_strain')
		])
		return polars_df

	def taxoncore_iterate_rules(self, polars_df):
		# TODO: I really don't like that we're iterating like this as it sort of blocks the advantage of using polars.
		# Is there a better way of doing this? Tried a few things but so far this one seems the most reliable.
		if self.cfg.taxoncore_ruleset is None:
			raise ValueError("A taxoncore ruleset failed to initialize, so we cannot use function taxoncore_iterate_rules!")
		
		for when, strain, lineage, organism, bacterial_group, comment in tqdm((entry.values() for entry in self.cfg.taxoncore_ruleset), desc="Processing taxonomy", total=len(self.cfg.taxoncore_ruleset),  ascii='➖🌱🐄', bar_format='{desc:<5.5}{percentage:3.0f}%|{bar:10}{r_bar}'):
			
			#if organism == "Mycobacterium tuberculosis sp.":
			#	print(f"strain: {strain} {type(strain)}\nlineage: {lineage} {type(lineage)}\norganism: {organism}\ngroup: {bacterial_group}")
			#	NeighLib.print_col_where(column="organism", equals="Mycobacterium tuberculosis sp.", cols_of_interest=kolumns.id_columns)

			if strain is pl.Null and lineage is pl.Null:
				polars_df = self.taxoncore_GO(polars_df, when, i_group=bacterial_group, i_organism=organism)
			elif strain is pl.Null:
				polars_df = self.taxoncore_GOL(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_lineage=lineage)
			elif lineage is pl.Null:
				polars_df = self.taxoncore_GOS(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_strain=strain)
			else:
				#self.logging.debug(f"strain: {strain} {type(strain)}\nlineage: {lineage} {type(lineage)}\norganism: {organism}\ngroup: {bacterial_group}")
				polars_df = self.taxoncore_GOLS(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_lineage=lineage, i_strain=strain)

			#if organism == "Mycobacterium tuberculosis sp.":
			#	print(f"strain: {strain} {type(strain)}\nlineage: {lineage} {type(lineage)}\norganism: {organism}\ngroup: {bacterial_group}")
			#	NeighLib.print_col_where(column="organism", equals="Mycobacterium tuberculosis sp.", cols_of_interest=kolumns.id_columns)


		return polars_df

	def sort_out_taxoncore_columns(self, polars_df, rm_phages=_cfg_rm_phages):
		"""
		Some columns in polars_df will be in list all_taxoncore_columns. We want to use these taxoncore columns to create three new columns:
		* i_organism should be of form "Mycobacterium" plus one more word, with no trailing "subsp." or "variant", if a specific organism can be imputed from a taxoncore column, else null
		* i_lineage should be of form "L" followed by a float if a specific lineage can be imputed from a taxoncore column, else null
		* i_strain are strings if a specific strain can be imputed from a taxoncore column, else null

		Rules:
		* Any column with value "Mycobacterium tuberculosis H37Rv" sets i_organism to "Mycobacterium tuberculosis", i_lineage to "L4.8", and i_strain to "H37Rv"
		* Any column with value "Mycobacterium variant bovis" sets i_organism to "Mycobacterium bovis"
		* Any column with "lineage" followed by numbers sets i_lineage to "L" plus the numbers, minus any whitespace (there may be periods between the numbers, keep them)

		"""
		group_column_name = "clade"
		assert 'i_group' not in polars_df.columns
		assert 'i_organism' not in polars_df.columns
		assert 'i_lineage' not in polars_df.columns
		assert 'i_strain' not in polars_df.columns
		assert 'taxoncore_list' not in polars_df.columns
		rm_phages = self._sentinal_handler(rm_phages)
		if group_column_name not in kolumns.columns_to_keep_after_rancheroize:
			self.logging.warning(f"Bacterial group column will have name {group_column_name}, but might get removed later. Add {group_column_name} to kolumns.equivalence!")
		if 'organism' in polars_df.columns and rm_phages:
			polars_df = self.rm_all_phages(polars_df)
		merge_these_columns = [col for col in polars_df.columns if col in sum(kolumns.special_taxonomic_handling.values(), [])]

		for col in merge_these_columns:
			self.logging.debug(f"Incoming taxoncore column {col} is type {polars_df.schema[col]}")
			if polars_df.schema[col] == pl.List:
				polars_df = polars_df.with_columns(pl.col(col).list.join(", ").alias(col))
				self.logging.debug("->Joined into string")
			#assert polars_df.schema[col] == pl.Utf8
		
		# taxoncore_list used for most matches,
		# but to extract lineages with regex we also need a column without lists
		polars_df = polars_df.with_columns(pl.concat_list([pl.col(col) for col in merge_these_columns]).alias("taxoncore_list"))
		polars_df = polars_df.with_columns(pl.col("taxoncore_list").list.join("; ").alias("taxoncore_str"))
		for col in merge_these_columns:
			polars_df = polars_df.drop(col)
		polars_df = polars_df.with_columns(i_group=None, i_lineage=None, i_organism=None, i_strain=None) # initalize new columns to prevent ColumnNotFoundError

		# try extracting lineages using regex
		polars_df = polars_df.with_columns([
			pl.when(pl.col('taxoncore_str').str.contains(r'\bL[0-9]{1}(\.[0-9]{1})*')
				.and_(~pl.col('taxoncore_str').str.contains(r'\b[Ll][0-9]{2,}'))
			)
			.then(pl.col('taxoncore_str').str.extract(r'\bL[0-9](\.[0-9]{1})*', 0)).otherwise(pl.col('i_lineage')).alias('i_lineage')])

		# now try taxoncore ruleset
		if self.cfg.taxoncore_ruleset is None:
			self.logging.warning("Taxoncore ruleset was not initialized, so only basic matching will be performed.")
		else:
			polars_df = self.taxoncore_iterate_rules(polars_df)
		
		polars_df = polars_df.with_columns(pl.col("i_group").alias(group_column_name))
		polars_df = polars_df.with_columns([pl.col("i_lineage").alias("lineage"), pl.col("i_organism").alias("organism"), pl.col("i_strain").alias("strain")])
		polars_df = polars_df.drop(['taxoncore_list', 'taxoncore_str', 'i_group', 'i_lineage', 'i_organism', 'i_strain'])
		assert 'i_strain' not in polars_df

		return polars_df

	def rm_all_phages(self, polars_df, inverse=False, column='organism'):
		assert column in polars_df.columns
		if not inverse:
			return polars_df.filter(~pl.col(column).str.contains_any(["phage"]))
		else:
			return polars_df.filter(pl.col(column).str.contains_any(["phage"]))

	def move_mismatches(self, polars_df, in_col, out_col, soft_overwrite=False, hard_overwrite=False):
		"""
		Where pl.col('matched') is False, move values from in_col into out_col.
		* soft overwrite: overwrite if out_col is not list, else simply add to list
		* hard overwrite: overwrite. just do it.
		"""
		if out_col not in polars_df.columns:
			polars_df = polars_df.with_columns(pl.Null.alias(out_col))
		
		# out_col expression -- true if write column is empty list, empty string, or pl.Null
		if polars_df[out_col].schema == pl.List:
			write_col_is_empty = (pl.col(out_col).is_null()).or_(pl.col(out_col).list.len() < 1)
		elif polars_df[out_col].schema == pl.Utf8:
			write_col_is_empty = (pl.col(out_col).is_null()).or_(pl.col(out_col).str.len_bytes() == 0)
		else:
			write_col_is_empty = pl.col(out_col).is_null()
		
		if hard_overwrite:
			polars_df = polars_df.with_columns([
				pl.when(pl.col('matched') == False).then(pl.col(in_col)).otherwise(pl.col(in_col)).alias(out_col),
				pl.when(pl.col('matched') == False).then(None).otherwise(pl.col(in_col)).alias(f"{in_col}_temp"), # avoid duplicate column errors
			])
		else:
			if polars_df[out_col].schema == pl.List:
				polars_df = polars_df.with_columns([
					pl.when(pl.col('matched') == False).then(pl.col(out_col).list.concat([pl.col(in_col)])).otherwise(pl.col(out_col)).alias(out_col),
					pl.when(pl.col('matched') == False).then(None).otherwise(pl.col(in_col)).alias(f"{in_col}_temp"),  # avoid duplicate column errors
				])
			else:
				polars_df = polars_df.with_columns([
					pl.when((pl.col('matched') == False).and_((write_col_is_empty).or_(soft_overwrite))).then(pl.col(in_col)).otherwise(pl.col(in_col)).alias(out_col),
					pl.when((pl.col('matched') == False).and_((write_col_is_empty).or_(soft_overwrite))).then(None).otherwise(pl.col(in_col)).alias(f"{in_col}_temp"), # avoid duplicate column errors
				])
		return polars_df.drop(in_col).rename({f"{in_col}_temp": in_col})

	def move_and_cleanup_after_tracked_match(self, polars_df, in_col, out_col):
		polars_df = self.move_mismatches(polars_df, in_col=in_col, out_col=out_col)
		polars_df = polars_df.drop(['matched', 'written'])
		assert 'matched' not in polars_df.columns()
		return polars_df
	
	def standardize_countries(self, polars_df, try_rm_geoloc_info=False):
		# We expect to be starting out with at least one of the following:
		# * country (type str)
		# * geoloc_info (type list)
		# Outputs:
		# country, region, and geoloc_info
		# If only country exists, ISO that list. Whatever doesn't get ISO'd gets moved to 'continent' if it matches a continent, otherwise will be moved to 'region' (no overwrite).
		# Region and continent keep type str the entire time.
		# If only geoloc_info exists, go through the list pulling out countries by ISO matching, then continents. Anything remaining move to region.
		# If both exist, ISO convert country column, then do continent/region matching on geoloc_info column.

		# TODO: assert intermediate columns like 'likely_country' not in df
		united_nations = {**countries.substring_match, **countries.exact_match}

		#####
		if self.logging.getEffectiveLevel() == 10:
			self.logging.debug("---- Start of function ----")
			NeighLib.print_a_where_b_is_in_list(polars_df, col_a='geoloc_info', col_b='run_index', list_to_match=['ERR841442', 'ERR5908244', 'SRR12380906', 'SRR18054772', 'SRR10394499', 'ERR732681', 'SRR9971324'], alsoprint=['country', 'matched', 'written', 'continent'])
		#####
		if 'country' in polars_df.columns and 'geoloc_info' in polars_df.columns:
			self.logging.debug("geoloc_info ✔️ country ✔️")
			# This DOES NOT force everything to be ISO standard in country column, since if you have stuff in that column already I assume you want it there

			for nation, ISO3166 in countries.substring_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', key=nation, value=ISO3166, substrings=True, overwrite=True, status_cols=False)
			for nation, ISO3166 in countries.exact_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', key=nation, value=ISO3166, substrings=False, overwrite=True, status_cols=False)

			# If geoloc_info can become a str 'region' column, and 'region' column doesn't already exist, let's do that
			# ...but that's computationally expensive and we want to parse geoloc_info for continents so actually let's not do this here
			#if try_rm_geoloc_info:
			#	polars_df = NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=['geoloc_info'])
			#	if polars_df['geoloc_info'].schema == pl.Utf8 and 'region' not in polars_df.columns:
			#		polars_df = polars_df.rename({'geoloc_info': 'region'})

		elif 'country' in polars_df.columns and 'geoloc_info' not in polars_df.columns:
			self.logging.debug("geoloc_info ✖️ country ✔️")
			# This DOES force everything to be ISO standard in country column
			for nation, ISO3166 in countries.substring_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', key=nation, value=ISO3166, substrings=True, overwrite=True, status_cols=False, remove_match_from_list=True)
			for nation, ISO3166 in countries.exact_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', key=nation, value=ISO3166, substrings=False, overwrite=True, status_cols=False, remove_match_from_list=True)		
			self.validate_col_country(polars_df)
			return polars_df
		
		elif 'geoloc_info' in polars_df.columns and 'country' not in polars_df.columns: # and not 'country'
			self.logging.debug("geoloc_info ✔️ country ✖️")
			# To handle "country: region" metadata without overwriting the region metadata, first we attempt to extract countries by looking for non-substring matches,
			# including the countries.substring_match stuff we usually just substring match upon.
			for nation, ISO3166 in tqdm(united_nations.items(), desc="Matching on countries...", ascii='➖🌱🐄', bar_format='{desc:<5.5}{percentage:3.0f}%|{bar:10}{r_bar}'):
				polars_df = self.dictionary_match(polars_df, match_col='geoloc_info', write_col='country', key=nation, value=ISO3166, substrings=False, overwrite=False, status_cols=False, remove_match_from_list=True)
		else:
			self.logging.warning("Neither 'country' nor 'geoloc_info' found in dataframe. Cannot standardize.")
			return polars_df

		# Our dataframe now is guranteed to have a country column and a geoloc_info column.
		# (TODO: Ensure the initial geoloc_info ✖️ country ✔️ case results in a geoloc_info column of type list, not str)
		assert polars_df.schema['geoloc_info'] == pl.List(pl.Utf8)
		
		# Now let's try to pull continent information from geoloc_info 
		for continent, that_same_continent in regions.continents.items():
			polars_df = self.dictionary_match(polars_df, match_col='geoloc_info', write_col='continent', key=continent, value=that_same_continent, substrings=False, overwrite=False, status_cols=False, remove_match_from_list=True)

		# Make sure we don't have junk from hypothetical previous runs, or weird columns
		assert 'likely_country' not in polars_df.columns
		polars_df = polars_df.with_columns(pl.lit(None).alias('likely_country')) # needs to be initialized since it's in an otherwise()
		assert 'def_country' not in polars_df.columns
		polars_df = polars_df.with_columns(pl.lit(None).alias('def_country')) # needs to be initialized since it's in an otherwise()
		assert 'region_as_list' not in polars_df.columns
		assert 'neo_region' not in polars_df.columns
		
		# We can allow a pre-existing region column though
		if 'region' not in polars_df.columns:
			polars_df = polars_df.with_columns(pl.lit(None).alias('region'))

		# Exact matches for continent and country have been moved, now look for "country: region" or "continent: country" matches
		# These use str.starts_with()
		for continent, that_same_continent in regions.continents.items():
			NeighLib.print_a_where_b_is_in_list(polars_df, col_a='geoloc_info', col_b='run_index', list_to_match=['ERR841442', 'ERR5908244', 'SRR12380906', 'SRR18054772', 'SRR10394499', 'SRR9971324', 'ERR732681', 'SRR23310897'], alsoprint=['country', 'continent'])
			assert polars_df.schema['geoloc_info'] == pl.List(pl.Utf8)
			polars_df = polars_df.with_columns([

				pl.when((pl.col('geoloc_info').list.eval(pl.element().str.starts_with(f"{continent}:")).list.sum() == 1)
				.and_(pl.col('continent').is_null()))
				.then(pl.lit(continent))
				.otherwise(pl.col('continent'))
				.alias('continent'),

				# move the other part to likely_country
				pl.when((pl.col('geoloc_info').list.eval(pl.element().str.starts_with(f"{continent}:")).list.sum() == 1)
				.and_(pl.col('country').is_null()))
				.then(
					pl.col("geoloc_info").list.eval(pl.element().filter(
						pl.element().str.starts_with(f"{continent}:"))
					).list.first().str.strip_prefix(f"{continent}:")
				)
				.otherwise(pl.col('likely_country'))
				.alias('likely_country')
			])

			# Remove what we just matched on from geoloc_info, using likely_country as our guide
			# The and_() tries to avoid nonsense when there's two values that start with 'continent:' 
			polars_df = polars_df.with_columns([
				pl.when((pl.col('likely_country').is_not_null())
				.and_((pl.col('geoloc_info').list.eval(pl.element().str.starts_with(f"{continent}:")).list.sum() == 1)))
				.then(
					pl.col('geoloc_info').list.eval(pl.element().filter(
						~pl.element().str.starts_with(f"{continent}:")))
				)
				.otherwise(pl.col('geoloc_info'))
				.alias('geoloc_info')
			])

		self.logging.debug("Finished checking for nested continents")
		# Strip leading whitespace from likely_country column, as we will be using starts_with() on it soon.
		polars_df = polars_df.with_columns(pl.col("likely_country").str.strip_chars_start(" "))
		for nation, ISO3166 in tqdm(united_nations.items(), desc="Matching on nested countries...", ascii='➖🌱🐄', bar_format='{desc:<5.5}{percentage:3.0f}%|{bar:10}{r_bar}'):
			polars_df = polars_df.with_columns([
				pl.when((pl.col('geoloc_info').list.eval(pl.element().str.starts_with(f"{nation}:")).list.sum() == 1)
				.and_(pl.col('country').is_null()))
				.then(pl.lit(ISO3166))
				.otherwise(pl.col('country'))
				.alias('country'),

				# move the other part to region if region is null
				# NOTE: this is purposely inconsistent with the expression above so we can still get region information if
				# we already had an exact match for country earlier -- eg, to handle a geoloc_info list like this:
				# ['Ireland', 'Ireland: Dublin']
				pl.when((pl.col('geoloc_info').list.eval(pl.element().str.starts_with(f"{nation}:")).list.sum() == 1)
				.and_(pl.col('region').is_null()))
				.then(
					pl.col("geoloc_info").list.eval(pl.element().filter(
						pl.element().str.starts_with(f"{nation}:"))
					).list.first().str.strip_prefix(f"{nation}:")
				)
				.otherwise(pl.col('region'))
				.alias('region'),

				# Keep in mind likely_country was originally a geoloc_info with a continent, and we are checking only
				# with starts_with, so if a geoloc_info was originally just ['Europe: Ireland, Dublin'] we'd get just
				# continent = Europe and def_country = IRL, and the Dublin info would be lost.
				pl.when(pl.col('likely_country').str.starts_with(f"{nation}")) # note lack of colon
				.then(pl.lit(ISO3166))
				.otherwise(pl.col('def_country'))
				.alias('def_country') # serves as a guide for geoloc_info removal
			])

			# likely_country and def_country fields were already removed from geoloc_info provided that
			# .and_((pl.col('geoloc_info').list.eval(pl.element().str.starts_with(f"{continent}:")).list.sum() == 1)))
			# always holds true, but it might be worth doing this just in case? commenting out for now as it may break things
			#polars_df = polars_df.with_columns([
				#pl.when(pl.col('def_country').is_not_null())
				#.then(pl.col('geoloc_info').list.eval(pl.element().filter(~pl.element().str.starts_with(f"{nation}:"))))
				#.otherwise(pl.col('geoloc_info'))
				#.alias('geoloc_info')
			#])

		polars_df = polars_df.with_columns([
			pl.when((pl.col('likely_country').is_not_null())
			.and_(pl.col('def_country').is_null())
			.and_(pl.col('region').is_null()))
			.then(pl.col('likely_country'))
			.otherwise(pl.col('region'))
			.alias('region')
		])
		polars_df = polars_df.with_columns(pl.coalesce(["country", "def_country"]).alias("country")) # not likely_country!
		polars_df = polars_df.drop(['likely_country', 'def_country'])

		# Final pass -- check every remaining element of geoloc_info for countries. We already got all of the
		# low-hanging fruit of exact matches and starts_with(), so there should really only be region information
		# in here.
		# We can only safely use countries.substring_match safely here; continents should be okay too but just to be safe let's not
		for nation, ISO3166 in tqdm(countries.substring_match.items(), desc="One final pass...", ascii='➖🌱🐄', bar_format='{desc:<5.5}{percentage:3.0f}%|{bar:10}{r_bar}'):
			polars_df = polars_df.with_columns([
				pl.when((pl.col("geoloc_info").list.eval(pl.element().str.contains(nation)).list.sum() != 0)
					.and_(pl.col('country').is_null()))
				.then(pl.lit(ISO3166))
				.otherwise(pl.col('country'))
				.alias('country')

				# Purposely do not remove matches from geoloc_info; this will keep stuff like ["Beijing China"] available
				# for regionafying, even though that means we get a country of CHN and a region of "Beijing China"
			])

		# We hereby declare anything remaining in geoloc_info to be a region
		polars_df = polars_df.with_columns([
			pl.when((pl.col("geoloc_info").list.len() > 0)
			.and_(pl.col('region').is_null()))
			.then(pl.col("geoloc_info"))
			.otherwise(None)
			.alias('region_as_list')
		])
		polars_df = NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, force_strings=True, just_these_columns=['region_as_list'])
		polars_df = polars_df.with_columns(pl.coalesce(["region", "region_as_list"]).alias("neo_region"))
		polars_df = polars_df.drop(['region', 'region_as_list', 'geoloc_info'])
		polars_df = polars_df.rename({'neo_region': 'region'})
		polars_df = polars_df.with_columns(pl.col("region").str.strip_chars_start(" "))
		
		# Any matches for country names in geoloc_name, country, likely_country, and def_country have already been ISO3166'd
		# Let's use that to convert some ISO3166'd countries into continents 
		for ISO3166, continent in countries.countries_to_continents.items():
			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', key=ISO3166, value=continent, substrings=False, overwrite=True)

		# manually deal with entries that have values for region but not country
		for region, ISO3166 in regions.regions_to_countries.items():
			polars_df = self.dictionary_match(polars_df, match_col="region", write_col="country", key=region, value=ISO3166, substrings=False, overwrite=True)
		for nation, ISO3166 in countries.substring_match.items():
			polars_df = self.dictionary_match(polars_df, match_col='region', write_col='country', key=nation, value=ISO3166, substrings=True, overwrite=False)
		for nation, ISO3166 in countries.exact_match.items():
			polars_df = self.dictionary_match(polars_df, match_col='region', write_col='country', key=nation, value=ISO3166, substrings=False, overwrite=True)
		
		# partial cleanup of the region column
		for region, shorter_region in regions.regions_to_smaller_regions.items():
			polars_df = self.dictionary_match(polars_df, match_col="region", write_col="region", key=region, value=shorter_region, substrings=True, overwrite=True)

		self.validate_col_country(polars_df)

		return polars_df
		

	def validate_col_country(self, polars_df):
		# TODO: now we have some that aren't just three bytes
		assert 'country' in polars_df.columns
		invalid_rows = polars_df.filter(pl.col('country').str.len_bytes() != 3)
		if len(invalid_rows) > 0:
			self.logging.error(
				f"The following rows have countries that failed to convert to ISO3166 format:"
			)
			print(invalid_rows.select(NeighLib.get_valid_id_columns(invalid_rows) + ['country']))
			raise ValueError
		if self.logging.getEffectiveLevel() == 10:
			self.logging.debug("---- After absolutely everything ----")
			NeighLib.print_a_where_b_is_in_list(polars_df, col_a='region', col_b='run_index', list_to_match=['ERR841442', 'ERR5908244', 'SRR23310897', 'SRR12380906', 'SRR18054772', 'SRR10394499', 'SRR9971324', 'ERR732681', 'SRR23310897'], alsoprint=['country', 'continent'])

	def standardize_TB_lineages(self,
		drop_non_standarized=True,
		guess_from_old_names=True,
		guess_from_ST=False,
		strains_of_note=True):
		"""
		drop_non_standarized: Stuff that cannot be standardized is turned into np.nan/null (if false, leave it untouched)
		guess_from_old_names: Guess lineage from older names, eg, assume "Beijing" means "L2.2.1"
		guess_from_ST: Assume "ST XXX" is SIT and use SITVIT2's dictionary to convert to a lineage
		strains_of_note: Maintain the names of notable strains such as Oshkosh, BCG, etc
		"""
		pass

	def standardize_sources(flag_hosts_and_locations=False): # default to true once things are working
		"""
		flag_hosts_and_locations: Additionally return a list of BioSamples and suspected hosts and locations. This
		can be helpful for situations where no host is listed, but the source says something like "human lung."
		"""
		pass
