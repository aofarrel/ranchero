from src.statics import host_species, sample_sources, kolumns, generated_taxoncore_dictionary, countries, regions
from .config import RancheroConfig
import polars as pl
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

	def _sentinal_handler(self, arg):
		"""Handles "allow overriding config" variables in function calls"""
		if arg in _SENTINEL_TO_CONFIG:
			config_attr = _SENTINEL_TO_CONFIG[arg]
			check_me = getattr(self.cfg, config_attr)
			assert check_me != arg, f"Configuration for '{config_attr}' is invalid or uninitialized"
			return check_me
		else:
			return arg

	def standardize_everything(self, polars_df):
		if any(column in polars_df.columns for column in ['geoloc_name', 'country', 'region']):
			self.logging.info("Standardizing countries...")
			polars_df = self.standardize_countries(polars_df)
		
		if ['date_collected_year', 'date_collected_month', 'date_collected_day', 'date_collected'] in polars_df.columns:
			#NeighLib.print_a_where_b_is_foo(polars_df, "date_collected", "sample_index", "SAMEA110052021")
			self.logging.info("Cleaning up dates...")
			polars_df = self.cleanup_dates(polars_df)
			#NeighLib.print_a_where_b_is_foo(polars_df, "date_collected", "sample_index", "SAMEA110052021")
		
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

		polars_df = self.drop_no_longer_useful_columns(polars_df)
		NeighLib.check_index(polars_df)
		NeighLib.print_a_where_b_is_foo(polars_df, "date_collected", "sample_index", "SAMEA110052021")

		return polars_df

	def standardize_sample_source(self, polars_df):
		if polars_df.schema['isolation_source'] == pl.List:
			return self.standardize_sample_source_as_list(polars_df)
		else:
			return self.standardize_sample_source_as_string(polars_df)

	def inject_metadata(self, polars_df, metadata_dictlist, dataset=None, overwrite=False):
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
		#keys_to_drop = []
		for ordered_dictionary in metadata_dictlist:
			for key in ordered_dictionary:
				if key not in polars_df.columns:
					self.logging.error(f"Attempted to inject {key} metadata, but existing column with that name doesn't exist")
					#if key != self.cfg.indicator_column:
					#	self.logging.error(f"Attempted to inject {key} metadata, but existing column with that name doesn't exist, nor is {key} equivalent to the config's indicator column {self.cfg.indicator_column}")
					#	raise ValueError
					#else:
					#	# this is an indicator column that needs special handling
					#	ordered_dictionary_copy = ordered_dictionary.copy()
					#	match = ordered_dictionary_copy.popitem(last=False) # FIFO
					#	match_key, match_value = match[0], match[1]
					#	indicators.append([match_key, match_value, ordered_dictionary[key]]) # ["BioProject", "PRJEB15463", "FZB_DRC"]
					#	keys_to_drop.append(ordered_dictionary[key])
		assert type(metadata_dictlist[0]) == OrderedDict
		#[ordered_dictionary.pop(key) for key in keys_to_drop if key in ordered_dictionary]

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
				match_column, match_value, indicator_column, indicator_value = indicator_list[0], indicator_list[1], self.cfg.indicator_column, indicator_list[2]
				self.logging.debug(f"When {match_column} is {match_value}, then concatenate {indicator_value} to {indicator_column}")
				this_expression = [
						pl.when(pl.col(match_column) == match_value)
						.then(pl.concat_list([pl.lit(indicator_value), self.cfg.indicator_column]))
						.otherwise(pl.col(self.cfg.indicator_column))
						.alias(self.cfg.indicator_column)
					]
			polars_df = polars_df.with_columns(all_indicator_expressions)

		return polars_df

	def drop_no_longer_useful_columns(self, polars_df):
		"""ONLY RUN THIS AFTER ALL METADATA PROCESSING"""
		return polars_df.drop(kolumn for kolumn in kolumns.columns_to_drop_after_rancheroize if kolumn in polars_df.columns)

	def simple_dictionary_match(self, polars_df, match_column: str, key: str, value, subtrings=False):
		"""
		Replace a pl.Utf8 column's values with the values in a dictionary per its key-value pairs.
		Case-insensitive. If subtrings, will match substrings (ex: "US Virgin Islands" matches "US")
		"""
		assert polars_df.schema[match_column] == pl.Utf8
		if subtrings:
			self.logging.debug(f"Checking {match_column}: \"\\b(?i){key}\\b\"-->\"{value}\"")
			polars_df = polars_df.with_columns([
				pl.when(pl.col(match_column) == f"(?i){key}")
				.then(pl.lit(value))
				.otherwise(pl.col(match_column))
				.alias(match_column)])
			
		else:
			self.logging.debug(f"Checking {match_column}: \"(?i){key}\"-->\"{value}\"")
			polars_df = polars_df.with_columns([
				pl.when(pl.col(match_column).str.contains(f"(?i){key}"))
				.then(pl.lit(value))
				.otherwise(pl.col(match_column))
				.alias(match_column)])
		return polars_df

	def dictionary_match_on_list(self, polars_df, match_column, key, value, substrings=False):
		assert polars_df.schema[match_column] == pl.List(pl.Utf8)
		if substrings:
			polars_df = polars_df.with_columns([
				pl.when(
					# list contains this exact string
					pl.col(match_column).list.contains(f"(?i){key}").any()
				)
				.then(pl.lit([value]))
				.otherwise(
					pl.when(pl.col(match_column).is_not_null())
					.then(pl.col(match_column))
					.otherwise(None)
				).alias(match_column)
			])

		else:
			polars_df = polars_df.with_columns([
				pl.when(
					# any element in the list contains the substring
					pl.col(match_column).list.eval(pl.element().str.contains(f"(?i){key}")).list.any()
				)
				.then(pl.lit([value]))
				.otherwise(
					pl.when(pl.col(match_column).is_not_null())
					.then(pl.col(match_column))
					.otherwise(None)
				).alias(match_column)
			])

		return polars_df

	def standardize_host_disease(self, polars_df):
		assert 'host_disease' in polars_df.columns
		for host_disease, simplified_host_disease in sample_sources.host_disease_exact_match.items():
			polars_df = self.simple_dictionary_match(polars_df, 'host_disease', host_disease, simplified_host_disease, subtrings=True)
		for host_disease, simplified_host_disease in sample_sources.host_disease.items():
			polars_df = self.simple_dictionary_match(polars_df, 'host_disease', host_disease, simplified_host_disease, subtrings=False)
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
			polars_df = self.dictionary_match_on_list(polars_df, 'isolation_source', sample_source, simplified_sample_source, substrings=False)		
		for sample_source, simplified_sample_source in sample_sources.sample_source_exact_match.items():
			polars_df = self.dictionary_match_on_list(polars_df, 'isolation_source', sample_source, simplified_sample_source, substrings=True)
		
		self.logging.info(f"The isolation_source column has type list. We will be .join()ing them into strings.") # done AFTER most standardization
		polars_df = polars_df.with_columns(
			pl.col("isolation_source").list.join(", ").alias("isolation_source")
		)

		return polars_df

	def standardize_sample_source_as_string(self, polars_df):
		assert 'isolation_source' in polars_df.columns
		assert polars_df.schema['isolation_source'] == pl.Utf8
		for sample_source, simplified_sample_source in sample_sources.sample_source_exact_match.items():
			polars_df = self.simple_dictionary_match(polars_df, 'isolation_source', sample_source, simplified_sample_source, subtrings=True)
		for sample_source, simplified_sample_source in sample_sources.sample_source.items():
			polars_df = self.simple_dictionary_match(polars_df, 'isolation_source', sample_source, simplified_sample_source, subtrings=False)
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

	def cleanup_dates(self, polars_df, keep_only_bad_examples=False):
		"""
		Notes:
		* keep_only_bad_examples is for debugging; it effectively hides dates that are probably good
		* len_bytes() is way faster than len_chars()
		* yeah you can have mutliple expressions in one with_columns() but that'd require tons of alias columns plus nullify so I'm not doing that
		"""

		if polars_df.schema['date_collected'] != pl.Utf8:
			self.logging.warning("Date column is not of type string. Will attempt to cast it as string.")
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
			.and_(pl.col('date_collected').str.count_matches("/") == 2))
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

		return NeighLib.nullify(polars_df, only_these_columns=['date_collected'])

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
				.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism')
			])
		else:
			polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
				.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
				.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism')
			])
		return polars_df
	
	def taxoncore_GOS(self, polars_df, match_string, i_group, i_organism, i_strain):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_strain)).otherwise(pl.col('i_strain')).alias('i_strain')
		])
		return polars_df
	
	def taxoncore_GOL(self, polars_df, match_string, i_group, i_organism, i_lineage):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_lineage)).otherwise(pl.col('i_lineage')).alias('i_lineage')
		])
		return polars_df
	
	def taxoncore_GOLS(self, polars_df, match_string, i_group, i_organism, i_lineage, i_strain):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_group)).otherwise(pl.col('i_group')).alias('i_group'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_lineage)).otherwise(pl.col('i_lineage')).alias('i_lineage'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_strain)).otherwise(pl.col('i_strain')).alias('i_strain')
		])
		return polars_df


	def only_stuff_probably_on_tree(self, polars_df, identifers=['tba4', 'tba3']):
		pass

	def taxoncore_iterate_rules(self, polars_df):
		if self.cfg.taxoncore_ruleset is None:
			raise ValueError("A taxoncore ruleset failed to initialize, so we cannot use function taxoncore_iterate_rules!")
		for when, strain, lineage, organism, bacterial_group, comment in (entry.values() for entry in self.cfg.taxoncore_ruleset):
			if strain is None and lineage is None:
				polars_df = self.taxoncore_GO(polars_df, when, i_group=bacterial_group, i_organism=organism)
			elif strain is None:
				polars_df = self.taxoncore_GOL(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_lineage=lineage)
			elif lineage is None:
				polars_df = self.taxoncore_GOS(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_strain=strains)
			else:
				polars_df = self.taxoncore_GOLS(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_lineage=lineage, i_strain=strain)
		return polars_df

	def sort_out_taxoncore_columns(self, polars_df, group_column_name="mycobact_type", rm_phages=_cfg_rm_phages):
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
		assert 'i_group' not in polars_df.columns
		assert 'i_organism' not in polars_df.columns
		assert 'i_lineage' not in polars_df.columns
		assert 'i_strain' not in polars_df.columns
		assert 'taxoncore_list' not in polars_df.columns
		rm_phages = self._sentinal_handler(_cfg_rm_phages)
		if group_column_name not in kolumns.columns_to_drop_after_rancheroize:
			self.logging.warning(f"Bacterial group column will have name {group_column_name}, but might get removed later. Add {group_column_name} to kolumns.equivalence!")
		if 'organism' in polars_df.columns and rm_phages:
			polars_df = self.rm_all_phages(polars_df)
		merge_these_columns = [col for col in polars_df.columns if col in sum(kolumns.special_taxonomic_handling.values(), [])]

		for col in merge_these_columns:
			if polars_df.schema[col] == pl.List:
				polars_df = polars_df.with_columns(pl.col(col).list.join(", ").alias(col))
			assert polars_df.schema[col] == pl.Utf8
		
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

	def iso_the_countries(self, polars_df, column):
		for nation, ISO3166 in countries.substring_match.items():
			polars_df = self.simple_dictionary_match(polars_df, column, nation, ISO3166, subtrings=False)
		for nation, ISO3166 in countries.exact_match.items():
			polars_df = self.simple_dictionary_match(polars_df, column, nation, ISO3166, subtrings=True)
		return polars_df
	
	def standardize_countries(self, polars_df):
		if 'geoloc_name' not in polars_df.columns:
			self.logging.debug("This looks partially standardized already, or was never rancheroized, given the lack of geoloc_name")
			if 'country' in polars_df.columns:
				polars_df = self.iso_the_countries(polars_df, "country")
				return polars_df

			elif 'region' not in polars_df.columns:
				self.logging.warning("Skipping standardization of locations, as no 'geoloc_name' nor 'region' column was found.")
				return polars_df
			else:
				self.logging.error("""Tried to standardize countries, but 'geoloc_name', as well as 'country 'and/or 'region', are missing from the dataframe's columns.
					Most likely, you need to rancheroize this dataframe to standardize its columns.""")
				exit(1) # you done messed up
		assert ['country_colon_region', 'new_region', 'new_country', 'all_geoloc_names', 'temp_probably_country', 'temp_probably_region'] not in polars_df.columns
		assert polars_df.schema['country'] == pl.Utf8

		# TODO: if polars_df.schema['geoloc_name'] == pl.Utf8, do a simpler version

		if polars_df.schema['geoloc_name'] == pl.List(pl.Utf8):
			self.logging.debug("Handling geoloc_name...")

			# ideal case: one colon across the entire "geoloc_name" list column
			polars_df = polars_df.with_columns([
				pl.when(pl.col("geoloc_name").list.eval(pl.element().str.count_matches(":")).list.sum() == 1)
				.then(
					pl.col("geoloc_name").list.eval(pl.element().filter(pl.element().str.contains(":")))
					.list.first().str.split(":")
				).alias("country_colon_region")
			])
			polars_df = polars_df.with_columns([
				pl.when(pl.col("country_colon_region").list.len() > 1)
				.then(
					pl.col("country_colon_region").list.first()
				).alias("new_country"),
				
				pl.when(pl.col("country_colon_region").list.len() > 1)
				.then(
					pl.col("country_colon_region").list.last().str.strip_chars_start().str.strip_chars_end('_1234567890') # strips leading whitespace and trailing _NN
				).alias("new_region")
			])

			# handle the less-than-ideal situations
			polars_df = polars_df.with_columns([
				# for some reason, adding `.and_(pl.col("geoloc_name").list.len() > 0))` will still accept list[null]
				# no reason to add and_() if we end up with nulls join()ing themselves anyway
				pl.when((pl.col("geoloc_name").list.eval(pl.element().str.count_matches(":")).list.sum() != 1))
				.then(
					pl.col("geoloc_name").list.join("; ").str.strip_chars_end('_1234567890')
				)
				.otherwise(None)
				.alias("all_geoloc_names")
			])
			polars_df = NeighLib.nullify(polars_df, only_these_columns=['all_geoloc_names']) # deal with those join()ed nulls that became empty strings
			polars_df = self.iso_the_countries(polars_df, "new_country")
			polars_df = self.iso_the_countries(polars_df, "all_geoloc_names")

			# all_geoloc_names values that are three bytes in size got ISO3166'd --> move to country column
			# all_geoloc_names values didn't get ISO3166'd --> move to region column
			polars_df = polars_df.with_columns([
				pl.when((pl.col('all_geoloc_names').str.len_bytes() == 3))
				.then(pl.col('all_geoloc_names')).alias("temp_probably_country"),
			])
			polars_df = polars_df.with_columns([
				pl.when((pl.col('all_geoloc_names').str.len_bytes() != 3))
				.then(pl.col('all_geoloc_names')).alias("temp_probably_region"),
			])

			polars_df = polars_df.with_columns(pl.coalesce(["new_country", "temp_probably_country"]).alias("country"))
			polars_df = polars_df.with_columns(pl.coalesce(["new_region", "temp_probably_region"]).alias("region"))
			polars_df = polars_df.drop(['country_colon_region', 'new_region', 'new_country', 'all_geoloc_names', 'temp_probably_country', 'temp_probably_region', 'geoloc_name'])

			# manually deal with some regions that don't have countries
			for region, ISO3166 in regions.regions_to_countries.items():
				polars_df = self.simple_dictionary_match(polars_df, "region", region, ISO3166, subtrings=False)
			return polars_df
		else:
			raise ValueError
		self.validate_col_country(polars_df)
		

	def validate_col_country(self, polars_df):
		assert 'country' in polars_df.columns
		invalid_rows = df.filter(df[column_name].str.len_bytes() != 3)
		if len(invalid_rows) > 0:
			raise ValueError(
				f"The following rows have values in column '{column_name}' "
				f"that don't seem to have been converted to ISO3166 format:\n{invalid_rows.select([kolumns.id_columns + ['country']])}"
			)

	

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