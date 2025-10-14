import sys
from datetime import datetime
from .statics import host_disease, host_species, sample_sources, kolumns, countries, regions
from .config import RancheroConfig
import polars as pl
from tqdm import tqdm
from collections import OrderedDict # dictionaries are ordered in Python 3.7+, but OrderedDict has a better popitem() function we need

# https://peps.python.org/pep-0661/
_DEFAULT_TO_CONFIGURATION = object()

class ProfessionalsHaveStandards():
	def __init__(self, configuration, naylib):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger
			self.taxoncore_ruleset = self.cfg.taxoncore_ruleset
			self.NeighLib = naylib

	def _default_fallback(self, cfg_var, value):
		if value == _DEFAULT_TO_CONFIGURATION:
			return self.cfg.get_config(cfg_var)
		return value

	def standardize_everything(self, polars_df, add_expected_nulls=True, assume_organism="Mycobacterium tuberculosis", assume_clade="tuberculosis", skip_sample_source=False, force_strings=True,
		organism_fallback=None, clade_fallback=None):
		if any(column in polars_df.columns for column in ['geoloc_info', 'country', 'region']):
			self.logging.info("Standardizing countries...")
			polars_df = self.standardize_countries(polars_df)
		
		if 'date_collected' in polars_df.columns:
			self.logging.info("Cleaning up dates...")
			polars_df = self.cleanup_dates(polars_df)
		
		# Because this one is VERY open to interpretation and I'm not a medical doctor, we will also have a "raw value" column.
		if 'isolation_source' in polars_df.columns and not skip_sample_source:
			self.logging.info("Standardizing isolation sources...")
			polars_df = polars_df.with_columns(pl.col('isolation_source').alias('isolation_source_raw'))
			polars_df = self.standardize_sample_source(polars_df) # must be before taxoncore and host, no need to force_strings as its already forced
		
		if 'host' in polars_df.columns:
			self.logging.info("Standardizing host organisms...")
			polars_df = self.standarize_hosts(polars_df)
		
		if 'host_disease' in polars_df.columns:
			self.logging.info("Standardizing host diseases...")
			polars_df = self.standardize_host_disease(polars_df)
		
		if any(column in polars_df.columns for column in ['genotype', 'lineage', 'strain', 'organism']):
			self.logging.info("Standardizing lineage, strain, and mycobacterial scientific names... (this may take a while)")
			polars_df = self.sort_out_taxoncore_columns(polars_df, force_strings=force_strings)
		elif add_expected_nulls:
			if 'organism' not in polars_df.columns:
				polars_df = self.NeighLib.add_column_of_just_this_value(polars_df, 'organism', assume_organism)
			if 'clade' not in polars_df.columns:
				polars_df = self.NeighLib.add_column_of_just_this_value(polars_df, 'clade', assume_clade)

		if organism_fallback is not None:
			polars_df = polars_df.with_columns(pl.col('organism').fill_null(organism_fallback))
		if clade_fallback is not None:
			polars_df = polars_df.with_columns(pl.col('organism').fill_null(clade_fallback))

		polars_df = self.drop_no_longer_useful_columns(polars_df)
		polars_df = self.NeighLib.null_lists_of_len_zero(self.NeighLib.rancheroize_polars(polars_df, nullify=False))
		return polars_df

	def standardize_sample_source(self, polars_df):
		"""
		Standardize sample_source and (if present) isolate_sam_ss_dpl100
		"""

		# Special handling for isolate_sam_ss_dpl100, which is usually sample names (booooo) but sometimes we can extract useful information from it.
		# In older versions this was merged into the isolation_source column via kolumns, but since it added so many sample names it really slowed
		# everything down for very little benefit. What we're gonna do here is search only isolate_sam_ss_dpl100, and only for a subset of isolation sources,
		# and then ignore everything else in it.
		if 'isolate_sam_ss_dpl100' in polars_df.columns:
			for sample_source, simplified_sample_source in tqdm(sample_sources.sample_source_exact_match.items(), desc="Checking isolate_sam_ss_dpl100 (exact)", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
				polars_df = self.dictionary_match(polars_df, match_col='isolate_sam_ss_dpl100', write_col='neo_isolation_source', key=sample_source, value=simplified_sample_source, substrings=False, overwrite=False, remove_match_from_list=True)
			polars_df = polars_df.drop('isolate_sam_ss_dpl100')

		if polars_df.schema['isolation_source'] == pl.List:
			polars_df = self.standardize_sample_source_as_list(polars_df)
			return polars_df
		else:
			return self.standardize_sample_source_as_string(polars_df)

	def inject_metadata(self, polars_df: pl.DataFrame, metadata_dictlist, dataset=None, overwrite=False):
		"""
		Modify a Rancheroized polars_df with BioProject-level metadata as controlled by a dictionary. For example:
		metadata_dictlist=[{"BioProject": "PRJEB15463", "country": "COD", "region": "Kinshasa"}]

		Will create these polars expressions if overwrite is False:
		pl.when(pl.col("BioProject") == "PRJEB15463").and_(pl.col("country").is_null()).then(pl.lit("COD")).otherwise(pl.col("country")).alias("country"), 
		pl.when(pl.col("BioProject") == "PRJEB15463").and_(pl.col("region").is_null()).then(pl.lit("Kinshasa")).otherwise(pl.col("region")).alias("region")

		If overwrite=True:
		pl.when(pl.col("BioProject") == "PRJEB15463").then(pl.lit("COD")).otherwise(pl.col("country")).alias("country"), 
		pl.when(pl.col("BioProject") == "PRJEB15463").then(pl.lit("Kinshasa")).otherwise(pl.col("region")).alias("region")
		"""
		indicators, dropped = [], []
		assert type(metadata_dictlist[0]) == OrderedDict

		# the first key in every OD is the name of the column the injector is matching upon
		first_keys = {next(iter(od)) for od in metadata_dictlist}
		if not first_keys.issubset(set(polars_df.columns)):
			self.logging.error(f"Injector wants to inject metadata where column(s) {first_keys - set(polars_df.columns)} is some value, but that column(s) is missing")
			raise ValueError

		# the not-first keys in every OD is the name of the column the injector will try to inject into
		other_keys = {k for od in metadata_dictlist for k in list(od.keys())[1:]}
		for key in other_keys:
			if key not in polars_df.columns:
				for od in metadata_dictlist:
					od.pop(key, None) # TODO: does this need to be popitem()?
				dropped.append(key)
		
		if len(dropped) > 0:
			self.logging.warning(f"Cannot inject metadata into non-existent columns (will be skipped): {dropped}")
			self.logging.warning("Tip: If you're trying to create new columns, add them as empty columns to the polars df first, then use the injector")

		for ordered_dictionary in metadata_dictlist:
			# {"BioProject": "PRJEB15463", "country": "DRC", "region": "Kinshasa"}
			match = ordered_dictionary.popitem(last=False) # FIFO
			match_column, match_value = match[0], match[1] # "BioProject", "PRJEB15463" (ie, when BioProject is PRJEB15463)

			for write_column, write_value in ordered_dictionary.items():
				if polars_df.schema[write_column] == pl.List and type(write_value) is not list:
					write_value = [write_value] # to avoid polars.exceptions.InvalidOperationError
				#self.logging.debug(f"write_column {write_column} ({polars_df.schema[write_column]}), write_value {write_value} ({type(write_value)}), match_column {match_column} ({type(match_column)}), match_value {match_value} ({type(match_value)})") # extremely verbose
				if overwrite:
					polars_expressions = [
						pl.when(pl.col(match_column) == match_value)
						.then(pl.lit(write_value))
						.otherwise(pl.col(write_column))
						.alias(write_column)
					]
				else:
					polars_expressions = [
						pl.when((pl.col(match_column) == match_value).and_(pl.col(write_column).is_null()))
						.then(pl.lit(write_value))
						.otherwise(pl.col(write_column))
						.alias(write_column)
					]
				polars_df = polars_df.with_columns(polars_expressions)

		# ['BioProject', 'PRJEB15463', 'FZB_DRC']
		if len(indicators) > 0:
			self.logging.info("Processing indicators...")
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
		status_cols_reset=True,
		remove_match_from_list=False):
		"""
		Replace a pl.Utf8 or pl.List(pl.Utf8) column's values with the values in a dictionary per its key-value pairs.
		Case-insensitive. If substrings, will match substrings (ex: "US Virgin Islands" matches "US")
		If match_col is pl.List, if any element in the list matches, that is considered a match.

		Matched and Written columns are not started over if already existed in case this is being called in a for loop
		"""
		#self.logging.debug(f"Where {key} is in {match_col}, write {value} in {write_col} (substrings {substrings}, overwrite {overwrite}, status_cols {status_cols}, remove_match_from_list {remove_match_from_list})")
		if status_cols:
			polars_df = polars_df.with_columns(pl.lit(False).alias('matched')) if 'matched' not in polars_df.columns else polars_df.with_columns(pl.col('matched').fill_null(False))
			polars_df = polars_df.with_columns(pl.lit(False).alias('written')) if 'written' not in polars_df.columns else polars_df.with_columns(pl.col('written').fill_null(False))
		if write_col not in polars_df.columns:
			self.logging.debug(f"Write column {write_col} not in polars_df yet so we'll add it")
			polars_df = polars_df.with_columns(pl.lit(None).alias(write_col)) if write_col not in polars_df.columns else polars_df

		# to start off, we define several polars expressions
		
		# define found_a_match
		if substrings and polars_df.schema[match_col] == pl.Utf8:
			found_a_match = pl.col(match_col).str.contains(f"(?i){key}")
		elif substrings and polars_df.schema[match_col] == pl.List(pl.Utf8):
			#found_a_match = pl.col(match_col).list.contains(f"(?i){key}").any() # doesn't properly match substrings
			found_a_match = pl.col(match_col).list.eval(pl.element().str.contains(f"(?i){key}")).list.any()
		elif not substrings and polars_df.schema[match_col] == pl.Utf8:
			found_a_match = pl.col(match_col).str.to_lowercase() == key.lower()
		elif not substrings and polars_df.schema[match_col] == pl.List(pl.Utf8):
			found_a_match = pl.col(match_col).list.eval(pl.element().str.to_lowercase() == key.lower()).list.any()
		else:
			self.logging.warning(f"Invalid type {polars[match_col].schema} for match_col named {match_col}, cannot do matching")
			return polars_df
		self.logging.debug(f"defined polars expression found_a_match as {found_a_match}")

		# define allowed_to_overwrite
		allowed_to_overwrite = (pl.lit(overwrite) == True).and_(pl.lit(value).is_not_null())
	
		# define write_col_is_empty (empty list, empty string, pl.Null)
		if polars_df.schema[write_col] == pl.List:
			write_col_is_empty = ((pl.col(write_col).is_null()).or_(pl.col(write_col).list.len() < 1))
			# also make sure we can write to the value to the list column
			if type(value) is not list:
				value = [value]
				self.logging.debug("turned value into a list, since write_col is a list, to avoid type errors")
		elif polars_df.schema[write_col] == pl.Utf8:
			write_col_is_empty = ((pl.col(write_col).is_null()).or_(pl.col(write_col).str.len_bytes() == 0))
		else:
			write_col_is_empty = pl.col(write_col).is_null()
		self.logging.debug(f"defined polars expression write_col_is_empty as {write_col_is_empty}")

		# define matched_false and written_false (for status columns)
		if status_cols_reset:
			matched_false = False
			written_false = False
		else:
			matched_false = pl.col('matched')
			written_false = pl.col('written')

		# use those expressions to actually do something
		if status_cols:
			polars_df = polars_df.with_columns([
				# match status
				pl.when(found_a_match).then(True).otherwise(matched_false).alias('matched'),

				# write status
				pl.when(
					(found_a_match)
					.and_(
						(allowed_to_overwrite)
						.or_(write_col_is_empty)
					)
				)
				.then(True)
				.otherwise(written_false)
				.alias('written'),

				# actual writing
				pl.when(
					(found_a_match)
					.and_(
						(allowed_to_overwrite)
						.or_(write_col_is_empty)
					)
				)
				.then(pl.lit(value))
				.otherwise(pl.col(write_col))
				.alias(write_col)
			])
		else:
			polars_df = polars_df.with_columns([
				pl.when(
					(
						(allowed_to_overwrite)
						.or_(write_col_is_empty)
					)
					.and_(found_a_match)
				)
				.then(pl.lit(value))
				.otherwise(pl.col(write_col))
				.alias(write_col)
			])

		if remove_match_from_list and polars_df.schema[match_col] == pl.List(pl.Utf8):
			if substrings:
				filter_exp = ~pl.element().str.contains(key)
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
				#print(polars_df.select(['run_id', write_col, 'geoloc_info', 'matched', 'written']))
			else:
				pass
				#print(polars_df.select(['run_id', write_col, 'geoloc_info']))
		return polars_df

	def standardize_host_disease(self, polars_df):
		assert 'host_disease' in polars_df.columns

		# exact matches
		if self.cfg.mycobacterial_mode:
			for disease, simplified_disease in host_disease.host_disease_exact_match_mycobacterial.items():
				polars_df = self.dictionary_match(polars_df, match_col='host_disease', write_col='host_disease', key=disease, value=simplified_disease, substrings=False, overwrite=True)
		for disease, simplified_disease in host_disease.host_disease_exact_match.items():
			polars_df = self.dictionary_match(polars_df, match_col='host_disease', write_col='host_disease', key=disease, value=simplified_disease, substrings=False, overwrite=True)
		
		# fuzzy matches
		if self.cfg.mycobacterial_mode:
			for disease, simplified_host_disease in host_disease.host_disease_substring_match_mycobacterial.items():
				polars_df = self.dictionary_match(polars_df, match_col='host_disease', write_col='host_disease', key=disease, value=simplified_disease, substrings=True, overwrite=True)
		for disease, simplified_host_disease in host_disease.host_disease_substring_match.items():
			polars_df = self.dictionary_match(polars_df, match_col='host_disease', write_col='host_disease', key=disease, value=simplified_disease, substrings=True, overwrite=True)
		return polars_df

	def standardize_sample_source_as_list(self, polars_df, write_hosts=True, write_lineages=True, write_host_disease=True):
		"""
		Sample source (rancheroized as isolation_source) is kind of a mess, because submitters can interpret it as very different things:
		* host organism species
		* host organism information
		* fluid/organ/body part the sample was isolated from
		* environmental information (soil type, etc)
		* geographic location
		"""
		assert 'isolation_source' in polars_df.columns
		assert polars_df.schema['isolation_source'] == pl.List

		if write_lineages:
			self.logging.info("Extracting taxonomic information from isolation_source...")
			if 'lineage_sam' in polars_df.columns and polars_df.schema['lineage_sam'] == pl.Utf8:
				lineage_column, skip_lineage = 'lineage_sam', False
			elif 'lineage' in polars_df.columns and polars_df.schema['lineage'] == pl.Utf8:
				lineage_column, skip_lineage = 'lineage', False
			else:
				self.logging.warning("write_lineages is True, but can't find a lineage column!")
				skip_lineage = True
			if 'strain_sam_ss_dpl139' in polars_df.columns and polars_df.schema['strain_sam_ss_dpl139'] == pl.Utf8:
				strain_column, skip_strain = 'strain_sam_ss_dpl139', False
			elif 'strain' in polars_df.columns and polars_df.schema['strain'] == pl.Utf8:
				strain_column, skip_strain = 'strain', False
			else:
				self.logging.warning("write_lineages is True, but can't find a strain column!")
				skip_strain = True
			
			if not skip_lineage:
				polars_df = polars_df.with_columns([
					pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)lineage4.6.2.2')).list.any())
					.then(pl.lit('lineage4.6.2.2'))
					.otherwise(pl.col(lineage_column))
					.alias(lineage_column)
				])
			if not skip_strain:
				polars_df = polars_df.with_columns([
					pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)H37Rv')).list.any())
					.then(pl.lit('H37Rv'))
					.otherwise(pl.col(strain_column))
					.alias(strain_column)
				])

		# TODO: maybe rewrite this section to use dictionary_match()
		if write_hosts and 'host' in polars_df.columns:
			self.logging.info("Extracting host information from isolation_source...")
			human = pl.lit(['Homo sapiens']) if polars_df.schema['host'] == pl.List else pl.lit('Homo sapiens') # high-confidence
			mouse = pl.lit(['Mus musculus']) if polars_df.schema['host'] == pl.List else pl.lit('mouse') # mid-confidence
			cow = pl.lit(['Bos tarus']) if polars_df.schema['host'] == pl.List else pl.lit('cattle') # mid-confidence 
			patient = pl.lit(['patient']) if polars_df.schema['host'] == pl.List else pl.lit('patient') # low-confidence human 
			that_one_tick = pl.lit(['Rhipicephalus microplus']) if polars_df.schema['host'] == pl.List else pl.lit('Rhipicephalus microplus')
			south_american_sea_lion = pl.lit(['Otaria flavescens']) if polars_df.schema['host'] == pl.List else pl.lit('Otaria flavescens')
			vet = pl.lit(['veterinary']) if polars_df.schema['host'] == pl.List else pl.lit('veterinary')
			
			polars_df = polars_df.with_columns([
				pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)human|sapiens|children')).list.any())
				.then(human)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)mouse|musculus')).list.any())
				.then(mouse)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)cow|taurus|dairy|beef')).list.any()) # do not match "bovine" as that could be taxoncore
				.then(cow)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('Affedcted Herd')).list.any()) # do not match "bovine" as that could be taxoncore
				.then(cow)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)patient|children')).list.any())
				.then(human)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('Rhipicephalus')).list.any())
				.then(that_one_tick)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('Otaria flavescens')).list.any())
				.then(south_american_sea_lion)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)vetrinary|veterinary|animal')).list.any())
				.then(vet)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			

		if write_host_disease:
			self.logging.info("Extracting host_disease...")
			for disease, simplified_disease in host_disease.host_disease_exact_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='host_disease', key=disease, value=simplified_disease, substrings=False, overwrite=False, remove_match_from_list=True)
			# DEBUGPRINT
			#self.NeighLib.print_a_where_b_equals_these(polars_df, col_a='isolation_source', col_b='run_id', list_to_match=['SRR16156818', 'SRR12380906', 'SRR23310897', 'ERR6198390', 'SRR6397336'])

		# here's where we actually beginning handling the stuff for this actual column!
		for unhelpful_value in tqdm(sample_sources.sample_sources_nonspecific, desc="Nulling bad isolation sources", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
			polars_df = polars_df.with_columns(
				pl.col('isolation_source').list.eval(pl.element().filter(pl.element() != unhelpful_value)).alias('isolation_source')
			)

		# if there's even a whiff of simulation, declare the whole list simulated
		self.logging.info("Looking for simulated data...")
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)simulated')).list.any()
				.or_(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)in silico')).list.any())
			)
			.then(pl.lit(['simulated/in silico']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])

		# This is an OR, not an AND!
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)laboratory strain')).list.any()
				.or_(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)lab strain')).list.any())
			)
			.then(pl.lit(['laboratory strain']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])

		# AFTER we have cleaned up very obvious things, from now on, write to a NEW COLUMN to help avoid accidentally overwriting past iterations (eg "culture from sputum" --> "sputum" or "culture")
		polars_df = self.NeighLib.add_column_of_just_this_value(polars_df, 'neo_isolation_source', None)

		for this, that, then in tqdm(sample_sources.if_this_and_that_then, desc="Checking for combo matches", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
			this_and_that = pl.col('isolation_source').list.eval(pl.element().str.contains(this)).list.any().and_(pl.col('isolation_source').list.eval(pl.element().str.contains(that)).list.any())
			polars_df = polars_df.with_columns([
				pl.when(this_and_that)
				.then(pl.lit(then))
				.otherwise(pl.col('neo_isolation_source')) # avoid overwriting previous iterations
				.alias('neo_isolation_source')

				# this goes from 30 iterations per second to 30 seconds per iteration! yikes!
				#pl.when(this_and_that)
				#.then(None)
				#.otherwise(pl.col('isolation_source'))
				#.alias('isolation_source')
			])
		for sample_source, simplified_sample_source in tqdm(sample_sources.sample_source_exact_match.items(), desc="Checking for exact matches", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='neo_isolation_source', key=sample_source, value=simplified_sample_source, substrings=False, overwrite=False, remove_match_from_list=True)
		for sample_source, simplified_sample_source in tqdm(sample_sources.sample_source_exact_match_body_parts.items(), desc="Checking for exact matches (body parts)", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='neo_isolation_source', key=sample_source, value=simplified_sample_source, substrings=False, overwrite=False, remove_match_from_list=True)
		for sample_source, simplified_sample_source in tqdm(sample_sources.comprehensive_fuzzy.items(), desc="Checking for fuzzy matches", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='neo_isolation_source', key=sample_source, value=simplified_sample_source, substrings=True, overwrite=False, remove_match_from_list=True)
		

		self.logging.info("Cleaning up...")

		# if it's a culture, then it's a culture. end of story.
		polars_df = polars_df.with_columns([
			pl.when(pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)culture')).list.any())
			.then(pl.lit('culture'))
			.otherwise(pl.col('neo_isolation_source'))
			.alias('neo_isolation_source')
		])

		# very last bit: drop any element of the list that contains a number, as that's likely a sample number. this is done last
		# to allow sample numbers within actually useful strings to still have their useful string bits extracted
		#polars_df = polars_df.with_columns([
		#	pl.col('isolation_source').list.eval(
		#		pl.element().filter(~pl.element().str.contains(r'\d'))
		#	).alias('isolation_source')
		#])
		# --> currently skipped since we don't use it
		
		#polars_df = polars_df.with_columns(
		#	pl.when(pl.col('neo_isolation_source').is_null().and_(pl.col('isolation_source').is_not_null().and_(pl.col('isolation_source').list.len() > 0)))
		#	.then(pl.lit("As reported: ") + pl.col('isolation_source').list.join(", "))
		#	.otherwise(pl.col('neo_isolation_source'))
		#	.alias('neo_isolation_source')
		#)
		# --> that's what isolation_source_raw is for!!

		polars_df = polars_df.drop(['isolation_source']).rename({'neo_isolation_source': 'isolation_source_cleaned'})
		assert polars_df.schema['isolation_source_cleaned'] != pl.List

		#self.logging.info(f"The isolation_source column has type list. We will be .join()ing them into strings.") # done AFTER most standardization
		#polars_df = polars_df.with_columns(
		#	pl.col("isolation_source").list.join(", ").alias("isolation_source")
		#)

		return polars_df

	def standardize_sample_source_as_string(self, polars_df):
		assert 'isolation_source' in polars_df.columns
		assert polars_df.schema['isolation_source'] == pl.Utf8
		for unhelpful_value in tqdm(sample_sources.sample_sources_nonspecific, desc="Nulling bad isolation sources", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
			polars_df = polars_df.with_columns(
				pl.when(pl.col('isolation_source').str.to_lowercase() == unhelpful_value.lower())
				.then(None)
				.otherwise(pl.col('isolation_source'))
				.alias('isolation_source'))
		for sample_source, simplified_sample_source in sample_sources.sample_source_exact_match.items():
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='isolation_source_cleaned', key=sample_source, value=simplified_sample_source, substrings=False, overwrite=False)
		for sample_source, simplified_sample_source in sample_sources.sample_source_exact_match_body_parts.items():
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='isolation_source_cleaned', key=sample_source, value=simplified_sample_source, substrings=False, overwrite=False)
		for sample_source, simplified_sample_source in sample_sources.comprehensive_fuzzy.items():
			polars_df = self.dictionary_match(polars_df, match_col='isolation_source', write_col='isolation_source_cleaned', key=sample_source, value=simplified_sample_source, substrings=True, overwrite=False)
		return polars_df
	
	def standarize_hosts(self, polars_df):
		if polars_df.schema['host'] == pl.List:
			#self.logging.info(f"The host column has type list. We will take the first value as the source of truth.") # done BEFORE most standardization
			#polars_df = polars_df.with_columns(pl.col('host').list.first().alias('host'))
			polars_df = polars_df.with_columns(pl.col('host').list.join(", ").alias('host'))
		assert polars_df.schema['host'] == pl.Utf8
		polars_df = self.standardize_hosts_eager(polars_df).drop('host')
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
				pl.when(pl.col('host').str.contains(f'(?i){host}'))
				.then(pl.lit(sciname))
				.otherwise(
					pl.when(pl.col('host_scienname').is_not_null())
					.then(pl.col('host_scienname'))
					.otherwise(None))
				.alias("host_scienname"),
				
				pl.when(pl.col('host').str.contains(f'(?i){host}'))
				.then(pl.lit(confidence))
				.otherwise(
					pl.when(pl.col('host_confidence').is_not_null())
					.then(pl.col('host_confidence'))
					.otherwise(None))
				.alias("host_confidence"),
				
				pl.when(pl.col('host').str.contains(f'(?i){host}'))
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

	def cleanup_dates(self, polars_df, keep_only_bad_examples=False, err_on_list=True, force_strings=True, in_format=None):
		"""
		Cleans up dates into ISO format.
		You can specify an input format if you know ALL dates in the dataframe conform to it. Currently implemented formats:

		"DD.MM.YYYY" (must be zero-padded)
		"MM/DD/YYYY" (does not need to be zero-padded)

		Notes:
		* keep_only_bad_examples is for debugging; it effectively hides dates that are probably good
		* len_bytes() is way faster than len_chars()
		* yeah you can have mutliple expressions in one with_columns() but that'd require tons of alias columns plus nullify so I'm not doing that
		"""

		if polars_df.schema['date_collected'] == pl.List:
			polars_df = self.NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=['date_collected'])
			if polars_df.schema['date_collected'] == pl.List:
				if err_on_list:
					self.logging.error("Tried to flatten date_collected, but there seems to be some rows with unique values.")
					print(self.NeighLib.get_rows_where_list_col_more_than_one_value(polars_df, 'date_collected').select([self.NeighLib.get_index_column(polars_df), 'date_collected']))
					exit(1)
				else:
					self.logging.warning("Tried to flatten date_collected, but there seems to be some rows with unique values. Will convert to string. This may be less accurate.")
					polars_df = self.NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, force_strings=True, just_these_columns=['date_collected'])

		if polars_df.schema['date_collected'] != pl.Utf8 and force_strings:
			self.logging.warning("date_collected column is not of type string. Will attempt to cast it as string.")
			polars_df = polars_df.with_columns(
				pl.col("date_collected").cast(pl.Utf8).alias("date_collected")
			)

		if in_format == None:

			# "YYYY/YYYY" --> null
			polars_df = polars_df.with_columns(
				pl.when((pl.col('date_collected').str.len_bytes() == 9)
				.and_(pl.col('date_collected').str.count_matches("/") == 1))
				.then(None).otherwise(pl.col('date_collected')).alias("date_collected")
			)

			# "YYYY/YYYY/YYYY" --> null
			polars_df = polars_df.with_columns(
				pl.when((pl.col('date_collected').str.len_bytes() == 14)
				.and_(pl.col('date_collected').str.count_matches("/") == 2))
				.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
			)

			# "YYYY/YYYY/YYYY/YYYY" --> null
			polars_df = polars_df.with_columns(
				pl.when((pl.col('date_collected').str.len_bytes() == 19)
				.and_(pl.col('date_collected').str.count_matches("/") == 3))
				.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
			)
			
			# "YYYY/MM" or "MM/YYYY" --> YYYY
			polars_df = polars_df.with_columns(
				pl.when((pl.col('date_collected').str.len_bytes() == 7)
				.and_(pl.col('date_collected').str.count_matches("/") == 1))
				.then(pl.col('date_collected').str.extract(r'[0-9][0-9][0-9][0-9]', 0)).otherwise(pl.col('date_collected')).alias("date_collected")
			)

			# "MM/DD/YYYY" or "DD/MM/YYYY"  --> YYYY
			polars_df = polars_df.with_columns(
				pl.when((pl.col('date_collected').str.len_bytes() == 10)
					.and_(
						(pl.col('date_collected').str.count_matches("/") == 2)
						.or_(pl.col('date_collected').str.count_matches("-") == 2)
					)
				)
				.then(pl.col('date_collected').str.extract(r'[0-9][0-9][0-9][0-9]', 0)).otherwise(pl.col('date_collected')).alias("date_collected")
			)

			# "YYYY-MM/YYYY-MM" --> null
			polars_df = polars_df.with_columns([
				pl.when((pl.col('date_collected').str.len_bytes() == 15)
				.and_(pl.col('date_collected').str.count_matches("/") == 1)
				.and_(pl.col('date_collected').str.count_matches("-") == 2))
				.then(None).otherwise(pl.col('date_collected')).alias("date_collected"),
			])

			# "YYYY-MM-DDT00:00:00Z"  --> YYYY-MM-DD
			polars_df = polars_df.with_columns([
				pl.when((pl.col('date_collected').str.len_bytes() == 20)
				.and_(pl.col('date_collected').str.count_matches("Z") == 1)
				.and_(pl.col('date_collected').str.count_matches(":") == 2))
				.then(pl.col('date_collected').str.extract(r'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]', 0)).otherwise(pl.col('date_collected')).alias("date_collected"),
			])

			# "YYYY-MM-DD/YYYY-MM-DD"  --> null
			polars_df = polars_df.with_columns(
				pl.when((pl.col('date_collected').str.len_bytes() == 21))
				.then(None)
				.otherwise(pl.col('date_collected'))
				.alias("date_collected"),
			)
		
		elif in_format == "DD.MM.YYYY":
			# All values MUST be zero-padded!
			# Polars regex doesn't support capture groups, so we have to use three expressions here.
			polars_df = polars_df.with_columns([

				pl.when(pl.col("date_collected").is_not_null())
				.then(pl.col("date_collected").str.extract(r"^(\d{2})", 1))
				.otherwise(None)
				.alias("TEMP_month"),

				pl.when(pl.col("date_collected").is_not_null())
				.then(pl.col("date_collected").str.extract(r"^\d{2}\.(\d{2})", 1))
				.otherwise(None)
				.alias("TEMP_day"),

				pl.when(pl.col("date_collected").is_not_null())
				.then(pl.col("date_collected").str.extract(r"(\d{4})$", 1))
				.otherwise(None)
				.alias("TEMP_year"),

				])

			polars_df = polars_df.with_columns(
				pl.when(pl.col("TEMP_year").is_not_null())
				.then(pl.concat_str([
					pl.col("TEMP_year"),
					pl.lit("-"),
					pl.col("TEMP_month"),
					pl.lit("-"),
					pl.col("TEMP_day"),
				]))
				.otherwise(pl.col("date_collected"))
				.alias("date_collected")
			).drop(["TEMP_month", "TEMP_day", "TEMP_year"])

		elif in_format == "MM/DD/YYYY":
			# This avoids strftime() to avoid platform-specific zero-padding nightmares.
			def datetime_parser(DD_slash_MM_slash_YYYY):
				if DD_slash_MM_slash_YYYY is None:
					return None
				parts = DD_slash_MM_slash_YYYY.strip().split("/")
				if len(parts) == 3:
					try:
						month, day, year = [int(p) for p in parts]
						return str(datetime(year, month, day).date().isoformat())
					except Exception as e:
						self.logging.debug(f"Failed to convert: raw={DD_slash_MM_slash_YYYY!r}, parsed={parts!r}, day={day}, month={month}, year={year}, error={e}")
						return None
				return None

			polars_df = polars_df.with_columns(
				pl.col("date_collected").map_elements(datetime_parser, return_dtype=pl.Utf8)
			)
			

		# handle known nonsense
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

		if 'sample_id' in polars_df.columns:
			polars_df = polars_df.with_columns(
				pl.when((pl.col('sample_id') == 'SAMEA5977381') # 2025/2026
					.or_(pl.col('sample_id') == 'SAMEA5977380') # 2025/2026
				)
				.then(None)
				.otherwise(pl.col('date_collected'))
				.alias("date_collected"),
			)

		if 'date_collected_year' in polars_df.columns:
			polars_df = self.NeighLib.try_nullfill_left(polars_df, 'date_collected', 'date_collected_year')[0]
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
		Badger is usually Meles meles, but there's some others out there, so we'll put confidence low in line
		with host_species currently setting "BADGER" to a confidence of 1
		"""
		if 'anonymised_badger_id_sam' in polars_df.columns:
			polars_df = polars_df.with_columns([
				pl.when((pl.col('anonymised_badger_id_sam').is_not_null()) & (pl.col('host_commonname').is_null()))
				.then(pl.lit('badger'))
				.otherwise(pl.col('host_commonname'))
				.alias('host_commonname'),

				pl.when((pl.col('anonymised_badger_id_sam').is_not_null()) & (pl.col('host_confidence').is_null()))
				.then(pl.lit(1))
				.otherwise(pl.col('host_confidence'))
				.alias('host_confidence'),

				pl.when((pl.col('anonymised_badger_id_sam').is_not_null()) & (pl.col('host_scienname').is_null()))
				.then(pl.lit('Meles meles'))
				.otherwise(pl.col('host_scienname'))
				.alias('host_scienname')
			]).drop('anonymised_badger_id_sam')
		return polars_df

	def unmask_mice(self, polars_df):
		if 'mouse_strain_sam' in polars_df.columns:
			polars_df = polars_df.with_columns([
				pl.when((pl.col('mouse_strain_sam').is_not_null()) & (pl.col('host_commonname').is_null()))
				.then(pl.lit('mouse'))
				.otherwise(pl.col('host_commonname'))
				.alias('host_commonname'),

				pl.when((pl.col('mouse_strain_sam').is_not_null()) & (pl.col('host_confidence').is_null()))
				.then(pl.lit(2))
				.otherwise(pl.col('host_confidence'))
				.alias('host_confidence'),

				pl.when((pl.col('mouse_strain_sam').is_not_null()) & (pl.col('host_scienname').is_null()))
				.then(pl.lit('Mus musculus'))
				.otherwise(pl.col('host_scienname'))
				.alias('host_scienname')
			]).drop('mouse_strain_sam')
		return polars_df

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
		elif self.cfg.taxoncore_ruleset == 'None':
			# something about how I changed defaults is causing this... well, strs are invalid anyway so. whatever.
			raise ValueError("A taxoncore ruleset failed to initialize, so we cannot use function taxoncore_iterate_rules!")
		
		for when, strain, lineage, organism, bacterial_group, comment in tqdm((entry.values() for entry in self.cfg.taxoncore_ruleset), desc="Standardizing taxonomy", total=len(self.cfg.taxoncore_ruleset),  ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
			if strain is pl.Null and lineage is pl.Null:
				polars_df = self.taxoncore_GO(polars_df, when, i_group=bacterial_group, i_organism=organism)
			elif strain is pl.Null:
				polars_df = self.taxoncore_GOL(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_lineage=lineage)
			elif lineage is pl.Null:
				polars_df = self.taxoncore_GOS(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_strain=strain)
			else:
				#self.logging.debug(f"strain: {strain} {type(strain)}\nlineage: {lineage} {type(lineage)}\norganism: {organism}\ngroup: {bacterial_group}")
				polars_df = self.taxoncore_GOLS(polars_df, when,  i_group=bacterial_group, i_organism=organism, i_lineage=lineage, i_strain=strain)
		return polars_df

	def sort_out_taxoncore_columns(self, polars_df, force_strings=True):
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
		if group_column_name not in kolumns.columns_to_keep_after_rancheroize:
			self.logging.warning(f"Bacterial group column will have name {group_column_name}, but might get removed later. Add {group_column_name} to kolumns.equivalence!")
		merge_these_columns = [col for col in polars_df.columns if col in sum(kolumns.special_taxonomic_handling.values(), [])]
		debug_incoming_taxoncore_columns = pl.DataFrame({
			"column": merge_these_columns,
			"dtype": [polars_df.schema[col] for col in merge_these_columns], # calculate this BEFORE converting to string
		})
		self.logging.debug("Incoming taxoncore columns (pl.List was joined into comma+space separated strings)")
		self.NeighLib.dfprint(debug_incoming_taxoncore_columns, loglevel=10)
		for col in merge_these_columns:
			if polars_df.schema[col] == pl.List:
				polars_df = polars_df.with_columns(pl.col(col).list.join(", ").alias(col))
			#assert polars_df.schema[col] == pl.Utf8
		if 'organism' in polars_df.columns and self.cfg.rm_phages:
			self.logging.info("Removing phages from organism column...")
			polars_df = self.rm_all_phages(polars_df)
		
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
		for col in ['clade', 'organism', 'lineage', 'strain']:
			polars_df = self.NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=[col])
			if polars_df.schema[col] == pl.List and self.logging.getEffectiveLevel() == 10:
				self.logging.debug(f'Found these multi-element lists in {col} after attempted flatten')
				self.NeighLib.print_only_where_col_list_is_big(polars_df, col) # DEBUGPRINT
				if force_strings:
					self.logging.debug('Forcing these multi-element lists into strings')
					polars_df = self.NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=[col], force_strings=True)

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

	def continent_from_country(self, polars_df, country_col, continent_col, overwrite=True): # overwrite is true to match standardize_countries() but maybe shouldn't be
		if continent_col not in polars_df:
			polars_df = self.NeighLib.add_column_of_just_this_value(polars_df, continent_col, None)
		self.validate_col_country(polars_df, country_col)
		for ISO3166, continent in countries.countries_to_continents.items():
			polars_df = self.dictionary_match(polars_df, match_col=country_col, write_col=continent_col, key=ISO3166, value=continent, substrings=False, overwrite=overwrite)
		return polars_df
	
	def standardize_countries(self, polars_df, try_rm_geoloc_info=False):
		# We expect to be starting out with at least one of the following:
		# * country (type str)
		# * geoloc_info (type list)
		# Outputs:
		# country, region, continent
		# If only country exists, ISO that list. Whatever doesn't get ISO'd gets moved to 'continent' if it matches a continent, otherwise will be moved to 'region' (no overwrite).
		# Region and continent keep type str the entire time.
		# If only geoloc_info exists, go through the list pulling out countries by ISO matching, then continents. Anything remaining move to region.
		# If both exist, ISO convert country column, then do continent/region matching on geoloc_info column.

		# TODO: assert intermediate columns like 'likely_country' not in df
		united_nations = {**countries.substring_match, **countries.exact_match}
		if 'country' in polars_df.columns and 'geoloc_info' in polars_df.columns:
			self.logging.debug("geoloc_info ✔️ country ✔️")
			# This DOES NOT force everything to be ISO standard in country column, since if you have stuff in that column already I assume you want it there

			for nation, ISO3166 in countries.substring_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', key=nation, value=ISO3166, substrings=True, overwrite=True, status_cols=False)
			for nation, ISO3166 in countries.exact_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', key=nation, value=ISO3166, substrings=False, overwrite=True, status_cols=False)
			for ISO3166, continent in countries.countries_to_continents.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', key=ISO3166, value=continent, substrings=False, overwrite=True)

			# If geoloc_info can become a str 'region' column, and 'region' column doesn't already exist, let's do that
			# ...but that's computationally expensive and we want to parse geoloc_info for continents so actually let's not do this here
			#if try_rm_geoloc_info:
			#	polars_df = self.NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=['geoloc_info'])
			#	if polars_df['geoloc_info'].schema == pl.Utf8 and 'region' not in polars_df.columns:
			#		polars_df = polars_df.rename({'geoloc_info': 'region'})

		elif 'country' in polars_df.columns and 'geoloc_info' not in polars_df.columns:
			self.logging.debug("geoloc_info ✖️ country ✔️")
			# This DOES force everything to be ISO standard in country column
			for nation, ISO3166 in countries.substring_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', key=nation, value=ISO3166, substrings=True, overwrite=True, status_cols=False, remove_match_from_list=True)
			for nation, ISO3166 in countries.exact_match.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', key=nation, value=ISO3166, substrings=False, overwrite=True, status_cols=False, remove_match_from_list=True)		
			for ISO3166, continent in countries.countries_to_continents.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', key=ISO3166, value=continent, substrings=False, overwrite=True)
			self.validate_col_country(polars_df)
			self.logging.debug("Returning early due to lack of geoloc_info column")
			return polars_df
		
		elif 'geoloc_info' in polars_df.columns and 'country' not in polars_df.columns: # and not 'country'
			self.logging.debug("geoloc_info ✔️ country ✖️")
			# To handle "country: region" metadata without overwriting the region metadata, first we attempt to extract countries by looking for non-substring matches,
			# including the countries.substring_match stuff we usually just substring match upon.
			for nation, ISO3166 in tqdm(united_nations.items(), desc="Standardizing countries", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
				polars_df = self.dictionary_match(polars_df, match_col='geoloc_info', write_col='country', key=nation, value=ISO3166, substrings=False, overwrite=False, status_cols=False, remove_match_from_list=True)
			for ISO3166, continent in countries.countries_to_continents.items():
				polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', key=ISO3166, value=continent, substrings=False, overwrite=True)
		
		else:
			self.logging.warning("Neither 'country' nor 'geoloc_info' found in dataframe. Cannot standardize.")
			return polars_df

		# Our dataframe now is guranteed to have a country column and a geoloc_info column.
		# (TODO: Ensure the initial geoloc_info ✖️ country ✔️ case results in a geoloc_info column of type list, not str)
		assert polars_df.schema['geoloc_info'] == pl.List(pl.Utf8)
		assert 'continent' in polars_df.columns
		
		# Now let's try to pull continent information from geoloc_info 
		for continent, that_same_continent in regions.continents.items():
			polars_df = self.dictionary_match(polars_df, match_col='geoloc_info', write_col='continent', key=continent, value=that_same_continent, substrings=False, overwrite=False, status_cols=False, remove_match_from_list=True)

		# Make sure we don't have junk from hypothetical previous runs, or weird columns
		assert 'likely_country' not in polars_df.columns
		polars_df = polars_df.with_columns(pl.lit(None).alias('likely_country')) # needs to be initialized since it's in an otherwise()
		assert 'def_country' not in polars_df.columns
		polars_df = polars_df.with_columns(pl.lit(None).alias('def_country')) # needs to be initialized since it's in an otherwise()
		assert 'geoloc_info_unhandled' not in polars_df.columns
		assert 'neo_region' not in polars_df.columns
		
		# We can allow a pre-existing region column though
		if 'region' not in polars_df.columns:
			polars_df = polars_df.with_columns(pl.lit(None).alias('region'))

		# Exact matches for continent and country have been moved, now look for "country: region" or "continent: country" matches
		# These use str.starts_with()
		for continent, that_same_continent in regions.continents.items():
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
		for nation, ISO3166 in tqdm(united_nations.items(), desc="Standardizing regions", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
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
		# TODO: Check if later region extraction script manages to pull out "Sinfra" for Ivory Coast samples (see SRR18334007)
		for nation, ISO3166 in tqdm(countries.substring_match.items(), desc="Finishing up", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'):
			null_start = self.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'country')
			polars_df = polars_df.with_columns([
				pl.when((pl.col("geoloc_info").list.eval(pl.element().str.contains(nation)).list.sum() != 0)
					.and_(pl.col('country').is_null()))
				.then(pl.lit(ISO3166))
				.otherwise(pl.col('country'))
				.alias('country')
				# Purposely do not remove matches from geoloc_info; this will keep stuff like ["Beijing China"] available
				# for regionafying, even though that means we get a country of CHN and a region of "Beijing China"
			])
			null_end = self.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'country')

		# We hereby declare anything remaining in geoloc_info to be a region
		polars_df = polars_df.with_columns([
			pl.when((pl.col("geoloc_info").list.len() > 0)
			.and_(pl.col('region').is_null()))
			.then(pl.col("geoloc_info").list.drop_nulls())
			.otherwise(None)
			.alias('geoloc_info_unhandled')
		])
		if self.logging.getEffectiveLevel() == 10:
			self.logging.debug("Found some stuff in geoloc_info we're not sure how to handle, will convert to region")
			self.NeighLib.print_only_where_col_list_is_big(polars_df, 'geoloc_info_unhandled')
		#polars_df = self.NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, force_strings=True, just_these_columns=['geoloc_info_unhandled'])
		polars_df = self.NeighLib.encode_as_str(polars_df, 'geoloc_info_unhandled')
		polars_df = polars_df.with_columns(pl.coalesce(["region", "geoloc_info_unhandled"]).alias("neo_region"))
		polars_df = polars_df.drop(['region', 'geoloc_info_unhandled', 'geoloc_info'])
		polars_df = polars_df.rename({'neo_region': 'region'})
		polars_df = polars_df.with_columns(pl.col("region").str.strip_chars_start(" "))

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

		# Any matches for country names in geoloc_name, country, likely_country, and def_country have already been ISO3166'd
		# Let's use that to convert some ISO3166'd countries into continents (this happens after region matching intentionally)
		for ISO3166, continent in countries.countries_to_continents.items():
			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', key=ISO3166, value=continent, substrings=False, overwrite=True)

		self.validate_col_country(polars_df)
		return polars_df
		

	def validate_col_country(self, polars_df, country_col='country'):
		# TODO: now we have some that aren't just three bytes
		assert country_col in polars_df.columns
		assert polars_df.schema[country_col] == pl.Utf8
		assert 'geoloc_info_unhandled' not in polars_df.columns
		invalid_rows = polars_df.filter(pl.col(country_col).str.len_bytes() != 3)
		if len(invalid_rows) > 0:
			# TODO: add check against a full list of ISO codes too?
			self.logging.error(
				f"The following rows have countries that are not in ISO3166 format:"
			)
			self.dfprint(invalid_rows.select(self.NeighLib.get_valid_id_columns(invalid_rows) + ['country']))
			raise ValueError
		self.logging.info(f"Column {country_col} for country metadata appears valid (all rows either null or 3 byte strings)")
		if self.logging.getEffectiveLevel() == 10:
			self.NeighLib.print_a_where_b_equals_these(polars_df, col_a='country', col_b='run_id',
				list_to_match=['SRR9614686', 'ERR046972', 'ERR2884698', 'ERR732680', 'ERR841442', 'ERR5908244', 'SRR23310897', 'SRR12380906', 'SRR18054772', 'SRR10394499', 'SRR9971324', 'ERR732681', 'SRR23310897'], 
				alsoprint=['region', 'continent'])

	# Here be dragons
	def test_neighlib_cfg_update_mycobact(self, via_another_module=None):
		self.NeighLib._testcfg_mycobact_is_false(via_another_module=True)

	def _testcfg_mycobact_is_false(self):
		assert self.cfg.mycobacterial_mode == False
		print("✅ Successfully updated mycobacterial_mode in Standardizer")

	def _testcfg_logger_is_debug(self):
		self.logging.debug("✅ Successfully updated loglevel in Standardizer")

	def test_neighlib_cfg_update(self, via_another_module=None):
		self.NeighLib._testcfg_logger_is_debug(via_another_module=True)
			
