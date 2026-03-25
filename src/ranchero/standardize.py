import sys
from datetime import datetime
from .statics import host_disease, host_species, sample_sources, kolumns, countries, regions
from .statics import sample_sources_wrong_column
from .config import RancheroConfig
import polars as pl
from tqdm import tqdm
import time
import re
from collections import OrderedDict # dictionaries are ordered in Python 3.7+, but OrderedDict has a better popitem() function we need
from itertools import islice

# https://peps.python.org/pep-0661/
_DEFAULT_TO_CONFIGURATION = object()

# overwritten in _setup_progress_bar()
TQDM_ENABLE = True
TQDM_FRMT = '{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'
TQDM_MOO = '➖🌱🐄'

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

	def _setup_progress_bar(self):
		global TQDM_MOO
		TQDM_MOO = self.cfg.get_config(tqdm_ascii)	
		global TQDM_FRMT
		TQDM_FRMT = self.cfg.get_config(tqdm_bar_format)
		global TQDM_ENABLE
		TQDM_ENABLE = not self.cfg.get_config(tqdm_disable) # flipped, hence why we don't use _DEFAULT_TO_CONFIGURATION

	def standardize_everything(self, polars_df, add_expected_nulls=True, assume_organism="Mycobacterium tuberculosis", assume_clade="tuberculosis", skip_sample_source=False, force_strings=True,
		organism_fallback=None, clade_fallback=None, retain_input=True):
		if any(column in polars_df.columns for column in ['geoloc_info', 'country', 'region']):
			self.logging.info("Standardizing countries...")
			polars_df = self.standardize_countries(polars_df)
		
		if 'date_collected' in polars_df.columns:
			self.logging.info("Cleaning up dates...")
			polars_df = self.cleanup_dates(polars_df)
		
		# Because this one is VERY open to interpretation and I'm not a medical doctor, we will also have a "raw value" column.
		# Must be before taxoncore and host, no need to force_strings as its already forced
		if 'isolation_source' in polars_df.columns and not skip_sample_source:
			self.logging.info("Standardizing isolation sources...")
			polars_df = self.standardize_isolation_source(polars_df, isolation_source_col='isolation_source', retain_input=retain_input)
		
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
			polars_df = self.NeighLib.add_column_of_value(polars_df, 'organism', assume_organism, if_already_exists='error')
			polars_df = self.NeighLib.add_column_of_value(polars_df, 'clade', assume_clade, if_already_exists='error')

		if organism_fallback is not None:
			polars_df = polars_df.with_columns(pl.col('organism').fill_null(organism_fallback))
		if clade_fallback is not None:
			polars_df = polars_df.with_columns(pl.col('organism').fill_null(clade_fallback))

		polars_df = self.drop_no_longer_useful_columns(polars_df)
		polars_df = self.NeighLib.null_lists_of_len_zero(self.NeighLib.rancheroize_polars(polars_df, nullify=False))
		return polars_df

	def pl_when_col_contains_str_or_str_write_to_same_col(self, polars_df, col, match_str1, match_str2, out_str):
		"""
		Decently common polars expression for both str and list cases
		"""
		if polars_df.schema[col] == pl.Utf8:
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col(col).str.contains(match_str1)
					.or_(pl.col(col).str.contains(match_str2))
				)
				.then(pl.lit(out_str))
				.otherwise(pl.col(col))
				.alias(col)
			])
			return polars_df
		elif polars_df.schema[col] == pl.List(pl.Utf8):
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col(col).list.eval(pl.element().str.contains(match_str1)).list.any()
					.or_(pl.col(col).list.eval(pl.element().str.contains(match_str2)).list.any())
				)
				.then(pl.lit([out_str]))
				.otherwise(pl.col(col))
				.alias(col)
			])
			return polars_df
		else:
			raise TypeError(f"Couldn't build polars expression for column of type {polars_df.schema[col]}")

	def standardize_isolation_source(self, polars_df, isolation_source_col='isolation_source', move_lost_metadata=True, collapse_culture=False, retain_input=True, progress_bar=TQDM_ENABLE):
		"""
		Sample source (rancheroized as isolation_source) is kind of a mess, because submitters can interpret it as very different things:
		* host organism species
		* host organism information
		* fluid/organ/body part the sample was isolated from
		* environmental information (soil type, etc)
		* geographic location

		So we have this function!
		* starts with isolate_sam_ss_dpl100 if present, which is usually just sample names and therefore only worth a quick check
		  for good stuff in that column before dropping it

		TODO: move_lost_metadata should be a cfg option
		"""
		start = time.time()
		if isolation_source_col is not 'isolation_source':
			self.logging.warning(f"Isolation source column {isolation_source_col} is not isolation_source; this may not be a rancheroized dataframe")
		assert 'isolation_source' in polars_df.columns
		if retain_input:
			polars_df = self.NeighLib.duplicate_col(polars_df, isolation_source_col, f"{isolation_source_col}_raw")
		
		if 'isolate_sam_ss_dpl100' in polars_df.columns:
			polars_df = self.dictionary_match(polars_df, match_col='isolate_sam_ss_dpl100', write_col=isolation_source_col, dictionary=sample_sources.exact_replacements,
				substrings=False, overwrite=False, progress_bar=progress_bar, progress_bar_desc="isolate_sam_ss_dpl100", remove_match_from_list=True)
			polars_df = polars_df.drop('isolate_sam_ss_dpl100')

		if self.cfg.mycobacterial_mode and move_lost_metadata:
			for destination_column, replacements in sample_sources_wrong_column.exact_one_column_writes_mycobacterial.items():
				if destination_column in polars_df:
					polars_df = self.dictionary_match(polars_df, isolation_source_col, destination_column, dictionary=replacements, 
						substrings=False, overwrite=False, remove_match_from_list=True, 
						progress_bar=progress_bar, progress_bar_desc=f"Wayward {destination_column}")

		if move_lost_metadata:
			for destination_column, replacements in sample_sources_wrong_column.exact_one_column_writes.items():
				polars_df = self.dictionary_match(polars_df, isolation_source_col, destination_column, dictionary=replacements,
						substrings=False, overwrite=False, remove_match_from_list=True, 
						progress_bar=progress_bar, progress_bar_desc=f"Wayward {destination_column} (exact)")

			for write_column_2, replacements in sample_sources_wrong_column.exact_two_column_writes.items():
				if destination_column in polars_df:
					polars_df = self.dictionary_match_two_col(polars_df, isolation_source_col, isolation_source_col, write_column_2, dictionary=replacements, 
						substrings=False, overwrite_1=True, overwrite_2=False, remove_match_from_list=True, 
						progress_bar=progress_bar, progress_bar_desc=f"Wayward {destination_column} (exact)")
			for destination_column, replacements in sample_sources_wrong_column.substring_two_column_writes.items():
				if destination_column in polars_df:
					polars_df = self.dictionary_match_two_col(polars_df, isolation_source_col, isolation_source_col, write_column_2, dictionary=replacements, 
						substrings=True, overwrite_1=True, overwrite_2=False, remove_match_from_list=True, 
						progress_bar=progress_bar, progress_bar_desc=f"Wayward {destination_column} (substring)")

		# Get rid of isolation source values that aren't actually helpful
		if polars_df.schema[isolation_source_col] == pl.List:
			for unhelpful_value in tqdm(sample_sources.exact_null_nonsensical, desc="Nonsense isolation_source", ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
				polars_df = polars_df.with_columns(
					pl.col(isolation_source_col).list.eval(pl.element().filter(pl.element() != unhelpful_value)).alias(isolation_source_col))
			for unhelpful_value in tqdm(sample_sources.exact_null_generic, desc="Generic isolation_source", ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
				polars_df = polars_df.with_columns(
					pl.col(isolation_source_col).list.eval(pl.element().filter(pl.element() != unhelpful_value)).alias(isolation_source_col))
		else:
			for unhelpful_value in tqdm(sample_sources.exact_null_nonsensical, desc="Nonsense isolation_source", ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
				polars_df = polars_df.with_columns(
					pl.when(pl.col(isolation_source_col).str.to_lowercase() == unhelpful_value.lower())
					.then(None)
					.otherwise(pl.col(isolation_source_col))
					.alias(isolation_source_col))
			for unhelpful_value in tqdm(sample_sources.exact_null_generic, desc="Generic isolation_source", ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
				polars_df = polars_df.with_columns(
					pl.when(pl.col(isolation_source_col).str.to_lowercase() == unhelpful_value.lower())
					.then(None)
					.otherwise(pl.col(isolation_source_col))
					.alias(isolation_source_col))

		# If there's even a whiff of simulation, declare the whole list simulated
		self.logging.info("Looking for simulated data...")
		polars_df = self.pl_when_col_contains_str_or_str_write_to_same_col(polars_df, isolation_source_col, '(?i)simulated', '(?i)in silico', 'simulated/in silico')
		self.logging.info("Looking for lab strains...")
		polars_df = self.pl_when_col_contains_str_or_str_write_to_same_col(polars_df, isolation_source_col, '(?i)laboratory strain', '(?i)lab strain', 'simulated/in silico')

		# AFTER we have cleaned up very obvious things, from now on, write to a NEW COLUMN to help avoid accidentally overwriting past iterations (eg "culture from sputum" --> "sputum" or "culture")
		temp_isolation_source = self.NeighLib.tempcol(polars_df, 'neo_isolation_source')
		polars_df = self.NeighLib.add_column_of_value(polars_df, tempcol, None, if_already_exists='error')

		if polars_df.schema[isolation_source_col] == pl.List:
			for this, that, then in tqdm(sample_sources.if_this_and_that_then, desc="Checking for combo matches", ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
				this_and_that = pl.col(isolation_source_col).list.eval(pl.element().str.contains(this)).list.any().and_(pl.col(isolation_source_col).list.eval(pl.element().str.contains(that)).list.any())
				polars_df = polars_df.with_columns([
					pl.when(this_and_that)
					.then(pl.lit(then))
					.otherwise(pl.col(temp_isolation_source)) # avoid overwriting previous iterations
					.alias(temp_isolation_source)

					# this goes from 30 iterations per second to 30 seconds per iteration! yikes!
					#pl.when(this_and_that)
					#.then(None)
					#.otherwise(pl.col(isolation_source_col))
					#.alias(isolation_source_col)
				])
		else:
			# TODO: ACTUALLY PUT THIS BACK IN!!
			self.logging.warning("Skipping this-that-then matches as they're not supported when isolation_source is string")
		
		polars_df = self.dictionary_match(polars_df, match_col=isolation_source_col, write_col=temp_isolation_source, dictionary=sample_sources.exact_replacements, 
			substrings=False, overwrite=False, remove_match_from_list=True, progress_bar=progress_bar, progress_bar_desc="Checking for exact matches")
		polars_df = self.dictionary_match(polars_df, match_col=isolation_source_col, write_col=temp_isolation_source, dictionary=sample_sources.comprehensive_fuzzy, 
			substrings=True, overwrite=False, remove_match_from_list=True, progress_bar=progress_bar, progress_bar_desc="Checking for fuzzy matches")

		self.logging.info("Cleaning up...")

		if collapse_culture:
			polars_df = polars_df.with_columns([
				pl.when(pl.col(isolation_source_col).list.eval(pl.element().str.contains('(?i)culture')).list.any())
				.then(pl.lit('culture'))
				.otherwise(pl.col(temp_isolation_source))
				.alias(temp_isolation_source)
			])
		polars_df = polars_df.drop(isolation_source_col).rename({temp_isolation_source: isolation_source_col})
		assert polars_df.schema[isolation_source_col] != pl.List
		self.logging.info(f"Standardized {isolation_source_col} in {time.time()-start:.4f} seconds")
		return polars_df

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

	def _parallelize(self, polars_df, match_col: str, write_col: str, dictionary: dict, 
		substrings, 
		overwrite, 
		retain_input,
		remove_match_from_list,
		expr_write_col_is_empty,
		progress_bar=TQDM_ENABLE,
		progress_bar_desc="Parallel matching..."):

		# TODO: use tempcol function as fallback?
		temp_write_cols = [f"{write_col}_{i}" for i in range(1, 6)]
		assert set(temp_write_cols).isdisjoint(polars_df.columns)

		progress_bar_max = len(dictionary) // 5 + (1 if len(dictionary) % 5 != 0 else 0)

		#for batch in self.chunk_dict(dictionary, 5):
		for batch in tqdm(self.chunk_dict(dictionary, 5), total=progress_bar_max, desc=progress_bar_desc, ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
			if len(batch) == 5:
				items = list(batch.items())
				k1, v1 = items[0]
				k2, v2 = items[1]
				k3, v3 = items[2]
				k4, v4 = items[3]
				k5, v5 = items[4]

				expr_allowed_to_overwrite_1, expr_filter_exp_1, expr_found_a_match_1 = self._setup_kv_expressions(polars_df, match_col, write_col, key=k1, value=v1, 
				substrings=substrings, overwrite=overwrite, remove_match_from_list=remove_match_from_list)
				expr_allowed_to_overwrite_2, expr_filter_exp_2, expr_found_a_match_2 = self._setup_kv_expressions(polars_df, match_col, write_col, key=k2, value=v2, 
				substrings=substrings, overwrite=overwrite, remove_match_from_list=remove_match_from_list)
				expr_allowed_to_overwrite_3, expr_filter_exp_3, expr_found_a_match_3 = self._setup_kv_expressions(polars_df, match_col, write_col, key=k3, value=v3, 
				substrings=substrings, overwrite=overwrite, remove_match_from_list=remove_match_from_list)
				expr_allowed_to_overwrite_4, expr_filter_exp_4, expr_found_a_match_4 = self._setup_kv_expressions(polars_df, match_col, write_col, key=k4, value=v4, 
				substrings=substrings, overwrite=overwrite, remove_match_from_list=remove_match_from_list)
				expr_allowed_to_overwrite_5, expr_filter_exp_5, expr_found_a_match_5 = self._setup_kv_expressions(polars_df, match_col, write_col, key=k5, value=v5, 
				substrings=substrings, overwrite=overwrite, remove_match_from_list=remove_match_from_list)

				if polars_df.schema[write_col] == pl.List(pl.Utf8) and type(v1) == str:
					# Calling function dictionary_match already asserted all values are of same type to each other,
					# this is making sure they match the type of write_col
					v1, v2, v3, v4, v5 = [v1], [v2], [v3], [v4], [v5]
				elif polars_df.schema[write_col] == pl.Utf8 and type(v1) == list:
					# Might not be the best practice...
					v1, v2, v3, v4, v5 = ' '.join(v1), ' '.join(v2), ' '.join(v3), ' '.join(v4), ' '.join(v5)

				polars_df = polars_df.with_columns([
					pl.when(
						(
							(expr_allowed_to_overwrite_1)
							.or_(expr_write_col_is_empty)
						)
						.and_(expr_found_a_match_1)
					)
					.then(pl.lit(v1))
					.otherwise(None)
					.alias(f"{write_col}_1"),

					pl.when(
						(
							(expr_allowed_to_overwrite_2)
							.or_(expr_write_col_is_empty)
						)
						.and_(expr_found_a_match_2)
					)
					.then(pl.lit(v2))
					.otherwise(None)
					.alias(f"{write_col}_2"),

					pl.when(
						(
							(expr_allowed_to_overwrite_3)
							.or_(expr_write_col_is_empty)
						)
						.and_(expr_found_a_match_3)
					)
					.then(pl.lit(v3))
					.otherwise(None)
					.alias(f"{write_col}_3"),

					pl.when(
						(
							(expr_allowed_to_overwrite_4)
							.or_(expr_write_col_is_empty)
						)
						.and_(expr_found_a_match_4)
					)
					.then(pl.lit(v4))
					.otherwise(None)
					.alias(f"{write_col}_4"),

					pl.when(
						(
							(expr_allowed_to_overwrite_5)
							.or_(expr_write_col_is_empty)
						)
						.and_(expr_found_a_match_5)
					)
					.then(pl.lit(v5))
					.otherwise(None)
					.alias(f"{write_col}_5")
				])

				polars_df = polars_df.with_columns(pl.coalesce(temp_write_cols+[write_col]).alias(write_col)).drop(temp_write_cols)
			
				if expr_filter_exp_1 is not None: # if remove_match_from_list and polars_df.schema[match_col] == pl.List(pl.Utf8)
					temp_match_cols = [f"{match_col}_{i}" for i in range(1, 6)]
					assert set(temp_match_cols).isdisjoint(polars_df.columns)

					polars_df = polars_df.with_columns([
						pl.when(expr_found_a_match_1)
						.then(pl.col(match_col).list.eval(pl.element().filter(expr_filter_exp_1)))
						.otherwise(None)
						.alias(f"{match_col}_1"),
						pl.when(expr_found_a_match_2)
						.then(pl.col(match_col).list.eval(pl.element().filter(expr_filter_exp_2)))
						.otherwise(None)
						.alias(f"{match_col}_2"),
						pl.when(expr_found_a_match_3)
						.then(pl.col(match_col).list.eval(pl.element().filter(expr_filter_exp_3)))
						.otherwise(None)
						.alias(f"{match_col}_3"),
						pl.when(expr_found_a_match_4)
						.then(pl.col(match_col).list.eval(pl.element().filter(expr_filter_exp_4)))
						.otherwise(None)
						.alias(f"{match_col}_4"),
						pl.when(expr_found_a_match_5)
						.then(pl.col(match_col).list.eval(pl.element().filter(expr_filter_exp_5)))
						.otherwise(None)
						.alias(f"{match_col}_5")
					])
					polars_df = polars_df.with_columns(pl.coalesce(temp_match_cols+[match_col]).alias(match_col)).drop(temp_match_cols)
			else: # fall back on the other method
				for key, value in batch.items():
					expr_allowed_to_overwrite, expr_filter_exp, expr_found_a_match = self._setup_kv_expressions(polars_df, match_col, write_col, key=key, value=value, 
						substrings=substrings, overwrite=overwrite, remove_match_from_list=remove_match_from_list)
					polars_df = self._kv_match(polars_df, match_col, write_col, key, value, False,
						expr_allowed_to_overwrite, expr_filter_exp, expr_found_a_match, None, expr_write_col_is_empty, None)
		return polars_df

	@staticmethod
	def _setup_consistent_expressions(polars_df, write_col, status_cols_reset=True):
		if status_cols_reset:
			matched_false = False
			written_false = False
		else:
			matched_false = pl.col('matched')
			written_false = pl.col('written')

		if polars_df.schema[write_col] == pl.List:
			write_col_is_empty = ((pl.col(write_col).is_null()).or_(pl.col(write_col).list.len() < 1))
			if type(value) is not list:
				value = [value] # needed to avoid type errors
		elif polars_df.schema[write_col] == pl.Utf8:
			write_col_is_empty = ((pl.col(write_col).is_null()).or_(pl.col(write_col).str.len_bytes() == 0))
		else:
			write_col_is_empty = pl.col(write_col).is_null()

		return matched_false, written_false, write_col_is_empty

	@staticmethod
	def _anchor_regex(dictionary):
		corrected_dict, warning_flag = {}, False
		
		for key, value in dictionary.items():
			new_key = key
			
			if not key.startswith('^'):
				new_key = '^' + new_key
				warning_flag = True
				
			if not key.endswith('$'):
				new_key = new_key + '$'
				warning_flag = True
			
			corrected_dict[new_key] = value
		
		if warning_flag: 
			self.logging.warning("Substrings false, experimental_contains_any true, but dictionary keys lacked regex anchors. Converted to anchors, will continue.")
		return corrected_dict

	@staticmethod
	def _setup_kv_expressions(polars_df, match_col, write_col, key, value, substrings, overwrite, remove_match_from_list):
		allowed_to_overwrite = (pl.lit(overwrite) == True).and_(pl.lit(value).is_not_null())
		
		# Not nesting these because this is easier to read (imho)
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
			# should never happen due to dictionary_match()'s assert
			self.logging.error(f"Invalid type {polars[match_col].schema} for match_col named {match_col}, cannot do matching")
			raise TypeError

		if remove_match_from_list and polars_df.schema[match_col] == pl.List(pl.Utf8):
			if substrings:
				filter_containing = ~pl.element().str.contains(key)
			else:
				filter_containing = pl.element().str.to_lowercase() != key.lower()
		else:
			filter_containing = None

		return allowed_to_overwrite, filter_containing, found_a_match

	def dictionary_match_two_col(self, polars_df, match_col, write_col_1, write_col_2, dictionary, overwrite_1, overwrite_2,
		substrings=False,retain_input=False, remove_match_from_list=True, progress_bar=TQDM_ENABLE, progress_bar_desc="Multi-col matching..."):
		assert match_col in polars_df.columns
		assert ( (polars_df.schema[match_col] == pl.Utf8) or (polars_df.schema[match_col] == pl.List(pl.Utf8)) )
		if retain_input:
			polars_df = polars_df.with_columns(pl.col(match_col).alias(f"{match_col}_raw")) if f"{match_col}_raw" not in polars_df.columns else polars_df
		polars_df = self.NeighLib.add_column_of_value(polars_df, write_col_1, None, if_already_exists='ignore')
		polars_df = self.NeighLib.add_column_of_value(polars_df, write_col_2, None, if_already_exists='ignore')
		_, _, expr_write_col_1_is_empty = self._setup_consistent_expressions(polars_df, write_col=write_col_1, status_cols_reset=True)
		for match_key, writes in tqdm(dictionary.items(), desc=progress_bar_desc, ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
			assert type(match_key) is str
			assert type(writes) is list
			assert len(writes) == 2
			_, _, expr_write_col_1_is_empty = self._setup_consistent_expressions(polars_df, write_col=write_col_1)
			_, _, expr_write_col_2_is_empty = self._setup_consistent_expressions(polars_df, write_col=write_col_2)
			expr_allowed_to_overwrite_1, expr_filter_exp, expr_found_a_match = self._setup_kv_expressions(polars_df, match_col, write_col_2, key=match_key, value=writes[0], 
				substrings=substrings, overwrite=overwrite_1, remove_match_from_list=remove_match_from_list)
			expr_allowed_to_overwrite_2, _, _ = self._setup_kv_expressions(polars_df, match_col, write_col_2, key=match_key, value=writes[1], 
				substrings=substrings, overwrite=overwrite_2, remove_match_from_list=remove_match_from_list)
			polars_df = self._kv_write_two_col(polars_df, match_col, write_col_1, write_col_2, writes,
				expr_allowed_to_overwrite_1, expr_allowed_to_overwrite_2, expr_filter_exp, expr_found_a_match,
				expr_write_col_1_is_empty, expr_write_col_2_is_empty)
		return polars_df

	def _kv_write_two_col(self, polars_df, match_col: str, write_col_1: str, write_col_2: str,
		writes: list,
		allowed_to_overwrite_1: pl.expr,
		allowed_to_overwrite_2: pl.expr,
		filter_exp: pl.expr,
		found_a_match: pl.expr,
		write_col_1_is_empty: pl.expr,
		write_col_2_is_empty: pl.expr
		):
		"""
		Relies on asserts from dictionary_match_two_col()
		"""
		polars_df = polars_df.with_columns([
				pl.when(
					((allowed_to_overwrite_1)
						.or_(write_col_1_is_empty)
					)
					.and_(found_a_match)
				)
				.then(pl.lit(writes[0]))
				.otherwise(pl.col(write_col_1))
				.alias(write_col_1),

				pl.when(
					((allowed_to_overwrite_2)
						.or_(write_col_2_is_empty)
					)
					.and_(found_a_match)
				)
				.then(pl.lit(writes[1]))
				.otherwise(pl.col(write_col_2))
				.alias(write_col_2)
			])
		return polars_df


	def dictionary_match(self, polars_df, match_col: str, write_col: str, dictionary: dict, 
		substrings=False,             # True: "US Virgin Islands" matches "US", False: "US Virgin Islands" doesn't match "US"
		overwrite=False,              # True: If write_col is not null, don't write dictionary value 
		retain_input=False,           # True: Create f"{match_col}_raw" before doing anything
		status_cols=False,            # True: Use 'matched'/'written' status columns (deprecated)
		status_cols_reset=True,       # True: If status columns already exist, clear them first (no-op if !status_cols)
		remove_match_from_list=True,  # True: If match_col is pl.List, remove string matches from list (ex: ["foo", "bar"] -> matches "foo" -> ["bar"])
		strict_write_col_type=False,  # True: If dictionary values (replacements) are str, write_col must be pl.Utf8; if values are list, write_col must be pl.List
		progress_bar=TQDM_ENABLE,     # True: Show a cute little progress bar (can turn off universally in configuration)
		progress_bar_desc="Standardizing...", # Progress bar description (no-op if !progress_bar)
		parallelize=True,              # True: Batch searches in groups of five (experimental but way quicker)
		only_replace_substring=False): # True: Use polars contains_any() and replace_many() behavior, NOT RECOMMENDED FOR MOST USE CASES
		"""
		Replace a pl.Utf8 or pl.List(pl.Utf8) column's values with the values in a dictionary per its key-value pairs.
		* Case-insensitive
		* If match_col is pl.List, if any element in the list matches, that is considered a match
		"""
		assert match_col in polars_df.columns
		assert ( (polars_df.schema[match_col] == pl.Utf8) or (polars_df.schema[match_col] == pl.List(pl.Utf8)) )
		if retain_input:
			polars_df = polars_df.with_columns(pl.col(match_col).alias(f"{match_col}_raw")) if f"{match_col}_raw" not in polars_df.columns else polars_df
		if status_cols:
			polars_df = polars_df.with_columns(pl.lit(False).alias('matched')) if 'matched' not in polars_df.columns else polars_df.with_columns(pl.col('matched').fill_null(False))
			polars_df = polars_df.with_columns(pl.lit(False).alias('written')) if 'written' not in polars_df.columns else polars_df.with_columns(pl.col('written').fill_null(False))
		polars_df = self.NeighLib.add_column_of_value(polars_df, write_col, None, if_already_exists='ignore')
		if strict_write_col_type:
			# Force that all dictionary values are the same type (if !parallelize this isn't strictly necessary but honestly just don't have mixed-type keys that's uncool)
			if all(isinstance(v, list) for v in dictionary.values()):
				if polars_df.schema[write_col] != pl.List:
					raise TypeError(f"strict_write_col_type = True, replacements are of type list, but write_col is type {polars_df.schema[write_col]}")
			elif all(isinstance(v, str) for v in dictionary.values()):
				if polars_df.schema[write_col] != pl.Utf8:
					raise TypeError(f"strict_write_col_type = True, replacements are of type str, but write_col is type {polars_df.schema[write_col]}")
			else:
				raise TypeError(f"strict_write_col_type = True, replacements are of unsupported type or multiple types")
		
		# This was originally an attempt to speed up replacements, but neither contains_any() nor replace_many() supports beginning/end string regex,
		# so what it actually does is just replace the matching substring with something else. So "blood" will match "Heart and Blood Institute" and
		# will return "Heart and blood Institute" which isn't useful for most situations.
		if only_replace_substring and polars_df.schema[match_col] == pl.Utf8 and substrings:
			if status_cols: self.logging.warning("Status columns not available for this type of replacement")
			has_a_match = pl.col(match_col).str.contains_any(list(dictionary.keys()), ascii_case_insensitive=True)
			str_replace_many = pl.col(match_col).str.replace_many(list(dictionary.keys()), list(dictionary.values()), ascii_case_insensitive=True)
			if overwrite:
				return polars_df.with_columns(str_replace_many.alias(write_col))
			else:
				return polars_df.with_columns(pl.when( (pl.col(write_col).is_null()).and_(has_a_match) ).then(str_replace_many).otherwise(write_col).alias(write_col))

		# these polars expressions don't depend on dictionary values so let's only calculate them once
		expr_matched_false, expr_written_false, expr_write_col_is_empty = self._setup_consistent_expressions(polars_df, write_col=write_col, status_cols_reset=status_cols_reset)

		# actually do the matching
		if parallelize and not status_cols:
			polars_df = self._parallelize(polars_df, match_col, write_col, dictionary, 
				substrings, overwrite, retain_input, remove_match_from_list, expr_write_col_is_empty, progress_bar=progress_bar, progress_bar_desc=progress_bar_desc)
		else:
			for key, value in tqdm(dictionary.items(), desc=progress_bar_desc, ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
				expr_allowed_to_overwrite, expr_filter_exp, expr_found_a_match = self._setup_kv_expressions(polars_df, match_col, write_col, key=key, value=value, 
					substrings=substrings, overwrite=overwrite, remove_match_from_list=remove_match_from_list)
				polars_df = self._kv_match(polars_df, match_col, write_col, key, value, status_cols,
					expr_allowed_to_overwrite, expr_filter_exp, expr_found_a_match, expr_matched_false, expr_write_col_is_empty, expr_written_false)
		return polars_df


	def _kv_match(self, polars_df, match_col: str, write_col: str, key: str, value: str, status_cols: bool,
		allowed_to_overwrite: pl.expr,
		filter_exp: pl.expr,
		found_a_match: pl.expr,
		matched_false, # either bool false or polars expression
		write_col_is_empty: pl.expr,
		written_false, # either bool false or polars expression
		):
		"""
		This function should be called by dictionary_match() because it uses predefined polars expressions and relies on some asserts	
		"""

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
					((allowed_to_overwrite)
						.or_(write_col_is_empty)
					)
					.and_(found_a_match)
				)
				.then(pl.lit(value))
				.otherwise(pl.col(write_col))
				.alias(write_col)
			])

		if filter_exp is not None: # if remove_match_from_list and polars_df.schema[match_col] == pl.List(pl.Utf8)
			polars_df = polars_df.with_columns([
				pl.when(found_a_match)
				.then(pl.col(match_col).list.eval(pl.element().filter(filter_exp)))
				.otherwise(pl.col(match_col))
				.alias(match_col)
			])
		#if self.logging.getEffectiveLevel() == 10:
		#	if status_cols:
		#		print(polars_df.select(['run_id', write_col, 'geoloc_info', 'matched', 'written']))
		#	else:
		#		print(polars_df.select(['run_id', write_col, 'geoloc_info']))
		return polars_df

	def standardize_host_disease(self, polars_df):
		assert 'host_disease' in polars_df.columns

		# exact matches
		if self.cfg.mycobacterial_mode:
			for disease, simplified_disease in host_disease.host_disease_exact_match_mycobacterial.items():
				polars_df = self.kv_match(polars_df, match_col='host_disease', write_col='host_disease', key=disease, value=simplified_disease, substrings=False, overwrite=True)
		for disease, simplified_disease in host_disease.host_disease_exact_match.items():
			polars_df = self.kv_match(polars_df, match_col='host_disease', write_col='host_disease', key=disease, value=simplified_disease, substrings=False, overwrite=True)
		
		# fuzzy matches
		if self.cfg.mycobacterial_mode:
			for disease, simplified_host_disease in host_disease.host_disease_substring_match_mycobacterial.items():
				polars_df = self.kv_match(polars_df, match_col='host_disease', write_col='host_disease', key=disease, value=simplified_disease, substrings=True, overwrite=True)
		for disease, simplified_host_disease in host_disease.host_disease_substring_match.items():
			polars_df = self.kv_match(polars_df, match_col='host_disease', write_col='host_disease', key=disease, value=simplified_disease, substrings=True, overwrite=True)
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
		self.logging.info("Cleaning up dates...")

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

	@staticmethod
	def chunk_dict(data, chunk_size):
		it = iter(data.items())
		for i in range(0, len(data), chunk_size):
			yield dict(islice(it, chunk_size))

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
			polars_df = self.NeighLib.add_column_of_value(polars_df, continent_col, None, if_already_exists='error')
		self.validate_col_country(polars_df, country_col)
		for ISO3166, continent in countries.countries_to_continents.items():
			polars_df = self.kv_match(polars_df, match_col=country_col, write_col=continent_col, key=ISO3166, value=continent, substrings=False, overwrite=overwrite)
		return polars_df
	
	def standardize_countries(self, polars_df, try_rm_geoloc_info=False, progress_bar=TQDM_ENABLE):
		# We expect to be starting out with at least one of the following:
		# * country (type str)
		# * geoloc_info (type list)
		# Outputs:
		# country, region, continent
		# If only country exists, ISO that list. Whatever doesn't get ISO'd gets moved to 'continent' if it matches a continent, otherwise will be moved to 'region' (no overwrite).
		# Region and continent keep type str the entire time.
		# If only geoloc_info exists, go through the list pulling out countries by ISO matching, then continents. Anything remaining move to region.
		# If both exist, ISO convert country column, then do continent/region matching on geoloc_info column.
		timer = time.time()

		# TODO: assert intermediate columns like 'likely_country' not in df
		united_nations = {**countries.substring_match, **countries.exact_match}
		if 'country' in polars_df.columns and 'geoloc_info' in polars_df.columns:
			self.logging.debug("geoloc_info ✔️ country ✔️")
			# This DOES NOT force everything to be ISO standard in country column, since if you have stuff in that column already I assume you want it there

			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', dictionary=countries.substring_match, 
				substrings=True, overwrite=True, status_cols=False, progress_bar=progress_bar, progress_bar_desc="Standardizing countries (substrings)")
			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', dictionary=countries.exact_match, 
				substrings=False, overwrite=True, status_cols=False, progress_bar=progress_bar, progress_bar_desc="Standardizing countries (exact)")
			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', dictionary=countries.countries_to_continents, 
				substrings=False, overwrite=True, status_cols=False, progress_bar=progress_bar, progress_bar_desc="Countries to continents")

			# TODO: why not call validate_col_country(polars_df)?

			# If geoloc_info can become a str 'region' column, and 'region' column doesn't already exist, let's do that
			# ...but that's computationally expensive and we want to parse geoloc_info for continents so actually let's not do this here
			#if try_rm_geoloc_info:
			#	polars_df = self.NeighLib.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=['geoloc_info'])
			#	if polars_df['geoloc_info'].schema == pl.Utf8 and 'region' not in polars_df.columns:
			#		polars_df = polars_df.rename({'geoloc_info': 'region'})

		elif 'country' in polars_df.columns and 'geoloc_info' not in polars_df.columns:
			self.logging.debug("geoloc_info ✖️ country ✔️")
			# This DOES force everything to be ISO standard in country column
			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', dictionary=countries.substring_match, 
				substrings=True, overwrite=True, status_cols=False, remove_match_from_list=True, progress_bar=progress_bar, progress_bar_desc="Standardizing countries (substrings)")
			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='country', dictionary=countries.exact_match, 
				substrings=False, overwrite=True, status_cols=False, remove_match_from_list=True, progress_bar=progress_bar, progress_bar_desc="Standardizing countries (exact)")		
			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', dictionary=countries.countries_to_continents, 
				substrings=False, overwrite=True, status_cols=False, progress_bar=progress_bar, progress_bar_desc="Countries to continents")
			self.validate_col_country(polars_df)
			self.logging.debug("Returning early due to lack of geoloc_info column")
			return polars_df
		
		elif 'geoloc_info' in polars_df.columns and 'country' not in polars_df.columns: # and not 'country'
			self.logging.debug("geoloc_info ✔️ country ✖️")
			# To handle "country: region" metadata without overwriting the region metadata, first we attempt to extract countries by looking for non-substring matches,
			# including the countries.substring_match stuff we usually just substring match upon.
			polars_df = self.dictionary_match(polars_df, match_col='geoloc_info', write_col='country', dictionary=united_nations, 
				substrings=False, overwrite=False, status_cols=False, remove_match_from_list=True, progress_bar=progress_bar, progress_bar_desc="Standardizing countries")
			polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', dictionary=countries.countries_to_continents, 
				substrings=False, overwrite=True, status_cols=False, progress_bar=progress_bar, progress_bar_desc="Countries to continents")
		
		else:
			self.logging.warning("Neither 'country' nor 'geoloc_info' found in dataframe. Cannot standardize.")
			return polars_df

		# Our dataframe now is guranteed to have a country column and a geoloc_info column.
		# (TODO: Ensure the initial geoloc_info ✖️ country ✔️ case results in a geoloc_info column of type list, not str)
		assert polars_df.schema['geoloc_info'] == pl.List(pl.Utf8)
		assert 'continent' in polars_df.columns
		
		# Now let's try to pull continent information from geoloc_info 
		polars_df = self.dictionary_match(polars_df, match_col='geoloc_info', write_col='continent', dictionary=regions.continents, 
			substrings=False, overwrite=False, status_cols=False, remove_match_from_list=True, progress_bar=progress_bar, progress_bar_desc="Continent from geoloc_info")

		# Make sure we don't have junk from hypothetical previous runs, or weird columns
		polars_df = self.NeighLib.add_column_of_value(polars_df, 'likely_country', None, if_already_exists='error')
		polars_df = self.NeighLib.add_column_of_value(polars_df, 'def_country', None, if_already_exists='error')
		assert 'geoloc_info_unhandled' not in polars_df.columns
		assert 'neo_region' not in polars_df.columns
		
		# We can allow a pre-existing region column though
		polars_df = self.NeighLib.add_column_of_value(polars_df, 'region', None, if_already_exists='ignore')

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
		for nation, ISO3166 in tqdm(united_nations.items(), desc="Standardizing regions", ascii='➖🌱🐄', bar_format='{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}', disable=(not progress_bar)):
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
		for nation, ISO3166 in tqdm(countries.substring_match.items(), desc="Finishing up countries", ascii=TQDM_MOO, bar_format=TQDM_FRMT, disable=(not progress_bar)):
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
		polars_df = self.dictionary_match(polars_df, match_col="region", write_col="country", dictionary=regions.regions_to_countries, 
			substrings=False, overwrite=True, progress_bar=progress_bar, progress_bar_desc="Region to country")
		polars_df = self.dictionary_match(polars_df, match_col='region', write_col='country', dictionary=countries.substring_match, 
			substrings=True, overwrite=False, progress_bar=progress_bar, progress_bar_desc="Country substrings")
		polars_df = self.dictionary_match(polars_df, match_col='region', write_col='country', dictionary=countries.exact_match, 
			substrings=False, overwrite=True, progress_bar=progress_bar, progress_bar_desc="Country exact match")

		# partial cleanup of the region column
		polars_df = self.dictionary_match(polars_df, match_col="region", write_col="region", dictionary=regions.regions_to_smaller_regions,
			substrings=True, overwrite=True, progress_bar=progress_bar, progress_bar_desc="Cleanup regions")

		# Any matches for country names in geoloc_name, country, likely_country, and def_country have already been ISO3166'd
		# Let's use that to convert some ISO3166'd countries into continents (this happens after region matching intentionally)
		polars_df = self.dictionary_match(polars_df, match_col='country', write_col='continent', dictionary=countries.countries_to_continents,
			substrings=False, overwrite=True, progress_bar=progress_bar, progress_bar_desc="Countries to continents")

		self.validate_col_country(polars_df)
		self.logging.info(f"Standardized countries in {time.time() - timer:.4f} seconds")
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
			
