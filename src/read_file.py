import pandas as pd
import polars as pl
from contextlib import suppress
import os
from tqdm import tqdm
from src.neigh import NeighLib
from src.dictionaries import columns
from src.dictionaries import null_values
from .config import RancheroConfig

tqdm.pandas()

class FileReader():

	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to FileReader class. Ranchero is designed to be initialized with a configuration.")
		else:
			self._actually_set_config(configuration=configuration)

	def _actually_set_config(self, configuration: RancheroConfig):
		self.cfg = configuration

	def _get_cfg_if_not_overwritten(self, arg, config_arg_name):
		"""Handles "allow overriding config" variables in function calls"""
		if arg is not None:
			return arg
		else:
			return getattr(self.cfg, config_arg_name)

	#####🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️########
	##                         polars functions                                  ##
	#####🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️🐻‍❄️########
	# Functions that OUTPUT a polars dataframe (but may touch pandas at some point)

	def polars_from_ncbi_run_selector(self, csv):
		run_raw = pl.read_csv(csv)
		run_renamed = NeighLib.rancheroize_polars(run_raw)  # for compatibility with other formats
		return run_renamed

	def polars_from_tsv(self, tsv, sep='\t', immediate_try_parse_dates=None, ignore_polars_read_errors=None, null_values=null_values.null_values):
		"""
		Read a TSV (or similar) and convert to Polars dataframe

		Configurations used:
		* ignore_polars_read_errors (can be overwritten)
		* immediate_try_parse_dates (can be overwritten)
		* immediate_biosample_merge (set)
		"""
		immediate_try_parse_dates = self._get_cfg_if_not_overwritten(immediate_try_parse_dates, "immediate_try_parse_dates")
		ignore_polars_read_errors = self._get_cfg_if_not_overwritten(ignore_polars_read_errors, "ignore_polars_read_errors")

		dataframe = pl.read_csv(tsv, separator=sep, try_parse_dates=immediate_try_parse_dates, null_values=null_values, ignore_errors=ignore_polars_read_errors)
		if self.cfg.immediate_biosample_merge:
			dataframe = polars_flatten(dataframe, upon='BioSample', keep_all_columns=False)
		return dataframe

	def polars_from_bigquery(self, bq_file, nullify=True, normalize_attributes=True):
		""" 
		1. Reads a bigquery JSON into a polars dataframe
		2. (optional) Splits the attributes columns into new columns (combines fixing the attributes column and JSON normalizing)
		3. Rancheroize columns
		4. (optional) Merge by BioSample
		Returns a polars dataframe.

		Configurations used:
		* immediate_biosample_merge (set)
		* immediate_rancheroize (set)
		"""
		bq_raw = pl.read_json(bq_file)
		if self.cfg.verbose: print(f"{bq_file} has {bq_raw.width} columns and {len(bq_raw)} rows")
		if self.cfg.immediate_rancheroize:
			if normalize_attributes and "attributes" in bq_raw.columns:  # if column doesn't exist, return false
				bq_norm = self.polars_fix_attributes_and_json_normalize(bq_raw)
				current =  NeighLib.rancheroize_polars(bq_norm)
			else:
				current =  NeighLib.rancheroize_polars(bq_raw)
		else:
			current = bq_raw
		# create nulls here, where we have as many not-list columns as possible (eg, after normalize attributes but before biosample merge)
		# TODO: do other replacememnts here too!
		if nullify: current = current.with_columns(pl.col(pl.Utf8).replace(null_values.null_values_dictionary))
		if self.cfg.immediate_biosample_merge:
			current = self.polars_flatten(current, upon='BioSample', keep_all_columns=False)
		return current


	def polars_json_normalize(self, polars_df, pandas_attributes_series, rancheroize=False, collection_date_sam_workaround=True):
		"""
		polars_df: polars df to concat to at the end
		pandas_attributes_series: pandas series of dictionaries that will json normalized

		We do this seperately so we can avoid converting the entire dataframe in and out of pandas.
		"""
		attributes_rows = pandas_attributes_series.shape[0]
		assert polars_df.shape[0] == attributes_rows, f"Polars dataframe has {polars_df.shape[0]} rows, but the pandas_attributes has {attributes_rows} rows" 
		
		print(f"Normalizing {attributes_rows} rows (this might take a while)...")
		just_attributes = pl.json_normalize(pandas_attributes_series, strict=False, max_level=1, infer_schema_length=100000)  # just_attributes is a polars dataframe
		assert polars_df.shape[0] == just_attributes.shape[0], f"Polars dataframe has {polars_df.shape[0]} rows, but normalized attributes we want to horizontally combine it with has {just_attributes.shape[0]} rows" 

		if self.cfg.verbose: print("Concatenating to the original dataframe...")
		if collection_date_sam_workaround:
			# polars_df already has a collection_date_sam which it converted to YYYY-MM-DD format. to avoid a merge conflict and to
			# fall back on the attributes version (which perserves dates that failed to YYYY-MM-DD convert), drop collection_date_sam
			# from polars_df before merging.
			bq_jnorm = pl.concat([polars_df.drop(['attributes', 'collection_date_sam']), just_attributes], how="horizontal")
		else:
			bq_jnorm = pl.concat([polars_df.drop('attributes'), just_attributes], how="horizontal")
		print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
		if self.cfg.verbose: print(f"Columns added: {just_attributes.columns}")
		with suppress(pl.exceptions.SchemaFieldNotFoundError): bq_jnorm.rename(columns.common_col_to_ranchero_col)
		if self.cfg.intermediate_files: NeighLib.polars_to_tsv(bq_jnorm, f'./intermediate/normalized_pure_polars.tsv')
		return bq_jnorm

	def polars_run_to_sample(self, polars_df):
		"""Public wrapper for polars_flatten()"""
		if 'sample_index' not in polars_df.columns:
			print("Could not find a sample-based column to make as the index!")
			exit(1)
		return self.polars_flatten(polars_df, upon='sample_index')

	def polars_flatten(self, polars_df, upon='BioSample', keep_all_columns=False):
		"""
		Flattens an input file using polars group_by().agg(). This is designed to essentially turn run accession indexed dataframes
		into BioSample-indexed dataframes. Assumes columns are already rancheroized.
		"""
		print(f"Flattening {upon}s...")

		not_flat = polars_df
		if self.cfg.verbose:
			non_uniques = not_flat.group_by(upon).count().filter(pl.col("count") > 1)[upon]
			number_non_unique = len(non_uniques)
			print(f"Found {number_non_unique} non-unique values for {upon}: {non_uniques}")
		
		if keep_all_columns:
			# not tested!
			columns_to_keep = list(not_flat.columns)
			with suppress(ValueError): columns_to_keep.remove(upon)  # if it's not in there, who cares?
			
			flat = not_flat.group_by(upon).agg(columns_to_keep)
			#for nested_column in not_flat.columns:
			#	flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})
			#	flat = flat_neo  # silly workaround for flat = flat.with_columns(...).rename(...) throwing an error about duped columns
			flat_neo = flat
		else:
			columns_to_keep = NeighLib.get_ranchero_column_dictionary_only_valid(not_flat)
			with suppress(ValueError): del columns_to_keep[upon]  # if it's not in there, who cares?
			flat = not_flat.group_by(upon).agg(pl.col(columns_to_keep))
			#if self.cfg.verbose: NeighLib.super_print_pl(flat, "flat") # this breaks on tba5
			
			#for nested_column in columns.recommended_sra_columns:
			#	if nested_column not in columns_in_df:
			#		continue
			#	else:
			#		if verbose: print(f"Nested column {nested_column} present")
			#		print(flat.with_columns(pl.col(nested_column).list.to_struct()))
			#		flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})

		#flat_neo = flat_neo.unique() # doesn't seem to drop anything but may as well leave it
		#if intermediate_files: NeighLib.polars_to_tsv(flat_neo, f"./intermediate/polars_flattened.tsv")
		return flat

	def polars_fix_attributes_and_json_normalize(self, polars_df, rancheroize=False, keep_all_primary_search=True):
		"""
		Uses NeighLib.concat_dicts to turn the weird format of the attributes column into flat dictionaries,
		then do some JSON normalization to output a polars dataframe.

		1. Create a tempoary pandas dataframe
		2. .apply(NeighLib.concat_dicts) to the attributes column in the pandas df
		3. Run polars_json_normalize to add new columns to the polars dataframe
		4. If rancheroize: rename columns
		5. Polars will default to str type for new columns; if cast_types, cast the most common not-string folders to
		   more correct types (example: tax_id --> pl.Int32). Note that this will also run on existing columns that
		   were not added by polars_json_normalize()!
		6. if intermediate_files: Write to ./intermediate/flatdicts.tsv
		7. Return polars dataframe.

		Performance is very good on my largest datasets, but I'm interested in avoiding the panadas conversion if possible.

		Configurations used:
		* cast_types (set)
		* intermediate_files (set)
		* verbose (set)
		"""
		temp_pandas_df = polars_df.to_pandas()  # TODO: probably faster to just convert the attributes column
		if keep_all_primary_search:  # TODO: benchmark these two options
			if self.cfg.verbose:
				print("Concatenating dictionaries with Pandas...")
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(NeighLib.concat_dicts_with_shared_keys)
			else:
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(NeighLib.concat_dicts_with_shared_keys)
		else:
			if self.cfg.verbose:
				print("Concatenating dictionaries with Pandas...")
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(NeighLib.concat_dicts)
			else:
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(NeighLib.concat_dicts)
		normalized = self.polars_json_normalize(polars_df, temp_pandas_df['attributes'])
		if rancheroize: normalized.rename(columns.common_col_to_ranchero_col)
		if self.cfg.cast_types: normalized = NeighLib.cast_politely(normalized)
		if self.cfg.intermediate_files: NeighLib.polars_to_tsv(normalized, f'./intermediate/flatdicts.tsv')
		return normalized


	#####🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼########
	##                          pandas functions                                 ##
	#####🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼🐼########
	# Functions that OUTPUT a pandas dataframe (but may touch polars at some point)
	#
	# Generally speaking, it's recommended to use polars instead.

	def pandas_from_ncbi_run_selector(csv):
		run_raw = pd.read_csv(csv)
		run_renamed = run_raw.rename(columns=columns.ncbi_run_selector_col_to_ranchero_col)  # for compatibility with other formats
		return run_renamed

	def pandas_json_normalize(self, pandas_df, use_polars=True, rancheroize=False):
		"""
		JSON-normalize the "attributes" column into new columns. use_polars is faster but might break things.
		Regardless of use_polars, input and output are pandas dataframes. Assumes pandas_fix_attributes_dictionaries() was run but
		that shouldn't be necessary.

		Config used:
		* intermediate_files (set)
		* verbose (set)
		"""
		outfile = './intermediate/normalized.tsv'
		print("Normalizing...")

		if use_polars:
			# polars-and-pandas version
			# Even if you set this up to not read the intermedite file to set pandas_df, you'll get less columns in the end?
			# However, this does seem to correctly pull out "bytes" and other JSON data, so we'll take it.
			just_attributes = pl.json_normalize(pandas_df['attributes'], strict=False)  # just_attributes is a polars dataframe
			# collection_date_sam is likely present in both dataframes, so rename one of them
			just_attributes = just_attributes.rename({'collection_date_sam': 'collection_date_from_attributes'})
			dupe_columns = NeighLib.get_dupe_columns_of_two_polars(just_attributes, pl.from_pandas(pandas_df))
			if len(dupe_columns) > 0:
				# TODO: just rename the columns like we did with collection_date_sam!!
				raise AssertionError("Found columns from the attributes section that conflict with an existing column")
			bq_jnorm = pd.concat([pandas_df.drop(columns=['attributes']), just_attributes.to_pandas()], axis=1)
			if self.cfg.verbose: print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
			if rancheroize: bq_jnorm.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)
			if self.cfg.intermediate_files:
				NeighLib.pandas_to_tsv(bq_jnorm, outfile)
				print(f"Wrote JSON-normalized dataframe to {outfile}")
		else:
			# pure pandas version
			# This is the slowest, but it acceptable enough. 
			just_attributes = pd.json_normalize(pandas_df['attributes'])  # just_attributes is a Python dictionary
			# collection_date_sam is likely present in both dataframes, so rename one of them
			just_attributes['collection_date_from_attributes'] = just_attributes.pop('collection_date_sam')
			bq_jnorm = pd.concat([pandas_df.drop(columns=['attributes']), just_attributes], axis=1)
			if self.cfg.verbose: print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
			if rancheroize: bq_jnorm.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)
			if self.cfg.intermediate_files: 
				NeighLib.pandas_to_tsv(bq_jnorm, outfile)
				print(f"Wrote JSON-normalized dataframe to {outfile}")
		
		return bq_jnorm

	def pandas_from_bigquery(self, bq_file, fix_attributes=True, normalize_attributes=True, polars_normalize=None):
		"""
		1) Read bigquery JSON
		2) if fix_attributes or normalize_attributes: Turns "attributes" column's lists of one-element k/v pairs into dictionaries
		3) if normalize_attributes: JSON-normalize the "attributes" column into their own columns
		4) if merge_into_biosamples: Merge run accessions with the same BioSample, under the assumption that they've the same metadata

		Notes:
		* normalize_attributes and fix_attributes work on the "attributes" column, not the "j_attr" column
		* if polars_normalize, use the under development polars version of json normalize -- this is faster but could be unstable

		Configurations used:
		* immediate_biosample_merge (set)
		* intermediate_files (set)
		* polars_normalize (can be overwritten)
		* verbose (set)


		TODO: intermediate_files() not used?
		"""
		polars_normalize = _get_cfg_if_not_overwritten(polars_normalize)

		bq_raw = pd.read_json(bq_file)
		if self.cfg.verbose: print(f"{bq_file} has {len(bq_raw.columns)} columns and {len(bq_raw.index)} rows")

		if fix_attributes or normalize_attributes:
			bq_fixed = self.pandas_fix_attributes_dictionaries(bq_raw)
			if self.cfg.verbose: print(f"{bq_file} has {len(bq_fixed.columns)} columns and {len(bq_fixed.index)} rows")
		if normalize_attributes:  # requires pandas_fix_attributes_dictionaries() to happen first
			bq_norm = self.pandas_json_normalize(bq_fixed, use_polars=polars_normalize)
			NeighLib.assert_unique_columns(bq_norm)
			if self.cfg.verbose: print(f"{bq_file} has {len(bq_norm.columns)} columns and {len(bq_norm.index)} rows")
		if cfg.immediate_biosample_merge:
			bq_to_merge = bq_norm if bq_norm is not None else (bq_fixed if bq_fixed is not None else bq_raw)
			bq_jnorm = (self.polars_flatten(pl.from_pandas(bq_to_merge), upon='BioSample', keep_all_columns=False, rancheroize=True)).to_pandas()
		return bq_jnorm if bq_jnorm is not None else bq_flatdicts  # bq_flatdircts if not normalize_attributes

	def pandas_fix_attributes_dictionaries(self, pandas_df, rancheroize=False):
		"""
		Uses NeighLib.concat_dicts to turn the weird format of the attributes column into flat dictionaries

		Configurations used:
		* intermediate_files (set)
		* verbose (set)
		"""
		if self.cfg.verbose:
			print("Concatenating dictionaries...")
			pandas_df['attributes'] = pandas_df['attributes'].progress_apply(NeighLib.concat_dicts)
		else:
			pandas_df['attributes'] = pandas_df['attributes'].apply(NeighLib.concat_dicts)
		pandas_flatdic, pandas_df = pandas_df.copy(), None  # supposedly increases effeciency
		if rancheroize: pandas_flatdic.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)
		if self.cfg.intermediate_files: NeighLib.pandas_to_tsv(pandas_flatdic, f'./intermediate/flatdicts.tsv')
		return pandas_flatdic
	
	def pandas_from_tsv(tsv):
		return pd.read_csv(tsv, sep='\t')

