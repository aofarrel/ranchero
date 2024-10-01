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

	def fix_bigquery_file(self, bq_file):
		out_file_path = f"{bq_file}_modified.json"
		with open(bq_file, 'r') as in_file:
			lines = in_file.readlines()
		with open(out_file_path, 'w') as out_file:
			out_file.write("[\n")
			for i, line in enumerate(lines):
				if i < len(lines) - 1:
					out_file.write(line.strip() + ",\n")
				else:
					out_file.write(line.strip() + "\n")
			out_file.write("]\n")
		print(f"Reformatted JSON saved to {out_file_path}")
		return out_file_path

	def polars_from_bigquery(self, bq_file, normalize_attributes=True):
		""" 
		1. Reads a bigquery JSON into a polars dataframe
		2. (optional) Splits the attributes columns into new columns (combines fixing the attributes column and JSON normalizing)
		3. Rancheroize columns
		4. (optional) Merge by BioSample
		Returns a polars dataframe.

		Configurations used:
		* immediate_biosample_merge (set)
		"""
		try:
			bq_raw = pl.read_json(bq_file)
			if self.cfg.verbose: print(f"{bq_file} has {bq_raw.width} columns and {len(bq_raw)} rows")
		except pl.exceptions.ComputeError:
			print("Caught exception reading JSON file. Attempting to reformat it...")
			try:
				bq_raw = pl.read_json(self.fix_bigquery_file(bq_file))
				if self.cfg.verbose: print(f"Fixed input file has {bq_raw.width} columns and {len(bq_raw)} rows")
			except pl.exceptions.ComputeError:
				print("Caught exception reading JSON file after attempting to fix it. Giving up!")
				exit(1)

		NeighLib.print_col_where(bq_raw, 'acc', 'ERR1768638') # disappears

		if normalize_attributes and "attributes" in bq_raw.columns:  # if column doesn't exist, return false
			current = self.polars_fix_attributes_and_json_normalize(bq_raw)


			NeighLib.print_col_where(current, 'acc', 'ERR1768638') # disappears


			if self.cfg.verbose: print(current.columns)
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
		if rancheroize: bq_jnorm = NeighLib.rancheroize_polars(bq_jnorm)
		if self.cfg.intermediate_files: NeighLib.polars_to_tsv(bq_jnorm, f'./intermediate/normalized_pure_polars.tsv')
		return bq_jnorm

	def polars_run_to_sample(self, polars_df):
		"""Public wrapper for polars_flatten()"""
		if 'sample_index' not in polars_df.columns:
			print("Could not find a sample-based column to make as the index!")
			exit(1)
		return self.polars_flatten(polars_df, upon='sample_index')

	def get_not_unique_in_col(self, polars_df, column):
		return polars_df.filter(pl.col(column).is_duplicated())
		# polars_df.filter(pl.col(column).is_duplicated()).select(column).unique()

	def merge_row_duplicates(self, polars_df, column):
		'''SRR1196512, 4.8, null + SRR1196512, 4.8, South Africa --> SRR1196512, 4.8, South Africa'''
		polars_df = polars_df.sort(column)
		polars_df = polars_df.group_by(column).agg(
			[pl.col(col).forward_fill().last().alias(col) for col in polars_df.columns if col != column]
		)
		return polars_df

	def polars_explode_delimited_rows_recklessly(self, polars_df, column="run_index", delimter=";", drop_new_non_unique=True):
		"""
		column 			some_other_column		
		"SRR123;SRR124"	SchemaFieldNotFoundError
		"SRR125" 		TapeError

		becomes

		column 			some_other_column		
		"SRR123"		SchemaFieldNotFoundError
		"SRR124"		SchemaFieldNotFoundError
		"SRR125" 		TapeError
		"""
		exploded = (polars_df.with_columns(pl.col(column).str.split(delimter)).explode(column)).unique()
		if len(polars_df) == len(polars_df.select(column).unique()):
			if len(exploded) != len(exploded.select(column).unique()):
				print(f"Exploding created non-unique values for the previously unique-only column {column}, so we'll be merging...")
				exploded = self.merge_row_duplicates(exploded, column)
				if len(exploded) != len(exploded.select(column).unique()): # probably should never happen
					print("Attempted to merge duplicates caused by exploding, but it didn't work.")
					print(exploded)
					print(len(exploded.select(column).unique()))
					print(len(exploded))
					print(self.get_not_unique_in_col(exploded, column))
					print(len(exploded.select(column).unique()))
					print(len(exploded))
					exit(1)
		else:
			# there aren't unique values to begin with so who cares lol
			pass
		return exploded



	def polars_flatten(self, polars_df, upon='sample_index', keep_all_columns=False):
		"""
		Flattens an input file using polars group_by().agg(). This is designed to essentially turn run accession indexed dataframes
		into BioSample-indexed dataframes. Assumes columns are already rancheroized.
		"""
		print(f"Flattening {upon}s...")

		NeighLib.print_col_where(polars_df, upon, "SAMN41453963")

		not_flat = polars_df
		if self.cfg.verbose:
			non_uniques = not_flat.group_by(upon).count().filter(pl.col("count") > 1)[upon]
			number_non_unique = len(non_uniques)
			print(f"Found {number_non_unique} non-unique values for {upon}: {non_uniques}")

		more_flat = NeighLib.rancheroize_polars(not_flat)
		
		


#		if keep_all_columns:
#			# not tested!
#			columns_to_keep = list(not_flat.columns)
#			with suppress(ValueError): columns_to_keep.remove(upon)  # if it's not in there, who cares?
#			
#			flat = not_flat.group_by(upon).agg(columns_to_keep)
#			for nested_column in not_flat.columns:
#				flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})
#				flat = flat_neo  # silly workaround for flat = flat.with_columns(...).rename(...) throwing an error about duped columns
#			flat_neo = flat
#		else:
#			columns_to_keep = list(item for item in columns.columns_to_keep if item in not_flat.columns)
#			with suppress(ValueError):  columns_to_keep.remove(upon)  # if it's not in there, who cares?
#			flat = not_flat.group_by(upon).agg(pl.col(columns_to_keep))
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
		NeighLib.print_col_where(more_flat, upon, "SAMN41453963")
		return more_flat

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
		if rancheroize: normalized = NeighLib.rancheroize_polars(normalized)
		if self.cfg.cast_types: normalized = NeighLib.cast_politely(normalized)
		if self.cfg.intermediate_files: NeighLib.polars_to_tsv(normalized, f'./intermediate/flatdicts.tsv')
		return normalized
