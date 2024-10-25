import pandas as pd
import polars as pl
from contextlib import suppress
import os
from tqdm import tqdm
from src.statics import kolumns, null_values
from .config import RancheroConfig
from . import _NeighLib as NeighLib
tqdm.pandas()

class FileReader():

	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to FileReader class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger

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
			dataframe = run_to_sample_index(dataframe, upon='BioSample', keep_all_columns=False)
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
		self.logging.warning(f"Reformatted JSON saved to {out_file_path}")
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
			if self.logging.getEffectiveLevel() == 10: self.logging.debug(f"{bq_file} has {bq_raw.width} columns and {len(bq_raw)} rows")
		except pl.exceptions.ComputeError:
			self.logging.warning("Caught exception reading JSON file. Attempting to reformat it...")
			try:
				bq_raw = pl.read_json(self.fix_bigquery_file(bq_file))
				if self.logging.getEffectiveLevel() == 10: self.logging.debug(f"Fixed input file has {bq_raw.width} columns and {len(bq_raw)} rows")
			except pl.exceptions.ComputeError:
				self.logging.error("Caught exception reading JSON file after attempting to fix it. Giving up!")
				exit(1)
		if normalize_attributes and "attributes" in bq_raw.columns:  # if column doesn't exist, return false
			current = self.polars_fix_attributes_and_json_normalize(bq_raw)
		if self.cfg.immediate_biosample_merge:
			current = self.run_to_sample_index(current, upon='BioSample', keep_all_columns=False)
		return current


	def polars_json_normalize(self, polars_df, pandas_attributes_series, rancheroize=False, collection_date_sam_workaround=True):
		"""
		polars_df: polars df to concat to at the end
		pandas_attributes_series: pandas series of dictionaries that will json normalized

		We do this seperately so we can avoid converting the entire dataframe in and out of pandas.
		"""
		attributes_rows = pandas_attributes_series.shape[0]
		assert polars_df.shape[0] == attributes_rows, f"Polars dataframe has {polars_df.shape[0]} rows, but the pandas_attributes has {attributes_rows} rows" 
		
		self.logging.info(f"Normalizing {attributes_rows} rows (this might take a while)...")
		just_attributes = pl.json_normalize(pandas_attributes_series, strict=False, max_level=1, infer_schema_length=100000)  # just_attributes is a polars dataframe
		assert polars_df.shape[0] == just_attributes.shape[0], f"Polars dataframe has {polars_df.shape[0]} rows, but normalized attributes we want to horizontally combine it with has {just_attributes.shape[0]} rows" 

		if self.logging.getEffectiveLevel() == 10: self.logging.info("Concatenating to the original dataframe...")
		if collection_date_sam_workaround:
			# polars_df already has a collection_date_sam which it converted to YYYY-MM-DD format. to avoid a merge conflict and to
			# fall back on the attributes version (which perserves dates that failed to YYYY-MM-DD convert), drop collection_date_sam
			# from polars_df before merging.
			bq_jnorm = pl.concat([polars_df.drop(['attributes', 'collection_date_sam']), just_attributes], how="horizontal")
		else:
			bq_jnorm = pl.concat([polars_df.drop('attributes'), just_attributes], how="horizontal")
		self.logging.info(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
		if self.logging.getEffectiveLevel() == 10: self.logging.debug(f"Columns added: {just_attributes.columns}")
		if rancheroize: bq_jnorm = NeighLib.rancheroize_polars(bq_jnorm)
		if self.cfg.intermediate_files: NeighLib.polars_to_tsv(bq_jnorm, f'./intermediate/normalized_pure_polars.tsv')
		return bq_jnorm

	def polars_run_to_sample(self, polars_df):
		"""Public wrapper for run_to_sample_index()"""
		if 'sample_index' not in polars_df.columns:
			self.logging.error("Could not find a sample-based column to make as the index!")
			exit(1)
		return self.run_to_sample_index(polars_df, upon='sample_index')

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

	def run_to_sample_index(self, polars_df, upon='sample_index', try_to_be_clever=True):
		"""
		Flattens an input file using polar. This is designed to essentially turn run accession indexed dataframes
		into BioSample-indexed dataframes. This will typically create columns of type list.
		REQUIRES run index to be called "run_index" and sample index to be called "sample_index" exactly.

		run_index | sample_index | foo
		-------------------------------
		SRR123    | SAMN1        | bar
		SRR124    | SAMN1        | buzz
		SRR125    | SAMN2        | bizz
		SRR126    | SAMN3        | bar
					 ⬇️
		run_index       | sample_index | foo
		---------------------------------------
		[SRR123,SRR124] | SAMN1        | [bar, buzz]
		[SRR125]        | SAMN2        | [bizz]
		[SRR126]        | SAMN3        | [bar]
		"""
		assert 'run_index' in polars_df.columns
		assert 'sample_index' in polars_df.columns
		self.logging.debug(f"Flattening {upon}s...")

		polars_df = NeighLib.rancheroize_polars(polars_df)
		NeighLib.print_col_where(polars_df, upon, "SAMN41453963")
		other_columns = [col for col in polars_df.columns if col not in ["run_index", "sample_index"]]

		# try to reduce the number of lists being concatenated -- this does mean running group_by() twice
		if try_to_be_clever:
			listbusters, listmakers, listexisters = [], [], [col for col, dtype in polars_df.schema.items() if (isinstance(dtype, pl.List) and dtype.inner == pl.Utf8)]
			temp_no_stringlist_df = polars_df.select([
				pl.col(col) for col, dtype in polars_df.schema.items() 
				if not (isinstance(dtype, pl.List) and dtype.inner == pl.Utf8) # for some reason n_unique works on lists of integers
			])
			#assert not set(temp_no_stringlist_df.columns) & set(listexisters)
			
			df_agg_nunique = temp_no_stringlist_df.group_by("sample_index").n_unique()
			for other_column in other_columns:
				if polars_df.schema[other_column] is not pl.List and other_column in df_agg_nunique.columns:
					if ((df_agg_nunique.select(pl.col(other_column) == 1).to_series()).all()):
						listbusters.append(other_column)
					else:
						listmakers.append(other_column)
			self.logging.debug(f"listbusters: {listbusters}")
			self.logging.debug(f"listmakers: {listmakers}")
			self.logging.debug(f"listexisters: {listexisters}")

			for already_list_col in listexisters:
				self.logging.debug(f"Listexister {already_list_col}:")
				self.logging.debug(polars_df.filter(pl.col(already_list_col).list.len() > 1).select([already_list_col, 'run_index']).head(15))

			grouped_df = (
				polars_df
				.group_by('sample_index')
				.agg([
					pl.concat_list("run_index").alias("run_index"),
					*[
						(pl.first(col).alias(col) if col in listbusters else pl.concat_list(col).alias(col))
						for col in other_columns
					]
				])
			)

		else:
			grouped_df = (
				polars_df
				.group_by('sample_index')
				.agg([
					pl.concat_list("run_index").alias("run_index"),
					*[pl.concat_list(col).alias(col) for col in other_columns]
				])
			)
		NeighLib.print_col_where(grouped_df, upon, "SAMN41453963")
		flat = NeighLib.flatten_nested_list_cols(grouped_df)
		NeighLib.print_col_where(flat, upon, "SAMN41453963")
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
			if self.logging.getEffectiveLevel() == 10:
				print("Concatenating dictionaries with Pandas...")
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(NeighLib.concat_dicts_with_shared_keys)
			else:
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(NeighLib.concat_dicts_with_shared_keys)
		else:
			if self.logging.getEffectiveLevel() == 10:
				print("Concatenating dictionaries with Pandas...")
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(NeighLib.concat_dicts)
			else:
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(NeighLib.concat_dicts)
		normalized = self.polars_json_normalize(polars_df, temp_pandas_df['attributes'])
		if rancheroize: normalized = NeighLib.rancheroize_polars(normalized)
		if self.cfg.cast_types: normalized = NeighLib.cast_politely(normalized)
		if self.cfg.intermediate_files: NeighLib.polars_to_tsv(normalized, f'./intermediate/flatdicts.tsv')
		return normalized
