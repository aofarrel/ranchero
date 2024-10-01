# general purpose functions

import polars as pl
import datetime
from src.dictionaries import columns, drop_zone, null_values
from polars.testing import assert_series_equal
from .config import RancheroConfig

class NeighLib:

	def __init__(cls, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			cls._actually_set_config(configuration=configuration)

	def _actually_set_config(cls, configuration: RancheroConfig):
		cls.cfg = configuration

	@classmethod
	def nullify(cls, polars_df):
		return polars_df.with_columns(pl.col(pl.Utf8).replace(null_values.null_values_dictionary))

	def print_col_where(polars_df, column="source", equals="Coscolla", cols_of_interest=['acc', 'run_index', 'source', 'literature_lineage', 'Biosample', 'sample_index', 'date_collected', 'Collection_Date', 'collection_date_sam', 'sample_collection_date_sam_s_dpl127', 'collection_date_orig_sam', 'collection_date_run', 'date_coll', 'date', 'colection_date_sam', 'collection', 'collection_right', 'concat_list']):
		cols_to_print = [thingy for thingy in cols_of_interest if thingy in polars_df.columns]
		with pl.Config(tbl_cols=-1):
			print(polars_df.filter(pl.col(column) == equals).select(cols_to_print))

	def mark_rows_with_value(polars_df, filter_func, true_value="M. avium complex", false_value='', new_column="bacterial_family", **kwargs):
		#polars_df = polars_df.with_columns(pl.lit("").alias(new_column))
		polars_df = polars_df.with_columns(
			pl.when(pl.col('organism').str.contains_any("Mycobacterium avium"))  # contains should return boolean
			.then(pl.lit(true_value))  # Set true_value where condition is True
			.otherwise(pl.lit(false_value))  # Set false_value otherwise
			.alias(new_column)  # Alias as the new column
		)
		#polars_df.with_columns(pl.when(pl.col("organism").str.contains_any(["Mycobacterium avium", "lalala"])).then(pl.lit(true_value)).alias(new_column))
		print(polars_df.select(pl.col(new_column).value_counts()))

		polars_df = polars_df.with_columns(
			pl.when(pl.col('organism').str.contains("Mycobacterium"))
			.then(pl.lit(true_value))
			.otherwise(pl.lit(false_value))
			.alias(new_column)
		)

		#polars_df = polars_df.with_columns(
		#	pl.when(filter_func(polars_df, **kwargs))
		#	.then(pl.lit(true_value))
		#	.otherwise(pl.lit(false_value))
		#	.alias(new_column)
		#)
		print(polars_df.select(pl.col(new_column).value_counts()))

	def likely_is_run_indexed(polars_df):
		# TODO: make more robust
		singular_runs = (
			("run_index" in polars_df.schema and polars_df.schema["run_index"] == pl.String) or
			("run_accession" in polars_df.schema and polars_df.schema["run_accession"] == pl.String) or
			("acc" in polars_df.schema and polars_df.schema["acc"] == pl.String)
		)
		if singular_runs:
			return True
		else:
			return False

	def print_value_counts(polars_df, skip_ids=True):
		ids = ['sample_index', 'biosample', 'BioSample', 'acc', 'acc_1', 'run_index', 'run_accession'] + columns.addl_ids
		for column in polars_df.columns:
			if skip_ids and column not in ids:
				counts = polars_df.select([pl.col(column).value_counts(sort=True)])
				print(counts)
			else:
				continue

	def get_valid_columns_list_from_arbitrary_list(polars_df, desired_columns: list):
		return [col for col in desired_columns if col in polars_df.columns]

	def check_columns_exist(polars_df, column_list: list, err=False, verbose=False):
		missing_columns = [col for col in column_list if col not in polars_df.columns]
		if len(missing_columns) == 0:
			#if cls.cfg.verbose: print("All requested columns exist in dataframe")
			return True
		else:
			#if cls.cfg.verbose: print(f"Missing these columns: {missing_columns}")
			if err: exit(1)
			return False

	def concat_dicts_with_shared_keys(dict_list: list):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}

		This version is aware of primary_search showing up multiple times and will
		keep all values for primary_search.
		"""
		combined_dict = {}
		primary_search = set()
		for d in dict_list:
			if 'k' in d and 'v' in d:
				if d['k'] == 'primary_search':
					primary_search.add(d['v'])
				else:
					combined_dict[d['k']] = d['v']
		combined_dict.update({"primary_search": list(primary_search)}) # convert to a list to avoid the polars column becoming type object
		return combined_dict

	def concat_dicts_risky(dict_list: list):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}

		This version assumes 'k' and 'v' are in the dictionaries and will error otherwise,
		and doesn't support shared keys (eg, it will pick a primary_serach value at random)
		"""
		combined_dict = {}
		for d in dict_list:
			if 'k' in d and 'v' in d:
				combined_dict[d['k']] = d['v']
		return combined_dict
	
	def concat_dicts(dict_list: list):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}
		"""
		combined_dict = {}
		for d in dict_list:
			if 'k' in d and 'v' in d:
				combined_dict[d['k']] = d['v']
		return combined_dict
	
	@staticmethod
	def super_print_pl(polars_df, header):
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")
		with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=500, fmt_table_cell_list_len=10):
			print(polars_df)

	@classmethod
	def nullfill_and_merge_these_columns(cls, polars_df, particular_columns: list, final_name: str):
		# THIS IGNORES ANY DISAGREEMENT AMONG COLUMNS!
		for i in range(len(particular_columns) - 1):
			col_A, col_B = particular_columns[i], particular_columns[i + 1]
			if polars_df.get_column(col_A).dtype == pl.List:
				polars_df = cls.stringify_one_list_column(polars_df, col_A)
			if polars_df.get_column(col_B).dtype == pl.List:
				polars_df = cls.stringify_one_list_column(polars_df, col_B)
			
			polars_df = polars_df.with_columns(pl.col(f"{col_B}").fill_null(pl.col(f"{col_A}")).alias(col_B))
			print(f"[{i}] filled {col_B} with {col_A}, dropping {col_A}")
			
			are_equal_now = polars_df.select(f"{col_A}").equals(polars_df.select(f"{col_A}"), null_equal=True)
			if any(particular_columns) in columns.rancheroize__warn_if_list_with_unique_values and not are_equal_now:
				print(f"ERROR: {col_A} and {col_B} had different values.")
				exit(1)
			polars_df = polars_df.drop(col_A)
			if i == (len(particular_columns) - 2):
				#print(f"Renaming {col_B} to {final_name}")
				polars_df = polars_df.rename({col_B: final_name})
			
		return polars_df
	
	@classmethod
	def rancheroize_polars(cls, polars_df):
		polars_df = cls.drop_known_unwanted_columns(polars_df)
		polars_df = cls.nullify(polars_df)

		for key, value in columns.equivalence.items():
			merge_these_columns = [v_col for v_col in value if v_col in polars_df.columns]
			if len(merge_these_columns) > 0:
				#print(f"Discovered {key} in column via {merge_these_columns}")
				if len(merge_these_columns) > 1:
					#polars_df = polars_df.with_columns(pl.implode(merge_these_columns)) # this gets sigkilled; don't bother!
					if key in columns.rts__drop:
						polars_df = polars_df.drop(col)
					if key in columns.rts__keep_as_list:
						polars_df = cls.nullfill_and_merge_these_columns(polars_df, merge_these_columns, key)
					else:
						polars_df = cls.nullfill_and_merge_these_columns(polars_df, merge_these_columns, key)
				else:
					#print(f"Renamed {merge_these_columns[0]} to {key}")
					polars_df = polars_df.rename({merge_these_columns[0]: key})
			
		return polars_df

	@classmethod
	def flatten_all_list_cols_as_much_as_possible(cls, polars_df, hard_stop=False, force_strings=False):
		"""
		Flatten list columns as much as possible. If a column is just a bunch of one-element lists, for
		instance, then just take the 0th value of that list and make a column that isn't a list.

		If force_strings, any remaining columns that are still lists are forced into strings.
		"""
		# unnest nested lists (recursive)
		polars_df = cls.flatten_nested_list_cols(polars_df)

		# flatten lists of only one value
		for col, datatype in polars_df.schema.items():
			#if cls.cfg.verbose: print(f"Flattening {col} with type {datatype}...")
			if datatype == pl.List and datatype.inner != datetime.datetime:
				n_rows_prior = polars_df.shape[0]
				exploded = polars_df.explode(col)
				temp_df = exploded.unique()
				n_rows_now = temp_df.shape[0]
				if n_rows_now > n_rows_prior:

					non_unique_rows = exploded.filter(pl.col("sample_index").is_duplicated()).select(["sample_index", col])
					print(f"DEBUG: Non-unique values in column {col}:")
					print(non_unique_rows)
					print(f"DEBUG: Number of non-unique rows in {col}: {non_unique_rows.shape[0]}")



					if col in columns.rts__list_to_float_via_sum:  # ignores temp_df
						polars_df = polars_df.with_columns(pl.col(col).list.sum().alias(f"{col}_sum"))
						polars_df = polars_df.drop(col)
					elif col in columns.rts__keep_as_set:  # because we exploded with unique(), we now have a set (sort of), but I think this is better than trying to do a column merge
						polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
					elif col in columns.rts__warn_if_list_with_unique_values:
						print(f"WARNING: Expected {col} to only have one non-null per sample, but that's not the case. Will keep as a set.")
						#####
						polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
						if hard_stop:
							exit(1)
						else:
							continue
					else:
						print(f"WARNING: Not sure how to handle {col}. Will treat it as a set.")
						#####
						polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
				else:
					polars_df = temp_df
			elif datatype == pl.List and datatype.inner == datetime.date:
				print(f"WARNING: {col} is a list of datetimes. Datetimes break typical handling of lists, so this column will be left alone.")
		if force_strings:
			polars_df = cls.stringify_all_list_columns(polars_df)
		return polars_df

	@classmethod
	def drop_non_tb_columns(cls, polars_df):
		dont_drop_these = [col for col in polars_df.columns if col not in drop_zone.clearly_not_tuberculosis]
		print(dont_drop_these)
		return polars_df.select(dont_drop_these)

	@classmethod
	def drop_known_unwanted_columns(cls, polars_df):
		return polars_df.select([col for col in polars_df.columns if col not in drop_zone.silly_columns])

	@staticmethod
	def flatten_nested_list_cols(polars_df):
		"""Flatten nested list columns"""

		# This version seems to breaking the schema:
		#polars_df = polars_df.with_columns(
		#	[pl.col(x).list.eval(pl.lit("'") + pl.element() + pl.lit('"')).list.join(",").alias(x) for x, y in polars_df.schema.items() if isinstance(y, pl.List) and isinstance(y.inner, pl.List)]
		#)

		nested_lists = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.List)]
		for col in nested_lists:
			#new_col = pl.col(col).list.eval(pl.element().cast(pl.Utf8).map_elements(lambda s: f"'{s}'")).alias(f"{col}_flattened")
			#new_col = pl.col(col).list.eval(pl.element().cast(pl.Utf8).map_elements(lambda s: f"'{s}'", return_dtype=str)).list.join(",").alias(f"{col}_flattened")
			polars_df = polars_df.with_columns(pl.col(col).list.eval(pl.element().flatten().drop_nulls()))
		
		# this recursion should, in theory, handle list(list(list(str))) -- but it's not well tested
		remaining_nests = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.List)]
		if len(remaining_nests) != 0:
			polars_df = flatten_nested_list(polars_df)
		return(polars_df)

	@staticmethod
	def stringify_one_list_column(polars_df, column):
		for col, datatype in polars_df.schema.items():
			if col==column and datatype == pl.List(pl.String):
				polars_df = polars_df.with_columns(
					pl.when(pl.col(col).list.len() <= 1) # don't add brackets if longest list is 1 or 0 elements
					.then(pl.col(col).list.eval(pl.element()).list.join(""))
					.otherwise(
						pl.lit("[")
						+ pl.col(col).list.eval(pl.lit("'") + pl.element() + pl.lit("'")).list.join(",")
						+ pl.lit("]")
					).alias(col)
				)
				return polars_df
			
			# pl.Int doesn't exist and pl.List(int) doesn't seem to work, so we'll take the silly route
			elif col==column and (datatype == pl.List(pl.Int8) or datatype == pl.List(pl.Int16) or datatype == pl.List(pl.Int32) or datatype == pl.List(pl.Int64)):
				polars_df = polars_df.with_columns((
					pl.lit("[")
					+ pl.col(col).list.eval(pl.lit("'") + pl.element().cast(pl.String) + pl.lit("'")).list.join(",")
					+ pl.lit("]")
				).alias(col))
				return polars_df
			
			elif col==column and datatype == pl.Object:
				polars_df = polars_df.with_columns((
					pl.col(col).map_elements(lambda s: "{" + ", ".join(f"{item}" for item in sorted(s)) + "}" if isinstance(s, set) else str(s), return_dtype=str)
				).alias(col))
				return polars_df

			elif col==column:
				print(f"Tried to make {col} into a string column, but we don't know what to do with type {datatype}")
				exit(1)

			else:
				continue
		print(f"Could not find {col} in dataframe")
		exit(1)
	

	@staticmethod
	def stringify_all_list_columns(polars_df):
		""" Unnests list/object data (but not the way explode() does it) so it can be writen to CSV format
		Heavily based on deanm0000 code, via https://github.com/pola-rs/polars/issues/17966#issuecomment-2262903178

		LIMITATIONS: This may not work as expected on pl.List(pl.Null). You may also see oddities on some pl.Object types.
		"""
		for col, datatype in polars_df.schema.items():
			if datatype == pl.List(pl.String):
				polars_df = polars_df.with_columns(
					pl.when(pl.col(col).list.len() <= 1) # don't add brackets if longest list is 1 or 0 elements
					.then(pl.col(col).list.eval(pl.element()).list.join(""))
					.otherwise(
						pl.lit("[")
						+ pl.col(col).list.eval(pl.lit("'") + pl.element() + pl.lit("'")).list.join(",")
						+ pl.lit("]")
					).alias(col)
				)

			# pl.Int doesn't exist and pl.List(int) doesn't seem to work, so we'll take the silly route
			elif (datatype == pl.List(pl.Int8) or datatype == pl.List(pl.Int16) or datatype == pl.List(pl.Int32) or datatype == pl.List(pl.Int64)):
				polars_df = polars_df.with_columns((
					pl.lit("[")
					+ pl.col(col).list.eval(pl.lit("'") + pl.element().cast(pl.String) + pl.lit("'")).list.join(",")
					+ pl.lit("]")
				).alias(col))
			
			elif datatype == pl.Object:
				polars_df = polars_df.with_columns((
					pl.col(col).map_elements(lambda s: "{" + ", ".join(f"{item}" for item in sorted(s)) + "}" if isinstance(s, set) else str(s), return_dtype=str)
				).alias(col))

		return polars_df

	@classmethod
	def print_polars_cols_and_dtypes(cls, polars_df):
		[print(f"{col}: {dtype}") for col, dtype in zip(polars_df.columns, polars_df.dtypes)]

	@classmethod
	def drop_null_columns(cls, polars_df):
		""" Drop columns of type null or list(null) """
		import polars.selectors as cs
		polars_df = polars_df.drop(cs.by_dtype(pl.Null))
		polars_df = polars_df.drop(cs.by_dtype(pl.List(pl.Null)))
		return polars_df

	@classmethod
	def polars_to_tsv(cls, polars_df, path: str):
		print("Writing to TSV. Lists and objects will converted to strings, and columns full of nulls will be dropped.")
		df_to_write = cls.drop_null_columns(polars_df)
		columns_with_type_list_or_obj = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if (dtype == pl.List or dtype == pl.Object)]
		if len(columns_with_type_list_or_obj) > 0:
			df_to_write = cls.stringify_all_list_columns(df_to_write)
		try:
			### DEBUG ###
			debug = pl.DataFrame({col: [dtype1, dtype2] for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if dtype2 != pl.String})
			print(debug)
			df_to_write.write_csv(path, separator='\t', include_header=True, null_value='')
			print(f"Wrote to {path}")
		except pl.exceptions.ComputeError:
			print("WARNING: Failed to write to TSV due to ComputeError. This is likely a data type issue.")
			debug = pl.DataFrame({col:  f"Was {dtype1}, now {dtype2}" for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if col in df_to_write.columns and dtype2 != pl.String and dtype2 != pl.List(pl.String)})
			cls.super_print_pl(debug, "Potentially problematic that may have caused the TSV write failure:")
			exit(2)

	def get_dupe_columns_of_two_polars(polars_df_a, polars_df_b, assert_shared_cols_equal=False):
		""" Check two polars dataframes share any columns """
		columns_a = list(polars_df_a.columns)
		columns_b = list(polars_df_b.columns)
		dupes = []
		for column in columns_a:
			if column in columns_b:
				dupes.append(column)
		if len(dupes) >= 0:
			if assert_shared_cols_equal:
				for dupe in dupes:
					assert_series_equal(polars_df_a[dupe], polars_df_b[dupe])
		return dupes
	
	def assert_unique_columns(pandas_df):
		"""Assert all columns in a pandas df are unique -- useful if converting to polars """
		if len(pandas_df.columns) != len(set(pandas_df.columns)):
			dupes = []
			not_dupes = set()
			for column in pandas_df.columns:
				if column in not_dupes:
					dupes.append(column)
				else:
					not_dupes.add(column)
			raise AssertionError(f"Pandas df has duplicate columns: {dupes}")
	
	def cast_politely(polars_df):
		""" 
		polars_df.cast({k: v}) just doesn't cut it, and casting is not in-place, so
		this does a very goofy full column replacement
		"""
		for k, v in columns.not_strings.items():
			try:
				to_replace_index = polars_df.get_column_index(k)
			except pl.exceptions.ColumnNotFoundError:
				continue
			casted = polars_df.select(pl.col(k).cast(v))
			polars_df.replace_column(to_replace_index, casted.to_series())
			#print(f"Cast {k} to type {v}")
		return polars_df
