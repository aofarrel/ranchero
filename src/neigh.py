# general purpose functions

import polars as pl
from src.dictionaries import columns, drop_zone, null_values
from polars.testing import assert_series_equal

# TODO: can we implement verbose without importing the whole config?

class NeighLib:

	def print_col_where(polars_df, column="source", equals="Coscolla"):
		print(polars_df.filter(pl.col(column) == equals))

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

	def get_ranchero_input_columns(cls):
		return cls.get_ranchero_column_dictionary.keys()

	def get_ranchero_output_nonstandardized_columns(cls):
		return list(set(cls.get_ranchero_column_dictionary.values()))

	def print_value_counts(polars_df, skip_ids=True):
		ids = ['sample_index', 'biosample', 'BioSample', 'acc', 'acc_1', 'run_index', 'run_accession'] + columns.addl_ids
		for column in polars_df.columns:
			if skip_ids and column not in ids:
				counts = polars_df.select([pl.col(column).value_counts(sort=True)])
				print(counts)
			else:
				continue

	@staticmethod
	def get_ranchero_column_dictionary():
		"""WARNING: This will generate duplicate values. Don't pipe directly to rename, filter out first."""
		almost_everything_worth_keeping = {}
		for key, value in columns.common_col_to_ranchero_col.items():
			if key in almost_everything_worth_keeping and almost_everything_worth_keeping[key] != value:
				raise ValueError(f"Collision detected for key '{key}' with different values: '{almost_everything_worth_keeping[key]}' and '{value}'")
			almost_everything_worth_keeping[key] = value
		for dictionary in columns.svr:
			for key, value in dictionary.items():
				if key in almost_everything_worth_keeping and almost_everything_worth_keeping[key] != value:
					raise ValueError(f"Collision detected for key '{key}' with different values: '{almost_everything_worth_keeping[key]}' and '{value}'")
				almost_everything_worth_keeping[key] = value
		for key, value in columns.extended_col_to_ranchero.items():
			if key in almost_everything_worth_keeping and almost_everything_worth_keeping[key] != value:
				raise ValueError(f"Collision detected for key '{key}' with different values: '{almost_everything_worth_keeping[key]}' and '{value}'")
			almost_everything_worth_keeping[key] = value
		# we want to keep stuff that is already rancheroized too!
		everything_worth_keeping = almost_everything_worth_keeping.copy()
		everything_worth_keeping.update({v: v for k, v in almost_everything_worth_keeping.items() if v not in almost_everything_worth_keeping.keys()})  # yes, this is VALUE: VALUE

		return everything_worth_keeping.copy()

	@classmethod
	def get_ranchero_column_dictionary_only_valid(cls, polars_df):
		ranchero = cls.get_ranchero_column_dictionary()
		return cls.get_valid_columns_dict_from_arbitrary_dict(polars_df, ranchero)

	@staticmethod
	def get_just_geoloc_columns(cls, polars_df, check_if_valid=False):
		if check_if_valid:
			ranchero_geoloc = [k for k, v in cls.get_ranchero_output_nonstandardized_columns().items() if k.str.startswith("geo")]
			return get_valid_columns_from_arbitrary_list(polars_df, ranchero_geoloc)
		else:
			return [k for k, v in cls.get_ranchero_output_nonstandardized_columns().items() if k.str.startswith("geo")]

	@staticmethod
	def get_valid_columns_list_from_arbitrary_list(polars_df, desired_columns: list):
		return [col for col in desired_columns if col in polars_df.columns]

	@staticmethod
	def get_valid_columns_dict_from_arbitrary_dict(polars_df, column_dict: dict):
		key_exists = {k:v for k, v in column_dict.items() if k in polars_df.columns}
		# force unique values only
		# TODO: there's better ways of handling similar columns; ideally we should merge them
		temp = {val: key for key, val in key_exists.items()}
		everything_worth_keeping = {val: key for key, val in temp.items()}
		return everything_worth_keeping
	
	def check_columns_exist(polars_df, column_list: list, err=False, verbose=False):
		missing_columns = [col for col in column_list if col not in polars_df.columns]
		if len(missing_columns) == 0:
			if verbose: print("All requested columns exist in dataframe")
			return True
		else:
			if verbose: print(f"Missing these columns: {missing_columns}")
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
		
	def get_x_y_column_pairs(pandas_df):
		"""
		Take in a pandas dataframe and return a list of lists of all of its
		_x and _y columns (and those columns basename). Designed for the
		aftermath of an outer merge of two dataframes. Asserts no duplicate
		_x and _y columns (eg, can't have two "BioSample_x" columns).
		"""
		all_columns = pandas_df.columns
		list_of_pairs = []
		for col in all_columns:
			if col.endswith("_x"):
				print(f"col {col} ends with _x")
				foo_x = col
				foo = foo_x[:-2]
				foo_y = foo + "_y"
				if foo_y in all_columns:
					list_of_pairs.append([foo_x, foo_y, foo])
				else:
					raise ValueError("Found {foo_x}, but no {foo_y} counterpart!")
		assert len(set(map(tuple, list_of_pairs))) == len(list_of_pairs)  # there should be no duplicate columns
		return list_of_pairs  # list of lists [["foo_x", "foo_y", "foo"]]
	
	def drop_some_assay_types(pandas_df, to_drop=['Tn-Seq', 'ChIP-Seq']):
		"""
		Drops a list of values for assay_type from a Pandas dataframe
		"""
		if 'assay_type' in pandas_df.columns:
			rows_before = len(pandas_df.index)
			for drop_me in to_drop:
				pandas_df = pandas_df[~pandas_df['assay_type'].str.contains(drop_me, case=False, na=False)]
			rows_after = len(incoming.index)
			print(f"Dropped {rows_before - rows_after} samples")
		return pandas_df

	def drop_metagenomic(pandas_df):
		"""
		Attempt to drop metagenomic data from a Pandas dataframe
		"""
		dropped = 0
		if 'organism' in pandas_df.columns:
			rows_before = len(pandas_df.index)
			dataframe = pandas_df[~pandas_df['organism'].str.contains('metagenome', case=False, na=False)]
			rows_after = len(pandas_df.index)
			dropped += rows_before - rows_after
		if 'librarysource' in pandas_df.columns:
			rows_before = len(pandas_df.index)
			dataframe = pandas_df[~pandas_df['librarysource'].str.contains('METAGENOMIC', case=False, na=False)]
			rows_after = len(pandas_df.index)
			dropped += rows_before - rows_after
		#if verbose: print(f"Dropped {dropped} metagenomic samples")
		return pandas_df

	def pandas_to_tsv(pandas_df, path: str):
		pandas_df.to_csv(path, sep='\t', index=False)
	
	@classmethod
	def rancheroize_polars(cls, polars_df):
		valid_renames = cls.get_ranchero_column_dictionary_only_valid(polars_df)
		try:
			return polars_df.rename(valid_renames)
		except pl.exceptions.SchemaFieldNotFoundError:  # should never happen
			print("WARNING: Failed to rename columns")

	@classmethod
	def flatten_all_list_cols_as_much_as_possible(cls, polars_df, verbose=False, hard_stop=False):
		"""If intelligent, assume sample indexed, and check lists actually make sense. For example,
		a country shouldn't be a list at all in a sample-indexed dataframe as a sample can only come
		from one country since NCBI data doesn't really make a distinction between host and prior
		nation for refugees/travelers"""

		# unnest nested lists (recursive)
		polars_df = cls.flatten_nested_list_cols(polars_df)

		# flatten lists of only one value
		for col, datatype in polars_df.schema.items():
			if datatype == pl.List:
				n_rows_prior = polars_df.shape[0]
				temp_df = polars_df.explode(col).unique()
				n_rows_now = temp_df.shape[0]
				if n_rows_now > n_rows_prior:
					if col in columns.rts__list_to_float_via_sum:  # ignores temp_df
						polars_df = polars_df.with_columns(pl.col(col).list.sum().alias(f"{col}_sum"))
						polars_df = polars_df.drop(col)
					elif col in columns.rts__drop:  # ignores temp_df
						polars_df = polars_df.drop(col)
					elif col in columns.rts__keep_as_list:  # ignores temp_df
						polars_df = polars_df
					elif col in columns.rts__keep_as_set:  # because we exploded with unique(), we now have a set (sort of), but I think this is better than trying to do a column merge
						polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
					elif col in columns.rts__warn_if_list_with_unique_values:
						print(f"WARNING: Expected {col} to only have one non-null per sample, but that's not the case.")
						if verbose:
							cls.super_print_pl(polars_df.select(col), "as passed in")
						else:
							print(polars_df.select(col))
						if hard_stop:
							exit(1)
						else:
							continue
					else:
						if verbose: print(f"WARNING: Unsure what to do with {col}, so we'll leave it as-is")
						polars_df = polars_df
					
					# debug
					if verbose:
						if col in polars_df.columns:
							print(polars_df.select(col))
						elif f"{col}_sum" in polars_df.columns:
							print(polars_df.select(f"{col}_sum"))
						else:
							pass
				else:
					polars_df = temp_df

		return polars_df

	@classmethod
	def drop_non_tb_columns(cls, polars_df):
		return polars_df.select([col for col in polars_df.columns if col not in drop_zone.clearly_not_tuberculosis])

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
	def stringify_list_columns(polars_df):
		""" Unnests list/object data (but not the way explode() does it) so it can be writen to CSV format
		Heavily based on deanm0000 code, via https://github.com/pola-rs/polars/issues/17966#issuecomment-2262903178

		LIMITATIONS: This may not work as expected on pl.List(pl.Null). You may also see oddities on some pl.Object types.
		"""
		for col, datatype in polars_df.schema.items():
			if datatype == pl.List(pl.String):
				polars_df = polars_df.with_columns((
					pl.lit("[")
					+ pl.col(col).list.eval(pl.lit("'") + pl.element() + pl.lit("'")).list.join(",")
					+ pl.lit("]")
				).alias(col))
			
			# TODO: pl.Int doesn't exist, but is there some kind of superset? pl.List(int) doesn't seem to work
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
			df_to_write = cls.stringify_list_columns(df_to_write)
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
