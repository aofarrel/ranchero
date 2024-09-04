# general purpose functions

import polars as pl
from src.dictionaries import columns, null_values
from polars.testing import assert_series_equal

# TODO: can we implement verbose without importing the whole config?

class NeighLib:

	def likely_is_run_indexed(polars_df):
		# TODO: make more robust
		singular_runs = ("run_index" in polars_df.schema and polars_df.schema["run_index"] == pl.Utf8) or ("run_accession" in polars_df.schema and polars_df.schema["run_accession"] == pl.Utf8)
		if singular_runs:
			return True
		else:
			return False

	def check_dataframe_type(dataframe, wanted):
		""" Checks if dataframe is polars and pandas. If it doesn't match wanted, throw an error."""
		pass

	def get_ranchero_input_columns(cls):
		return cls.get_ranchero_column_dictionary.keys()

	def get_ranchero_output_nonstandardized_columns(cls):
		return list(set(cls.get_ranchero_column_dictionary.values ()))

	@staticmethod
	def get_ranchero_column_dictionary():
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
		return {k:v for k, v in column_dict.items() if k in polars_df.columns}
	
	def check_columns_exist(polars_df, column_list: list):
		missing_columns = [col for col in column_list if col not in polars_df.columns]
		if not missing_columns:
			return True
		else:
			# TODO: only print if verbose
			print(f"Missing columns: {missing_columns}")
			return False
	
	def concat_dicts_with_shared_keys(dict_list: list, shared_keys_with_values_to_keep: set):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}

		This version is aware of primary_serach showing up multiple times.
		Since these functions run as .apply() I decided to make a new function
		rather than adding another if to 
		"""
		combined_dict = {}
		primary_search = set()
		for d in dict_list:
			if 'k' in d and 'v' in d:
				if d['k'] in shared_keys_with_values_to_keep:
					primary_search.add(d['v'])
				else:
					combined_dict[d['k']] = d['v']
		combined_dict.update({"primary_search": primary_search})
		return combined_dict

	def concat_dicts_risky(dict_list: list):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}

		This version assumes 'k' and 'v' are in the dictionaries and will error otherwise.
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
	
	def super_print_pl(polars_df, header):
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")
		with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=200):
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

	@staticmethod
	def unnest_polars_list_columns(polars_df):
		""" Unnests list data (but not the way explode() does it) so it can be writen to CSV format
		Credit: deanm0000 on GitHub, via https://github.com/pola-rs/polars/issues/17966#issuecomment-2262903178
		"""
		return polars_df.with_columns(
			(
				pl.lit("[")
				+ pl.col(x).list.eval(pl.lit('"') + pl.element() + pl.lit('"')).list.join(",")
				+ pl.lit("]")
			).alias(x)
			for x, y in polars_df.schema.items()
			if y == pl.List(pl.String)
		)

	@staticmethod
	def unnest_polars_set_columns(polars_df):
		""" Unnests set data so it can be writen to CSV format
		Heavily based on deanm0000's code (see unnest_polars_list_columns())
		"""
		return polars_df.with_columns(
			(pl.col(x).map_elements(lambda s: "{" + ', '.join(f'"{item}"' for item in sorted(s)) + "}" if isinstance(s, set) else str(s), return_dtype=str)).alias(x)
			for x, y in polars_df.schema.items()
			if y == pl.Object
			)

	@classmethod
	def polars_to_tsv(cls, polars_df, path: str):
		print("Writing to TSV...")
		columns_with_type_list = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if dtype == pl.List]
		columns_with_type_obj = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if dtype == pl.Object]
		df_to_write = polars_df
		if len(columns_with_type_list) > 0:
			print("Lists will be converted to strings before writing to file")
			df_to_write = cls.unnest_polars_list_columns(df_to_write)
		if len(columns_with_type_obj) > 0:
			print("WARNING: Columns of type object detected, will be converted as if sets")
			df_to_write = cls.unnest_polars_set_columns(df_to_write)
		try:
			df_to_write.write_csv(path, separator='\t', include_header=True, null_value='')
			print(f"Wrote to {path}")
		except pl.exceptions.ComputeError:
			print("WARNING: Caught ComputeError trying to write to TSV")

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
