# general purpose functions

import polars as pl
from src.dictionaries import columns, null_values
from polars.testing import assert_series_equal

# TODO: can we implement verbose without importing the whole config?

class NeighLib:
	def guess_if_biosample_indexed_or_run_indexed(polars_df):
		# get a column and check if list or not list
		# should be something that doesn't get renamed nor is in attributes
		pass

	def check_dataframe_type(dataframe, wanted):
		""" Checks if dataframe is polars and pandas. If it doesn't match wanted, throw an error."""
		pass
	
	def get_valid_recommended_columns_list(polars_df):
		return [col for col in columns.recommended_sra_columns if col in polars_df.columns]
	
	def check_columns_exist(polars_df, column_list):
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
	
	def super_print_pl(polars_df):
		with pl.Config(tbl_cols=-1, fmt_str_lengths=200):
			print(polars_df)
		
	def get_x_y_column_pairs(dataframe):
		"""
		Take in a pandas dataframe and return a list of lists of all of its
		_x and _y columns (and those columns basename). Designed for the
		aftermath of an outer merge of two dataframes. Asserts no duplicate
		_x and _y columns (eg, can't have two "BioSample_x" columns).
		"""
		all_columns = dataframe.columns
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
	
	def drop_some_assay_types(dataframe, to_drop=['Tn-Seq', 'ChIP-Seq']):
		"""
		Drops a list of values for assay_type from a Pandas dataframe
		"""
		if 'assay_type' in incoming.columns:
			rows_before = len(incoming.index)
			for drop_me in to_drop:
				incoming = incoming[~incoming['assay_type'].str.contains(drop_me, case=False, na=False)]
			rows_after = len(incoming.index)
			print(f"Dropped {rows_before - rows_after} samples")

	def drop_metagenomic(dataframe):
		"""
		Attempt to drop metagenomic data from a Pandas dataframe
		"""
		dropped = 0
		if 'organism' in dataframe.columns:
			rows_before = len(dataframe.index)
			dataframe = dataframe[~dataframe['organism'].str.contains('metagenome', case=False, na=False)]
			rows_after = len(dataframe.index)
			dropped += rows_before - rows_after
		if 'librarysource' in dataframe.columns:
			rows_before = len(dataframe.index)
			dataframe = dataframe[~dataframe['librarysource'].str.contains('METAGENOMIC', case=False, na=False)]
			rows_after = len(dataframe.index)
			dropped += rows_before - rows_after
		#if verbose: print(f"Dropped {dropped} metagenomic samples")
		return dataframe

	def pandas_to_tsv(pandas_df, path):
		pandas_df.to_csv(path, sep='\t', index=False)
	
	def rancheroize_polars(polars_df):
		try:
			return polars_df.rename(columns.bq_col_to_ranchero_col)
		except pl.exceptions.SchemaFieldNotFoundError:
			return polars_df.rename(columns.bq_col_to_ranchero_col_minimal)

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

	def polars_to_tsv(polars_df, path):
		columns_with_type_list = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if dtype == pl.List]
		if len(columns_with_type_list) > 0:
			print("Lists will be converted to strings before writing to file")
			#if verbose: print(f"Columns to convert: {columns_with_type_list}")
		prepared_polars_df = unnest_polars_list_columns(polars_df)
		columns_with_type_list = [col for col, dtype in zip(prepared_polars_df.columns, prepared_polars_df.dtypes) if dtype == pl.List]
		prepared_polars_df.write_csv(path, separator='\t', include_header=True, null_value='')
	
	def get_dupe_columns_of_two_polars(polars_df_a, polars_df_b):
		""" Assert two polars dataframes do not share any columns """
		columns_a = list(polars_df_a.columns)
		columns_b = list(polars_df_b.columns)
		dupes = []
		for column in columns_a:
			if column in columns_b:
				dupes.append(column)
		for column in columns_b:
			if column in columns_a:
				dupes.append(column)
		if len(dupes) >= 0:
			#raise AssertionError(f"Polars dataframes have duplicated columns: {dupes}")
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
