# general purpose functions

import polars as pl
import datetime
from src.statics import kolumns, drop_zone, null_values
from polars.testing import assert_series_equal
from .config import RancheroConfig

class NeighLib:
	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger

	@classmethod
	def nullify(self, polars_df, only_this_column=None):
		if only_this_column is None:
			return polars_df.with_columns(pl.col(pl.Utf8).replace(null_values.null_values_dictionary))
		else:
			return polars_df.with_columns(pl.col(only_this_column).replace(null_values.null_values_dictionary))

	def print_col_where(self, polars_df, column="source", equals="Coscolla", cols_of_interest=['acc', 'run_index', 'source', 'literature_lineage', 'Biosample', 'sample_index', 'concat_list', 'coscolla_lineage']+kolumns.equivalence['date_collected']):
		if column not in polars_df.columns:
			self.logging.warning(f"Tried to print where {column} equals {equals}, but that column isn't in the dataframe")
			return
		
		# I am not adding all the various integer types in polars here. go away. you'll get a try/except block at best.
		elif type(equals) == list and polars_df.schema[column] != pl.List:
			self.logging.warning(f"Tried to print where {column} equals list {equals}, but that column has type {polars_df.schema[column]}")
			return
		elif type(equals) == str and polars_df.schema[column] != pl.Utf8:
			self.logging.info("This is a list column and you passed in a string -- I'm assuming you are looking for the string in the list")
			filtah = polars_df.filter(pl.col(column).list.contains(equals))
		else:
			filtah = polars_df.filter(pl.col(column) == equals)
		cols_to_print = [thingy for thingy in cols_of_interest if thingy in polars_df.columns]
		with pl.Config(tbl_cols=-1):
			self.logging.info(filtah.select(cols_to_print))

	def print_only_where_col_list_is_big(self, polars_df, column_of_lists):
		if column_of_lists not in polars_df.columns:
			self.logging.warning(f"Tried to print {column_of_lists}, but that column isn't even in the dataframe!")
		elif polars_df.schema[column_of_lists] != pl.List:
			self.logging.warning(f"Tried to print where {column_of_lists} has multiple values, but that column isn't a list!")
		else:
			cols_of_interest = kolumns.equivalence['run_index'] + kolumns.equivalence['sample_index'] + [column_of_lists]
			cols_to_print = [thingy for thingy in cols_of_interest if thingy in polars_df.columns]
			with pl.Config(tbl_cols=-1, tbl_rows=20):
				print(polars_df.filter(pl.col(column_of_lists).list.len() > 1).select(cols_to_print))

	def print_only_where_col_not_null(self, polars_df, column):
		if column not in polars_df.columns:
			self.logging.warning(f"Tried to print where {column} is not null, but that column isn't even in the dataframe!")
		else:
			cols_of_interest = kolumns.equivalence['run_index'] + kolumns.equivalence['sample_index'] + [column]
			cols_to_print = [thingy for thingy in cols_of_interest if thingy in polars_df.columns]
			with pl.Config(tbl_cols=-1, tbl_rows=20):
				print(polars_df.filter(pl.col(column).is_not_null()).select(cols_to_print))

	def mark_rows_with_value(self, polars_df, filter_func, true_value="M. avium complex", false_value='', new_column="bacterial_family", **kwargs):
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
		#   pl.when(filter_func(polars_df, **kwargs))
		#   .then(pl.lit(true_value))
		#   .otherwise(pl.lit(false_value))
		#   .alias(new_column)
		#)
		print(polars_df.select(pl.col(new_column).value_counts()))

	def print_value_counts(self, polars_df, only_these_columns=None, skip_ids=True):
		ids = kolumns.equivalence['run_index'] + kolumns.equivalence['sample_index']
		for column in polars_df.columns:
			if skip_ids and column not in ids:
				if only_these_columns is not None and column in only_these_columns:
					with pl.Config(fmt_str_lengths=500, tbl_rows=300):
						counts = polars_df.select([pl.col(column).value_counts(sort=True)])
						print(counts)
				elif only_these_columns is None:
					with pl.Config(fmt_str_lengths=500, tbl_rows=300):
						counts = polars_df.select([pl.col(column).value_counts(sort=True)])
						print(counts)
				else:
					continue
			else:
				continue

	def get_valid_columns_list_from_arbitrary_list(polars_df, desired_columns: list):
		return [col for col in desired_columns if col in polars_df.columns]

	def check_columns_exist(polars_df, column_list: list, err=False, verbose=False):
		missing_columns = [col for col in column_list if col not in polars_df.columns]
		if len(missing_columns) == 0:
			#if self.cfg.verbose: print("All requested columns exist in dataframe")
			return True
		else:
			#if self.cfg.verbose: print(f"Missing these columns: {missing_columns}")
			if err: exit(1)
			return False

	def concat_dicts_with_shared_keys(self, dict_list: list):
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
	def big_print_polars(polars_df, header, these_columns):
		assert len(these_columns) >= 3
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")
		filtered = polars_df.select(these_columns)
		filtered = filtered.filter(
			(pl.col(these_columns[1]).is_not_null()) | 
			(pl.col(these_columns[2]).is_not_null())
		)
		with pl.Config(tbl_cols=10, tbl_rows=200, fmt_str_lengths=200, fmt_table_cell_list_len=10):
			print(filtered)

	@staticmethod
	def super_print_pl(polars_df, header):
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")
		with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=500, fmt_table_cell_list_len=10):
			print(polars_df)

	#.str.contains_any("Mycobacterium avium")
	def merge_right_columns(self, polars_df, fallback_on_left=True, err_on_matching_failure=True):
		"""
		Takes in a polars_df with some number of columns ending in "_right", where each _right column has
		a matching column with the same basename (ie, "foo_right" matches "foo"), and merges each base:right
		pair's columns. The resulting merged columns will inherit the base columns name.

		Generally, we want to avoid creating columns of type list whenever possible.

		If column in kolumns.rancheroize__warn... and fallback_on_left, keep only left value(s)
		If column in kolumns.rancheroize__warn... and !fallback_on_left, keep only right values(s)

		Additional special handling for:
		* organism and lineage (kolumns.organism_and_lineage_combined)
		"""
		right_columns = [col for col in polars_df.columns if col.endswith("_right")]
		for right_col in right_columns:
			nullfilled = False
			self.logging.debug(f"[{right_columns.index(right_col)}/{len(right_columns)}] Trying to merge {right_col}...")
			base_col = right_col.replace("_right", "")

			if base_col not in polars_df.columns and not err_on_matching_failure:
				self.logging.warning(f"Found {right_col}, but {base_col} not in dataframe -- will drop that column and continue, but this may break things later")
				polars_df = polars_df.drop(right_col)
				continue
			elif base_col not in polars_df.columns:
				self.logging.error(f"Found {right_col}, but {base_col} not in dataframe -- this is a sign something broke in an earlier function")
				exit(1)
			else:
				pass # intentional

			self.logging.debug(f"\n\n{base_col}: {polars_df[base_col].dtype}\n{right_col}: {polars_df[right_col].dtype}")
			
			try:
				polars_df = polars_df.with_columns(pl.col(base_col).fill_null(pl.col(right_col)))
				polars_df = polars_df.with_columns(pl.col(right_col).fill_null(pl.col(base_col)))
				nullfilled = True
			except pl.exceptions.InvalidOperationError:
				self.logging.debug("Could not nullfill (this isn't an error, nulls will be filled if pl.Ut8 or list[str])")
				nullfilled = False
			
			try:
				# if they are equal after filling in nulls, we don't need to turn anything into a list
				assert_series_equal(polars_df[base_col], polars_df[right_col].alias(base_col))
				polars_df = polars_df.drop(right_col)
				self.logging.debug(f"All values in {base_col} and {right_col} are the same after an attempted nullfill, so no lists are necessary. Dropped {right_col}.")
			
			except AssertionError:
				# not equal after filling in nulls (or nullfill errored)
				if base_col in kolumns.merge__special_taxonomic_handling:
					if fallback_on_left:
						self.logging.info(f"Found conflicting metadata in columns that appear to have organism or lineage metadata, but special handling should have happened earlier. Falling back on {base_col}.")
						polars_df = polars_df.drop(right_col)
					else:
						self.logging.info(f"Found conflicting metadata in columns that appear to have organism or lineage metadata, but special handling should have happened earlier. Falling back on {right_col}.")
						polars_df = polars_df.drop(base_col).rename({right_col: base_col})
				
				elif base_col in kolumns.merge__error:
					self.logging.error("Found conflicting metadata in columns that should not have conflicting metadata!")
					self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select([base_col, right_col]), f"conflicts")
					exit(1)
				
				elif base_col in kolumns.merge__warn_then_pick_arbitrarily_to_keep_singular:
					self.logging.debug(f"Not all values in {base_col} and {right_col} are the same, but we want to avoid creating lists. Falling back on {base_col if fallback_on_left else right_col}")
					if base_col == 'date_collected' and self.logging.getEffectiveLevel() == 10:
						self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select(['sample_index', base_col, right_col]), "date collected conflicts")
					if fallback_on_left:
						polars_df = polars_df.drop(right_col)
					else:
						polars_df = polars_df.drop(base_col).rename({right_col: base_col})
				
				elif base_col in kolumns.merge__sum:
					self.logging.error("TODO NOT IMPLEMENTED")
					exit(1)
				elif base_col in kolumns.merge__drop:
					self.logging.error("TODO NOT IMPLEMENTED")
					exit(1)
				
				else:
					if nullfilled:
						# previous nullfilled succceeded, but columns aren't exactly the same aftwards. hopefully both
						# columns are list[str] or pl.Ut8 at this point (probably, since nullfill worked).
						# this is known to work with base_col and right_col are both pl.Ut8, or when both are list[str]

						if base_col in kolumns.merge__warn_then_pick_arbitrarily_to_keep_singular:
							self.logging.debug(f"Not all values in {base_col} and {right_col} are the same (after successful nullfill), but we want to avoid creating lists. Falling back on {base_col if fallback_on_left else right_col}")
							if base_col == 'date_collected' and self.logging.getEffectiveLevel() == 10:
								self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select(['sample_index', base_col, right_col]), "date collected conflicts")
							if fallback_on_left:
								polars_df = polars_df.drop(right_col)
							else:
								polars_df = polars_df.drop(base_col).rename({right_col: base_col})
						else:
							# TODO: differenciate between merge__make_list and merge__make_set						
							self.logging.debug("Columns seem compatiable with concat_list, will try to merge with that")
							polars_df = polars_df.with_columns(
								pl.when(pl.col(base_col) != pl.col(right_col))             # When a row has different values for base_col and right_col,
								.then(pl.concat_list([base_col, right_col]).list.unique()) # make a list of base_col and right_col, but keep only uniq values
								.otherwise(pl.concat_list([base_col]))                     # otherwise, make list of just base_col (doesn't seem to nest if already a list, thankfully)
								.alias(base_col)
							).drop(right_col)
							#if veryverbose: NeighLib.super_print_pl(polars_df.select(base_col), f"after merging to make {base_col} to a list")
							assert polars_df.select(pl.col(base_col)).dtypes == [pl.List]
					else:
						if polars_df[base_col].dtype != pl.List(pl.Utf8):
							self.logging.debug(f"{base_col} is not a list, but we will make it one")
							polars_df = polars_df.with_columns(pl.col(base_col).cast(pl.List(str)))
						if polars_df[right_col].dtype != pl.List(pl.Utf8):
							self.logging.debug(f"{right_col} is not a list, but we will make it one")
							polars_df = polars_df.with_columns(pl.col(right_col).cast(pl.List(str)))
						polars_df = polars_df.with_columns(pl.col(right_col).fill_null(pl.col(base_col)).alias(base_col))
						polars_df = polars_df.drop(right_col)
						# TODO: double check we're not dropping what we just nullfilled. check alias!!

			assert base_col in polars_df.columns
			assert right_col not in polars_df.columns

		right_columns = [col for col in polars_df.columns if col.endswith("_right")]
		if len(right_columns) > 0:
			self.logging.error(f"Failed to remove some _right columns: {right_columns}")
			exit(1)
		# non-unique rows might be dropped here, fyi
		return polars_df

	def iteratively_merge_these_columns(self, polars_df, merge_these_columns: list, equivalence_key=None, recursion_depth=0):
		"""
		Merges columns named in merged_these_columns.

		When all is said and done, the final merged column will be named equivalene_key's value if not None.
		"""
		assert len(merge_these_columns) > 1
		assert all(col in polars_df.columns for col in merge_these_columns)
		assert all(not col.endswith("_right") for col in polars_df.columns)
		if recursion_depth != 0:
			self.logging.debug(f"Intending to merge:\n\t{merge_these_columns}")
		
		left_col, right_col = merge_these_columns[0], merge_these_columns[1]

		self.logging.debug(f"\n\t\tIteration {recursion_depth}\n\t\tLeft: {left_col}\n\t\tRight: {right_col} (renamed to {left_col}_right)")
		polars_df = polars_df.rename({right_col: f"{left_col}_right"})
		polars_df = self.merge_right_columns(polars_df)

		del merge_these_columns[1] # NOT ZERO!!!

		if len(merge_these_columns) > 1:
			#self.logging.debug(f"merge_these_columns is {merge_these_columns}, which we will pass in to recurse")
			polars_df = self.iteratively_merge_these_columns(polars_df, merge_these_columns)
		return polars_df.rename({left_col: equivalence_key}) if equivalence_key is not None else polars_df

	def get_rows_where_list_col_more_than_one_value(self, polars_df, list_col, force_uniq=False):
		assert polars_df.schema[list_col] == pl.List
		if force_uniq:
			polars_df = polars_df.with_columns(pl.col(list_col).list.unique().alias(f"{list_col}_uniq"))
			return polars_df.filter(pl.col(f"{list_col}_uniq").list.len() > 1)
		else:
			return polars_df.filter(pl.col(list_col).list.len() > 1)
	
	def rancheroize_polars(self, polars_df):
		polars_df = self.drop_known_unwanted_columns(polars_df)
		polars_df = self.nullify(polars_df)
		polars_df = self.drop_null_columns(polars_df)
		polars_df = self.flatten_all_list_cols_as_much_as_possible(polars_df, force_strings=False) # this makes merging better for "geo_loc_name_sam" but is slow

		# check date columns, our arch-nemesis
		for column in polars_df.columns:
			if column in kolumns.equivalence['date_collected']:
				if polars_df[column].dtype is not pl.Date:
					self.logging.warning(f"Found likely date column {column}, but it has type {polars_df[column].dtype}")
				else:
					self.logging.debug(f"Likely date column {column} has pl.Date type")

		for key, value in kolumns.equivalence.items():
			merge_these_columns = [v_col for v_col in value if v_col in polars_df.columns and v_col not in sum(kolumns.merge__special_taxonomic_handling.values(), [])]
			if len(merge_these_columns) > 0:
				self.logging.debug(f"Discovered {key} in column via:")
				for some_column in merge_these_columns:
					self.logging.debug(f"  * {some_column}: {polars_df.schema[some_column]}")

				if len(merge_these_columns) > 1:
					#polars_df = polars_df.with_columns(pl.implode(merge_these_columns)) # this gets sigkilled; don't bother!
					if key in kolumns.rts__drop:
						polars_df = polars_df.drop(col)
					#don't add kolumns.rts__list_to_float_via_sum here, that's not what it's made for and it'll cause errors
					else:
						self.logging.info(f"  Merging these columns: {merge_these_columns}")
						polars_df = self.iteratively_merge_these_columns(polars_df, merge_these_columns, equivalence_key=key)
				else:
					self.logging.debug(f"  Renamed {merge_these_columns[0]} to {key}")
					polars_df = polars_df.rename({merge_these_columns[0]: key})
				assert key in polars_df.columns
		# do not flatten list cols again, at least not yet. use the equivalence columns for standardization.
		return polars_df

	def is_sample_indexed(self, polars_df):
		index = self.get_index_column(polars_df)
		return True if index in kolumns.equivalence['sample_index'] else False

	def is_run_indexed(self, polars_df):
		index = self.get_index_column(polars_df)
		return True if index in kolumns.equivalence['run_index'] else False

	def get_index_column(self, polars_df):
		sample_indeces = kolumns.equivalence['sample_index']
		sample_matches = [col for col in sample_indeces if col in polars_df.columns]
		run_indeces = kolumns.equivalence['run_index']
		run_matches = [col for col in run_indeces if col in polars_df.columns]

		if len(sample_matches) > 1:
			raise ValueError(f"Tried to find dataframe index, but there's multiple possible sample indeces: {sample_matches}")
	
		if len(sample_matches) == 1:
			if len(run_matches) > 1:
				raise ValueError(f"Tried to find dataframe index, but there's multiple possible run indeces (indicates failed run->sample conversion):  {run_matches}")
			
			if len(run_matches) == 1:
				if isinstance(run_indeces, list):
					return sample_matches[0]
				elif len(set(sample_indeces)) == len(sample_indeces):  # check samples are unique
					return sample_matches[0]
				else:
					return run_matches[0]
			
			return sample_matches[0]  # no run accessions

		if len(run_matches) == 1:
			return run_matches[0]
	
		raise ValueError(f"No valid index column found in polars_df! Columns available: {polars_df.columns}")

	def coerce_to_not_list_if_possible(self, polars_df, column, index_column):
		if len(self.get_rows_where_list_col_more_than_one_value(polars_df, column, False)) == 0:
			self.logging.debug(f"-->After removing non-uniques, we can turn {column} into single-value")
			return polars_df.with_columns(pl.col(column).list.first().alias(column))
		else:
			if self.logging.getEffectiveLevel() == 10:
				self.logging.debug(f"-->After removing non-uniques, we still have {len(self.get_rows_where_list_col_more_than_one_value(polars_df, column, False))} multi-element lists in {column}")
				debug_print = self.get_rows_where_list_col_more_than_one_value(polars_df, column, False)
				self.super_print_pl(debug_print.select([index_column, f"{column}"]).head(30), f"polars_df, after set treatment (true len {len(debug_print)})")
			return polars_df

	def flatten_all_list_cols_as_much_as_possible(self, polars_df, hard_stop=False, force_strings=False):
		"""
		Flatten list columns as much as possible. If a column is just a bunch of one-element lists, for
		instance, then just take the 0th value of that list and make a column that isn't a list.

		If force_strings, any remaining columns that are still lists are forced into strings.
		"""

		# unnest nested lists (recursive)
		polars_df = self.flatten_nested_list_cols(polars_df)
		number_of_columns = len(polars_df.schema)
		what_was_done = []

		index_column = self.get_index_column(polars_df)
		for col, datatype in polars_df.schema.items():
			self.logging.debug(f"Evaulating on {col} of type {datatype}")
			
			if col in kolumns.rts__drop:
				polars_df.drop(col)
				self.logging.debug(f"-->Dropped {col} per kolumns rules")
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': pl.Null, 'result': 'dropped'})
				continue
			
			assert number_of_columns == len(polars_df.schema), f"There should be {number_of_columns} columns, but there's now {len(polars_df.schema)}"
			if datatype == pl.List and datatype.inner != datetime.datetime:
				
				if polars_df[col].drop_nulls().shape[0] == 0:
					self.logging.warning(f"{col} has datatype {datatype} but seems empty or contains only nulls, skipping...")
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (empty/nulls)'})
					continue

				if col in kolumns.rts__list_to_float_via_sum:
					self.logging.debug(f"-->Summing {col}")
					if datatype.inner == pl.String:
						polars_df = polars_df.with_columns(
							pl.col(col).list.eval(
								pl.when(pl.element().is_not_null())
								.then(pl.element().cast(pl.Int32))
								.otherwise(None) # TODO: .otherwise(pl.lit("null")) may be necessary
							).alias(f"{col}_sum")
						)
					else:
						polars_df = polars_df.with_columns(pl.col(col).list.sum().alias(f"{col}_sum"))
					polars_df = polars_df.drop(col)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[f"{col}_sum"], 'result': 'namechange + summed'})
					continue
				
				elif col in kolumns.rts__keep_as_list:
					self.logging.debug(f"-->Handling {col} as a list")
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'list-and-shrink'})
					continue
				
				elif col in kolumns.rts__keep_as_set: 
					self.logging.debug(f"-->Handling {col} as a set per kolumn rules")
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set-and-shrink'})
					continue
					
				elif col in kolumns.rts__warn_if_list_with_unique_values:
					self.logging.warning(f"Expected {col} to only have one non-null per sample, but that's not the case. Will keep as a set.")
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set-and-shrink (!!!WARNING!!)'})
					if hard_stop:
						exit(1)
					else:
						continue
				else:
					self.logging.warning(f"-->Not sure how to handle {col}, will treat it as a set")
					polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set (no rules)'})
					continue

			elif datatype == pl.List and datatype.inner == datetime.date:
				self.logging.warning(f"{col} is a list of datetimes. Datetimes break typical handling of lists, so this column will be left alone.")
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (date.date)'})
			
			else:
				self.logging.debug(f"-->Leaving {col} as-is")
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'not a list'})
		
		if force_strings:
			polars_df = self.stringify_all_list_columns(polars_df)
		
		self.logging.info("Finished flattening all list columns as much as possible.")
		report = pl.DataFrame(what_was_done)
		NeighLib.super_print_pl(report, "column flattening results")
		return polars_df

	def drop_non_tb_columns(self, polars_df):
		dont_drop_these = [col for col in polars_df.columns if col not in drop_zone.clearly_not_tuberculosis]
		return polars_df.select(dont_drop_these)

	def drop_known_unwanted_columns(self, polars_df):
		return polars_df.select([col for col in polars_df.columns if col not in drop_zone.silly_columns])

	def flatten_nested_list_cols(self, polars_df):
		"""Flatten nested list columns"""

		# This version seems to breaking the schema:
		#polars_df = polars_df.with_columns(
		#   [pl.col(x).list.eval(pl.lit("'") + pl.element() + pl.lit('"')).list.join(",").alias(x) for x, y in polars_df.schema.items() if isinstance(y, pl.List) and isinstance(y.inner, pl.List)]
		#)

		nested_lists = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.List)]
		for col in nested_lists:
			#new_col = pl.col(col).list.eval(pl.element().cast(pl.Utf8).map_elements(lambda s: f"'{s}'")).alias(f"{col}_flattened")
			#new_col = pl.col(col).list.eval(pl.element().cast(pl.Utf8).map_elements(lambda s: f"'{s}'", return_dtype=str)).list.join(",").alias(f"{col}_flattened")
			polars_df = polars_df.with_columns(pl.col(col).list.eval(pl.element().flatten().drop_nulls()))
		
		# this recursion should, in theory, handle list(list(list(str))) -- but it's not well tested
		remaining_nests = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.List)]
		if len(remaining_nests) != 0:
			polars_df = self.flatten_nested_list_cols(polars_df)
		return(polars_df)

	def stringify_one_list_column(self, polars_df, column):

		# TODO: iterating through the entire df is ridiculous. why the hell did I write it like this?

		assert column in polars_df.columns
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
				raise ValueError(f"Tried to make {col} into a string column, but we don't know what to do with type {datatype}")

			else:
				continue
		raise LookupError(f"Could not find {col} in dataframe")	

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
			elif (datatype == pl.List(pl.Int8) or datatype == pl.List(pl.Int16) or datatype == pl.List(pl.Int32) or datatype == pl.List(pl.Int64) or datatype == pl.List(pl.Float64)):
				polars_df = polars_df.with_columns((
					pl.lit("[")
					+ pl.col(col).list.eval(pl.lit("'") + pl.element().cast(pl.String) + pl.lit("'")).list.join(",")
					+ pl.lit("]")
				).alias(col))
			
			elif datatype == pl.Object:
				polars_df = polars_df.with_columns((
					pl.col(col).map_elements(lambda s: "{" + ", ".join(f"{item}" for item in sorted(s)) + "}" if isinstance(s, set) else str(s), return_dtype=str)
				).alias(col))

			elif datatype == pl.List(pl.Datetime(time_unit='us', time_zone='UTC')):
				polars_df = polars_df.with_columns((
					pl.col(col).map_elements(lambda s: "[" + ", ".join(f"{item}" for item in sorted(s)) + "]" if isinstance(s, set) else str(s), return_dtype=str)
				).alias(col))

		return polars_df

	def print_polars_cols_and_dtypes(self, polars_df):
		[print(f"{col}: {dtype}") for col, dtype in zip(polars_df.columns, polars_df.dtypes)]

	def drop_null_columns(self, polars_df):
		""" Drop columns of type null or list(null) """
		import polars.selectors as cs
		polars_df = polars_df.drop(cs.by_dtype(pl.Null))
		polars_df = polars_df.drop(cs.by_dtype(pl.List(pl.Null)))
		return polars_df

	def polars_to_tsv(self, polars_df, path: str):
		print("Writing to TSV. Lists and objects will converted to strings, and columns full of nulls will be dropped.")
		df_to_write = self.drop_null_columns(polars_df)
		columns_with_type_list_or_obj = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if (dtype == pl.List or dtype == pl.Object)]
		if len(columns_with_type_list_or_obj) > 0:
			df_to_write = self.stringify_all_list_columns(df_to_write)
		try:
			### DEBUG ###
			debug = pl.DataFrame({col: [dtype1, dtype2] for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if dtype2 != pl.String})
			self.logging.debug(f"Non-string types, and what they converted to: {debug}")
			df_to_write.write_csv(path, separator='\t', include_header=True, null_value='')
			self.logging.info(f"Wrote dataframe to {path}")
		except pl.exceptions.ComputeError:
			print("WARNING: Failed to write to TSV due to ComputeError. This is likely a data type issue.")
			debug = pl.DataFrame({col:  f"Was {dtype1}, now {dtype2}" for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if col in df_to_write.columns and dtype2 != pl.String and dtype2 != pl.List(pl.String)})
			self.super_print_pl(debug, "Potentially problematic that may have caused the TSV write failure:")
			exit(1)

	def get_dupe_columns_of_two_polars(self, polars_df_a, polars_df_b, assert_shared_cols_equal=False):
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
	
	def assert_unique_columns(self, pandas_df):
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
	
	def cast_politely(self, polars_df):
		""" 
		polars_df.cast({k: v}) just doesn't cut it, and casting is not in-place, so
		this does a very goofy full column replacement
		"""
		for k, v in kolumns.not_strings.items():
			try:
				to_replace_index = polars_df.get_column_index(k)
			except pl.exceptions.ColumnNotFoundError:
				continue
			casted = polars_df.select(pl.col(k).cast(v))
			polars_df.replace_column(to_replace_index, casted.to_series())
			#print(f"Cast {k} to type {v}")
		return polars_df
