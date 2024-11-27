# general purpose functions

import polars as pl
import datetime
from src.statics import kolumns, drop_zone, null_values
from polars.testing import assert_series_equal
import polars.selectors as cs
from .config import RancheroConfig

# my crummy implementation of https://peps.python.org/pep-0661/
globals().update({f"_cfg_{name}": object() for name in [
	"force_SRR_ERR_DRR_run_index", "force_SAMN_SAME_SAMD_sample_index",
	"check_index", "indicator_column",
	"intermediate_files", "rm_dupes", "rm_not_pared_illumina"
]})
_SENTINEL_TO_CONFIG = {
	_cfg_force_SRR_ERR_DRR_run_index: "force_SRR_ERR_DRR_run_index",
	_cfg_force_SAMN_SAME_SAMD_sample_index: "force_SAMN_SAME_SAMD_sample_index",
	_cfg_rm_dupes: "rm_dupes",
	_cfg_check_index: "check_index",
	_cfg_intermediate_files: "intermediate_files",
	_cfg_indicator_column: "indicator_column",
	_cfg_rm_not_pared_illumina: "rm_not_pared_illumina",
}

class NeighLib:
	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger

	def _sentinal_handler(self, arg):
		"""Handles "allow overriding config" variables in function calls"""
		if arg in _SENTINEL_TO_CONFIG:
			config_attr = _SENTINEL_TO_CONFIG[arg]
			check_me = getattr(self.cfg, config_attr)
			assert check_me != arg, f"Configuration for '{config_attr}' is invalid or uninitialized"
			return check_me
		else:
			return arg

	def get_number_of_x_in_column(self, polars_df, x, column):
		return len(polars_df.filter(pl.col(column) == x))

	def print_a_where_b_is_foo(self, polars_df, col_a, col_b, foo, alsoprint=None, valuecounts=False):
		if col_a not in polars_df.columns or col_b not in polars_df.columns:
			self.logging.warning(f"Tried to print column {col_a} where column {col_b} equals {foo}, but at least one of those columns aren't in the dataframe!")
			return
		if type(foo) == str:
			assert polars_df.schema[col_b] == pl.Utf8
		print_df = polars_df.with_columns(pl.when(pl.col(col_b) == foo).then(pl.col(col_a)).otherwise(None).alias(f"{col_a}_filtered")).drop_nulls(subset=f"{col_a}_filtered")
		valid_ids = self.get_valid_id_columns(polars_df)
		if col_a in valid_ids or col_b in valid_ids:  # this check avoids polars.exceptions.DuplicateError
			print_cols = + [f"{col_a}_filtered", col_b] + alsoprint if alsoprint is not None else [f"{col_a}_filtered", col_b]
		else:
			print_cols = self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b] + alsoprint if alsoprint is not None else self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b]
		self.super_print_pl(print_df.select(print_cols), f"{col_a} where {col_b} is {foo}")
		if valuecounts: self.print_value_counts(polars_df, only_these_columns=col_a)

	def print_a_where_b_is_null(self, polars_df, col_a, col_b, alsoprint=None, valuecounts=False):
		if col_a not in polars_df.columns or col_b not in polars_df.columns:
			self.logging.warning(f"Tried to print column {col_a} where column {col_b} equals {foo}, but at least one of those columns aren't in the dataframe!")
			return
		print_df = polars_df.with_columns(pl.when(pl.col(col_b).is_null()).then(pl.col(col_a)).otherwise(None).alias(f"{col_a}_filtered")).drop_nulls(subset=f"{col_a}_filtered")
		print_cols = self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b] + alsoprint if alsoprint is not None else self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b]
		self.super_print_pl(print_df.select(print_cols), f"{col_a} where {col_b} is pl.Null")
		if valuecounts: self.print_value_counts(polars_df, only_these_columns=col_a)

	def get_null_count_in_column(self, polars_df, column_name, warn=True, error=False):
		series = polars_df.get_column(column_name)
		null_count = series.null_count()
		if null_count > 0 and warn:
			self.logging.warning(f"Found {null_count} nulls in column {column_name}")
		elif null_count > 0 and error:
			self.logging.error(f"Found {null_count} nulls in column {column_name}")
			raise AssertionError
		return null_count

	def nullify(self, polars_df, only_these_columns=None, no_match_NA=False, skip_ids=True):
		"""
		Turns stuff like "not collected" and "n/a" into pl.Null values, per null_values.py
		"""
		all_cols = only_these_columns if only_these_columns is not None else polars_df.columns
		if skip_ids:
			string_cols = [col for col in all_cols if polars_df.schema[col] == pl.Utf8 and col not in kolumns.id_columns]
			list_cols = [col for col in all_cols if polars_df.schema[col] == pl.List(pl.Utf8) and col not in kolumns.id_columns]
		else:
			string_cols = [col for col in all_cols if polars_df.schema[col] == pl.Utf8]
			list_cols = [col for col in all_cols if polars_df.schema[col] == pl.List(pl.Utf8)]

		# use contains_any() for the majority of checks, as it is much faster than iterating through a list + contains()
		# the downside of contains_any() is that it doesn't allow for regex
		# in either case, we do string columns first, then list columns
		self.logging.debug("Nullifying with contains_any()")
		polars_df = polars_df.with_columns([
			pl.when(pl.col(col).str.contains_any(null_values.nulls_pl_contains_any, ascii_case_insensitive=True))
			.then(None)
			.otherwise(pl.col(col))
			.alias(col) for col in string_cols])
		polars_df = polars_df.with_columns([
			pl.col(col).list.eval(
				pl.element().filter(~pl.element().str.contains_any(null_values.nulls_pl_contains_any, ascii_case_insensitive=True))
			)
			for col in list_cols])

		self.logging.debug("Nullifying with contains()")
		contains_list = null_values.nulls_pl_contains if no_match_NA else null_values.nulls_pl_contains_plus_NA
		for null_value in contains_list:
			self.logging.debug(f"-->Nullifying {null_value}...")
			polars_df = polars_df.with_columns([
				pl.when(pl.col(col).str.contains(null_value))
				.then(None)
				.otherwise(pl.col(col))
				.alias(col) for col in string_cols])
			polars_df = polars_df.with_columns([
				pl.col(col).list.eval(
					pl.element().filter(~pl.element().str.contains(null_value))
				)
				for col in list_cols])
		return polars_df

	def print_col_where(self, polars_df, column="source", equals="Coscolla", cols_of_interest=kolumns.id_columns, everything=False):
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
		if not everything:
			cols_to_print = list(set([thingy for thingy in cols_of_interest if thingy in polars_df.columns] + [column]))
		else:
			cols_to_print = polars_df.columns
		with pl.Config(tbl_cols=-1, tbl_rows=40):
			print(filtah.select(cols_to_print))

	def get_count_of_x_in_column_y(self, polars_df, x, column_y):
		if x is not None:
			return polars_df.select((pl.col(column_y) == x).sum()).item()
		else:
			return polars_df.select((pl.col(column_y).is_null()).sum()).item()

	def print_only_where_col_list_is_big(self, polars_df, column_of_lists):
		if column_of_lists not in polars_df.columns:
			self.logging.warning(f"Tried to print {column_of_lists}, but that column isn't even in the dataframe!")
		elif polars_df.schema[column_of_lists] != pl.List:
			self.logging.warning(f"Tried to print where {column_of_lists} has multiple values, but that column isn't a list!")
		else:
			cols_of_interest = kolumns.id_columns + [column_of_lists]
			cols_to_print = [thingy for thingy in cols_of_interest if thingy in polars_df.columns]
			with pl.Config(tbl_cols=-1, tbl_rows=10, fmt_str_lengths=200, fmt_table_cell_list_len=10):
				print(polars_df.filter(pl.col(column_of_lists).list.len() > 1).select(cols_to_print))

	def print_only_where_col_not_null(self, polars_df, column, cols_of_interest=kolumns.id_columns):
		if column not in polars_df.columns:
			self.logging.warning(f"Tried to print where {column} is not null, but that column isn't even in the dataframe!")
		else:
			cols_of_interest.append(column)
			cols_to_print = [thingy for thingy in cols_of_interest if thingy in polars_df.columns]
			with pl.Config(tbl_cols=-1, tbl_rows=250, fmt_str_lengths=200, fmt_table_cell_list_len=10):
				print(polars_df.filter(pl.col(column).is_not_null()).select(cols_to_print))

	def mark_rows_with_value(self, polars_df, filter_func, true_value="M. avium complex", false_value='', new_column="bacterial_family", **kwargs):
		#polars_df = polars_df.with_columns(pl.lit("").alias(new_column))
		polars_df = polars_df.with_columns(
			pl.when(pl.col('organism').str.contains_any("Mycobacterium avium"))
			.then(pl.lit(true_value))
			.otherwise(pl.lit(false_value))
			.alias(new_column)
		)
		print(polars_df.select(pl.col(new_column).value_counts()))

		polars_df = polars_df.with_columns(
			pl.when(pl.col('organism').str.contains("Mycobacterium"))
			.then(pl.lit(true_value))
			.otherwise(pl.lit(false_value))
			.alias(new_column)
		)
		print(polars_df.select(pl.col(new_column).value_counts()))

	def print_value_counts(self, polars_df, only_these_columns=None, skip_ids=True):
		for column in polars_df.columns:
			if skip_ids and column not in kolumns.id_columns:
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

	def get_valid_columns_list_from_arbitrary_list(self, polars_df, desired_columns: list):
		return [col for col in desired_columns if col in polars_df.columns]

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

	def print_schema(self, polars_df):
		schema_df = pl.DataFrame({
			"COLUMN": [name for name, _ in df.schema.items()],
			"TYPE": [dtype.__name__ for _, dtype in df.schema.items()]
		})
		print(schema_df)

	def try_nullfill(self, polars_df, left_col, right_col):
		try:
			polars_df = polars_df.with_columns(pl.col(left_col).fill_null(pl.col(right_col)))
			status = True
		except pl.exceptions.InvalidOperationError:
			self.logging.debug("Could not nullfill (this isn't an error, nulls will be filled if pl.Ut8 or list[str])")
			status = False
		return polars_df, status

	def cast_to_list(self, polars_df, column):
		if polars_df[base_col].dtype != pl.List:
			return polars_df.with_columns(pl.col(base_col).cast(pl.List(str)))
		else:
			return polars_df

	def check_base_and_right_in_df(self, polars_df, left_col, right_col):
		if left_col not in polars_df.columns and not escalate_warnings:
			self.logging.warning(f"Found {right_col}, but {left_col} not in dataframe -- will drop that column and continue, but this may break things later")
			polars_df = polars_df.drop(right_col)
			return 1
		elif left_col not in polars_df.columns:
			self.logging.error(f"Found {right_col}, but {left_col} not in dataframe -- this is a sign something broke in an earlier function")
			exit(1)
		self.logging.debug(f"\n{left_col}: {polars_df[left_col].dtype}\n{right_col}: {polars_df[right_col].dtype}")
		return 0

	def concat_columns_list(self, polars_df, left_col, right_col, uniq):
		if uniq:
			polars_df = polars_df.with_columns(
				pl.when(pl.col(left_col) != pl.col(right_col))             # When a row has different values for base_col and right_col,
				.then(pl.concat_list([left_col, right_col]).list.unique()) # make a list of base_col and right_col, but keep only uniq values
				.otherwise(pl.concat_list([left_col]))                     # otherwise, make list of just base_col (doesn't seem to nest if already a list, thankfully)
				.alias(left_col)
			).drop(right_col)
		else:
			polars_df = polars_df.with_columns(
				pl.when(pl.col(left_col) != pl.col(right_col))             # When a row has different values for base_col and right_col,
				.then(pl.concat_list([left_col, right_col]))               # make a list of base_col and right_col,
				.otherwise(pl.concat_list([left_col]))                     # otherwise, make list of just base_col (doesn't seem to nest if already a list, thankfully)
				.alias(left_col)
			).drop(right_col)
		assert polars_df.select(pl.col(left_col)).dtypes == [pl.List]
		return polars_df


	def merge_right_columns(self, polars_df, fallback_on_left=True, escalate_warnings=True):
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
		index_column = self.get_index_column(polars_df)
		assert index_column not in right_columns
		for right_col in right_columns:
			self.logging.debug(f"[{right_columns.index(right_col)}/{len(right_columns)}] Trying to merge {right_col}...")
			base_col, nullfilled = right_col.replace("_right", ""), False
			if self.check_base_and_right_in_df(polars_df, base_col, right_col) == 1:
				continue
			
			polars_df, nullfilled = self.try_nullfill(polars_df, base_col, right_col)
			try:
				# TODO: this breaks in situations like when we add Brites before Bos, since Brites has three run accessions with no sample_index,
				# resulting in assertionerror but no printed conflicts
				assert_series_equal(polars_df[base_col], polars_df[right_col].alias(base_col))
				polars_df = polars_df.drop(right_col)
				self.logging.debug(f"All values in {base_col} and {right_col} are the same after an filling in each other's nulls. Dropped {right_col}.")
				continue
			except AssertionError:
				self.logging.debug(f"Not equal after filling in nulls (or nullfill errored so they're definitely not equal)")
			
			# everything past this point in this for loop only fires if the assertion error happened!
			if base_col in kolumns.special_taxonomic_handling:
				self.logging.warning(f"[kolumns.special_taxonomic_handling] {base_col} --> Falling back on {base_col if fallback_on_left else right_col}")
				polars_df = polars_df.drop(right_col) if fallback_on_left else polars_df.drop(base_col).rename({right_col: base_col})
			
			elif base_col in kolumns.list_throw_error:
				self.logging.error(f"[kolumns.list_throw_error] {base_col} --> Fatal error. There should never be lists in this column.")
				print_cols = [base_col, right_col, index_column, self.cfg.indicator_column] if self.cfg.indicator_column in polars_df.columns else [base_col, right_col, index_column]
				self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select(print_cols), f"conflicts")
				exit(1)
			
			elif base_col in kolumns.list_fallback_or_null:
				if escalate_warnings:
					self.logging.error(f"[list_fallback_or_null] {base_col} --> Fatal error due to escalate_warnings=True")
					self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select([base_col, right_col, index_column]), f"conflicts")
					exit(1)
				else:
					self.logging.warning(f"[kolumns.list_fallback_or_null] {base_col} --> Falling back on {base_col if fallback_on_left else right_col}")
					polars_df = polars_df.drop(right_col) if fallback_on_left else polars_df.drop(base_col).rename({right_col: base_col})
			
			elif base_col in kolumns.list_to_null:
				self.logging.debug(f"[kolumns.list_to_null] {base_col} --> Conflicts turned to null")
				polars_df = polars_df.with_columns([
					pl.when(polars_df[right_col] == polars_df[base_col]).then(pl.col(right_col)).otherwise(None).alias(right_col),
					pl.when(polars_df[right_col] == polars_df[base_col]).then(pl.col(base_col)).otherwise(None).alias(base_col),
				])
				if not nullfilled:
					polars_df, nullfilled = self.try_nullfill(polars_df, base_col, right_col) # small chance more nulls solved the issue
				polars_df = polars_df.drop(right_col)

			elif base_col in kolumns.list_to_float_sum:
				self.logging.error("TODO NOT IMPLEMENTED")
				exit(1)

			elif base_col in kolumns.list_to_list_silent:
				self.logging.debug(f"[kolumns.list_to_list_silent] {base_col} --> concat_list")
				if not nullfilled:
					polars_df = self.cast_to_list(polars_df, base_col)
					polars_df = self.cast_to_list(polars_df, right_col)
				polars_df = self.concat_columns_list(polars_df, base_col, right_col, False)

			elif base_col in kolumns.list_to_set_uniq:
				self.logging.debug(f"[kolumns.list_to_set_uniq] {base_col} --> concat_list only uniq")
				if not nullfilled:
					polars_df = self.cast_to_list(polars_df, base_col)
					polars_df = self.cast_to_list(polars_df, right_col)
				polars_df = self.concat_columns_list(polars_df, base_col, right_col, True)

			else:
				self.logging.debug(f"[not in kolumns] {base_col} --> concat_list only uniq")
				if not nullfilled:
					polars_df = self.cast_to_list(polars_df, base_col)
					polars_df = self.cast_to_list(polars_df, right_col)
				polars_df = self.concat_columns_list(polars_df, base_col, right_col, True)

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
			polars_df = self.iteratively_merge_these_columns(polars_df, merge_these_columns, recursion_depth=recursion_depth+1)
		return polars_df.rename({left_col: equivalence_key}) if equivalence_key is not None else polars_df

	def get_rows_where_list_col_more_than_one_value(self, polars_df, list_col):
		""" See also print_only_where_col_list_is_big()
		The .and_() is necessary for now due to https://github.com/pola-rs/polars/issues/19987"""
		assert polars_df.schema[list_col] == pl.List
		return polars_df.filter(pl.col(list_col).list.len().and_(pl.col(list_col).list.first().is_not_null()) > 1)

	def get_paired_illumina(self, polars_df, inverse=False):
		rows_before = polars_df.shape[0]
		if 'librarysource' in polars_df.columns and 'platform' in polars_df.columns:
			if polars_df.schema['platform'] == pl.Utf8 and polars_df.schema['librarylayout'] == pl.Utf8:
				if not inverse:
					self.logging.info("Filtering data to include only PE Illumina reads")
					polars_df = polars_df.filter(
						(pl.col('platform') == 'ILLUMINA') & 
						(pl.col('librarylayout') == 'PAIRED')
					)
					self.logging.info(f"Excluded {rows_before-polars_df.shape[0]} rows of non-paired/non-Illumina data")
				else:
					self.logging.info("Filtering data to exclude PE Illumina reads")
					polars_df = polars_df.filter(
						(pl.col('platform') != 'ILLUMINA') & 
						(pl.col('librarylayout') != 'PAIRED')
					)
					self.logging.info(f"Excluded {rows_before-polars_df.shape[0]} rows of PE Illumina data")
			else:
				self.logging.warning("Failed to filter out non-PE Illumina as platform and/or librarylayout columns aren't type string")
		else:
			self.logging.warning("Failed to filter out non-PE Illumina as platform and/or librarylayout columns aren't present")
		return polars_df

	def unique_bioproject_per_center_name(self, polars_df: pl.DataFrame, center_name="FZB"):
		return (
			polars_df.filter(pl.col("center_name") == center_name)
			.select("BioProject").unique().to_series().to_list()
		)

	def get_valid_id_columns(self, polars_df):
		return self.get_valid_columns_list_from_arbitrary_list(polars_df, kolumns.id_columns)
	
	def rancheroize_polars(self, polars_df, drop_non_mycobact_columns=True, nullify=True, flatten=True, disallow_right=True, check_index=True):
		self.logging.debug(f"Dataframe shape before rancheroizing: {polars_df.shape[0]}x{polars_df.shape[1]}")
		polars_df = self.drop_known_unwanted_columns(polars_df)
		self.get_null_count_in_column(polars_df, self.get_index_column(polars_df), warn=True, error=True)
		if drop_non_mycobact_columns:
			polars_df = self.drop_non_tb_columns(polars_df)
		if nullify:
			polars_df = self.drop_null_columns(self.nullify(polars_df))
			self.get_null_count_in_column(polars_df, self.get_index_column(polars_df), warn=True, error=True)
		if flatten:
			polars_df = self.flatten_all_list_cols_as_much_as_possible(polars_df, force_strings=False) # this makes merging better for "geo_loc_name_sam"
		if disallow_right:
			assert len([col for col in polars_df.columns if col.endswith("_right")]) == 0
		if self.cfg.paired_illumina_only:
			polars_df = self.get_paired_illumina(polars_df)

		# check date columns, our arch-nemesis
		for column in polars_df.columns:
			if column in kolumns.equivalence['date_collected']:
				if polars_df[column].dtype is not pl.Date:
					self.logging.debug(f"Found likely date column {column}, but it has type {polars_df[column].dtype}")
				else:
					self.logging.debug(f"Likely date column {column} has pl.Date type")

		for key, value in kolumns.equivalence.items():
			merge_these_columns = [v_col for v_col in value if v_col in polars_df.columns and v_col not in sum(kolumns.special_taxonomic_handling.values(), [])]
			if len(merge_these_columns) > 0:
				self.logging.debug(f"Discovered {key} in column via:")
				for some_column in merge_these_columns:
					self.logging.debug(f"  * {some_column}: {polars_df.schema[some_column]}")

				if len(merge_these_columns) > 1:
					#polars_df = polars_df.with_columns(pl.implode(merge_these_columns)) # this gets sigkilled; don't bother!
					if key in drop_zone.silly_columns:
						polars_df = polars_df.drop(col)
					#don't add kolumns.list_to_float_sum here, that's not what it's made for and it'll cause errors
					else:
						self.logging.info(f"  Merging these columns: {merge_these_columns}")
						polars_df = self.iteratively_merge_these_columns(polars_df, merge_these_columns, equivalence_key=key)
				else:
					self.logging.debug(f"  Renamed {merge_these_columns[0]} to {key}")
					polars_df = polars_df.rename({merge_these_columns[0]: key})
				assert key in polars_df.columns
		
		# do not flatten list cols again, at least not yet. use the equivalence columns for standardization.
		self.check_index(polars_df)
		self.logging.debug(f"Dataframe shape after rancheroizing: {polars_df.shape[0]}x{polars_df.shape[1]}")
		return polars_df

	def is_sample_indexed(self, polars_df):
		index = self.get_index_column(polars_df)
		return True if index in kolumns.equivalence['sample_index'] else False

	def is_run_indexed(self, polars_df):
		index = self.get_index_column(polars_df)
		return True if index in kolumns.equivalence['run_index'] else False

	def get_index_column(self, polars_df, quiet=False):
		"""
		Does NOT check for duplicates in the index column(s)
		"""
		sample_indeces = kolumns.equivalence['sample_index']
		sample_matches = [col for col in sample_indeces if col in polars_df.columns]
		run_indeces = kolumns.equivalence['run_index']
		run_matches = [col for col in run_indeces if col in polars_df.columns]

		# more than one sample index, arbitrary number of run indeces
		if len(sample_matches) > 1:
			if not quiet:
				raise ValueError(f"Tried to find dataframe index, but there's multiple possible sample indeces: {sample_matches}")
			else:
				return [2, sample_matches]
	
		# one sample index, arbitrary number of run indeces
		elif len(sample_matches) == 1:
			if len(run_matches) > 1:
				if not quiet:
					raise ValueError(f"Tried to find dataframe index, but there's multiple possible run indeces (may indicate failed run->sample conversion):  {run_matches}")
				else:
					return [3, run_matches]
			
			elif len(run_matches) == 1:
				if polars_df.schema[run_matches[0]] == pl.List:
					return str(sample_matches[0])
				else:
					return str(run_matches[0])

			else:
				return str(sample_matches[0])  # no run indeces, just one sample index

		# no sample index, multiple run indeces
		elif len(run_matches) > 1:
			if not quiet:
				raise ValueError(f"Tried to find dataframe index, but there's multiple possible run indeces: {run_matches}")
			else:
				return [4, run_matches]
		
		# no sample index, one run index
		elif len(run_matches) == 1:
			return str(run_matches[0])

		else:
			if not quiet:
				raise ValueError(f"No valid index column found in polars_df! Columns available: {polars_df.columns}")
			else:
				return [5]

	def coerce_to_not_list_if_possible(self, polars_df, column, index_column, prefix_arrow=False):
		arrow = '-->' if prefix_arrow else ''
		assert column != index_column
		if len(self.get_rows_where_list_col_more_than_one_value(polars_df, column)) == 0:
			self.logging.debug(f"{arrow}No rows of {column} have a length more than one, converting to inner type")
			return polars_df.with_columns(pl.col(column).list.first().alias(column))
		else:
			if self.logging.getEffectiveLevel() == 10:
				debug_print = self.get_rows_where_list_col_more_than_one_value(polars_df, column)
				self.logging.debug(f"-->{len(debug_print)} multi-element lists in {column}")
				self.super_print_pl(debug_print.select([index_column, f"{column}"]).head(30), f"list cols with more than one value (true len {len(debug_print)})")
			return polars_df

	def flatten_list_col_as_set(self, polars_df, column):
		polars_df = self.flatten_one_nested_list_col(polars_df, column) # recursive
		index_column = self.get_index_column(polars_df)
		polars_df = polars_df.with_columns(pl.col(column).list.unique().alias(f"{column}"))
		polars_df = self.coerce_to_not_list_if_possible(polars_df, column, index_column)
		return polars_df

	def flatten_all_list_cols_as_much_as_possible(self, polars_df, hard_stop=False, force_strings=False):
		"""
		Flatten list columns as much as possible. If a column is just a bunch of one-element lists, for
		instance, then just take the 0th value of that list and make a column that isn't a list.

		If force_strings, any remaining columns that are still lists are forced into strings.
		"""
		# Do not run check index first, as it will break when this is run right after run-to-sample conversion
		index_column = self.get_index_column(polars_df)

		# unnest nested lists (recursive)
		self.logging.debug("Recursively unnesting lists...")
		polars_df = self.flatten_nested_list_cols(polars_df)
		self.logging.debug("Unnested lists. Index seems okay.")
		what_was_done = []
		
		for col, datatype in polars_df.schema.items():
			#self.logging.debug(f"Evaulating on {col} of type {datatype}") # summary table at the end should suffice
			
			if col in drop_zone.silly_columns:
				polars_df.drop(col)
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': pl.Null, 'result': 'dropped'})
				continue
			
			if datatype == pl.List and datatype.inner != datetime.datetime:
				
				if polars_df[col].drop_nulls().shape[0] == 0:
					#self.logging.warning(f"{col} has datatype {datatype} but seems empty or contains only nulls, skipping...")
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (empty/nulls)'})
					continue

				if col in kolumns.list_to_float_sum:
					self.logging.debug(f"{col}\n-->[[kolumns.list_to_float_sum]")
					if datatype.inner == pl.String:
						self.logging.debug(f"-->Inner type is string, casting to pl.Int32 first")
						polars_df = polars_df.with_columns(
							pl.col(col).list.eval(
								pl.when(pl.element().is_not_null())
								.then(pl.element().cast(pl.Int32))
								.otherwise(None)
							).alias(f"{col}_sum")
						)
					else:
						polars_df = polars_df.with_columns(pl.col(col).list.sum().alias(f"{col}_sum"))
					polars_df = polars_df.drop(col)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[f"{col}_sum"], 'result': 'namechange + summed'})
					continue
				
				elif col in kolumns.list_to_list_silent:
					self.logging.debug(f"{col}\n-->[[kolumns.list_to_list_silent]")
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': '.'})
					continue

				elif col in kolumns.list_to_null:
					self.logging.debug(f"{col}\n-->[[kolumns.list_to_null]")
					polars_df = polars_df.with_columns([
						pl.when(pl.col(col).list.len() <= 1).then(pl.col(col)).otherwise(None).alias(col)
					])
					self.logging.debug(f"-->Set null in conflicts, now trying to delist")
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'null conflicts'})
					continue
				
				elif col in kolumns.list_to_set_uniq: 
					self.logging.debug(f"{col}\n-->[[kolumns.list_to_set_uniq]")
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					self.logging.debug("-->Used uniq, now checking if we can delist")
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set-and-shrink'})
					continue
					
				elif col in kolumns.list_fallback_or_null:
					# If this had happened during a merge of two dataframes, we would be falling back on one df or the other. But here, we
					# don't know what value to fall back upon, so it's better to just null this stuff.
					self.logging.warning(f"{col}\n-->[kolumns.list_fallback_or_null] Expected {col} to only have one non-null per sample, but that's not the case. Will null those bits.")
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					polars_df = polars_df.with_columns([
						pl.when(pl.col(col).list.len() <= 1).then(pl.col(col)).otherwise(None).alias(col)
					])
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					#assert len(self.get_rows_where_list_col_more_than_one_value(polars_df, col, False)) == 0 # https://github.com/pola-rs/polars/issues/19987
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set-and-shrink (!!!WARNING!!)'})
					if hard_stop:
						exit(1)
					else:
						continue
				else:
					self.logging.warning(f"-->Not sure how to handle {col}, will treat it as a set")
					polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set (no rules)'})
					continue

			elif datatype == pl.List and datatype.inner == datetime.date:
				self.logging.warning(f"{col} is a list of datetimes. Datetimes break typical handling of lists, so this column will be left alone.")
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (date.date)'})
			
			else:
				#self.logging.debug(f"-->Leaving {col} as-is")
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': '-'})
		
		if force_strings:
			polars_df = self.stringify_all_list_columns(polars_df)
		
		report = pl.DataFrame(what_was_done)
		if self.logging.getEffectiveLevel() <= 10:
			NeighLib.super_print_pl(report, "Finished flattening list columns. Results:")
		return polars_df

	def rstrip(self, polars_df, column, strip_char=" "):
		return polars_df.with_columns([
			pl.when(pl.col(column).str.ends_with(" "))
			.then(pl.col(column).str.slice(-1))
			.otherwise(pl.col(column))
			.alias(column)
		])
	
	def recursive_rstrip(self, polars_df, column, strip_char=" "):
		while polars_df[column].str.ends_with(" ").any():
			self.logging.info("Recursing...")
			polars_df = self.rstrip(polars_df, column, strip_char)
		return polars_df

	def check_index(self, polars_df, 
			manual_index_column=None, 
			rm_dupes=_cfg_rm_dupes, 
			force_NCBI_runs=_cfg_force_SRR_ERR_DRR_run_index, 
			force_BioSamples=_cfg_force_SAMN_SAME_SAMD_sample_index,
			rstrip=False, # VERY SLOW AND NOT WELL TESTED!
			force_str_index=True):
		"""
		Check a polars dataframe's apparent index, which is expected to be either run accessions or sample accessions, for the following issues:
		* pl.null/None values
		* duplicates
		* incompatiable index columns (eg, two run index columns)

		Unless manual_index_column is not none, this function will use kolumns.equivalence to figure out what your index column(s) are.
		"""
		rm_dupes = self._sentinal_handler(_cfg_rm_dupes)
		force_NCBI_runs = self._sentinal_handler(_cfg_force_SRR_ERR_DRR_run_index)
		force_BioSamples = self._sentinal_handler(_cfg_force_SAMN_SAME_SAMD_sample_index)
		apparent_index_column = self.get_index_column(polars_df)
		
		if force_str_index:
			assert polars_df.schema[apparent_index_column] == pl.Utf8

		if rstrip:
			polars_df = self.recursive_rstrip(polars_df, apparent_index_column, strip_char=" ")

		if manual_index_column is not None:
			if manual_index_column not in polars_df.columns:
				self.logging.error(f"manual_index_column is {manual_index_column}, but that column isn't in the dataframe!")
				raise ValueError
			elif manual_index_column != apparent_index_column:
				self.logging.error(f"Manual index column set to {apparent_index_column}, which is in the dataframe, but there's additional index columns too: {apparent_index_column}")
				self.logging.error("Consider dropping these columns before proceeding further with this dataframe, or adjusting kolumns.equivalence as needed.")
				raise ValueError
			else:
				index_column = manual_index_column # this is valid, so override the get_index_column() result so we can properly check for dupes
		else:
			index_column = apparent_index_column
		
		if type(index_column) == list:
			if index_column[0] == 2:
				self.logging.error(f"Dataframe has multiple possible sample based indeces: {index_column[1]}")
				raise ValueError
			elif index_column[0] == 3:
				# in theory you could get away with this, since there is a sample index, but I won't support that
				self.logging.error(f"Dataframe has multiple possible run indeces: {index_column[1]}")
				raise ValueError
			elif index_column[0] == 4:
				self.logging.error(f"Dataframe has multiple possible run indeces: {index_column[1]}")
				raise ValueError
			elif index_column[0] == 5:
				self.logging.error(f"Could not find any valid index column. You can set valid index columns in kolumns.py's equivalence dictionary.")
				self.logging.error(f"Current possible run index columns (key for kolumns.equivalence['run_index']): {kolumns.equivalence['run_index']}")
				self.logging.error(f"Current possible sample index columns (key for kolumns.equivalence['sample_index']): {kolumns.equivalence['sample_index']}")
				self.logging.error(f"Your dataframe's columns: {polars_df.columns}")
				raise ValueError
			else:
				raise ValueError # who knows what happened in get_index_column()?
		
		# at this point index_column can only be a string
		self.logging.debug(f"Index column appears to be {index_column}")
		assert polars_df.schema[index_column] != pl.List
		subset = polars_df.unique(subset=[index_column], keep="first")
		duplicates = polars_df.filter(polars_df[index_column].is_duplicated())
		if len(polars_df) != len(polars_df.unique(subset=[index_column], keep="first")):
			run_or_sample = 'run_index' if index_column in kolumns.equivalence['run_index'] else 'sample_index'
			if rm_dupes:
				self.logging.warning(f"Dataframe has {len(polars_df) - len(subset)} duplicates in {run_or_sample} column named {index_column} -- will attempt to remove them (THIS MAY LEAD TO DATA LOSS)")
				self.logging.warning(f"Duplicates: {duplicates.select(index_column)}")
				return subset
			else:
				self.logging.error(f"Dataframe has {len(polars_df) - len(subset)} duplicates in {run_or_sample} column named {index_column} -- not removing as per cfg perferences")
				raise ValueError
		nulls = self.get_null_count_in_column(polars_df, index_column, warn=False, error=False)
		if nulls > 0:
			self.logging.error(f"Found {nulls} null value(s) in index column {index_column}:")
			print(polars_df.with_columns(pl.when(pl.col(col_b).is_null())))
			raise ValueError
		
		# if applicable, make sure there's no nonsense in our index columns -- also, we're checking run AND sample columns if both are present,
		# to prevent issues if we do a run-to-sample conversion later
		# also, thanks to earlier checks, we know there should only be a maximum of one sample index and one run index.
		for column in polars_df.columns:
			if column in kolumns.equivalence['sample_index'] and force_BioSamples:
				good = (
					polars_df[column].str.starts_with("SAMN") |
					polars_df[column].str.starts_with("SAME") |
					polars_df[column].str.starts_with("SAMD")
				)
				invalid_rows = polars_df.filter(~good)
				valid_rows = polars_df.filter(good)
				if len(invalid_rows) > 0:
					self.logging.warning("Found samples that don't start with SAMN/SAME/SAMD (will be dropped):")
					print(invalid_rows)
					return valid_rows
			elif column in kolumns.equivalence['run_index'] and force_NCBI_runs and polars_df.schema[column] != pl.List:
				good = (
					polars_df[column].str.starts_with("SRR") |
					polars_df[column].str.starts_with("ERR") |
					polars_df[column].str.starts_with("DRR")
				)
				invalid_rows = polars_df.filter(~good)
				valid_rows = polars_df.filter(good)
				if len(invalid_rows) > 0:
					self.logging.warning("Found runs that don't start with SRR/ERR/DRR (will be dropped):")
					print(invalid_rows)
					return valid_rows
			else:
				continue
		return 0

	def drop_non_tb_columns(self, polars_df):
		dont_drop_these = [col for col in polars_df.columns if col not in drop_zone.clearly_not_tuberculosis]
		return polars_df.select(dont_drop_these)

	def drop_known_unwanted_columns(self, polars_df):
		return polars_df.select([col for col in polars_df.columns if col not in drop_zone.silly_columns])

	def flatten_one_nested_list_col(self, polars_df, column):
		if polars_df[column].schema == pl.List(pl.List):
			polars_df = polars_df.with_columns(pl.col(column).list.eval(pl.element().flatten().drop_nulls()))
		if polars_df[column].schema == pl.List(pl.List):
			polars_df = self.flatten_one_nested_list_col(polars_df, column)
		return polars_df


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

	def add_column_of_just_this_value(self, polars_df, column, value):
		assert column not in polars_df.columns
		return polars_df.with_columns(pl.lit(value).alias(column))

	def drop_column(self, polars_df, column):
		assert column in polars_df.columns
		return polars_df.drop(column)

	def drop_null_columns(self, polars_df):
		""" Drop columns of type null or list(null) """
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
