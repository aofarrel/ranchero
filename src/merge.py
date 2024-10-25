from .config import RancheroConfig
from . import _NeighLib as NeighLib
import polars as pl
from polars.testing import assert_series_equal

# TODO: set merge column to indicator column in config, and set rancheroize to use that column too

class Merger:

	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger

	def aggregate_conflicting_metadata(self, polars_df, column_key):
		"""
		Returns a numeric representation of n_unique values for rows that have matching column_key values. This representation can later
		be semi-merged backed to the original data if you want the real data.
		"""
		from functools import reduce
		# this works if there aren't already any lists, but panics otherwise
		#agg_values = polars_df.group_by(column_key).agg([pl.col(c).n_unique().alias(c) for c in polars_df.columns if c != column_key])
		agg_values = polars_df.group_by(column_key).agg([999 if polars_df.schema[c] == pl.List else pl.col(c).n_unique().alias(c) for c in polars_df.columns if c != column_key])
		NeighLib.super_print_pl(agg_values, "agg_values")

		# to match in cool_rows, ALL rows must have a value of 1
		# to match in uncool_rows, ANY rows must have a value of not 1
		cool_rows = agg_values.filter(pl.col(c) == 1 for c in agg_values.columns if c != column_key).sort(column_key)
		uncool_rows = agg_values.filter(reduce(lambda acc, expr: acc | expr, (pl.col(c) != 1 for c in agg_values.columns if c != column_key)))

		# to get the original data for debugging purposes you can use: semi_rows = polars_df.join(uncool_rows, on="run_index", how="semi")
		return uncool_rows

	def get_columns_with_any_row_above_1(self, polars_df, column_key):
		"""
		Designed to be run on the uncool_rows output of aggregate_conflicting_metadata()
		"""
		filtered_uncool_rows = polars_df.select(
			[pl.col(column_key)] + [
				pl.col(c) for c in polars_df.columns 
				if c != column_key and polars_df.select(pl.col(c) > 1).to_series().any()
			]
		)
		return filtered_uncool_rows

	def get_partial_self_matches(self, polars_df, column_key: str):
		"""
		Reports all columns of all rows where (1) at least two rows share a key and (2) at least one column between rows with a matching
		key has a mismatch.
		"""
		# the agg table method is preferred, however, it doesn't work if the rows we're combining contain lists

		agg_table = self.aggregate_conflicting_metadata(polars_df, column_key)
		columns_we_will_merge_and_their_column_keys = self.get_columns_with_any_row_above_1(agg_table, column_key)  # type: polars df
		will_be_catagorical = columns_we_will_merge_and_their_column_keys.columns  # type: list
		will_be_catagorical.remove(column_key)
		print(f"--> {len(agg_table)} {column_key}s (rows) have conflicting data")
		print(f"--> {len(will_be_catagorical)} fields (columns) will need to become lists:")
		print(will_be_catagorical)

		assert column_key in columns_we_will_merge_and_their_column_keys.columns
		assert column_key in polars_df.columns
		for catagorical in will_be_catagorical:
			assert catagorical in polars_df.columns

		restored_data = polars_df.join(columns_we_will_merge_and_their_column_keys, on="run_index", how="semi") # get our real data back (eg, not agg integers)
		restored_catagorical_data = restored_data.group_by(column_key).agg([pl.col(column).alias(column) for column in restored_data.columns if column != column_key and column in will_be_catagorical])
		NeighLib.super_print_pl(restored_catagorical_data, "restored catagorical data")

		return restored_catagorical_data

	def check_if_unexpected_rows(self,
		merged_df, 
		merge_upon,
		intersection_values, 
		exclusive_left_values, 
		exclusive_right_values, 
		n_rows_left, 
		n_rows_right,
		right_name,
		right_name_in_this_column):
		n_rows_merged = merged_df.shape[0]
		n_row_expected = sum([len(intersection_values), len(exclusive_left_values), len(exclusive_right_values)])

		# we expect n_rows_merged = intersection_values + exclusive_left_values + exclusive_right_values
		if n_rows_merged == n_row_expected:
			return
		else:
			print("-------")
			self.logging.warning(f"Expected {n_row_expected} rows in merged dataframe but got {n_rows_merged}")
			if right_name_in_this_column is not None:
				self.logging.debug("%s n_rows_right (%s exclusive)" % (n_rows_right, len(exclusive_right_values)))
				self.logging.debug("%s n_rows_left (%s exclusive)" % (n_rows_left, len(exclusive_left_values)))
				self.logging.debug("%s intersections" % len(intersection_values))
				if merged_df.schema[right_name_in_this_column] == pl.Utf8:
					self.logging.debug("%s rows have right_name in indicator column" % len(merged_df.filter(pl.col(right_name_in_this_column) == right_name)))
					self.logging.debug("%s has right_name and in intersection" % len(merged_df.filter(pl.col(right_name_in_this_column) == right_name, pl.col(merge_upon).is_in(intersection_values))))
					self.logging.debug("%s has right_name and in exclusive left" % len(merged_df.filter(pl.col(right_name_in_this_column) == right_name, pl.col(merge_upon).is_in(exclusive_left_values))))
					self.logging.debug("%s has right_name and in exclusive right" % len(merged_df.filter(pl.col(right_name_in_this_column) == right_name, pl.col(merge_upon).is_in(exclusive_right_values))))
				elif merged_df.schema[right_name_in_this_column] == pl.List:
					self.logging.debug("%s rows have right_name in indicator column" % len(merged_df.filter(pl.col(right_name_in_this_column).list.contains(right_name))))
					self.logging.debug("%s has right_name and in intersection" % len(merged_df.filter(pl.col(right_name_in_this_column).list.contains(right_name), pl.col(merge_upon).is_in(intersection_values))))
					self.logging.debug("%s has right_name and in exclusive left" % len(merged_df.filter(pl.col(right_name_in_this_column).list.contains(right_name), pl.col(merge_upon).is_in(exclusive_left_values))))
					self.logging.debug("%s has right_name and in exclusive right" % len(merged_df.filter(pl.col(right_name_in_this_column).list.contains(right_name), pl.col(merge_upon).is_in(exclusive_right_values))))
			duplicated_indices = merged_df.filter(pl.col(merge_upon).is_duplicated())
			if len(duplicated_indices) > 0:
				self.logging.error(f"Found {len((duplicated_indices).unique())} duplicated values in column {merge_upon} -- why didn't they merge?")
				print(duplicated_indices.unique())
				exit(1)
			else:
				self.logging.info("Did not find duplicated values in the merge column, indicating that the right may just be adding new values.")
			print("-------")

	def merge_polars_dataframes(self, 
		left: pl.dataframe.frame.DataFrame, 
		right: pl.dataframe.frame.DataFrame, 
		merge_upon: str, left_name ="left", right_name="right", put_right_name_in_this_column=None,
		fallback_on_left=True):
		"""
		Merge two polars dataframe upon merge_upon. 

		
		put_right_name_in_this_column: If not None, adds a row of right_name to the dataframe. Designed for marking the source of data when
		merging dataframes multiple times. If right_name is None, a right_name column will be created temporarily but 
		dropped before returning.
		"""
		self.logging.info(f"Merging {left_name} and {right_name} upon {merge_upon}")
		n_rows_left, n_rows_right = left.shape[0], right.shape[0]
		n_cols_left, n_cols_right = left.shape[1], right.shape[1]
		assert n_rows_left != 0 and n_rows_right != 0
		assert n_cols_left != 0 and n_cols_right != 0

		left = NeighLib.drop_null_columns(left)
		right = NeighLib.drop_null_columns(right)

		for df, name in zip([left,right], [left_name,right_name]):
			if merge_upon not in df.columns:
				raise ValueError(f"Attempted to merge dataframes upon {merge_upon}, but no column with that name in {name} dataframe")
			if merge_upon == 'run_index' or merge_upon == 'run_accession':
				if not NeighLib.is_run_indexed(df):
					self.logging.warning(f"Merging upon {merge_upon}, which looks like a run accession, but {name} dataframe appears to not be indexed by run accession")
			if len(df.filter(pl.col(merge_upon).is_null())[merge_upon]) != 0:
				print(df.filter(pl.col(merge_upon).is_null()))
				raise ValueError(f"Attempted to merge dataframes upon shared column {merge_upon}, but the {name} dataframe has {len(left.filter(pl.col(merge_upon).is_null())[merge_upon])} nulls in that column")

		left_values, right_values = left[merge_upon], right[merge_upon]
		intersection = left_values.is_in(right_values)
		intersection_values = left.filter(intersection).select(merge_upon)
		exclusive_left, exclusive_right = ~left_values.is_in(right_values), ~right_values.is_in(left_values)
		exclusive_left_values, exclusive_right_values = left.filter(exclusive_left).select(merge_upon), right.filter(exclusive_right).select(merge_upon)
		if len(intersection) == 0:
			print(f"WARNING: No values in {merge_upon} are shared across the dataframes")
		self.logging.info(f"--> Intersection: {len(intersection_values)}")
		self.logging.info(f"--> Exclusive to {left_name}: {len(exclusive_left_values)}")
		self.logging.info(f"--> Exclusive to {right_name}: {len(exclusive_right_values)}")
		if len(exclusive_right_values) > 0:
			print_me = exclusive_right_values[:10] if len(exclusive_right_values) > 10 else exclusive_right_values
			self.logging.info(f"Some values exclusive to {right_name}: {print_me}")

		# TODO: this is here just so we have better testing of list merges, but later it's probably better to just
		# put something like this at the end by concat_list()ing pl.lit() the name into the column
		# ie, right['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		if put_right_name_in_this_column is not None:
			if put_right_name_in_this_column not in left.columns:
				self.logging.debug("No indicator column in left")
				left = left.with_columns(pl.lit(left_name).alias(put_right_name_in_this_column))
			else:
				self.logging.debug("Already an indicator in left")
				
			right = right.with_columns(pl.lit(right_name).alias(put_right_name_in_this_column))
			n_cols_right = right.shape[1]
			n_cols_left = left.shape[1]

		shared_columns = NeighLib.get_dupe_columns_of_two_polars(left, right, assert_shared_cols_equal=False)
		shared_columns.remove(merge_upon)
		left_list_cols = [col for col, dtype in zip(left.columns, left.dtypes) if dtype == pl.List]
		right_list_cols = [col for col, dtype in zip(right.columns, right.dtypes) if dtype == pl.List]

		if len(shared_columns) == 0:
			self.logging.debug("These dataframes do not have any columns in common.")
			# actually merge
			if n_cols_right == n_cols_left:
				initial_merge = left.sort(merge_upon).merge_sorted(right.sort(merge_upon), merge_upon).unique().sort(merge_upon)
				self.logging.info(f"Merged a {n_rows_left} row dataframe with a {n_rows_right} rows dataframe. Final dataframe has {initial_merge.shape[0]} rows (difference: {initial_merge.shape[0] - n_rows_left})")
				merged_dataframe = initial_merge
			else:
				merged_dataframe = left.join(right, merge_upon, how="outer_coalesce").unique()

			self.logging.debug("End of merge")
			#NeighLib.print_col_where(merged_dataframe, "run_index", "SRR1013561")

		elif len(left_list_cols) == 0 and len(right_list_cols) == 0:
			self.logging.debug(f"Neither {left_name} nor {right_name} have columns of type pl.List")

			nullfilled_left = left.join(right, on=merge_upon, how="left").with_columns(
					[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in left.columns if col != merge_upon and col in right.columns]
			).select(left.columns).sort(merge_upon)
			nullfilled_right = right.join(left, on=merge_upon, how="left").with_columns(
				[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in right.columns if col != merge_upon and col in left.columns]
			).select(right.columns).sort(merge_upon)
			nullfilled_left = NeighLib.drop_null_columns(nullfilled_left)
			nullfilled_right = NeighLib.drop_null_columns(nullfilled_right)
		
			if set(left.columns) == set(right.columns):
				self.logging.debug("Set of left and right columns match")
				initial_merge = nullfilled_left.join(nullfilled_right, merge_upon, how="outer_coalesce").unique().sort(merge_upon)
				really_merged = NeighLib.merge_right_columns(initial_merge)
			else:
				self.logging.debug("Set of left and right columns DO NOT match")
				initial_merge = nullfilled_left.join(nullfilled_right, merge_upon, how="outer_coalesce").unique().sort(merge_upon)
				really_merged = NeighLib.merge_right_columns(initial_merge)
			
			# update left values and right values for later debugging
			left_values, right_values = nullfilled_left[merge_upon], nullfilled_right[merge_upon]
			exclusive_left, exclusive_right = ~left_values.is_in(right_values), ~right_values.is_in(left_values)

			really_merged_no_dupes = really_merged.unique()
			self.logging.info(f"Merged a {n_rows_left} row dataframe with a {n_rows_right} rows dataframe. Final dataframe has {really_merged_no_dupes.shape[0]} rows (difference: {really_merged_no_dupes.shape[0] - n_rows_left})")
			merged_dataframe = really_merged_no_dupes
			self.logging.debug("End of merge")

		else:
			# shared list columns will be concatenated (such as the put_right_name_in_this_column column)
			self.logging.debug("Found columns in common and also some list columns")

			yargh = list(left.columns)
			yargh.remove(merge_upon)

			for left_column in yargh:
				if left_column in right.columns:
					if left.select(left_column).dtypes == [pl.List(pl.String)]: # TODO: get this to work on integers
						if right.select(left_column).dtypes == [pl.List(pl.String)]:
							self.logging.debug(f"* {left_column}: LIST | LIST")

							# let merge_right_columns() handle it

						else:
							self.logging.debug(f"* {left_column}: LIST | SING")
							small_left = left.select([merge_upon, left_column])
							small_right = right.select([merge_upon, left_column])
							temp = small_left.join(small_right, merge_upon, how="outer_coalesce")
							#if merge_upon == "run_index":
							#	NeighLib.print_col_where(small_left, "run_index", "ERR751929")
							#	NeighLib.print_col_where(small_right, "run_index", "ERR751929")
							#	NeighLib.print_col_where(temp, "run_index", "ERR751929")
							#if logging.root.level == logging.DEBUG:
							#	print("---temp--")
							#	print("---temp--")
							#	NeighLib.print_col_where(temp, "run_index", "SRR1013561")
							#	NeighLib.print_col_where(temp, "run_index", "ERR1023252")
							#	NeighLib.print_col_where(temp, "run_index", "ERR751929")
								

							# TODO: this basically replaces merge right columns which doesn't work for this somehow
							temp = temp.with_columns(concat_list=pl.concat_list([left_column, f"{left_column}_right"]).list.drop_nulls())
							temp = temp.drop(left_column).drop(f"{left_column}_right")
							temp = temp.rename({"concat_list": left_column})
							#if logging.root.level == logging.DEBUG:
							#	print("---concat_list--")
							#	print("---concat_list--")
							#	NeighLib.print_col_where(temp, "run_index", "SRR1013561")
							#	NeighLib.print_col_where(temp, "run_index", "ERR1023252")
							#	NeighLib.print_col_where(temp, "run_index", "ERR751929")

							right = right.drop(left_column) # prevent merge right columns from running after full merge
							left = left.drop(left_column)
							left = left.join(temp, merge_upon, how='outer_coalesce')
							#if logging.root.level == logging.DEBUG:
							#	print("---back to left--")
							#	print("---back to left--")
							#	NeighLib.print_col_where(left, "run_index", "SRR1013561")
							#	NeighLib.print_col_where(left, "run_index", "ERR1023252")
							#	NeighLib.print_col_where(left, "run_index", "ERR751929")

					else:
						if right.select(left_column).dtypes == [pl.List(pl.String)]:
							self.logging.debug(f"* {left_column}: SING | LIST")
							self.logging.error("Merging a right list with a left singular is not implemented")
							exit(1)
						else:
							self.logging.debug(f"* {left_column}: SING | SING")


							#small_left = left.select([merge_upon, left_column])
							#small_right = right.select([merge_upon, left_column])
							
							##temp = small_left.with_columns(concat_list=pl.concat_list(put_right_name_in_this_column, pl.lit(left_name)))
							#nullfilled_left = small_left.join(small_right, on=merge_upon, how="left").with_columns(
							#	pl.col(f"{left_column}").fill_null(pl.col(f"{left_column}_right")).alias(left_column)
							#)
							#NeighLib.print_col_where(nullfilled_left, "run_index", "SRR1013561")

				else:
					# left column, which may or may not be a list, is not in right column. who cares.
					#logging.debug(f"* {left_column}: IDFC | NONE")
					pass


		#	for left_column in left_list_cols:
	#			if left_column in right_list_cols:
	#				# workaround for pl.concat_list() propagating nulls
	#				left = left.with_columns(pl.col(left_column).fill_null(value=pl.lit("null")))
	#				right = right.with_columns(pl.col(left_column).fill_null(value=pl.lit("null")).alias(f"{left_column}_right"))
	#			else:
	#				#left = NeighLib.stringify_one_list_column(left, left_column)
	#				pass
	#
		#	# flatten exclusive right list columns
	#		for right_column in right_list_cols:
	#			if right_column in left_list_cols:
	#				continue
	#			else:
	#				#right = NeighLib.stringify_one_list_column(right, right_column)
	#				pass
	#		NeighLib.print_col_where(left, "run_index", "SRR1013561")
	#		NeighLib.print_col_where(right, "run_index", "SRR1013561")

	#		nullfilled_left = left.join(right, on=merge_upon, how="left").with_columns(
	#				[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in left.columns if col != merge_upon and col in right.columns]
	#		).select(left.columns).sort(merge_upon)
	#		nullfilled_right = right.join(left, on=merge_upon, how="left").with_columns(
	#			[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in right.columns if col != merge_upon and col in left.columns]
	#		).select(right.columns).sort(merge_upon)
	#		nullfilled_left = NeighLib.drop_null_columns(nullfilled_left)
	#		nullfilled_right = NeighLib.drop_null_columns(nullfilled_right)
	#
	#		NeighLib.print_col_where(nullfilled_left, "run_index", "SRR1013561")
	#		NeighLib.print_col_where(nullfilled_right, "run_index", "SRR1013561")

	#
	#		# update left values and right values for later debugging
	#		left_values, right_values = nullfilled_left[merge_upon], nullfilled_right[merge_upon]
	#		exclusive_left, exclusive_right = ~left_values.is_in(right_values), ~right_values.is_in(left_values)

	#		initial_merge = nullfilled_left.join(nullfilled_right, merge_upon, how="outer_coalesce").unique().sort(merge_upon)
			initial_merge = left.join(right, merge_upon, how="outer_coalesce").unique().sort(merge_upon)
			really_merged = NeighLib.merge_right_columns(initial_merge, fallback_on_left=fallback_on_left)

			really_merged_no_dupes = really_merged.unique()
			self.logging.info(f"Merged a {n_rows_left} row dataframe with a {n_rows_right} rows dataframe. Final dataframe has {really_merged_no_dupes.shape[0]} rows (difference: {really_merged_no_dupes.shape[0] - n_rows_left})")
			merged_dataframe = really_merged_no_dupes

			if self.logging.getEffectiveLevel() == 10:
				self.logging.debug("End of merge")
				NeighLib.print_col_where(merged_dataframe, "run_index", "SRR1013561")
				NeighLib.print_col_where(merged_dataframe, "sample_index", "SAMN02360560")

		merged_dataframe.drop_nulls()

		self.check_if_unexpected_rows(merged_dataframe, merge_upon=merge_upon, 
			intersection_values=intersection_values, exclusive_left_values=exclusive_left_values, exclusive_right_values=exclusive_right_values, 
			n_rows_left=n_rows_left, n_rows_right=n_rows_right, right_name=right_name, right_name_in_this_column=put_right_name_in_this_column)
		return merged_dataframe
