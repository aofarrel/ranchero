import polars as pl
from polars.testing import assert_series_equal
from .statics import kolumns, null_values, drop_zone

# https://peps.python.org/pep-0661/
_DEFAULT_TO_CONFIGURATION = object()

class Merger:

	def __init__(self, configuration, naylib):
		if configuration is None:
			raise ValueError("No configuration was passed to Merger class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger
			self.NeighLib = naylib

		def _default_fallback(self, cfg_var, value):
			if value == _DEFAULT_TO_CONFIGURATION:
				return self.cfg.get_config(cfg_var)
			return value

	def aggregate_conflicting_metadata(self, polars_df, column_key):
		"""
		Returns a numeric representation of n_unique values for rows that have matching column_key values. This representation can later
		be semi-merged backed to the original data if you want the real data.
		"""
		from functools import reduce
		# this works if there aren't already any lists, but panics otherwise
		#agg_values = polars_df.group_by(column_key).agg([pl.col(c).n_unique().alias(c) for c in polars_df.columns if c != column_key])
		agg_values = polars_df.group_by(column_key).agg([999 if polars_df.schema[c] == pl.List else pl.col(c).n_unique().alias(c) for c in polars_df.columns if c != column_key])
		self.NeighLib.super_print_pl(agg_values, "agg_values")

		# to match in cool_rows, ALL rows must have a value of 1
		# to match in uncool_rows, ANY rows must have a value of not 1
		cool_rows = agg_values.filter(pl.col(c) == 1 for c in agg_values.columns if c != column_key).sort(column_key)
		uncool_rows = agg_values.filter(reduce(lambda acc, expr: acc | expr, (pl.col(c) != 1 for c in agg_values.columns if c != column_key)))

		# to get the original data for debugging purposes you can use: semi_rows = polars_df.join(uncool_rows, on="run_id", how="semi")
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

		restored_data = polars_df.join(columns_we_will_merge_and_their_column_keys, on="run_id", how="semi") # get our real data back (eg, not agg integers)
		restored_catagorical_data = restored_data.group_by(column_key).agg([pl.col(column).alias(column) for column in restored_data.columns if column != column_key and column in will_be_catagorical])
		self.NeighLib.super_print_pl(restored_catagorical_data, "restored catagorical data")

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
		right_name_in_this_column,
		manual_index_column=None):
		n_rows_merged = merged_df.shape[0]
		n_rows_expected = sum([len(intersection_values), len(exclusive_left_values), len(exclusive_right_values)])

		merged_df = self.NeighLib.check_index(merged_df, manual_index_column=manual_index_column)

		# we expect n_rows_merged = intersection_values + exclusive_left_values + exclusive_right_values
		if n_rows_merged == n_rows_expected:
			self.logging.debug("Did not find any unexpected rows")
			return
		else:
			self.logging.debug("-------")
			self.logging.debug(f"Expected {n_rows_expected} rows in merged dataframe but got {n_rows_merged}")
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
				self.logging.error(f"Found {len((duplicated_indices).unique())} duplicated values in column {merge_upon}! This indicates a merge failure.")
				self.logging.error(duplicated_indices.unique().select(merge_upon))
				exit(1)
			else:
				self.logging.info(f"Right-hand dataframe appears to have added {n_rows_expected - n_rows_merged} new samples.")
				self.logging.info(f"(Expected {n_rows_expected} rows, got {n_rows_merged}, found no duplicate values in {merge_upon})")
			self.logging.debug("-------")

	def merge_polars_dataframes(self, 
		left: pl.dataframe.frame.DataFrame, 
		right: pl.dataframe.frame.DataFrame, 
		merge_upon: str, left_name ="left", right_name="right", indicator=_DEFAULT_TO_CONFIGURATION,
		bad_list_error=True,
		fallback_on_left=True, drop_exclusive_right=False,
		escalate_warnings=False,
		force_index=None):
		"""
		Merge two polars dataframe upon merge_upon. 

		bad_list_behavior:
		* fallback_on_left
		* fallback_on_right
		* throw_error

		
		indicator: If not None, adds a row of right_name to the dataframe. Designed for marking the source of data when
		merging dataframes multiple times. If right_name is None, or indicator is explictly set to None, a right_name
		column will be created temporarily but dropped before returning.
		"""
		self.logging.debug(f"Preparing to merge {left_name} and {right_name} upon {merge_upon}...")
		n_rows_left, n_rows_right = left.shape[0], right.shape[0]
		n_cols_left, n_cols_right = left.shape[1], right.shape[1]
		assert n_rows_left != 0 and n_rows_right != 0
		assert n_cols_left != 0 and n_cols_right != 0
		if indicator is _DEFAULT_TO_CONFIGURATION:
			indicator = self.cfg.indicator_column
		self.logging.debug(f"Dropping null columns from {left_name} and {right_name}...")
		left, right = self.NeighLib.drop_null_columns(left), self.NeighLib.drop_null_columns(right)

		# merge_upon is not necessarily the index of either dataframe, but in the short term we want it to act like one (that is to say, fully
		# unique, no nulls, etc)
		self.logging.debug(f"Checking {left_name}'s index...")
		left = self.NeighLib.check_index(left, force_INSDC_runs=False, force_INSDC_samples=False, manual_index_column=merge_upon, allow_bad_name=True, df_name=left_name)
		self.logging.debug(f"Checking {right_name}'s index...")
		right = self.NeighLib.check_index(right, force_INSDC_runs=False, force_INSDC_samples=False, manual_index_column=merge_upon, allow_bad_name=True, df_name=right_name)

		for df, name in zip([left,right], [left_name,right_name]):
			if merge_upon not in df.columns:
				self.logging.error(f"Couldn't find {merge_upon} in {name} (which seems to have index {self.NeighLib.get_index(df, guess=True)})")
				raise ValueError(f"Attempted to merge dataframes upon {merge_upon}, but no column with that name in {name} dataframe")
			if merge_upon == 'run_id' or merge_upon == 'run_accession':
				if not self.NeighLib.is_run_indexed(df):
					self.logging.warning(f"Merging upon {merge_upon}, which looks like a run accession, but {name} dataframe appears to not be indexed by run accession")
			if len(df.filter(pl.col(merge_upon).is_null())[merge_upon]) != 0:
				self.logging.error("Dataframe has null values for the merge column:")
				if merge_upon != "sample_id" and "sample_id" in df.columns:
					print(df.filter(pl.col(merge_upon).is_null()).select(["sample_id", merge_upon]))
				else:
					print(df.filter(pl.col(merge_upon).is_null()))
				raise ValueError(f"Attempted to merge dataframes upon shared column {merge_upon}, but the {name} dataframe has {len(left.filter(pl.col(merge_upon).is_null())[merge_upon])} nulls in that column")
		self.logging.info(f"Merging {left_name} and {right_name} upon {merge_upon}")

		# right/left-hand dataframe's index's values (SRR16156818, SRR12380906, etc) ONLY -- all other columns excluded
		assert left.schema[merge_upon] == right.schema[merge_upon]
		left_values, right_values = left[merge_upon], right[merge_upon]

		# left-hand dataframe with true-false for whether index at that position is also present somewhere in the right dataframe
		# ex: if the 0th row in left_values is 'SRR16156818' and that is present in some row in right_values, then the 0th row in intersection is true
		intersection = left_values.is_in(right_values)
		
		# the index values where instersection is true
		# false rows are excluded (as oppposed to None), so intersection_values will usually have a less rows than the intersection dataframe
		# ex: following previous example, this would include 'SRR16156818'
		intersection_values = left.filter(intersection).select(merge_upon)

		# left/right-hand dataframe with true-false for whether index at that position is NOT also present in the right/left dataframe
		exclusive_left, exclusive_right = ~left_values.is_in(right_values), ~right_values.is_in(left_values)

		# the index values where exclusive_left/right is true
		exclusive_left_values, exclusive_right_values = left.filter(exclusive_left).select(merge_upon), right.filter(exclusive_right).select(merge_upon)
		
		if (~intersection).all():
			if drop_exclusive_right:
				self.logging.warning(f"No values in {merge_upon} are shared across the dataframes, but drop_exclusive_right is True, so the dataframe is unchanged")
				return left
			else:
				self.logging.warning(f"No values in {merge_upon} are shared across the dataframes -- merge can continue, but no rows in {right_name} will merge with existing rows in {left_name}")
		self.logging.info(f"--> Intersection: {len(intersection_values)}")
		self.logging.info(f"--> Exclusive to {left_name}: {len(exclusive_left_values)}")
		if drop_exclusive_right==True:
			self.logging.info(f"--> Exclusive to {right_name}: {len(exclusive_right_values)} (will be dropped)")
			#if len(exclusive_right_values) > 0:
				#self.logging.debug(f"Some of the exclusive right values, which will be dropped: {exclusive_right_values}")
			
			# recalculate these variables to prevent issues with the post-merge check function
			right = right.filter(~exclusive_right)
			n_rows_right = right.shape[0]
			exclusive_right_values = pl.DataFrame()
		else:
			self.logging.info(f"--> Exclusive to {right_name}: {len(exclusive_right_values)}")
			if len(exclusive_right_values) > 0:
				self.logging.debug(f"-----> Some of the exclusive right values: {exclusive_right_values}")

		# TODO: this is here just so we have better testing of list merges, but later it's probably better to just
		# put something like this at the end by concat_list()ing pl.lit() the name into the column
		# ie, right['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		if indicator is not None:
			if indicator not in left.columns and left_name != "left": # this wasn't an issue before!! why is it an issue now?!
				self.logging.debug("--> No indicator column in left")
				left = left.with_columns(pl.lit(left_name).alias(indicator))
			else:
				self.logging.debug("--> Already an indicator in left")
				
			right = right.with_columns(pl.lit(right_name).alias(indicator))
			n_cols_right = right.shape[1]
			n_cols_left = left.shape[1]

		shared_columns = self.NeighLib.get_dupe_columns_of_two_polars(left, right, assert_shared_cols_equal=False)
		shared_columns.remove(merge_upon)
		merged_columns = [] # for printing at the end
		left_list_cols = [col for col, dtype in zip(left.columns, left.dtypes) if dtype == pl.List]
		right_list_cols = [col for col, dtype in zip(right.columns, right.dtypes) if dtype == pl.List]

		if len(shared_columns) == 0:
			self.logging.debug("These dataframes do not have any non-index columns in common.")
			if n_cols_right == n_cols_left:
				initial_merge = left.sort(merge_upon).merge_sorted(right.sort(merge_upon), merge_upon).unique().sort(merge_upon)
				infostr1 = f"Merged a {n_rows_left}x{n_cols_left} df with a {n_rows_right}x{n_cols_right} df upon {merge_upon}. "
				infostr2 = f"Final dataframe is {initial_merge.shape} and index {self.NeighLib.get_index(initial_merge)}.  "
				self.logging.info(infostr1 + infostr2)
				merged_dataframe = initial_merge
			else:
				merged_dataframe = left.join(right, merge_upon, how="outer_coalesce").unique()

		else:
			self.logging.info(f"--> Shared columns: {', '.join(thing for thing in shared_columns)}")

			# The problem with concat'ing list columns is that a .join() considers ["foo", "bar"] and ["bar", "foo"] to be different,
			# which means that we can end up with duplicates in the index. So, we're going to check if left and right are
			# identical for all columns except left_column. If yes, we will concat the lists.

			shared_list_cols = list()
			yargh = list(left.columns)
			yargh.remove(merge_upon)
			for left_column in yargh:
				assert f"{merge_upon}_right" not in left.columns
				if left_column in drop_zone.silly_columns: # no need to handle this if it's getting yote
					left, right = left.drop(left_column), right.drop(left_column, strict=False)
				elif left_column in right.columns:
					if left.schema[left_column] == pl.List(pl.String):
						shared_list_cols.append(left_column)
					elif right.schema[left_column] == pl.List(pl.String):
						shared_list_cols.append(left_column)

			self.logging.debug(f"Shared columns where left or right (or both) have type list: {shared_list_cols}")
			self.logging.debug("Keep an eye on them, they can be an issue later...")

			for left_column in yargh:
				assert f"{merge_upon}_right" not in left.columns
				if left_column in right.columns:
					merged_columns.append(left_column)

					if left_column in drop_zone.silly_columns:
						left, right = left.drop(left_column), right.drop(left_column)

					elif left_column in kolumns.list_throw_error:
						if left.schema[left_column] == pl.List or right.schema[left_column] == pl.List:
							if bad_list_error:
								self.logging.error(f'{left_column} is marked as "error if becomes a list when merging" but is already a list in {left_name} and/or {right_name}!')
								self.logging.error(f'Rancheroize and flatten your lists.')
								exit(1)
							else:
								self.logging.warning('A column in kolumns.list_throw_error was merged. Checking if everything is okay...')
								mini_merged = left.join(right, on=merge_upon, how="inner")
								assert f"{mini_merged}_right" not in mini_merged

					elif left.schema[left_column] == pl.List(pl.String): # TODO: get this to work on integers
						
						if right.schema[left_column] == pl.List(pl.String):
							self.logging.debug(f"* {left_column}: LIST | LIST")

							# let merge_right_columns() handle it... or rename the shared column so they aren't shared anymore?

						else:
							# This section looks like nonsense but it is ESSENTIAL given just how annoying concat_list() can get!!
							# Previously we tried something like this:
							#    small_left, small_right = left.select([merge_upon, left_column]), right.select([merge_upon, left_column])
							#    small_merge = small_left.join(small_right, merge_upon, how="outer_coalesce")
							#    small_merge = small_merge.with_columns(concat_list=pl.concat_list([left_column, f"{left_column}_right"]).list.drop_nulls())
							#    small_merge = small_merge.drop(left_column).drop(f"{left_column}_right").rename({"concat_list": left_column})
							#    left, right = left.drop(left_column), right.drop(left_column) # prevent merge right columns from running after full merge
							#    left = left.join(small_merge, merge_upon, how='outer_coalesce')
							# But that uses concat_list() too early, which results in nulls propagating before drop_nulls() can save us.
							self.logging.debug(f"* {left_column}: LIST | SING")
							right_column = f"{left_column}_right"
							assert right_column not in left.columns # shouldn't exist until after small_merge is created

							# Let's say merge_upon = "sample_id", left_column = "foo", right_column = "foo_right"
							# Create small_merge dataframe by merging left and right upon "sample_id". This
							# creates a new dataframe with columns "sample_id", "foo", and "foo_right".
							small_merge = (
								left.select([merge_upon, left_column])
								.join(right.select([merge_upon, left_column]), on=merge_upon, how="full", coalesce=True)
							)
							if drop_exclusive_right and left.shape[0] != small_merge.shape[0]:
								self.logging.warning("drop_exclusive_right = True, but small_merge and left have different lengths")
								self.logging.warning("This might indicate a merge failure and/or duplicated indeces, but will attempt to continue")

							# Wherever left column (list) is null, cast the right column (str) to list and use that
							# value for new column "{left_column}_nullfilled_with_right_col".
							# Otherwise (ie when left column is not null), keep that value for the new column.
							# Keep in mind that right_column still exists!
							small_merge = small_merge.with_columns(
								pl.when(pl.col(left_column).is_null())   
								.then(pl.col(right_column).cast(pl.List(str)))
								.otherwise(pl.col(left_column)) 
								.alias(f"{left_column}_nullfilled_with_right_col")
							)
							# Now that the left column has had as many nulls removed as possible, we want to remove nulls 
							# from the right column, because nulls in the right column would also cause issues with concat_list.
							small_merge = small_merge.with_columns(
								pl.when(pl.col(right_column).is_null())
								.then(pl.lit(""))
								.otherwise(pl.col(right_column))
								.alias(f"{right_column}_nullfilled_with_empty_str")
							)
							# Only now is it safe to use pl.concat_list() without worrying about nulls propagating.
							small_merge = small_merge.with_columns(
								merged=pl.concat_list([f"{left_column}_nullfilled_with_right_col", f"{right_column}_nullfilled_with_empty_str"])
								.list.unique().list.drop_nulls()
							).drop([f"{left_column}_nullfilled_with_right_col", f"{right_column}_nullfilled_with_empty_str", right_column, left_column])

							# Remove empty strings from the list
							small_merge = small_merge.with_columns(
								pl.col("merged").list.eval(
									pl.element().filter(pl.element().str.len_chars() > 0)
								)
							)
							left, right = left.drop(left_column), right.drop([left_column]) # prevent merge right columns from running after full merge
							left = left.join(small_merge, on=merge_upon, how="full", coalesce=True).rename({"merged" : left_column})
							self.logging.debug(f"left rows after merge with small_merge: {left.shape[0]}")

					else:
						if right.schema[left_column] == pl.List(pl.String):
							self.logging.debug(f"* {left_column}: SING | LIST")
							right_column = f"{left_column}_right"
							assert right_column not in left.columns

							# Let's say merge_upon = "sample_id", left_column = "foo", right_column = "foo_right"
							# Create small_merge dataframe by merging left and right upon "sample_id". This
							# creates a new dataframe with columns "sample_id", "foo", and "foo_right".
							small_merge = (
								left.select([merge_upon, left_column])
								.join(right.select([merge_upon, left_column]), on=merge_upon, how="full", coalesce=True)
							)
							# Wherever RIGHT column (list) is null, cast the LEFT column (list) to list and use that
							# value for new column "{right_column}_nullfilled_with_left_col".
							# Otherwise (ie when right column is not null), keep that value for the new column.
							small_merge = small_merge.with_columns(
								pl.when(pl.col(right_column).is_null())   
								.then(pl.col(left_column).cast(pl.List(str)))
								.otherwise(pl.col(right_column)) 
								.alias(f"{right_column}_nullfilled_with_left_col")
							)
							# Now that the right (list) column has had as many nulls removed as possible, we want to remove nulls 
							# from the left (str) column, because nulls in the left column would also cause issues with concat_list.
							small_merge = small_merge.with_columns(
								pl.when(pl.col(left_column).is_null())   
								.then(pl.lit(""))
								.otherwise(pl.col(left_column)) 
								.alias(f"{left_column}_nullfilled_with_empty_str"),
							)

							# Only now is it safe to use pl.concat_list() without worrying about nulls propagating.
							small_merge = small_merge.with_columns(
								merged=pl.concat_list([f"{left_column}_nullfilled_with_empty_str", f"{right_column}_nullfilled_with_left_col"])
								.list.unique().list.drop_nulls()
							)

							# Remove empty strings from the list
							small_merge = small_merge.with_columns(
								pl.col("merged").list.eval(
									pl.element().filter(pl.element().str.len_chars() > 0)
								)
							)
							small_merge = small_merge.drop([f"{left_column}_nullfilled_with_empty_str", f"{right_column}_nullfilled_with_left_col", right_column, left_column])	
							left, right = left.drop(left_column), right.drop([left_column])
							left = left.join(small_merge, on=merge_upon, how="full", coalesce=True).rename({"merged": left_column})
							###self.logging.warning("Merging a singular left column with a list right column is untested")
							###small_left, small_right = left.select([merge_upon, left_column]), right.select([merge_upon, left_column])
							###small_merge = small_left.join(small_right, merge_upon, how="outer_coalesce")
							###small_merge = small_merge.with_columns(concat_list=pl.concat_list([left_column, f"{left_column}_right"]).list.drop_nulls())
							###small_merge = small_merge.drop(left_column).drop(f"{left_column}_right").rename({"concat_list": left_column})
							###left, right = left.drop(left_column), right.drop(left_column) # prevent merge right columns from running after full merge
							###left = left.join(small_merge, merge_upon, how='outer_coalesce')
						else:
							self.logging.debug(f"* {left_column}: SING | SING")
				else:
					pass

			# update left values and right values for later debugging
			left_values, right_values = left[merge_upon], right[merge_upon]
			exclusive_left, exclusive_right = ~left_values.is_in(right_values), ~right_values.is_in(left_values)

			initial_merge = left.join(right, merge_upon, how="outer_coalesce").unique().sort(merge_upon)
			self.logging.debug(f"after initial join but before merge right columns, {initial_merge.shape[0]} rows")
			really_merged = self.NeighLib.merge_right_columns(initial_merge, fallback_on_left=fallback_on_left, escalate_warnings=escalate_warnings, force_index=force_index)
			really_merged_no_dupes = really_merged.unique() # this doesn't actually help with duplicate indeces
			duplicated_indices = really_merged_no_dupes.filter(pl.col(merge_upon).is_duplicated())
			assert duplicated_indices.shape[0] == 0 # TODO: unless we allow dupes in index i guess?? why would we do that though
			merged_dataframe = really_merged_no_dupes

			# print stats
			left_added_columns = [thing for thing in left.columns if thing not in merged_columns]
			rite_added_columns = [thing for thing in right.columns if thing not in merged_columns]
			infostr1 = f"Merged a {n_rows_left}x{n_cols_left} df with a {n_rows_right}x{n_cols_right} df upon {merge_upon}. "
			infostr2 = f"Final dataframe is {merged_dataframe.shape} and index {self.NeighLib.get_index(merged_dataframe)}. "
			infostr3 = f"The columns that were merged were: "
			infostr4 = f"\n\t* {'\n\t* '.join(thing for thing in merged_columns)}"
			infostr5 = f"\nThe left dataframe added {len(left_added_columns)} columns: "
			infostr6 = f"\n\t* {'\n\t* '.join(left_added_columns)}"
			infostr7 = f"\nThe right dataframe added {len(rite_added_columns)} columns: "
			infostr8 = f"\n\t* {'\n\t* '.join(rite_added_columns)}"
			self.logging.info(infostr1 + infostr2 + infostr3 + infostr4 + infostr5 + infostr6 + infostr7 + infostr8)
			
		self.logging.debug("Checking merged dataframe for unexpected rows...")
		self.check_if_unexpected_rows(merged_dataframe, merge_upon=merge_upon, 
			intersection_values=intersection_values, exclusive_left_values=exclusive_left_values, exclusive_right_values=exclusive_right_values, 
			n_rows_left=n_rows_left, n_rows_right=n_rows_right, right_name=right_name, right_name_in_this_column=indicator, manual_index_column=force_index)
		self.logging.debug("Checking merged dataframe's index...")
		self.NeighLib.check_index(merged_dataframe, manual_index_column=force_index)
		self.logging.debug("Trying to null newly created empty lists...")
		merged_dataframe = self.NeighLib.null_lists_of_len_zero(merged_dataframe)
		return merged_dataframe
