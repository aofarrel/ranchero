from src.neigh import NeighLib
import polars as pl
from polars.testing import assert_series_equal
verbose = False  # TODO: do this better

def aggregate_conflicting_metadata(polars_df, column_key):
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

def get_columns_with_any_row_above_1(polars_df, column_key):
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

def get_partial_self_matches(polars_df, column_key: str):
	"""
	Reports all columns of all rows where (1) at least two rows share a key and (2) at least one column between rows with a matching
	key has a mismatch.
	"""
	# the agg table method is preferred, however, it doesn't work if the rows we're combining contain lists

	agg_table = aggregate_conflicting_metadata(polars_df, column_key)
	columns_we_will_merge_and_their_column_keys = get_columns_with_any_row_above_1(agg_table, column_key)  # type: polars df
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

def merge_right_columns(polars_df, quick_cast=True):

	right_columns = [col for col in polars_df.columns if col.endswith("_right")]
	for right_col in right_columns:
		base_col = right_col.replace("_right", "")

		if base_col not in polars_df.columns:
			print(f"WARNING: Found {right_col}, but {base_col} not in dataframe -- will continue, but this may break things later")
			NeighLib.super_print_pl(polars_df, "DEBUG: Current dataframe")
			continue

		if verbose: NeighLib.super_print_pl(polars_df, f"before fill null for {base_col}")
		polars_df = polars_df.with_columns(pl.col(base_col).fill_null(pl.col(right_col)))
		polars_df = polars_df.with_columns(pl.col(right_col).fill_null(pl.col(base_col)))
		if verbose: NeighLib.super_print_pl(polars_df, f"after fill null for {base_col}")
		
		try:
			# if they are equal after filling in nulls, we don't need to turn anything into a list
			assert_series_equal(polars_df[base_col], polars_df[right_col].alias(base_col))
			polars_df = polars_df.drop(right_col)
			if verbose: print(f"All values in {base_col} and {right_col} are the same, so they won't become a list.")
		except AssertionError:

			# TODO: quick_cast exists because this is how the agg table method works, but maybe we can get rid of it?
			if quick_cast:
				polars_df = polars_df.with_columns(pl.col(base_col).cast(pl.List(str)))
				#polars_df = polars_df.with_columns(pl.col(right_col).cast(pl.List(str))) # might mess up the fill null???
				polars_df = polars_df.with_columns(pl.col(right_col).fill_null(pl.col(base_col)).alias(base_col))
				polars_df = polars_df.drop(right_col)
			else:
				# this is known to work with base_col and right_col are both pl.Ut8, or when both are list[str]
				polars_df = polars_df.with_columns(
					pl.when(pl.col(base_col) != pl.col(right_col))             # When a row has different values for base_col and right_col,
					.then(pl.concat_list([base_col, right_col]).list.unique()) # make a list of base_col and right_col, but keep only uniq values
					.otherwise(pl.concat_list([base_col]))                     # otherwise, make list of just base_col (doesn't seem to nest if already a list, thankfully)
					.alias(base_col)
				).drop(right_col)
				if verbose: NeighLib.super_print_pl(polars_df, f"after merging to make {base_col} to a list")

	# non-unique rows might be dropped here, fyi
	return polars_df

	

def merge_polars_dataframes(left, right, merge_upon, left_name ="left", right_name="right", put_right_name_in_this_column=None):
	"""
	Merge two polars dataframe upon merge_upon. 

	
	put_right_name_in_this_column: If not None, adds a row of right_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times. If right_name is None, an right_name column will be created temporarily but 
	dropped before returning.
	"""
	n_rows_left, n_rows_right = left.shape[0], right.shape[0]
	n_cols_left, n_cols_right = left.shape[1], right.shape[1]

	assert n_rows_left != 0 and n_rows_right != 0
	assert n_cols_left != 0 and n_cols_right != 0

	for df, name in zip([left,right], [left_name,right_name]):
		if merge_upon not in df.columns:
			raise ValueError(f"Attempted to merge dataframes upon {merge_upon}, but no column with that name in {name} dataframe")
		if merge_upon == 'run_index' or merge_upon == 'run_accession':
			if not NeighLib.likely_is_run_indexed(df):
				print(f"WARNING: Merging upon {merge_upon}, which looks like a run accession, but {name} dataframe appears to not be indexed by run accession")
		if len(df.filter(pl.col(merge_upon).is_null())[merge_upon]) != 0:
			raise ValueError(f"Attempted to merge dataframes upon shared column {merge_upon}, but the {name} dataframe has {len(left.filter(pl.col(merge_upon).is_null())[merge_upon])} nulls in that column")

			# TODO: add a check for columns with duplicate names (if thats even possible in polars)

	
	# everything passed, let's get started
	print(f"Merging {left_name} and {right_name} on {merge_upon}")
	if put_right_name_in_this_column is not None:
		# ie, right['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		right = right.with_columns(pl.lit(right_name).alias(put_right_name_in_this_column))
		n_cols_right = right.shape[1]

	# check columns
	#                                   | at least one column with type list exists?
	# share columns besides merge_upon? |  true             | false
	# ++++++++++++++++++++++++++++++++++|+++++++++++++++++++++++++++++++++++++++++
	#                              true | big oof           | agg_table
	#                             false | easy merge        | easy merge
	#
	# big oof: if no shared cols are list: drop lists, agg table, then readd lists
	#          else: painstaking list comprehension on shared keys, then drop lists, agg table, readd lists
	
	shared_columns = NeighLib.get_dupe_columns_of_two_polars(left, right, assert_shared_cols_equal=False)
	shared_columns.remove(merge_upon)
	left_list_cols = [col for col, dtype in zip(left.columns, left.dtypes) if dtype == pl.List]
	right_list_cols = [col for col, dtype in zip(right.columns, right.dtypes) if dtype == pl.List]
	
	# no columns are shared except merge_upon, just use merge_sorted()
	if len(shared_columns) == 0:
		left = NeighLib.drop_null_columns(left.sort(merge_upon))
		right = NeighLib.drop_null_columns(right.sort(merge_upon))
		initial_merge = left.merge_sorted(right, merge_upon).unique().sort(merge_upon)
		return initial_merge

	# we can use agg tables to check for partial self matches
	elif len(left_list_cols) == 0 and len(right_list_cols) == 0:

		# fill in null values
		nullfilled_left = left.join(right, on=merge_upon, how="left").with_columns(
				[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in left.columns if col != merge_upon and col in right.columns]
		).select(left.columns).sort(merge_upon)
		nullfilled_right = right.join(left, on=merge_upon, how="left").with_columns(
			[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in right.columns if col != merge_upon and col in left.columns]
		).select(right.columns).sort(merge_upon)
		nullfilled_left = NeighLib.drop_null_columns(nullfilled_left)
		nullfilled_right = NeighLib.drop_null_columns(nullfilled_right)
	
		# TODO: not convinced get_partial_self_matches() (agg table) is better than the approach in the else block
		# it might be best to drop the agg table entirely. commenting it out for now...
		if set(left.columns) == set(right.columns):
			if verbose: print("set of left and right columns match")
			initial_merge = nullfilled_left.join(nullfilled_right, merge_upon, how="outer_coalesce").unique().sort(merge_upon)
			really_merged = merge_right_columns(initial_merge, quick_cast=False)
			#initial_merge = nullfilled_left.merge_sorted(nullfilled_right, merge_upon).unique().sort(merge_upon)
			#restored_catagorical_data = get_partial_self_matches(initial_merge, merge_upon).sort(merge_upon)
			#merged = restored_catagorical_data.join(restored_catagorical_data, on=merge_upon, how="left")
			#really_merged = merge_right_columns(merged)
		else:
			initial_merge = nullfilled_left.join(nullfilled_right, merge_upon, how="outer_coalesce").unique().sort(merge_upon)
			really_merged = merge_right_columns(initial_merge, quick_cast=False)
		
		really_merged_no_dupes = really_merged.unique()
		return really_merged_no_dupes

	else:
		# fill in null values
		nullfilled_left = left.join(right, on=merge_upon, how="left").with_columns(
				[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in left.columns if col != merge_upon and col in right.columns]
		).select(left.columns).sort(merge_upon)
		nullfilled_right = right.join(left, on=merge_upon, how="left").with_columns(
			[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in right.columns if col != merge_upon and col in left.columns]
		).select(right.columns).sort(merge_upon)
		
		nullfilled_left = NeighLib.drop_null_columns(nullfilled_left)
		nullfilled_right = NeighLib.drop_null_columns(nullfilled_right)

		initial_merge = nullfilled_left.join(nullfilled_right, merge_upon, how="outer_coalesce").unique().sort(merge_upon)
		really_merged = merge_right_columns(initial_merge, quick_cast=False)

		really_merged_no_dupes = really_merged.unique()
		return really_merged_no_dupes

def merge_pandas_dataframes(left, right, merge_upon, right_name="merged", put_right_name_in_this_column=None):
	"""
	Merge two pandas dataframe upon merge_upon. 

	If right_name is None, an right_name column will be created temporarily but dropped before returning.
	put_right_name_in_this_column: If not None, adds a row of right_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times.

	Handling _x and _y conflicts is not fully implemented.
	"""
	import pandas as pd
	import numpy as np

	print(f"Merging {right_name} on {merge_upon}")

	# TODO: this doesn't work!!
	if put_right_name_in_this_column is not None:
		# ie, right['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		right[put_right_name_in_this_column] = right_name

	merge = pd.merge(left, right, on=merge_upon, how='outer', indicator='merge_status_unprocessed')
	rows_left, rows_right, len_current = len(left.index), len(right.index), len(merge.index)
	for conflict in NeighLib.get_x_y_column_pairs(merge):
		foo_x, foo_y, foo = conflict[0], conflict[1], conflict[2]
		merge[foo_x] = merge[foo_x].fillna(merge[foo_y])
		merge[foo_y] = merge[foo_y].fillna(merge[foo_x])
		if not merge[foo_x].equals(merge[foo_y]):
			# try again, but avoid the NaN != NaN curse
			merge[foo_x] = merge[foo_x].fillna("missing")
			merge[foo_y] = merge[foo_y].fillna("missing")
			if not merge[foo_x].equals(merge[foo_y]):
				print(f"Inconsistent {foo} found")
				if verbose:
					uh_oh = merge.loc[np.where(merge[foo_x] != merge[foo_y])]
					print(uh_oh[[foo_x, foo_y]])

				#merge[['BioSample'] == 'SAMEA3318415', 'BioProject_x'] == 'PRJEB9003' # falling back to what's on SRA web

			# TODO: actually implement fixing this

			merge = merge.rename(columns={foo_x: foo})
			merge = merge.drop(foo_y, axis=1)
			# TODO: drop the "missing" values into nans
	else:
		if verbose: print(f"{foo} seem unchanged")
	
	if verbose: NeighLib.mega_debug_merge(merge, merge_upon)



	#perhaps_merged_rows = not_merged_rows.apply(merge_unmerged_rows_pandas, axis=1)
	#print(perhaps_merged_rows)

	#merged_df = pd.concat([merged_df, not_merged_rows])

	# make new right_name column
	merge[right_name] = merge['merge_status_unprocessed'].apply(lambda x: True if x in ['right_only', 'both'] else False)
	merge.drop('merge_status_unprocessed', axis=1, inplace=True) 
	
	return merge