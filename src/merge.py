from src.neigh import NeighLib
import polars as pl
verbose = True  # TODO: do this better

def aggregate_conflicting_metadata(polars_df, column_key):
	"""
	Returns a numeric representation of n_unique values for rows that have matching column_key values. This representation can later
	be semi-merged backed to the original data if you want the real data.
	"""
	from functools import reduce
	agg_values = polars_df.group_by(column_key).agg([pl.col(c).n_unique().alias(c) for c in polars_df.columns if c != column_key])

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
	agg_table = aggregate_conflicting_metadata(polars_df, column_key)
	columns_we_will_merge_and_their_column_keys = get_columns_with_any_row_above_1(agg_table, column_key)

	print("The following columns will become catagorical due to conflicting metadata:")
	goofy_data = columns_we_will_merge_and_their_column_keys.columns
	goofy_data.remove(column_key)
	print(goofy_data)


	# TODO: This needs to be fixed so that only columns in goofy_data are combined, but the other columns still exist in the dataframe.
	semi_rows = polars_df.join(columns_we_will_merge_and_their_column_keys, on="run_index", how="semi") # get our real data back (eg, not agg integers)
	cool_stuff = semi_rows.group_by(column_key).agg([pl.col(column).alias(column) for column in semi_rows.columns if column != column_key and column in goofy_data])
	wow = polars_df.join(cool_stuff, on="run_index", how="inner")
	print(cool_stuff)
	print(wow)


	exit(1)

	# merge back to get original data
	
	return semi_rows


def self_merge_polars(polars_df, column_key: str):
	result_df = polars_df.groupby("A").agg([])

def merge_polars_dataframes(left, right, merge_upon, new_data_name="merged", put_name_in_this_column=None):
	"""
	Merge two polars dataframe upon merge_upon. 

	
	put_name_in_this_column: If not None, adds a row of new_data_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times. If new_data_name is None, an new_data_name column will be created temporarily but 
	dropped before returning.
	"""

	# Check for continigencies
	if merge_upon not in left.columns:
		raise ValueError(f"Attempted to merge dataframes upon {merge_upon}, but no column with that name in left-hand dataframe")
	if merge_upon not in right.columns:
		raise ValueError(f"Attempted to merge dataframes upon {merge_upon}, but no column with that name in right-hand dataframe")
	if merge_upon == 'run_index' or merge_upon == 'run_accession':
		if not NeighLib.likely_is_run_indexed(left):
			print(f"WARNING: Merging upon {merge_upon}, but left-hand dataframe appears to not be indexed by run accession")
		if not NeighLib.likely_is_run_indexed(right):
			print(f"WARNING: Merging upon {merge_upon}, but right-hand dataframe appears to not be indexed by run accession")
	if len(left.filter(pl.col(merge_upon).is_null())[merge_upon]) != 0:
		raise ValueError(f"Attempted to merge dataframes upon shared column {merge_upon}, but the left-hand dataframe has {len(left.filter(pl.col(merge_upon).is_null())[merge_upon])} nulls in that column")
	if len(right.filter(pl.col(merge_upon).is_null())[merge_upon]) != 0:
		raise ValueError(f"Attempted to merge dataframes upon shared column {merge_upon}, but the right-hand dataframe has {len(right.filter(pl.col(merge_upon).is_null())[merge_upon])} nulls in that column")

	left_nulls = left.select(pl.all().is_null().sum())
	right_nulls = right.select(pl.all().is_null().sum())
	n_left_nulls = left_nulls.sum()
	n_right_nulls = right_nulls.sum()
	print(f"Merging {new_data_name} on {merge_upon}")

	if put_name_in_this_column is not None:
		# ie, right['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		right[put_name_in_this_column] = new_data_name

	# use .join to fill in null values in both dataframes
	nullfilled_left = left.join(right, on=merge_upon, how="left").with_columns(
		[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in left.columns if col != merge_upon]
	).select(left.columns).sort(merge_upon)
	nullfilled_right = right.join(left, on=merge_upon, how="left").with_columns(
		[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in right.columns if col != merge_upon]
	).select(right.columns).sort(merge_upon)

	# actually merge the dataframes
	# we'll use merge_sorted() as there is less to clean up than .join(how="full")
	merge = nullfilled_left.merge_sorted(nullfilled_right, merge_upon).unique().sort(merge_upon)

	get_partial_self_matches(merge, merge_upon)

	# TODO: figure out how to handle rows with conflicts

	len_left, len_right, len_current = left.shape[0], right.shape[0], merge.shape[0]
	len_new_rows = len_current - len_left
	if verbose:
		print(f"Was {len_left} rows, merged with {len_right} rows, now {len_current} rows")
		print(f"This implies {len_new_rows} new rows were added (or failed to merge)")

	NeighLib.super_print_pl(merge, "merge")
	exit(0)

	return merge

def merge_pandas_dataframes(left, right, merge_upon, new_data_name="merged", put_name_in_this_column=None):
	"""
	Merge two pandas dataframe upon merge_upon. 

	If new_data_name is None, an new_data_name column will be created temporarily but dropped before returning.
	put_name_in_this_column: If not None, adds a row of new_data_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times.

	Handling _x and _y conflicts is not fully implemented.
	"""
	import pandas as pd
	import numpy as np

	print(f"Merging {new_data_name} on {merge_upon}")

	if put_name_in_this_column is not None:
		# ie, right['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		right[put_name_in_this_column] = new_data_name

	merge = pd.merge(left, right, on=merge_upon, how='outer', indicator='merge_status_unprocessed')
	len_left, len_right, len_current = len(left.index), len(right.index), len(merge.index)
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

	# make new new_data_name column
	merge[new_data_name] = merge['merge_status_unprocessed'].apply(lambda x: True if x in ['right_only', 'both'] else False)
	merge.drop('merge_status_unprocessed', axis=1, inplace=True) 
	
	return merge