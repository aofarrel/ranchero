from src.neigh import NeighLib
verbose = True  # TODO: do this better

def merge_polars_dataframes(left, right, merge_upon, new_data_name, name_in_column=None):
	"""
	Merge two polars dataframe upon merge_upon. 

	If new_data_name is None, an new_data_name column will be created temporarily but dropped before returning.
	name_in_column: If not None, adds a row of new_data_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times.
	"""
	import polars as pl

	if merge_upon not in left.columns:
		raise ValueError(f"Attempted to merge dataframes upon {merge_upon}, but no column with that name in left-hand dataframe")
	if merge_upon not in right.columns:
		raise ValueError(f"Attempted to merge dataframes upon {merge_upon}, but no column with that name in right-hand dataframe")

	if merge_upon == 'run_index' or merge_upon == 'run_accession':

		# check if left and right are actually indexed by run
		if not NeighLib.likely_is_run_indexed(left):
			print(f"WARNING: Merging upon {merge_upon}, but left-hand dataframe appears to not be indexed by run accession")
		if not NeighLib.likely_is_run_indexed(right):
			print(f"WARNING: Merging upon {merge_upon}, but right-hand dataframe appears to not be indexed by run accession")

	# check for nulls in what we're merging upon
	if len(left.filter(pl.col(merge_upon).is_null())[merge_upon]) != 0:
		raise ValueError(f"Attempted to merge dataframes upon shared column {merge_upon}, but the left-hand dataframe has {len(left.filter(pl.col(merge_upon).is_null())[merge_upon])} nulls in that column")
	if len(right.filter(pl.col(merge_upon).is_null())[merge_upon]) != 0:
		raise ValueError(f"Attempted to merge dataframes upon shared column {merge_upon}, but the right-hand dataframe has {len(right.filter(pl.col(merge_upon).is_null())[merge_upon])} nulls in that column")

	left_nulls = left.select(pl.all().is_null().sum())
	right_nulls = right.select(pl.all().is_null().sum())
	n_left_nulls = left_nulls.sum()
	n_right_nulls = right_nulls.sum()
	print(f"Merging {new_data_name} on {merge_upon}")

	if name_in_column is not None:
		# ie, right['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		right[name_in_column] = new_data_name

	# Not sure why, but the way polars does a join is to duplicate every column, even identical ones, except for merge_upon.
	# This means we need to iterate through every column, pair them, see if they're equal, & then decide what to do with 'em.
	conflicts = set()
	not_conflicts = set()

	nullfilled_left = left.join(right, on=merge_upon, how="left").with_columns(
		[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in left.columns if col != merge_upon]
	).select(left.columns).sort(merge_upon)
	nullfilled_right = right.join(left, on=merge_upon, how="left").with_columns(
		[pl.col(f"{col}").fill_null(pl.col(f"{col}_right")).alias(col) for col in right.columns if col != merge_upon]
	).select(right.columns).sort(merge_upon)
	NeighLib.super_print_pl(nullfilled_left, "sorted filled in left")
	NeighLib.super_print_pl(nullfilled_right, "sorted filled in right")

	merge_via_join = nullfilled_left.join(nullfilled_right, on=merge_upon, how="full")
	merge_via_sort_merge = nullfilled_left.merge_sorted(nullfilled_right, merge_upon)
	NeighLib.super_print_pl(merge_via_join, "merge_via_join")
	NeighLib.super_print_pl(merge_via_sort_merge, "merge_via_sort_merge")
	NeighLib.super_print_pl(merge_via_sort_merge.unique().sort(merge_upon), "merge_via_sort_merge uniq")

	# TODO: figure out how to handle rows with conflicts

	len_left, len_right, len_current = left.shape[0], right.shape[0], merge.shape[0]
	len_new_rows = len_current - len_left
	if verbose:
		print(f"Was {len_left} rows, merged with {len_right} rows, now {len_current} rows")
		print(f"This implies {len_new_rows} new rows were added (or failed to merge)")
		print(f"{len(conflicts)} columns had conflicting data: {conflicts}")
		print(f"{len(not_conflicts)} columuns merged cleanly: {not_conflicts}")

	NeighLib.super_print_pl(merge, "merge")
	exit(0)

	return merge

def merge_pandas_dataframes(left, right, merge_upon, new_data_name, name_in_column=None):
	"""
	Merge two pandas dataframe upon merge_upon. 

	If new_data_name is None, an new_data_name column will be created temporarily but dropped before returning.
	name_in_column: If not None, adds a row of new_data_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times.

	Handling _x and _y conflicts is not fully implemented.
	"""
	import pandas as pd
	import numpy as np

	print(f"Merging {new_data_name} on {merge_upon}")

	if name_in_column is not None:
		# ie, right['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		right[name_in_column] = new_data_name

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