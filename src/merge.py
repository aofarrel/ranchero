from src.neigh import NeighLib
import pandas as pd
import numpy as np
import polars as pl

verbose = True  # TODO: do this better

def merge_polars_dataframes(previous, incoming, merge_upon, new_data_name, name_in_column=None):
	"""
	Merge two polars dataframe upon merge_upon. 

	If new_data_name is None, an new_data_name column will be created temporarily but dropped before returning.
	name_in_column: If not None, adds a row of new_data_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times.
	"""
	if merge_upon not in previous.columns:
		raise ValueError(f"Tried to merge upon {merge_upon}, but no column with that name in left-hand dataframe")
	if merge_upon not in incoming.columns:
		raise ValueError(f"Tried to merge upon {merge_upon}, but no column with that name in right-hand dataframe")

	if merge_upon == 'run_index' or merge_upon == 'run_accession':
		if not NeighLib.likely_is_run_indexed(previous):
			print(f"WARNING: Merging upon {merge_upon}, but left-hand dataframe appears to not be indexed by run accession")
		if not NeighLib.likely_is_run_indexed(incoming):
			print(f"WARNING: Merging upon {merge_upon}, but right-hand dataframe appears to not be indexed by run accession")

	print(f"Merging {new_data_name} on {merge_upon}")

	if name_in_column is not None:
		# ie, incoming['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		incoming[name_in_column] = new_data_name

	merge = previous.join(incoming, on=merge_upon)

	# Not sure why, but the way polars does a join is to duplicate every column, even identical ones, except for merge_upon.
	# This means we need to iterate through every column, pair them, see if they're equal, & then decide what to do with 'em.
	conflicts = set()
	not_conflicts = set()
	for col in merge.columns:
		if col != merge_upon and "_right" not in col:
			previous_col = merge[f"{col}"]
			incoming_col = merge[f"{col}_right"]

			# Fill in null values before doing comparisons, as (previous_col == incoming_col).all() returns true even if
			# one of them has nulls the other doesn't (which would be fine in and of itself, but if the null is on the
			# left column then the not-null value would just get dropped by merge = merge.drop(f"{col}_right").
			previous_filled_in_nulls = pl.when(previous_col.is_null()).then(incoming_col).when(incoming_col.is_null()).then(previous_col).otherwise(previous_col)
			incoming_filled_in_nulls = pl.when(incoming_col.is_null()).then(previous_col).when(previous_col.is_null()).then(incoming_col).otherwise(incoming_col)



			# TODO: WE SHOULD NOT HAVE TWO COPIES OF PINELAND

			print(f"Column is {col}")
			NeighLib.super_print_pl(merge.with_columns(previous_filled_in_nulls))
			NeighLib.super_print_pl(merge.with_columns(incoming_filled_in_nulls))

			if (previous_col == incoming_col).all():
				# This returns true even if one column has nulls and the other does not.
				# For example, if previous_col has values ["foo", null, "bar"] and
				# incoming_col has values ["foo", "bizz", "bar"] then they are equal.
				merge = merge.drop(f"{col}_right")
				not_conflicts.add(col)

				# Add the filled-in-nulls column we stored earlier -- doesn't matter which one since they're equal
				merge = merge.drop(f"{col}")
				merge = merge.with_columns(previous_filled_in_nulls.alias(col))

			else:
				merge = merge.drop(f"{col}")
				merge = merge.drop(f"{col}_right")
				merge = merge.with_columns(previous_filled_in_nulls.alias(f"{col}_left"))
				merge = merge.with_columns(previous_filled_in_nulls.alias(f"{col}_right"))
				conflicts.add(col)

	len_previous, len_incoming, len_current = previous.shape[0], incoming.shape[0], merge.shape[0]
	len_new_rows = len_current - len_previous
	if verbose:
		print(f"Was {len_previous} rows, merged with {len_incoming} rows, now {len_current} rows")
		print(f"This implies {len_new_rows} new rows were added (or failed to merge)")
		print(f"{len(conflicts)} columns had conflicting data: {conflicts}")
		print(f"{len(not_conflicts)} columuns merged cleanly: {not_conflicts}")

	NeighLib.super_print_pl(merge)
	exit(0)

	return merge



def merge_polars_dataframes_basic(previous, incoming, merge_upon, new_data_name, name_in_column=None):
	"""
	Merge two polars dataframe upon merge_upon. 

	If new_data_name is None, an new_data_name column will be created temporarily but dropped before returning.
	name_in_column: If not None, adds a row of new_data_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times.

	This does not handle the duplicated columnns that can handle a full merge. They're left in place.
	"""
	if merge_upon not in previous.columns:
		raise ValueError(f"Tried to merge upon {merge_upon}, but no column with that name in left-hand dataframe")
	if merge_upon not in incoming.columns:
		raise ValueError(f"Tried to merge upon {merge_upon}, but no column with that name in right-hand dataframe")

	if merge_upon == 'run_index' or merge_upon == 'run_accession':
		if not NeighLib.likely_is_run_indexed(previous):
			print(f"WARNING: Merging upon {merge_upon}, but left-hand dataframe appears to not be indexed by run accession")
		if not NeighLib.likely_is_run_indexed(incoming):
			print(f"WARNING: Merging upon {merge_upon}, but right-hand dataframe appears to not be indexed by run accession")


	print(f"Merging {new_data_name} on {merge_upon}")

	if name_in_column is not None:
		# ie, incoming['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		incoming[name_in_column] = new_data_name

	merge = previous.join(incoming, on=merge_upon, how='full', coalesce=True, join_nulls=True)
	#len_previous, len_incoming, len_current = previous.shape[0], incoming.shape[0], merge.shape[0]
	return merge


def merge_pandas_dataframes(previous, incoming, merge_upon, new_data_name, name_in_column=None):
	"""
	Merge two pandas dataframe upon merge_upon. 

	If new_data_name is None, an new_data_name column will be created temporarily but dropped before returning.
	name_in_column: If not None, adds a row of new_data_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times.

	Handling _x and _y conflicts is not fully implemented.
	"""
	print(f"Merging {new_data_name} on {merge_upon}")

	if name_in_column is not None:
		# ie, incoming['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		incoming[name_in_column] = new_data_name

	merge = pd.merge(previous, incoming, on=merge_upon, how='outer', indicator='merge_status_unprocessed')
	len_previous, len_incoming, len_current = len(previous.index), len(incoming.index), len(merge.index)
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