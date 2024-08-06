from src.neigh import NeighLib

def merge_dataframes(previous, incoming, merge_upon, new_data_name, name_in_column=None):
	"""
	Merge two pandas dataframe upon merge_upon. 

	If new_data_name is None, an new_data_name column will be created temporarily but dropped before returning.
	name_in_column: If not None, adds a row of new_data_name to the dataframe. Designed for marking the source of data when
	merging dataframes multiple times.
	"""
	print(f"Merging {new_data_name} on {merge_upon}")

	if name_in_column is not None:
		# ie, incoming['literature_shorthand'] = "CRyPTIC Antibiotic Study"
		incoming[name_in_column] = new_data_name

	merge = pd.merge(previous, incoming, on=merge_upon, how='outer', indicator='merge_status_unprocessed')
	len_previous, len_incoming, len_current = len(previous.index), len(incoming.index), len(merge.index)
	for conflict in get_x_y_column_pairs(merge):
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

			# NOW ACTUALLY FIX THOSE!!!

			merge = merge.rename(columns={foo_x: foo})
			merge = merge.drop(foo_y, axis=1)
			# TODO: drop the "missing" values into nans
	else:
		if verbose: print(f"{foo} seem unchanged")
	
	if verbose: mega_debug_merge(merge)



	#perhaps_merged_rows = not_merged_rows.apply(merge_unmerged_rows_pandas, axis=1)
	#print(perhaps_merged_rows)

	#merged_df = pd.concat([merged_df, not_merged_rows])

	# make new new_data_name column
	merge[new_data_name] = merge['merge_status_unprocessed'].apply(lambda x: True if x in ['right_only', 'both'] else False)
	merge.drop('merge_status_unprocessed', axis=1, inplace=True) 
	
	return merge