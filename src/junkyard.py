# Functions that don't quite work
# I don't want to look at them, but for now they're not getting deleted


# Made various attempts to avoid using pandas in polars_fix_attributes_and_json_normalize():
#polars_df.select(pl.map_batches(["attributes"], NeighLib.concat_dicts)) # could not determine output type
#
#for row in polars_df.select("attributes"):
#	row.to_frame().map_rows(NeighLib.concat_dicts_tuple) # could not determine output type
#
# doesn't error, but not what we're looking for:
#exploded = polars_df.explode("attributes")
#attributes = polars_flatten(exploded, upon="biosample", keep_all_columns=True, rancheroize=False)
#
# Since it runs below ten seconds even on the full size dataframe, I consider the pandas usage acceptable,
# even though it annoys me on priniciple.

def store_known_multi_accession_metadata(pandas_df_indexed_by_runs):
	"""
	Stores some metadata from run accessions that share a BioSample so you can later verify things didn't get lost when
	they are BioSample-indexed.
	"""
	pass

def check_dataframe_type(dataframe, wanted):
	""" Checks if dataframe is polars and pandas. If it doesn't match wanted, throw an error."""
	pass

def concat_dicts_tuple(dict_list: list):
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
	return combined_dict.items()

def polars_flatten_both_ways(input_file, input_polars, upon='BioSample', keep_all_columns=False, rancheroize=True):
	"""
	Flattens an input file using polars group_by().agg(). This is designed to essentially turn run accession indexed dataframes
	into BioSample-indexed dataframes. Because Ranchero uses a mixture of Pandas and Polars, this function writes the output
	to the disk and returns the path to that file, rather than trying to retrun the dataframe itself.

	If rancheroize, attempt to rename columns to ranchero format.
	"""
	print(f"Flattening {upon}...")
	not_flat_1 = polars_from_tsv(input_file)
	not_flat_2 = input_polars

	#print(verify_acc_and_acc1(not_flat)) # TODO: make this actually do something, like drop acc_1

	if rancheroize:
		#if verbose: print(list(not_flat.columns))
		not_flat = not_flat.rename(columns.bq_col_to_ranchero_col)
		#if verbose: print(list(not_flat.columns))
	
	if keep_all_columns:
		# not tested!
		columns_to_keep = not_flat.col.copy().remove(upon)
		flat = not_flat.group_by(upon).agg(columns_to_keep)
		for nested_column in not_flat.col:
			flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})
			flat = flat_neo  # silly workaround for flat = flat.with_columns(...).rename(...) throwing an error about duped columns
	else:
		columns_to_keep = columns.recommended_sra_columns
		columns_to_keep.remove(upon)
		flat = not_flat.group_by(upon).agg(pl.col(columns_to_keep))
		for nested_column in columns.recommended_sra_columns:
			flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})
			flat = flat_neo  # silly workaround for flat = flat.with_columns(...).rename(...) throwing an error about duped columns

	flat_neo = flat_neo.unique() # doesn't seem to drop anything but may as well leave it
	path = f"./intermediate/{os.path.basename(input_file)}_flattened.tsv"
	polars_to_tsv(flat_neo, path)
	return path

