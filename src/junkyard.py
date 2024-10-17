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

	def likely_is_run_indexed(polars_df):
		# TODO: make more robust
		singular_runs = (
			("run_index" in polars_df.schema and polars_df.schema["run_index"] == pl.String) or
			("run_accession" in polars_df.schema and polars_df.schema["run_accession"] == pl.String) or
			("acc" in polars_df.schema and polars_df.schema["acc"] == pl.String)
		)
		if singular_runs:
			return True
		else:
			return False

	@classmethod
	def iteratively_merge_these_columns(cls, polars_df, merge_these_columns: list, equivalence_key=None: str):
		"""
		Merges columns named in merged_these_columns.

		If equivalence_key is not None, columns are assumed to all share the same kolumns.equivalence key, and ones with a lower
		index number (in the value-list for their shared kolumns.equivalence key) will be given priority when there's a conflict.
		Additionally, when all is said and done, the final merged column will be named equivalene_key's value.
		"""
		assert len(merge_these_columns) > 1
		assert all(col in polars_df.columns for col in merge_these_columns)
		assert all(not col.endswith("_right") for col in polars_df.columns)
		debug_columns = ['collection_date_sam', 'sample_collection_date_sam_s_dpl127', 'collection_date_run', 'colection_date_sam']
		
		left_col, right_col = merge_these_columns[0], merge_these_columns[1]

		logging.debug(f"Contains:\n\t{[col for col in polars_df.columns if col in debug_columns]}\nIntending to merge:\n\t{merge_these_columns}\n\t\tLeft:{left_col}\n\t\tRight:{right_col}")

		if equivalence_key is not None:

		logging.debug(f"Merging {left_col} and {right_col} by renaming {right_col} to {left_col}_right")
		polars_df = polars_df.rename({right_col: f"{left_col}_right"})
		

		# TODO: only rename and call merge_right_columns if not in a special handling kolumns. else, use other merge thinking.


		polars_df = cls.merge_right_columns(polars_df)
		logging.debug(f"Date columns after right merge: {[col for col in polars_df.columns if col in debug_columns]}")

		del merge_these_columns[1]

		if len(merge_these_columns) > 1:
			logging.debug(f"merge_these_columns is {merge_these_columns}, which we will pass in to recurse")
			polars_df = cls.iteratively_merge_these_columns(polars_df, merge_these_columns)

		#if and_rancheroize_the_name:
		#	logging.debug(f"Date columns after and_rancheroize_the_name: {[col for col in polars_df.columns if col in debug_columns]}")
		#	polars_df = cls.rancheroize_polars(polars_df) # recursion? sure why not
		#	logging.debug(f"Date columns after and_rancheroize_the_name: {[col for col in polars_df.columns if col in debug_columns]}")
		
		return polars_df

def nullfill_and_merge_these_columns(polars_df, particular_columns: list, final_name: str):
	"""DO NOT USE. USE MERGE RIGHT INSTEAD"""
	for i in range(len(particular_columns) - 1):
		col_A, col_B = particular_columns[i], particular_columns[i + 1]
		if polars_df.get_column(col_A).dtype == pl.List:
			polars_df = cls.stringify_one_list_column(polars_df, col_A)
		if polars_df.get_column(col_B).dtype == pl.List:
			polars_df = cls.stringify_one_list_column(polars_df, col_B)
		
		polars_df = polars_df.with_columns(pl.col(f"{col_B}").fill_null(pl.col(f"{col_A}")).alias(col_B))
		print(f"[{i}] filled {col_B} with {col_A}, dropping {col_A}")
		
		are_equal_now = polars_df.select(f"{col_A}").equals(polars_df.select(f"{col_A}"), null_equal=True)
		if any(particular_columns) in kolumns.rancheroize__warn_if_list_with_unique_values and not are_equal_now:
			print(f"ERROR: {col_A} and {col_B} had different values.")
			exit(1)
		polars_df = polars_df.drop(col_A)
		if i == (len(particular_columns) - 2):
			#print(f"Renaming {col_B} to {final_name}")
			polars_df = polars_df.rename({col_B: final_name})
		
	return polars_df

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

