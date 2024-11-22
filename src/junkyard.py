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

	def nullify(self, polars_df, only_these_columns=None, no_match_NA=False):
		"""
		Turns stuff like "not collected" and "n/a" into pl.Null values, per null_values.py
		Be aware that when matching on pl.List(pl.Utf8) columns, you CAN match on empty strings to properly remove them from
		the list, but when matching on pl.Utf8 columns, you CANNOT match on empty strings or else the entire column gets wiped.
		"""
		import time
		
		all_cols = only_these_columns if only_these_columns is not None else polars_df.columns
		string_cols = [col for col in all_cols if polars_df.schema[col] == pl.Utf8 and col not in kolumns.equivalence['run_index'] and col not in kolumns.equivalence['run_index']]
		list_cols = [col for col in all_cols if polars_df.schema[col] == pl.List(pl.Utf8)]
		real_null_values = null_values.null_values_regex if no_match_NA else null_values.null_values_regex_plus_NA

		self.logging.info("METHOD ONE: contains()")
		self.logging.info(string_cols)
		start = time.time()
		temp_polars_df = polars_df
		for null_value in real_null_values:
			self.logging.debug(f"Nullifying {null_value}...")
			temp_polars_df = temp_polars_df.with_columns([
				pl.when(pl.col(col).str.contains(null_value))
				.then(None)
				.otherwise(pl.col(col))
				.alias(col) for col in string_cols])
			temp_polars_df = temp_polars_df.with_columns([
				pl.col(col).list.eval(
					pl.element().filter(~pl.element().str.contains(null_value))
				)
				for col in list_cols])
		self.logging.info(f"Finished nullifying in {time.time() - start} seconds")
		self.print_value_counts(temp_polars_df, only_these_columns=['date_of_collection_sam', 'host_sam', 'organism_sam'])
		self.logging.info("METHOD TWO: contains_any()")
		start = time.time()
		temp_polars_df = polars_df.with_columns([
			pl.when(pl.col(col).str.contains_any(null_values.null_values_case_insensitive, ascii_case_insensitive=True))
			.then(None)
			.otherwise(pl.col(col))
			.alias(col) for col in string_cols])
		temp_polars_df = temp_polars_df.with_columns([
			pl.col(col).list.eval(
				pl.element().filter(~pl.element().str.contains_any(null_values.null_values_case_insensitive, ascii_case_insensitive=True))
			)
			for col in list_cols])
		self.logging.info(f"Finished nullifying in {time.time() - start} seconds")
		self.print_value_counts(temp_polars_df, only_these_columns=['date_of_collection_sam', 'host_sam', 'organism_sam'])
		self.logging.info("METHOD THREE: is_in()")
		self.logging.info("For reasons I don't understand, this ALSO doesn't seem to work on the host_sam column. See 2711 values of 'missing'!")
		string_cols = [col for col, dtype in polars_df.schema.items() if dtype == pl.Utf8]
		temp_polars_df = polars_df
		temp_polars_df = temp_polars_df.with_columns([
			pl.when(pl.col(col).is_in(null_values.null_values_regex))
			.then(None)
			.otherwise(pl.col(col))
			.alias(col) for col in string_cols])

		temp_polars_df = temp_polars_df.with_columns(pl.col(pl.List(pl.Utf8)).list.eval(
			pl.element().filter(~pl.element().is_in(null_values.null_values_regex))
		))
		self.logging.info(f"Finished nullifying in {time.time() - start} seconds")
		self.print_value_counts(temp_polars_df, only_these_columns=['date_of_collection_sam', 'host_sam', 'organism_sam'])
		self.logging.info("METHOD FOUR: replace()")
		self.logging.info("This just straight-up doesn't work.")
		start = time.time()
		temp_polars_df = polars_df.with_columns(pl.col(pl.Utf8).replace(null_values.null_values_plus_weird_stuff_dictionary))
		self.logging.info(f"Finished half-nullifying in {time.time() - start} seconds")
		temp_polars_df = temp_polars_df.with_columns(pl.col(pl.List(pl.Utf8)).replace(null_values.null_values_plus_weird_stuff_dictionary))
		self.logging.info(f"Finished list-nullifying in {time.time() - start} seconds")
		self.print_value_counts(temp_polars_df, only_these_columns=['date_of_collection_sam', 'host_sam', 'organism_sam'])


		# TODO: add in an assertion that sample/run IDs didn't get dropped

		exit(1)
		return polars_df

	def nullify(self, polars_df, only_these_columns=None, no_match_NA=False):
		"""
		Turns stuff like "not collected" and "n/a" into pl.Null values, per null_values.py
		Be aware that when matching on pl.List(pl.Utf8) columns, you CAN match on empty strings to properly remove them from
		the list, but when matching on pl.Utf8 columns, you CANNOT match on empty strings or else the entire column gets wiped.


		Previously, this avoided for loops, but only worked on columns of type string and had case sensitivity.
		There is probably a more effecient way to achieve these goals, but this is what I'm sticking to for now.
		"""
		null_values_plain = null_values.null_values if no_match_NA else null_values.null_values_plus_NA
		null_values_regex = null_values.null_values_regex if no_match_NA else null_values.null_values_regex_plus_NA
		#for null_value in null_values_regex:
		if only_these_columns is None:
			string_cols = [col for col, dtype in polars_df.schema.items() if dtype == pl.Utf8]
			polars_df = polars_df.with_columns([
				pl.when(pl.col(col).is_in(null_values_regex))
				#pl.when(pl.col(col).str.contains(null_value))
				#pl.when(pl.col(col).str.contains_any(null_values_plain))
				.then(None)
				.otherwise(pl.col(col))
				.alias(col) for col in string_cols])

			polars_df = polars_df.with_columns(pl.col(pl.List(pl.Utf8)).list.eval(
				pl.element().filter(~pl.element().is_in(null_values_regex))
				#pl.element().filter(~pl.element().is_in(null_values_plain))
				#pl.element().filter(~pl.element().str.contains_any(null_values.null_values, ascii_case_insensitive=True)))
			))
		else:
			for only_this_column in only_these_columns:
				if polars_df.schema[only_this_column] == pl.Utf8:
					polars_df = polars_df.with_columns([
						pl.when(pl.col(only_this_column).is_in(null_values_plain))
						.then(None)
						.otherwise(pl.col(only_this_column))
						.alias(only_this_column)])
				elif polars_df.schema[only_this_column] == pl.List:
					 polars_df = polars_df.with_columns(pl.col(only_this_column).list.eval(
					 	pl.element().filter(~pl.element().is_in(null_values_plain)))
					 )
				#polars_df = polars_df.with_columns(
				#	 	pl.col(only_this_column).list.eval(pl.element().filter(~pl.element().str.contains_any(null_values.null_values, ascii_case_insensitive=True)))
				#	 )
				else:
					logging.error(f"Tried to nullify {only_this_column} but has incompatiable type {polars_df[only_this_column].schema}")
		return polars_df

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

