# general purpose functions

import polars as pl
from polars.testing import assert_series_equal

class NeighLib:
	def concat_dicts(dict_list):
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
		return combined_dict
		
	def get_x_y_column_pairs(dataframe):
		"""
		Take in a pandas dataframe and return a list of lists of all of its
		_x and _y columns (and those columns basename). Designed for the
		aftermath of an outer merge of two dataframes. Asserts no duplicate
		_x and _y columns (eg, can't have two "BioSample_x" columns).
		"""
		all_columns = dataframe.columns
		list_of_pairs = []
		for col in all_columns:
			if col.endswith("_x"):
				foo_x = col
				foo = foo_x[:-2]
				foo_y = foo + "_y"
				if foo_y in all_columns:
					list_of_pairs.append([foo_x, foo_y, foo])
				else:
					raise ValueError("Found {foo_x}, but no {foo_y} counterpart!")
		assert len(set(map(tuple, list_of_pairs))) == len(list_of_pairs)  # there should be no duplicate columns
		return list_of_pairs  # list of lists [["foo_x", "foo_y", "foo"]]
	
	def polars_cast(polars_df, dictionary):
		# untested!!!
		for col_name, cast_type in dictionary: 
			polars_df.select(pl.col(col_name).cast(cast_type))
		return polars_df
	
	def drop_some_assay_types(dataframe, to_drop=['Tn-Seq', 'ChIP-Seq']):
		"""
		Drops a list of values for assay_type from a Pandas dataframe
		"""
		if 'assay_type' in incoming.columns:
			rows_before = len(incoming.index)
			for drop_me in to_drop:
				incoming = incoming[~incoming['assay_type'].str.contains(drop_me, case=False, na=False)]
			rows_after = len(incoming.index)
			print(f"Dropped {rows_before - rows_after} samples")

	def drop_metagenomic(dataframe):
		"""
		Attempt to drop metagenomic data from a Pandas dataframe
		"""
		dropped = 0
		if 'organism' in dataframe.columns:
			rows_before = len(dataframe.index)
			dataframe = dataframe[~dataframe['organism'].str.contains('metagenome', case=False, na=False)]
			rows_after = len(dataframe.index)
			dropped += rows_before - rows_after
		if 'librarysource' in dataframe.columns:
			rows_before = len(dataframe.index)
			dataframe = dataframe[~dataframe['librarysource'].str.contains('METAGENOMIC', case=False, na=False)]
			rows_after = len(dataframe.index)
			dropped += rows_before - rows_after
		if verbose: print(f"Dropped {dropped} metagenomic samples")
		return dataframe
	
	def get_paired_illumina(dataframe):
		return dataframe.loc[dataframe['platform'] == 'ILLUMINA'].loc[dataframe['librarylayout'] == 'PAIRED']
	
	def check_dataframe_type(dataframe, wanted):
		""" Checks if dataframe is polars and pandas. If it doesn't match wanted, throw an error."""
		pass
	
	def pandas_vs_polars():
		"""
		Just an example for now. Basically what we've learned is to use from_pandas() to get schemas correctly.

		print("Pandas to polars:")
		ptp = pl.from_pandas(bq_to_merge)
		print("Pandas to Polars to Pandas to Polars:")
		ptptptp = pl.from_pandas(ptp.to_pandas())
		print("Pandas to TSV to polars:")
		ptttp = polars_from_tsv(f'./intermediate/{os.path.basename(bq_file)}_temp_read_polars.tsv')
		print("Pandas to TSV to Pandas to Polars:")
		ptttptp = pl.from_pandas(pandas_from_tsv(f'./intermediate/{os.path.basename(bq_file)}_temp_read_polars.tsv'))

		pl.testing.assert_frame_equal(ptp, ptptptp)
		pl.testing.assert_frame_equal(ptp, ptttp)
		pl.testing.assert_frame_equal(ptp, ptttptp)
		"""
	
	def get_dupe_columns_of_two_polars(polars_df_a, polars_df_b):
		""" Assert two polars dataframes do not share any columns """
		columns_a = list(polars_df_a.columns)
		columns_b = list(polars_df_b.columns)
		dupes = []
		for column in columns_a:
			if column in columns_b:
				dupes.append(column)
		for column in columns_b:
			if column in columns_a:
				dupes.append(column)
		if len(dupes) >= 0:
			#raise AssertionError(f"Polars dataframes have duplicated columns: {dupes}")
			for dupe in dupes:
				assert_series_equal(polars_df_a[dupe], polars_df_b[dupe])
			return dupes
		
	
	def assert_unique_columns(pandas_df):
		"""Assert all columns in a pandas df are unique -- useful if converting to polars """
		if len(pandas_df.columns) != len(set(pandas_df.columns)):
			dupes = []
			not_dupes = set()
			for column in pandas_df.columns:
				if column in not_dupes:
					dupes.append(column)
				else:
					not_dupes.add(column)
			raise AssertionError(f"Pandas df has duplicate columns: {dupes}")

	def mega_debug_merge(merge, merge_upon):
		n_mergefails =  merge['merge_status_unprocessed'].value_counts()['right_only']
		#print(f"Added {len_incoming} {merge_upon}s to the dataframe (was {len_previous} BioSamples, current length {len_current})")
		#print(f"{n_mergefails} {merge_upon}s seem to have failed to merge")
		print("Samples that failed to merge (right_only):")
		print(merge.loc[merge['merge_status_unprocessed'] == 'right_only', ['BioSample', 'run_accession']])
		
		#definitely_shouldve_merged = get_paired_illumina(dataframe)
		#print(f"--> Of these, {len(definitely_shouldve_merged)} seem to be paired-end Illumina")
		#if len(definitely_shouldve_merged) > 0:
		#	print("Unmerged paired illumina:")
		#	print(definitely_shouldve_merged[['BioSample', 'run_accession', 'assay_type']])

		unmerged_A = merge[merge['merge_status_unprocessed'] == 'right_only']  # incoming
		unmerged_B = merge[merge['merge_status_unprocessed'] == 'left_only']   # previous
		unmerged_A_nans = unmerged_A[unmerged_A[merge_upon].isna()]
		unmerged_B_nans = unmerged_B[unmerged_B[merge_upon].isna()]
		print("unmerged_A")
		print(unmerged_A[['BioSample', 'run_accession']])
		print("unmerged_B")
		print(unmerged_B[['BioSample', 'run_accession']])
		print("unmerged_A_nans")
		print(unmerged_A_nans[['BioSample', 'run_accession']])
		print("unmerged_B_nans")
		print(unmerged_B_nans[['BioSample', 'run_accession']])

		if not unmerged_A_nans.empty:
			print("Incoming dataframe has nans on the column we need to merge upon")
			exit(1)
		if not unmerged_B_nans.empty:
			# will break if merge_upon is BioSample but I think this will only fire on the first one which is run accessions?
			print("Existing dataframe has nans on the column we need to merge upon, will attempt a BioSample merge")

		#set_A = column_to_set(unmerged_A[merge_upon])
		#set_B = column_to_set(unmerged_B[merge_upon])
		#print("unmerged set A")
		#print(set_A)
		#print("unmerged set B")
		#print(set_B)
	
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
