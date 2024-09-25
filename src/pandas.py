
# Old pandas functions
# NOT SUPPORTED. NOT USED.

def progress_apply_with_tqdm_if_available(pandas_df, column, function, *kwargs):
	try:
		import tqdm
		tqdm.pandas()
		tqdm_exists = True
	except ImportError:
		tqdm_exists = False
	if tqdm_exists:
		# progress apply
	else:
		print("Failed to import tqdm -- code will continue to execute, but there will be no progress bar.")
		# progress apply

def pandas_from_ncbi_run_selector(csv):
	run_raw = pd.read_csv(csv)
	run_renamed = run_raw.rename(columns=columns.ncbi_run_selector_col_to_ranchero_col)  # for compatibility with other formats
	return run_renamed

def pandas_json_normalize(self, pandas_df, use_polars=True, rancheroize=False):
	"""
	JSON-normalize the "attributes" column into new columns. use_polars is faster but might break things.
	Regardless of use_polars, input and output are pandas dataframes. Assumes pandas_fix_attributes_dictionaries() was run but
	that shouldn't be necessary.

	Config used:
	* intermediate_files (set)
	* verbose (set)
	"""
	outfile = './intermediate/normalized.tsv'
	print("Normalizing...")

	if use_polars:
		# polars-and-pandas version
		# Even if you set this up to not read the intermedite file to set pandas_df, you'll get less columns in the end?
		# However, this does seem to correctly pull out "bytes" and other JSON data, so we'll take it.
		just_attributes = pl.json_normalize(pandas_df['attributes'], strict=False)  # just_attributes is a polars dataframe
		# collection_date_sam is likely present in both dataframes, so rename one of them
		just_attributes = just_attributes.rename({'collection_date_sam': 'collection_date_from_attributes'})
		dupe_columns = NeighLib.get_dupe_columns_of_two_polars(just_attributes, pl.from_pandas(pandas_df))
		if len(dupe_columns) > 0:
			# TODO: just rename the columns like we did with collection_date_sam!!
			raise AssertionError("Found columns from the attributes section that conflict with an existing column")
		bq_jnorm = pd.concat([pandas_df.drop(columns=['attributes']), just_attributes.to_pandas()], axis=1)
		if self.cfg.verbose: print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
		if rancheroize: bq_jnorm.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)
		if self.cfg.intermediate_files:
			NeighLib.pandas_to_tsv(bq_jnorm, outfile)
			print(f"Wrote JSON-normalized dataframe to {outfile}")
	else:
		# pure pandas version
		# This is the slowest, but it acceptable enough. 
		just_attributes = pd.json_normalize(pandas_df['attributes'])  # just_attributes is a Python dictionary
		# collection_date_sam is likely present in both dataframes, so rename one of them
		just_attributes['collection_date_from_attributes'] = just_attributes.pop('collection_date_sam')
		bq_jnorm = pd.concat([pandas_df.drop(columns=['attributes']), just_attributes], axis=1)
		if self.cfg.verbose: print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
		if rancheroize: bq_jnorm.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)
		if self.cfg.intermediate_files: 
			NeighLib.pandas_to_tsv(bq_jnorm, outfile)
			print(f"Wrote JSON-normalized dataframe to {outfile}")
	
	return bq_jnorm

def pandas_from_bigquery(self, bq_file, fix_attributes=True, normalize_attributes=True, polars_normalize=None):
	"""
	1) Read bigquery JSON
	2) if fix_attributes or normalize_attributes: Turns "attributes" column's lists of one-element k/v pairs into dictionaries
	3) if normalize_attributes: JSON-normalize the "attributes" column into their own columns
	4) if merge_into_biosamples: Merge run accessions with the same BioSample, under the assumption that they've the same metadata

	Notes:
	* normalize_attributes and fix_attributes work on the "attributes" column, not the "j_attr" column
	* if polars_normalize, use the under development polars version of json normalize -- this is faster but could be unstable

	Configurations used:
	* immediate_biosample_merge (set)
	* intermediate_files (set)
	* polars_normalize (can be overwritten)
	* verbose (set)


	TODO: intermediate_files() not used?
	"""
	polars_normalize = _get_cfg_if_not_overwritten(polars_normalize)

	bq_raw = pd.read_json(bq_file)
	if self.cfg.verbose: print(f"{bq_file} has {len(bq_raw.columns)} columns and {len(bq_raw.index)} rows")

	if fix_attributes or normalize_attributes:
		bq_fixed = self.pandas_fix_attributes_dictionaries(bq_raw)
		if self.cfg.verbose: print(f"{bq_file} has {len(bq_fixed.columns)} columns and {len(bq_fixed.index)} rows")
	if normalize_attributes:  # requires pandas_fix_attributes_dictionaries() to happen first
		bq_norm = self.pandas_json_normalize(bq_fixed, use_polars=polars_normalize)
		NeighLib.assert_unique_columns(bq_norm)
		if self.cfg.verbose: print(f"{bq_file} has {len(bq_norm.columns)} columns and {len(bq_norm.index)} rows")
	if cfg.immediate_biosample_merge:
		bq_to_merge = bq_norm if bq_norm is not None else (bq_fixed if bq_fixed is not None else bq_raw)
		bq_jnorm = (self.polars_flatten(pl.from_pandas(bq_to_merge), upon='BioSample', keep_all_columns=False, rancheroize=True)).to_pandas()
	return bq_jnorm if bq_jnorm is not None else bq_flatdicts  # bq_flatdircts if not normalize_attributes

def pandas_fix_attributes_dictionaries(self, pandas_df, rancheroize=False):
	"""
	Uses NeighLib.concat_dicts to turn the weird format of the attributes column into flat dictionaries

	Configurations used:
	* intermediate_files (set)
	* verbose (set)
	"""
	if self.cfg.verbose:
		print("Concatenating dictionaries...")
		pandas_df['attributes'] = pandas_df['attributes'].progress_apply(NeighLib.concat_dicts)
	else:
		pandas_df['attributes'] = pandas_df['attributes'].apply(NeighLib.concat_dicts)
	pandas_flatdic, pandas_df = pandas_df.copy(), None  # supposedly increases effeciency
	if rancheroize: pandas_flatdic.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)
	if self.cfg.intermediate_files: NeighLib.pandas_to_tsv(pandas_flatdic, f'./intermediate/flatdicts.tsv')
	return pandas_flatdic

def pandas_from_tsv(tsv):
	return pd.read_csv(tsv, sep='\t')

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

def get_x_y_column_pairs(pandas_df):
	"""
	Take in a pandas dataframe and return a list of lists of all of its
	_x and _y columns (and those columns basename). Designed for the
	aftermath of an outer merge of two dataframes. Asserts no duplicate
	_x and _y columns (eg, can't have two "BioSample_x" columns).
	"""
	all_columns = pandas_df.columns
	list_of_pairs = []
	for col in all_columns:
		if col.endswith("_x"):
			print(f"col {col} ends with _x")
			foo_x = col
			foo = foo_x[:-2]
			foo_y = foo + "_y"
			if foo_y in all_columns:
				list_of_pairs.append([foo_x, foo_y, foo])
			else:
				raise ValueError("Found {foo_x}, but no {foo_y} counterpart!")
	assert len(set(map(tuple, list_of_pairs))) == len(list_of_pairs)  # there should be no duplicate columns
	return list_of_pairs  # list of lists [["foo_x", "foo_y", "foo"]]

def drop_some_assay_types(pandas_df, to_drop=['Tn-Seq', 'ChIP-Seq']):
	"""
	Drops a list of values for assay_type from a Pandas dataframe
	"""
	if 'assay_type' in pandas_df.columns:
		rows_before = len(pandas_df.index)
		for drop_me in to_drop:
			pandas_df = pandas_df[~pandas_df['assay_type'].str.contains(drop_me, case=False, na=False)]
		rows_after = len(incoming.index)
		print(f"Dropped {rows_before - rows_after} samples")
	return pandas_df

def drop_metagenomic(pandas_df):
	"""
	Attempt to drop metagenomic data from a Pandas dataframe
	"""
	dropped = 0
	if 'organism' in pandas_df.columns:
		rows_before = len(pandas_df.index)
		dataframe = pandas_df[~pandas_df['organism'].str.contains('metagenome', case=False, na=False)]
		rows_after = len(pandas_df.index)
		dropped += rows_before - rows_after
	if 'librarysource' in pandas_df.columns:
		rows_before = len(pandas_df.index)
		dataframe = pandas_df[~pandas_df['librarysource'].str.contains('METAGENOMIC', case=False, na=False)]
		rows_after = len(pandas_df.index)
		dropped += rows_before - rows_after
	#if verbose: print(f"Dropped {dropped} metagenomic samples")
	return pandas_df

def pandas_to_tsv(pandas_df, path: str):
	pandas_df.to_csv(path, sep='\t', index=False)


