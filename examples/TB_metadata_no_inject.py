rc = "rc172025-07-08TREEONLY"

import polars as pl

import time
start = time.time()
import gc
import src as Ranchero
_b_ = "\033[1m"
_bb_ = "\033[0m"
_c_ = "\033[0;36m"
print(f"Module import time: {time.time() - start:.4f}")
start_from_scratch = True
inject = True
do_run_index_merges = True

module_start = time.time()

def inital_file_parse():

	start, tba6 = time.time(),Ranchero.from_bigquery("./inputs/BQ/tba6_no_tax_table_bq_2024-09-19.json_modified.json")
	print(f"Parsed tba6 file from bigquery in {time.time() - start:.4f} seconds")  # should be under five minutes for tba5, less for tba6
	start, tba6 = time.time(), Ranchero.drop_non_tb_columns(tba6)
	print(f"Dropped non-TB-related columns in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.rancheroize(tba6)
	print(f"Rancheroized in {time.time() - start:.4f} seconds")
	Ranchero.NeighLib.report(tba6)

	# since we will be merging with an injected table later, let's drop almost every column
	cool_columns = ['libraryselection', 'librarysource', 'librarylayout', 'platform', 'SRS_id',
					'sample_index', 'run_index', 'assay_type', 'center_name', 'SRX_id', 'sra_study']
	for column in tba6.columns:
		if column not in cool_columns:
			tba6 = tba6.drop(column)

	#start, tba6 = time.time(), Ranchero.standardize_everything(tba6)
	#print(f"Standardized in {time.time() - start:.4f} seconds")

	Ranchero.NeighLib.report(tba6)
	gc.collect()
	return tba6

def sample_index_merges(merged_runs):
	merged = merged_runs

	# merged with sample-indexed data
	# merged with sample-indexed data
	print(f"{_b_}Preparing to swap index{_bb_}")
	start = time.time()
	merged_flat = Ranchero.hella_flat(merged)
	sra_only = Ranchero.run_index_to_sample_index(merged_flat)
	sra_only = Ranchero.hella_flat(sra_only)
	sra_only_sample_count = sra_only.shape[0]
	print(f"{_b_}Converted {merged_runs.shape[0]} run indeces into {sra_only_sample_count} sample indeces in {time.time() - start:.4f} seconds{_bb_}")
	Ranchero.to_tsv(sra_only, f"./merged_per_sample_{rc}.tsv")

	print(f"We started with {sra_only.shape[0]} samples for all of the genus (this EXCLUDES Fran's samples and denylist)")
	sra_PE = sra_only.filter(pl.col('librarylayout').list.contains("PAIRED"))
	print(f"Of which {sra_PE.shape[0]} are paired")
	sra_PEILL = sra_PE.filter(pl.col('platform').list.contains('ILLUMINA'))
	print(f"Of which are {sra_PEILL.shape[0]} are paired illumina")

	# input lists
	start = time.time()
	print(f"{_b_}Processing inputs, outputs, denylist, and what's on the tree{_bb_}")

	final_tree = Ranchero.from_tsv("./ranchero_output_archive/2025-07-08-FINAL_ranchero_rc17.subset.annotated.tsv", auto_rancheroize=False, list_columns=['run_index', 'pheno_source', 'SRX_id']).drop('collection', strict=False)
	for column in tree.columns:
		if column in sra_only and column not in ['sample_index']:
			sra_only = sra_only.drop(column)
	tree_only_merged = Ranchero.merge_dataframes(sra_only, final_tree, merge_upon="sample_index", right_name="tree", indicator="collection", drop_exclusive_right=True)
	sra_only_merged  = Ranchero.merge_dataframes(sra_only, final_tree, merge_upon="sample_index", right_name="tree", indicator="collection", drop_exclusive_right=True)
	all_merged       = Ranchero.merge_dataframes(sra_only, final_tree, merge_upon="sample_index", right_name="tree", indicator="collection", drop_exclusive_right=False)
	print(f"Probable final_tree file ({final_tree.shape[0]}):")
	print(f"->{sra_only_merged.filter(pl.col('collection') == pl.lit('tree')).shape[0]} SRA samples had metadata added")
	print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples would have been added")

	fake_tree = Ranchero.from_tsv("./inputs/pipeline/samples on tree 2024-12-12.txt", auto_rancheroize=False)
	tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, fake_tree, merge_upon="sample_index", right_name="fake_tree", indicator="collection", drop_exclusive_right=True)
	sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, fake_tree, merge_upon="sample_index", right_name="fake_tree", indicator="collection", drop_exclusive_right=True)
	all_merged      = Ranchero.merge_dataframes(all_merged, fake_tree, merge_upon="sample_index", right_name="fake_tree", indicator="collection", drop_exclusive_right=False)
	print(f"Probable fake_tree file ({fake_tree.shape[0]}):")
	print(f"->{sra_only_merged.filter(pl.col('collection') == pl.lit('fake_tree')).shape[0]} SRA samples had metadata added")
	print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples would have been added")

	diffs = Ranchero.from_tsv("./inputs/pipeline/probable_diffs.txt", auto_rancheroize=False)
	tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, diffs, merge_upon="sample_index", right_name="diffs", indicator="collection", drop_exclusive_right=True)
	sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, diffs, merge_upon="sample_index", right_name="diff", indicator="collection", drop_exclusive_right=True)
	all_merged      = Ranchero.merge_dataframes(all_merged, diffs, merge_upon="sample_index", right_name="diff", indicator="collection", drop_exclusive_right=False)
	print(f"Probable diffs file ({diffs.shape[0]}):")
	print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('diff'))).shape[0]} SRA samples had metadata added")
	print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples would have been added")

	tbprofiler = Ranchero.from_tsv("./inputs/tbprofiler/tbprofiler_basically_everything_rancheroized.tsv", auto_rancheroize=False)
	tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=True)
	sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=True)
	all_merged      = Ranchero.merge_dataframes(all_merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=False)
	print(f"Probable tbprofiler file ({tbprofiler.shape[0]}): KNOWN TO EXCLUDE SOME SAMPLES)")
	print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('tbprofiler'))).shape[0]} SRA samples had metadata added")
	print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples would have been added")
	
	inputs = Ranchero.from_tsv("./inputs/pipeline/probable_inputs.txt", auto_rancheroize=False)
	tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, inputs, merge_upon="sample_index", right_name="inputs", indicator="collection", drop_exclusive_right=True)
	sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, inputs, merge_upon="sample_index", right_name="input", indicator="collection", drop_exclusive_right=True)
	all_merged      = Ranchero.merge_dataframes(all_merged, inputs, merge_upon="sample_index", right_name="input", indicator="collection", drop_exclusive_right=False)
	print(f"Probable inputs file ({inputs.shape[0]}):")
	print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('input'))).shape[0]} SRA samples had metadata added")
	print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples would have been added")

	denylist = Ranchero.from_tsv("./inputs/pipeline/denylist_2024-07-23_lessdupes.tsv", auto_rancheroize=False)
	tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=True)
	sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=True)
	all_merged      = Ranchero.merge_dataframes(all_merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=False)
	print(f"Probable denylist file ({denylist.shape[0]}):")
	print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('denylist'))).shape[0]} SRA samples had metadata added")
	print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples would have been added")

	print("What's on the tree WITHOUT TBProfiler information?")
	print(tree_only_merged.filter(pl.col('tbprof_median_coverage').is_null()))
	print("Out of the samples on the tree, what is median and mean of the median coverage?")
	print(tree_only_merged.filter(pl.col('tbprof_median_coverage').is_not_null()).select('tbprof_median_coverage').pl.median('tbprof_median_coverage'))
	print(tree_only_merged.filter(pl.col('tbprof_median_coverage').is_not_null()).select('tbprof_median_coverage').pl.mean('tbprof_median_coverage'))

	print("What's on the tree with host information?")
	with pl.Config(tbl_cols=-1, tbl_rows=40):
		counts = tree_only_merged.select(pl.col('host_commonname').filter(pl.col('host_commonname').is_not_null()).value_counts(sort=True))
		print(counts)
	print(f"Looks like {counts.filter(pl.col('host_commonname') == pl.lit('human'))} are explictly human")


	print("What's on the tree with TBProfiler lineage information? (sometimes tbprofiler can't assign a lineage)")
	print(tree_only_merged.filter(pl.col('tbprof_main_lin').is_not_null()))
	print("Let's look at stuff on the tree and what their lineages seem to be, according to most sources")
	with pl.Config(tbl_cols=-1, tbl_rows=40): print(counts)
		print(tree_only_merged.filter(pl.col('tbprof_main_lin').is_not_null()).select(['tbprof_main_lin', 'tbprofiler_lineage_usher', 'lineage']))
	


	#print(tree_only_merged.filter(pl.col('tbprof_main_lin').is_not_null()).select(['tbprof_main_lin', 'tbprofiler_lineage_usher', 'lineage']))
#	print(tree_only_merged.select(pl.col('tbprof_median_coverage').filter(pl.col('tbprof_median_coverage').is_not_null()).value_counts(sort=True)))

	print("Have TBProfiler lineage but not normal lineage (excluding Fran and deny)")
	sra_only_merged_tbprofilerlineage = sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('tbprofiler')))
	sra_only_merged_sralineage = sra_only_merged.filter(pl.col('lineage').is_not_null())
	print("Do we have any TBProfiler files (including Fran and deny) that are not tagged as PE Illumina?")
	print(all_merged.filter(pl.col('collection').list.contains(pl.lit('tbprofiler'))))
	
	print(f"Merged with pipeline information in {time.time() - start:.4f} seconds")

	print(f"We started with {merged.shape[0]} samples for all of the genus")
	merged_filtered = merged.filter(
		(pl.col('organism') == pl.lit('Mycobacterium tuberculosis'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium bovis'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium africanum'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium cannettii'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium tuberculosis complex sp.'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium caprae'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium orygis'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium microti'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium pinnipedii')))
	print(f"Of which {merged_filtered.shape[0]} samples are MTB, bovis, africanum, or cannettii")
	merged_filtered = merged_filtered.filter(pl.col('librarylayout').list.contains("PAIRED"))
	print(f"Of which {merged_filtered.shape[0]} are paired")
	merged_filtered = merged_filtered.filter(pl.col('platform').list.contains('ILLUMINA'))
	print(f"Of which are {merged_filtered.shape[0]} are paired illumina")
	merged_filtered = merged_filtered.filter(~pl.col('collection').list.contains(pl.lit('denylist')))
	print(f"Of which are {merged_filtered.shape[0]} are not on the denylist")
	merged_filtered = merged_filtered.filter(pl.col('collection').list.contains(pl.lit('input')))
	print(f"Of which are {merged_filtered.shape[0]} were likely entered into the pipeline")
	merged_filtered = merged_filtered.filter(pl.col('collection').list.contains(pl.lit('tbprofiler')))
	print(f"Of which are {merged_filtered.shape[0]} have tbprofiler information")
	merged_filtered = merged_filtered.filter(pl.col('collection').list.contains(pl.lit('diff')))
	print(f"Of which are {merged_filtered.shape[0]} seems to have made a diff file")
	merged_filtered = merged_filtered.filter(pl.col('collection').list.contains(pl.lit('tree')))
	print(f"Of which are {merged_filtered.shape[0]} were on the tree as of July 8th 2025")

	print("Let's start from inputs instead...")
	merged_alternative = merged.filter(pl.col('collection').list.contains(pl.lit('input')))
	print(f"Of which are {merged_alternative.shape[0]} were likely entered into the pipeline")
	merged_alternative = merged_alternative.filter(pl.col('collection').list.contains(pl.lit('tbprofiler')))
	print(f"Of which are {merged_alternative.shape[0]} have tbprofiler information")
	merged_alternative = merged_alternative.filter(pl.col('collection').list.contains(pl.lit('diff')))
	print(f"Of which are {merged_alternative.shape[0]} seems to have made a diff file")
	merged_alternative = merged_alternative.filter(pl.col('collection').list.contains(pl.lit('tree')))
	print(f"Of which are {merged_alternative.shape[0]} were on the tree as of July 8th 2025")

	print("Let's take a step back and just filter by what's on the tree and forget about everything else.")
	merged_sliced = merged.filter(pl.col('collection').list.contains(pl.lit('tree')))
	print(f"That gives us {merged_sliced.shape[0]} samples")
	return merged_sliced


########################################################################

tba6_standardized = inital_file_parse()
merged = sample_index_merges(tba6_standardized)

merged = Ranchero.hella_flat(merged)
Ranchero.print_schema(merged)
Ranchero.NeighLib.print_value_counts(merged, ['sample_source'])
Ranchero.NeighLib.print_value_counts(merged, ['host_scienname', 'host_confidence', 'host_streetname'])
Ranchero.NeighLib.print_value_counts(merged, ['date_collected'])
Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage'])
Ranchero.NeighLib.print_value_counts(merged, ['country', 'continent', 'region'])
Ranchero.NeighLib.report(merged)

Ranchero.to_tsv(merged, f"./ranchero_{rc}_full_columns.tsv")
merged = merged.drop(['primary_search', 'mbases_sum', 'bases_sum', 'bytes_sum', 'libraryselection', 'librarysource', 'instrument', 'host_info'], strict=False) # for less_columns version
Ranchero.to_tsv(merged, f"./ranchero_{rc}_less_columns.tsv")


print(f"Finished entire module in {time.time() - module_start} seconds")


