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
	print(f"{_b_}Preparing to swap index{_bb_}")
	start = time.time()
	merged_flat = Ranchero.hella_flat(merged)
	merged_by_sample = Ranchero.run_index_to_sample_index(merged_flat)
	merged_by_sample = Ranchero.hella_flat(merged_by_sample)
	print(f"{_b_}Converted run indeces to sample indeces in {time.time() - start:.4f} seconds{_bb_}")
	Ranchero.to_tsv(merged_by_sample, f"./merged_per_sample_{rc}.tsv")
	merged = merged_by_sample

	# input lists
	start = time.time()
	print(f"{_b_}Processing inputs, outputs, denylist, and what's on the tree{_bb_}")

	tree = Ranchero.from_tsv("./ranchero_output_archive/2025-07-08-FINAL_ranchero_rc17.subset.annotated.tsv", auto_rancheroize=False, list_columns=['run_index', 'pheno_source', 'SRX_id']).drop('collection', strict=False)
	for column in tree.columns:
		if column in merged and column not in ['sample_index']:
			merged = merged.drop(column)
	merged = Ranchero.merge_dataframes(merged, tree, merge_upon="sample_index", right_name="tree", indicator="collection", drop_exclusive_right=False)
	Ranchero.NeighLib.report(merged)

	inputs = Ranchero.from_tsv("./inputs/pipeline/probable_inputs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, inputs, merge_upon="sample_index", right_name="input", indicator="collection", drop_exclusive_right=False)
	
	diffs = Ranchero.from_tsv("./inputs/pipeline/probable_diffs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, diffs, merge_upon="sample_index", right_name="diff", indicator="collection", drop_exclusive_right=False)

	tbprofiler = Ranchero.from_tsv("./inputs/TBProfiler/tbprofiler_basically_everything_rancheroized.tsv").drop(['tbprof_rd', 'tbprof_spoligotype', 'tbprof_frac'], strict=False)
	tbprofiler = tbprofiler.drop(['tbprof_main_lin', 'tbprof_family', 'superbatch'])
	merged = Ranchero.merge_dataframes(merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=False)
	
	denylist = Ranchero.from_tsv("./inputs/pipeline/denylist_2024-07-23_lessdupes.tsv", auto_rancheroize=False).drop('reason')
	merged = Ranchero.merge_dataframes(merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=False)

	reports = Ranchero.from_tsv('./inputs/pipeline/reports.20250702.tsv', auto_rancheroize=False)
	reports = reports.with_columns(Ranchero.NeighLib.multiply_and_trim("float_below_10x_coverage").alias("percent_below_10x_coverage")).drop('float_below_10x_coverage')
	merged = Ranchero.merge_dataframes(merged, reports, merge_upon="sample_index", right_name="reports", indicator="collection", drop_exclusive_right=False)
	
	print(f"Merged with pipeline information in {time.time() - start:.4f} seconds")

	Ranchero.NeighLib.print_value_counts(merged, ['collection'])
	Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism'])

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


