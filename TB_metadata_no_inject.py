rc = "rc0"

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
	#we don't immediately rancheroize as this is faster (probably)
	start, tba6 = time.time(),Ranchero.from_bigquery("./inputs/BQ/tba6_no_tax_table_bq_2024-09-19.json_modified.json")
	print(f"Parsed tba6 file from bigquery in {time.time() - start:.4f} seconds")  # should be under five minutes for tba5, less for tba6
	start, tba6 = time.time(), Ranchero.drop_non_tb_columns(tba6)
	print(f"Dropped non-TB-related columns in {time.time() - start:.4f} seconds")
	tba6 = tba6.drop(['lat', 'lon', 'date_collected_year', 'date_collected_month', 'reason', 'host_info', 'geoloc_info', 'mbytes_sum_sum', 'geoloc_name'], strict=False)
	Ranchero.NeighLib.print_value_counts(tba6, ['librarylayout', 'platform'])

	start, tba6 = time.time(), Ranchero.rancheroize(tba6)
	print(f"Rancheroized in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.standardize_everything(tba6)
	print(f"Standardized in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.drop_lowcount_columns(tba6)
	print(f"Removed columns with few values in {time.time() - start:.4f}s seconds") # should be done last
	Ranchero.NeighLib.report(tba6)
	gc.collect()
	return tba6

def inject_metadata(tba6):
	return tba6

def run_merges(tba6):
	merged = tba6
	tba6 = None # avoid copy-paste mistakes
	return merged

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
	inputs = Ranchero.from_tsv("./inputs/pipeline/probable_inputs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, inputs, merge_upon="__index__sample_index", right_name="input", indicator="collection", drop_exclusive_right=False)
	
	diffs = Ranchero.from_tsv("./inputs/pipeline/probable_diffs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, diffs, merge_upon="__index__sample_index", right_name="diff", indicator="collection", drop_exclusive_right=False)
	
	tree = Ranchero.from_tsv("./inputs/pipeline/samples on tree 2024-12-12.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, tree, merge_upon="__index__sample_index", right_name="tree", indicator="collection", drop_exclusive_right=False)

	tbprofiler = Ranchero.from_tsv("./inputs/TBProfiler/tbprofiler_basically_everything_rancheroized.tsv")
	tbprofiler = tbprofiler.drop(['tbprof_main_lin', 'tbprof_family', 'superbatch'])
	merged = Ranchero.merge_dataframes(merged, tbprofiler, merge_upon="__index__sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=False)
	
	denylist = Ranchero.from_tsv("./inputs/pipeline/denylist_2024-07-23_lessdupes.tsv", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, denylist, merge_upon="__index__sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=False)
	
	print(f"Merged with pipeline information in {time.time() - start:.4f} seconds")

	Ranchero.NeighLib.print_value_counts(merged, ['collection'])
	Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism'])

	print(f"We started with {merged.shape[0]} samples for all of the genus")
	merged = merged.filter(
		(pl.col('organism') == pl.lit('Mycobacterium tuberculosis'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium bovis'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium africanum'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium cannettii'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium tuberculosis complex sp.'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium caprae'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium orygis'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium microti'))
		.or_(pl.col('organism') == pl.lit('Mycobacterium pinnipedii')))
	print(f"Of which {merged.shape[0]} samples are MTB, bovis, africanum, or cannettii")
	merged = merged.filter(pl.col('librarylayout').list.contains("PAIRED"))
	print(f"Of which {merged.shape[0]} are paired")
	merged = merged.filter(pl.col('platform').list.contains('ILLUMINA'))
	print(f"Of which are {merged.shape[0]} are paired illumina")
	merged = merged.filter(~pl.col('collection').list.contains(pl.lit('denylist')))
	print(f"Of which are {merged.shape[0]} are not on the denylist")
	merged = merged.filter(pl.col('collection').list.contains(pl.lit('input')))
	print(f"Of which are {merged.shape[0]} were likely entered into the pipeline")
	merged = merged.filter(pl.col('collection').list.contains(pl.lit('tbprofiler')))
	print(f"Of which are {merged.shape[0]} have tbprofiler information")
	merged = merged.filter(pl.col('collection').list.contains(pl.lit('diff')))
	print(f"Of which are {merged.shape[0]} seems to have made a diff file")
	merged = merged.filter(pl.col('collection').list.contains(pl.lit('tree')))
	print(f"Of which are {merged.shape[0]} were on the tree as of December 12th 2024")

	exit(0)



	return merged


########################################################################

if start_from_scratch:
	tba6_standardized = inital_file_parse()
	tba6_injected = inject_metadata(tba6_standardized)
	merged_runs = run_merges(tba6_injected)
	merged_samps = sample_index_merges(merged_runs)
else:
	if inject:
		print("Reading from tba6_standardized.tsv")
		tba6_standardized = Ranchero.from_tsv("tba6_standardized.tsv", auto_standardize=False)
		tba6_injected = inject_metadata(tba6_standardized)
		merged_runs = run_merges(tba6_injected)
		merged_samps = sample_index_merges(merged_runs)
	else:
		if do_run_index_merges:
			print("Reading from tba6_injected.tsv")
			tba6_injected = Ranchero.from_tsv("tba6_injected.tsv", auto_standardize=False)
			merged_runs = run_merges(tba6_injected)
			merged_samps = sample_index_merges(merged_runs)
		else:
			print("Reading from merged_by_run.tsv")
			merged_runs = Ranchero.from_tsv("merged_by_run.tsv", auto_standardize=False)
			merged_samps = sample_index_merges(merged_runs)

# fix a BioProject-level injection


merged = merged_samps

merged = merged.drop(['lat', 'lon', 'date_collected_year', 'date_collected_month', 'reason', 'host_info', 'geoloc_info', 'mbytes_sum_sum', 'geoloc_name'], strict=False)
merged = merged.drop(['tbprof_rd', 'tbprof_spoligotype', 'tbprof_frac'], strict=False) # seem to be from the main lineage only, not the sublineage

Ranchero.to_tsv(merged, f"./ranchero_{rc}_full_columns.tsv")
merged = merged.drop(['primary_search', 'mbases_sum', 'bases_sum', 'bytes_sum', 'libraryselection', 'librarysource', 'instrument', 'host_info'], strict=False) # for less_columns version


merged = Ranchero.hella_flat(merged)
Ranchero.print_schema(merged)
Ranchero.NeighLib.print_value_counts(merged, ['sra_study'])
Ranchero.NeighLib.print_value_counts(merged, ['libraryselection'])
Ranchero.NeighLib.print_value_counts(merged, ['sample_source'])
Ranchero.NeighLib.print_value_counts(merged, ['host_scienname', 'host_confidence', 'host_streetname'])
Ranchero.NeighLib.print_value_counts(merged, ['date_collected'])
Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage'])
Ranchero.NeighLib.print_value_counts(merged, ['country', 'continent', 'region'])

Ranchero.NeighLib.report(merged)
Ranchero.to_tsv(merged, f"./ranchero_{rc}_less_columns.tsv")


exit(1)

tree_metadata_v8_rc10 = Ranchero.from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
tree_metadata_v8_rc10 = Ranchero.rancheroize(tree_metadata_v8_rc10)
tree_metadata_v8_rc10.drop(['BioProject', 'isolation_source', 'host']) # we are parsing these directly from SRA now
print(f"Finished reading a bunch more metadata in  {time.time() - start:.4f} seconds")
start = time.time()
merged = Ranchero.merge_dataframes(merged, tree_metadata_v8_rc10, merge_upon="__index__sample_index", right_name="tree_metadata_v8_rc10", indicator="collection", fallback_on_left=False)
print(f"Merged with old tree metadata file in {time.time() - start:.4f} seconds")
Ranchero.NeighLib.big_print_polars(tree_metadata_v8_rc10, "v8rc10 hosts and dates", ['sample_index', 'date_collected', 'host'])

print(f"Finished entire module in {time.time() - module_start} seconds")


