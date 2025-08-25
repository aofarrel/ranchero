# Starting from list of diff files and a bigquery output, return a file of diffs
# that are M. cannettii.
rc = "17a"

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
do_run_id_merges = True

module_start = time.time()

def inital_file_parse():
	start, tba6 = time.time(),Ranchero.from_bigquery("./inputs/BQ/tba6_no_tax_table_bq_2024-09-19.json_modified.json")
	print(f"Parsed tba6 file from bigquery in {time.time() - start:.4f} seconds")  # should be under five minutes for tba5, less for tba6
	start, tba6 = time.time(), Ranchero.drop_non_tb_columns(tba6)
	print(f"Dropped non-TB-related columns in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.rancheroize(tba6)
	print(f"Rancheroized in {time.time() - start:.4f} seconds")
	Ranchero.NeighLib.report(tba6)

	# we don't need these columns for what we're tryna do
	cool_columns = ['libraryselection', 'librarysource', 'librarylayout', 'platform', 'SRS_id',
					'sample_id', 'run_id', 'assay_type', 'center_name', 'SRX_id', 'sra_study',
					'organism', 'clade', 'strain', 'lineage']
	for column in tba6.columns:
		if column not in cool_columns:
			tba6 = tba6.drop(column)

	start, tba6 = time.time(), Ranchero.standardize_everything(tba6)
	print(f"Standardized in {time.time() - start:.4f} seconds")

	Ranchero.NeighLib.report(tba6)
	gc.collect()
	return tba6

def sample_id_merges(merged_runs):
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
	print(f"{_b_}Processing diffs{_bb_}")

	diffs = Ranchero.from_tsv("./inputs/pipeline/probable_diffs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, diffs, merge_upon="sample_id", right_name="diff", indicator="collection", drop_exclusive_right=False)
	
	denylist = Ranchero.from_tsv("./inputs/pipeline/denylist_2024-07-23_lessdupes.tsv", auto_rancheroize=False).drop('reason')
	merged = Ranchero.merge_dataframes(merged, denylist, merge_upon="sample_id", right_name="denylist", indicator="collection", drop_exclusive_right=False)

	print(f"Merged with pipeline information in {time.time() - start:.4f} seconds")
	return merged

########################################################################

tba6_standardized = inital_file_parse()
merged = sample_id_merges(tba6_standardized)


print(f"We started with {merged.shape[0]} samples for all of the genus")
merged_just_diffs = merged.filter(pl.col('collection').list.contains(pl.lit('diff')))
print(f"Of which are {merged_just_diffs.shape[0]} seems to have made a diff file")
merged_just_canettii_diffs = merged_just_diffs.filter(pl.col('organism') == pl.lit('Mycobacterium canettii'))
print(f"Of which {merged_just_canettii_diffs.shape[0]} samples are canettii")
Ranchero.to_tsv(merged_just_canettii_diffs.select('sample_id'), f"./ranchero_{rc}_just_canettii_diffs.tsv")

print(f"We started with {merged.shape[0]} samples for all of the genus")
merged_just_canettii = merged.filter(pl.col('organism') == pl.lit('Mycobacterium canettii'))
print(f"Of which {merged_just_canettii.shape[0]} samples are canettii (including ones without a diff)")
Ranchero.to_tsv(merged_just_canettii.select('sample_id'), f"./ranchero_{rc}_just_canettii.tsv")

print(f"Finished entire module in {time.time() - module_start} seconds")





