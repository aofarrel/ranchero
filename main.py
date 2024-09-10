import time
start = time.time()
import src as Ranchero
print(f"Import time: {time.time() - start}")


# Test file parsing
start = time.time()
coscolla = Ranchero.from_tsv("./inputs/coscolla.tsv")
tiny_test = Ranchero.from_bigquery("./inputs/truncated/smol.json")
flatten_test = Ranchero.from_bigquery("./inputs/truncated/bq-flatten-test.json")
PRJNA834606_sra = Ranchero.from_ncbi_run_selector("./inputs/PRJNA834606_sra.csv")
print(f"Quick parsing time: {time.time() - start}")

start = time.time()
v8 = Ranchero.from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
print(f"v8 parsing time: {time.time() - start}")

left = Ranchero.from_bigquery("./inputs/test/left.json")
right = Ranchero.from_bigquery("./inputs/test/right.json")

# Test analysis
start = time.time()
print(Ranchero.get_paired_illumina(tiny_test))
print(Ranchero.get_paired_illumina(tiny_test, flip=True))
print(f"Analysis time: {time.time() - start}")



# Test merging
start = time.time()
zeroth_merge = Ranchero.merge_polars_dataframes(tiny_test, flatten_test, 'run_index', 'small')
print(zeroth_merge)
Ranchero.to_tsv(zeroth_merge, "./zeroth.tsv")
zeroth_merge = Ranchero.run_index_to_sample_index(zeroth_merge)
zeroth_merge = Ranchero.hella_flat(zeroth_merge)
Ranchero.to_tsv(zeroth_merge, "./zeroth_per_sample.tsv")
print(f"Merge time: {time.time() - start}")

start = time.time()
leftright_merge = Ranchero.merge_polars_dataframes(left, right, 'run_index', 'hello', 'there', 'folks')
print(f"Leftright time: {time.time() - start}")

start = time.time()
rightleft_merge = Ranchero.merge_polars_dataframes(right, left, 'run_index')
print(f"Rightleft time: {time.time() - start}")


# Merging merged datasets to prove that the "country" column doesn't gain duplicates
start = time.time()
zippo = Ranchero.merge_polars_dataframes(leftright_merge, rightleft_merge, 'run_index')
zippo = Ranchero.run_index_to_sample_index(zippo)
zippo = Ranchero.hella_flat(zippo)
print(zippo)
print(f"Zippo time: {time.time() - start}")

start = time.time()
tba5 = Ranchero.from_bigquery("./inputs/bq-results-20240710-211044-1720646162304.json")
print(f"tba5 parse time: {time.time() - start}")  # should be under five minutes
start = time.time()
tba5 = Ranchero.drop_non_tb_columns(tba5)
print(f"tba5 drop column time: {time.time() - start}")
start = time.time()
tba5 = Ranchero.run_index_to_sample_index(tba5)
print(f"tba5 run to sample time: {time.time() - start}")
start = time.time()
tba5 = Ranchero.hella_flat(tba5)
print(f"tba5 flat time: {time.time() - start}")
Ranchero.to_tsv(tba5, "./tba5_per_sample.tsv")



