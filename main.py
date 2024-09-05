import time
start = time.time()
import src as Ranchero
print(f"Import time: {time.time() - start}")

start = time.time()
what_about_primary_search = [{"k":"bases","v":1000},{"k":"primary_search","v":"foo"},{"k":"primary_search","v":"bar"}]
print(Ranchero.NeighLib.concat_dicts_with_shared_keys(what_about_primary_search, set(["primary_search"])))
print(f"Primary search split time: {time.time() - start}")


# Test file parsing
start = time.time()
coscolla = Ranchero.polars_from_tsv("./inputs/coscolla.tsv")
tiny_test = Ranchero.polars_from_bigquery("./inputs/truncated/smol.json")
flatten_test = Ranchero.polars_from_bigquery("./inputs/truncated/bq-flatten-test.json")
PRJNA834606_sra = Ranchero.polars_from_ncbi_run_selector("./inputs/PRJNA834606_sra.csv")
print(f"Quick parsing time: {time.time() - start}")

start = time.time()
v8 = Ranchero.polars_from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
print(f"v8 parsing time: {time.time() - start}")


#tba5 = Ranchero.polars_from_bigquery("./inputs/bq-results-20240710-211044-1720646162304.json")
#Ranchero.NeighLib.super_print_pl()

left = Ranchero.polars_from_bigquery("./inputs/test/left.json")
right = Ranchero.polars_from_bigquery("./inputs/test/right.json")

# Test analysis
start = time.time()
print(Ranchero.get_paired_illumina(tiny_test))
print(Ranchero.get_paired_illumina(tiny_test, flip=True))
print(f"Analysis time: {time.time() - start}")



# Test merging
start = time.time()
zeroth_merge = Ranchero.merge_polars_dataframes(tiny_test, flatten_test, 'run_index', 'small')
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
print(f"Zippo time: {time.time() - start}")


