import src as Ranchero

what_about_primary_search = [{"k":"bases","v":1000},{"k":"primary_search","v":"foo"},{"k":"primary_search","v":"bar"}]
print(Ranchero.NeighLib.concat_dicts_with_shared_keys(what_about_primary_search, set(["primary_search"])))


# Test file parsing
coscolla = Ranchero.polars_from_tsv("./inputs/coscolla.tsv")
tiny_test = Ranchero.polars_from_bigquery("./inputs/truncated/smol.json")
small_test = Ranchero.polars_from_bigquery("./inputs/truncated/bq-flatten-test.json")
v8 = Ranchero.polars_from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
tba5 = Ranchero.polars_from_bigquery("./inputs/bq-results-20240710-211044-1720646162304.json")
PRJNA834606_sra = Ranchero.polars_from_ncbi_run_selector("./inputs/PRJNA834606_sra.csv")




# Test analysis
print(get_paired_illumina(tiny_test))
print(get_paired_illumina(tiny_test, flip=True))



# Test merging
#
# TODO: Need better understanding of how polar merges work to properly implement fillna() and
# the _x/_y comparisons.
zeroth_merge = merge_polars_dataframes(tiny_test, small_test, 'BioSample', 'small')

