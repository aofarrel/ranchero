from src.analyze import *
from src.neigh import *
from src.merge import *
from src.read_file import *
from src.standardize import *

# Test file parsing
#
# TODO: "primary search" attributes are not working as I would like. It's flattening it down
# to just a single string, but I want a list of all primary search values.
# 
tiny_test = polars_from_bigquery("./inputs/smol.json", merge_into_biosamples=False)
#small_test = polars_from_bigquery("./inputs/bq-flatten-test.json", merge_into_biosamples=False)
#coscolla = pandas_from_tsv("./inputs/coscolla.tsv")
#v8 = polars_from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
#tba5 = polars_from_bigquery("./inputs/bq-results-20240710-211044-1720646162304.json", merge_into_biosamples=False)
#PRJNA834606_sra = polars_from_ncbi_run_selector("./inputs/PRJNA834606_sra.csv")


what_about_primary_search = [{"k":"bases","v":1000},{"k":"primary_search","v":"foo"},{"k":"primary_search","v":"bar"}]
print(NeighLib.concat_dicts_primary_search(what_about_primary_search))

# Test analysis
print(get_paired_illumina(tiny_test))
print(get_paired_illumina(tiny_test, flip=True))



# Test merging
#
# TODO: Need better understanding of how polar merges work to properly implement fillna() and
# the _x/_y comparisons.
#zeroth_merge = merge_polars_dataframes(tiny_test, small_test, 'BioSample', 'small')

