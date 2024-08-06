from src.neigh import *
from src.merge import *
from src.read_file import *
from src.standardize import *

v8 = pandas_from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
#tba5 = bigquery_to_pandas("./inputs/bq-results-20240710-211044-1720646162304.json")
tba5 = pandas_from_bigquery("./inputs/bq-333-samples.json")
coscolla = pandas_from_tsv("./inputs/coscolla.tsv")
#PRJNA834606_sra = ncbi_run_selector_to_pandas("./inputs/PRJNA834606_sra.csv")

print(tba5)
print(v8)

zeroth_merge = merge_dataframes(tba5, v8, 'BioSample', 'coscolla')



#tba5.drop(, axis=1, inplace=True)
