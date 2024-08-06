from src.neigh import *
from src.merge import *
from src.read_file import *
from src.standardize import *

#v8 = pandas_from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
#tba5 = bigquery_to_pandas("./inputs/bq-results-20240710-211044-1720646162304.json")
tba5 = pandas_from_bigquery("./inputs/bq-333-samples.json")
coscolla = tsv_to_pandas("./inputs/coscolla.tsv")
#PRJNA834606_sra = ncbi_run_selector_to_pandas("./inputs/PRJNA834606_sra.csv")


zeroth_merge = merge_dataframes(v8, coscolla, 'BioSample', 'coscolla')



#tba5.drop(, axis=1, inplace=True)
