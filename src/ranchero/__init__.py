import sys
assert sys.version_info >= (3, 9), f"Use Python 3.9 or newer -- you are using {sys.version_info[0]}.{sys.version_info[1]}"

from .config import RancheroConfig
Configuration = RancheroConfig()
logger = Configuration.logger

from .neigh import NeighLib
NeighLib = NeighLib(Configuration)

from .merge import *
Merger = Merger(Configuration, NeighLib)

from .standardize import ProfessionalsHaveStandards
Standardizer = ProfessionalsHaveStandards(Configuration, NeighLib)

from .read_file import FileReader
FileReader = FileReader(Configuration, NeighLib, Standardizer)

from .extract import Extractor
Extractor = Extractor(Configuration, NeighLib)

from .query import Query
Query = Query(Configuration, NeighLib)

#from .analyze import *


__all__ = [
	"RancheroConfig",
	"logger",
	"Query",
	"Extractor",
	"Merger",
	"Standardizer",
	"FileReader",
	"NeighLib",
]

# NeighLib
to_tsv = NeighLib.polars_to_tsv
flatten_nested_list_cols = NeighLib.flatten_nested_list_cols
hella_flat = NeighLib.flatten_all_list_cols_as_much_as_possible
drop_non_tb_columns = NeighLib.drop_non_tb_columns
super_print = NeighLib.super_print_pl
print_col_where = NeighLib.print_col_where
print_a_where_b_is_null = NeighLib.print_a_where_b_is_null
print_a_where_b_equals_this = NeighLib.print_a_where_b_equals_this
unique_bioproject_per_center_name = NeighLib.unique_bioproject_per_center_name
rancheroize = NeighLib.rancheroize_polars
print_schema = NeighLib.print_schema
add_column_with_this_value = NeighLib.add_column_of_just_this_value
dfprint = NeighLib.dfprint
fix_index = NeighLib.check_index
get_index = NeighLib.get_index
translate_HPRC_IDs = NeighLib.translate_HPRC_IDs
check_index = NeighLib.check_index
valid_cols = NeighLib.valid_cols
report = NeighLib.report

# Extractor
extract_primary_lineage = Extractor.extract_primary_lineage
extract_simplified_primary_search = Extractor.extract_simplified_primary_search
extract_filename = Extractor.extract_filename

# FileReader
from_tsv = FileReader.polars_from_tsv
from_bigquery = FileReader.polars_from_bigquery
from_run_selector = FileReader.polars_from_ncbi_run_selector
from_efetch = FileReader.from_efetch
from_edirect = FileReader.from_efetch
fix_json = FileReader.fix_bigquery_file
injector_from_tsv = FileReader.read_metadata_injection
run_index_to_sample_index = FileReader.run_to_sample_index
explode_delimited_index = FileReader.polars_explode_delimited_rows
normalize_attr = FileReader.polars_fix_attributes_and_json_normalize

# Merger
merge_dataframes = Merger.merge_polars_dataframes

# Standardizer
inject_metadata = Standardizer.inject_metadata
standardize_everything = Standardizer.standardize_everything
standardize_hosts = Standardizer.standarize_hosts
standardize_countries = Standardizer.standardize_countries
cleanup_dates = Standardizer.cleanup_dates
standardize_sample_source = Standardizer.standardize_sample_source
standardize_host_disease = Standardizer.standardize_host_disease
unmask_badgers = Standardizer.unmask_badgers
taxoncore = Standardizer.sort_out_taxoncore_columns
