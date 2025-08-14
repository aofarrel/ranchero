import sys
assert sys.version_info >= (3, 9), f"Use Python 3.9 or newer -- you are using {sys.version_info[0]}.{sys.version_info[1]}"

from .config import RancheroConfig
from .neigh import NeighLib
Configuration = RancheroConfig()  # creates a default config
_NeighLib = NeighLib(Configuration)
logger = Configuration.logger
from .analyze import *
from .extract import Extractor
_Extractor = Extractor(Configuration)
from .merge import *
_Merger = Merger(Configuration)
from .standardize import ProfessionalsHaveStandards
_Standardizer = ProfessionalsHaveStandards(Configuration)
from .read_file import FileReader
_FileReader = FileReader(Configuration)


# exposed classes of global instances
to_tsv = _NeighLib.polars_to_tsv
flatten_nested_list_cols = _NeighLib.flatten_nested_list_cols
hella_flat = _NeighLib.flatten_all_list_cols_as_much_as_possible
drop_non_tb_columns = _NeighLib.drop_non_tb_columns
super_print = _NeighLib.super_print_pl
print_col_where = _NeighLib.print_col_where
print_a_where_b_is_null = _NeighLib.print_a_where_b_is_null
print_a_where_b_equals_this = _NeighLib.print_a_where_b_equals_this
unique_bioproject_per_center_name = _NeighLib.unique_bioproject_per_center_name
rancheroize = _NeighLib.rancheroize_polars
print_schema = _NeighLib.print_schema
add_column_with_this_value = _NeighLib.add_column_of_just_this_value
dfprint = _NeighLib.dfprint
fix_index = _NeighLib.check_index
get_index = _NeighLib.get_index
translate_HPRC_IDs = _NeighLib.translate_HPRC_IDs
check_index = _NeighLib.check_index

from_tsv = _FileReader.polars_from_tsv
from_bigquery = _FileReader.polars_from_bigquery
from_run_selector = _FileReader.polars_from_ncbi_run_selector
from_efetch = _FileReader.from_efetch
from_edirect = _FileReader.from_efetch # might as well sure
fix_json = _FileReader.fix_bigquery_file
injector_from_tsv = _FileReader.read_metadata_injection
run_index_to_sample_index = _FileReader.polars_run_to_sample
explode_delimited_index = _FileReader.polars_explode_delimited_rows

merge_dataframes = _Merger.merge_polars_dataframes

extract_primary_lineage = _Extractor.extract_primary_lineage
extract_simplified_primary_search = _Extractor.extract_simplified_primary_search
extract_filename = _Extractor.extract_filename

inject_metadata = _Standardizer.inject_metadata
standardize_everything = _Standardizer.standardize_everything
standardize_hosts = _Standardizer.standarize_hosts
standardize_countries = _Standardizer.standardize_countries
cleanup_dates = _Standardizer.cleanup_dates
standardize_sample_source = _Standardizer.standardize_sample_source
standardize_host_disease = _Standardizer.standardize_host_disease
unmask_badgers = _Standardizer.unmask_badgers
taxoncore = _Standardizer.sort_out_taxoncore_columns
