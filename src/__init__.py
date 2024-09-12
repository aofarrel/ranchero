from .analyze import *
from .config import RancheroConfig
from .merge import *
from .neigh import NeighLib
from .read_file import FileReader
from .standardize import *
from .verify import *

Configuration = RancheroConfig()  # creates a default config

# global instances
_FileReader = FileReader(Configuration)
_NeighLib = NeighLib()

# exposed classes of global instances
from_tsv = _FileReader.polars_from_tsv
from_bigquery = _FileReader.polars_from_bigquery
from_ncbi_run_selector = _FileReader.polars_from_ncbi_run_selector
run_index_to_sample_index = _FileReader.polars_run_to_sample

to_tsv = _NeighLib.polars_to_tsv
flatten_nested_list_cols = _NeighLib.flatten_nested_list_cols
hella_flat = _NeighLib.flatten_all_list_cols_as_much_as_possible
drop_non_tb_columns = _NeighLib.drop_non_tb_columns
super_print = _NeighLib.super_print_pl