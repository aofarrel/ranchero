from .config import RancheroConfig
from .neigh import NeighLib
Configuration = RancheroConfig()  # creates a default config

_NeighLib = NeighLib(Configuration)
logger = Configuration.logger

from .analyze import *
from .merge import *
from .read_file import FileReader
from .standardize import *
from .verify import *


_FileReader = FileReader(Configuration)
_Merger = Merger(Configuration)



# exposed classes of global instances
to_tsv = _NeighLib.polars_to_tsv
flatten_nested_list_cols = _NeighLib.flatten_nested_list_cols
hella_flat = _NeighLib.flatten_all_list_cols_as_much_as_possible
drop_non_tb_columns = _NeighLib.drop_non_tb_columns
super_print = _NeighLib.super_print_pl
print_col_where = _NeighLib.print_col_where
rancheroize = _NeighLib.rancheroize_polars


from_tsv = _FileReader.polars_from_tsv
from_bigquery = _FileReader.polars_from_bigquery
from_ncbi_run_selector = _FileReader.polars_from_ncbi_run_selector
run_index_to_sample_index = _FileReader.polars_run_to_sample
explode_delimited_index = _FileReader.polars_explode_delimited_rows_recklessly

merge_dataframes = _Merger.merge_polars_dataframes