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

# exposed classes of global instances
polars_from_tsv = _FileReader.polars_from_tsv
polars_from_bigquery = _FileReader.polars_from_bigquery