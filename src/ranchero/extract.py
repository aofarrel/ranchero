import polars as pl
from polars.testing import assert_series_equal
from .statics import kolumns, null_values, drop_zone, file_extensions

# https://peps.python.org/pep-0661/
_DEFAULT_TO_CONFIGURATION = object()

class Extractor:

	def __init__(self, configuration, naylib):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger
			self.NeighLib = naylib

		def _default_fallback(self, cfg_var, value):
			if value == _DEFAULT_TO_CONFIGURATION:
				return self.cfg.get_config(cfg_var)
			return value

	def extract_primary_lineage(self, polars_df, lineage_column, output_column):
		"""CAVEAT: exepcts tbprofiler format (eg "lineage" or "La")"""
		return polars_df.with_columns(
			pl.when(pl.col(lineage_column).is_not_null() & ~pl.col(lineage_column).str.contains(";"))
			.then(pl.col(lineage_column).str.extract(r"(lineage\s*\d+|La\s*\d+)"))
			.otherwise(None)
			.alias(output_column)
		)

	def extract_simplified_primary_search(self, polars_df, input_column, output_column):
		"""
		Removes some (but not all) extra information from the input_column to make it more useful.
		input_column is an argument here but 99% of the time it's probably gonna be "primary_search"
		This is a little hacky due to the fact polars doesn't yet support named columns in list.eval()
		TODO: also remove bp0 and extra bioprojects
		"""
		relevent_primary_search_columns = self.NeighLib.valid_cols(polars_df, kolumns.common_primary_search_values)
		polars_df = polars_df.with_columns(
			pl.col(input_column).list.set_difference(
				pl.concat_list(relevent_primary_search_columns))
			.alias(output_column))

		return polars_df.with_columns(
			pl.col(output_column).list.eval(
				pl.element().filter(~pl.element().str.contains(r"^\d+$")) # regex: entire string is an integer
			).alias(output_column)
		)

	def extract_filename(self, polars_df, input_column, output_column):
		"""
		Attempts to parse a list column (typically primary_search) for filenames. This is a little busted
		and will only return one filename even if there's multiple matches.
		"""
		
		polars_df = self.extract_simplified_primary_search(polars_df, input_column, output_column)

		pattern = r"(" + "|".join(file_extensions.all_extensions) + r")$"

		return polars_df.with_columns(
			pl.col(input_column).list.eval(
				pl.element().filter(pl.element().str.contains(pattern))
			).alias(output_column)
		)
