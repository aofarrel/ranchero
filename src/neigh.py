# general purpose functions

import os
import re
import polars as pl
import datetime
from src.statics import kolumns, drop_zone, null_values, HPRC_sample_ids
from polars.testing import assert_series_equal
import polars.selectors as cs
from .config import RancheroConfig

# for debugging
import traceback

# my crummy implementation of https://peps.python.org/pep-0661/
globals().update({f"_cfg_{name}": object() for name in [
	"force_SRR_ERR_DRR_run_index", "force_SAMN_SAME_SAMD_sample_index",
	"check_index", "indicator_column",
	"intermediate_files", "dupe_index_handling", "rm_not_pared_illumina"
]})
_SENTINEL_TO_CONFIG = {
	_cfg_force_SRR_ERR_DRR_run_index: "force_SRR_ERR_DRR_run_index",
	_cfg_force_SAMN_SAME_SAMD_sample_index: "force_SAMN_SAME_SAMD_sample_index",
	_cfg_dupe_index_handling: "dupe_index_handling",
	_cfg_check_index: "check_index",
	_cfg_intermediate_files: "intermediate_files",
	_cfg_indicator_column: "indicator_column",
	_cfg_rm_not_pared_illumina: "rm_not_pared_illumina",
}

class NeighLib:
	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger

	def _sentinal_handler(self, arg):
		"""Handles "allow overriding config" variables in function calls"""
		if arg in _SENTINEL_TO_CONFIG:
			config_attr = _SENTINEL_TO_CONFIG[arg]
			check_me = getattr(self.cfg, config_attr)
			assert check_me != arg, f"Configuration for '{config_attr}' is invalid or uninitialized"
			return check_me
		else:
			return arg

	# --------- GET FUNCTIONS --------- #

	def get_number_of_x_in_column(self, polars_df, x, column):
		return len(polars_df.filter(pl.col(column) == x))

	def get_a_where_b_is_null(self, polars_df, col_a, col_b):
		if col_a not in polars_df.columns or col_b not in polars_df.columns:
			self.logging.warning(f"Tried to get column {col_a} where column {col_b} is pl.Null, but at least one of those columns aren't in the dataframe!")
			return
		get_df = polars_df.with_columns(pl.when(pl.col(col_b).is_null()).then(pl.col(col_a)).otherwise(None).alias(f"{col_a}_filtered")).drop_nulls(subset=f"{col_a}_filtered")
		return get_df

	def get_most_common_non_null_and_its_counts(self, polars_df, col, and_its_counts=True):
		counts = polars_df.select(
			pl.col(col)
			.filter(pl.col(col).is_not_null())
			.value_counts(sort=True) # creates struct[2] column named col, sorted in descending order
		)
		counts = counts.unnest(col) # splits col into col and "counts" columns
		try:
			return tuple(counts.row(0))
		except Exception:
			self.logging.warning(f"Could not calculate mode for {col} -- is it full of nulls?")
			return ('ERROR', 'N/A')

	def get_null_count_in_column(self, polars_df, column_name, warn=True, error=False):
		series = polars_df.get_column(column_name)
		null_count = series.null_count()
		if null_count > 0 and warn:
			self.logging.warning(f"Found {null_count} nulls in column {column_name}")
		elif null_count > 0 and error:
			self.logging.error(f"Found {null_count} nulls in column {column_name}")
			raise AssertionError
		return null_count

	def get_count_of_x_in_column_y(self, polars_df, x, column_y):
		if x is not None:
			return polars_df.select((pl.col(column_y) == x).sum()).item()
		else:
			return polars_df.select((pl.col(column_y).is_null()).sum()).item()

	def get_valid_id_columns(self, polars_df):
		return self.valid_cols(polars_df, kolumns.id_columns)

	def get_rows_where_list_col_more_than_one_value(self, polars_df, list_col):
		""" Assumes https://github.com/pola-rs/polars/issues/19987 has been fixed, and that you have already
		run drop_nulls() if you wanted to.
		A partial workaround for older versions of polars: 
		no_nulls = polars_df.filter(pl.col(list_col).list.first.is_not_null())
		"""
		assert polars_df.schema[list_col] == pl.List
		return polars_df.filter(pl.col(list_col).list.len() > 1)

	def get_paired_illumina(self, polars_df, inverse=False):
		rows_before = polars_df.shape[0]
		if 'librarysource' in polars_df.columns and 'platform' in polars_df.columns:
			if polars_df.schema['platform'] == pl.Utf8 and polars_df.schema['librarylayout'] == pl.Utf8:
				if not inverse:
					self.logging.info("Filtering data to include only PE Illumina reads")
					polars_df = polars_df.filter(
						(pl.col('platform') == 'ILLUMINA') & 
						(pl.col('librarylayout') == 'PAIRED')
					)
					self.logging.info(f"Excluded {rows_before-polars_df.shape[0]} rows of non-paired/non-Illumina data")
				else:
					self.logging.info("Filtering data to exclude PE Illumina reads")
					polars_df = polars_df.filter(
						(pl.col('platform') != 'ILLUMINA') & 
						(pl.col('librarylayout') != 'PAIRED')
					)
					self.logging.info(f"Excluded {rows_before-polars_df.shape[0]} rows of PE Illumina data")
			else:
				self.logging.warning("Failed to filter out non-PE Illumina as platform and/or librarylayout columns aren't type string")
		else:
			self.logging.warning("Failed to filter out non-PE Illumina as platform and/or librarylayout columns aren't present")
		return polars_df

	def get_index_column(self, polars_df, quiet=False):
		"""
		Does NOT check for duplicates in the index column(s)
		"""
		sample_indeces = kolumns.equivalence['sample_index']
		sample_matches = [col for col in sample_indeces if col in polars_df.columns]
		run_indeces = kolumns.equivalence['run_index']
		run_matches = [col for col in run_indeces if col in polars_df.columns]

		# more than one sample index, arbitrary number of run indeces
		if len(sample_matches) > 1:
			if not quiet:
				raise ValueError(f"Tried to find dataframe index, but there's multiple possible sample indeces: {sample_matches}")
			else:
				return [2, sample_matches]
	
		# one sample index, arbitrary number of run indeces
		elif len(sample_matches) == 1:
			if len(run_matches) > 1:
				if not quiet:
					raise ValueError(f"Tried to find dataframe index, but there's multiple possible run indeces (may indicate failed run->sample conversion):  {run_matches}")
				else:
					return [3, run_matches]
			
			elif len(run_matches) == 1:
				if polars_df.schema[run_matches[0]] == pl.List:
					return str(sample_matches[0])
				else:
					return str(run_matches[0])

			else:
				return str(sample_matches[0])  # no run indeces, just one sample index

		# no sample index, multiple run indeces
		elif len(run_matches) > 1:
			if not quiet:
				raise ValueError(f"Tried to find dataframe index, but there's multiple possible run indeces: {run_matches}")
			else:
				return [4, run_matches]
		
		# no sample index, one run index
		elif len(run_matches) == 1:
			return str(run_matches[0])

		else:
			if not quiet:
				raise ValueError(f"No valid index column found in polars_df! Columns available: {polars_df.columns}")
			else:
				return [5]

	def get_dupe_columns_of_two_polars(self, polars_df_a, polars_df_b, assert_shared_cols_equal=False):
		""" Check two polars dataframes share any columns """
		columns_a = list(polars_df_a.columns)
		columns_b = list(polars_df_b.columns)
		dupes = []
		for column in columns_a:
			if column in columns_b:
				dupes.append(column)
		if len(dupes) >= 0:
			if assert_shared_cols_equal:
				for dupe in dupes:
					assert_series_equal(polars_df_a[dupe], polars_df_b[dupe])
		return dupes

	# --------- PRINT FUNCTIONS --------- #

	def print_cols_and_dtypes(self, polars_df):
		[print(f"{col}: {dtype}") for col, dtype in zip(polars_df.columns, polars_df.dtypes)]

	def print_a_where_b_equals_these(self, polars_df, col_a, col_b, list_to_match: list, alsoprint=None, valuecounts=False, header=None, and_id_columns=True, and_return_filtered=False):
		header = header if header is not None else f"{col_a} where {col_b} in {list_to_match}"
		print_columns = set(self.get_valid_id_columns(polars_df) + [col_a, col_b]) if and_id_columns else set([col_a, col_b])
		print_columns = list(print_columns.union(self.valid_cols(polars_df, alsoprint))) if alsoprint is not None else list(print_columns)
		
		if col_a not in polars_df.columns or col_b not in polars_df.columns:
			self.logging.warning(f"Tried to print column {col_a} where column {col_b} is in {list_to_match}, but at least one of those columns aren't in the dataframe!")
			return
		if polars_df.schema[col_b] == pl.Utf8:
			print_df = polars_df.with_columns(pl.when(pl.col(col_b).is_in(list_to_match)).then(pl.col(col_a)).otherwise(None).alias(col_a)).filter(pl.col(col_a).is_not_null())
			self.super_print_pl(print_df.select(print_columns), header)
			if valuecounts: self.print_value_counts(print_df, only_these_columns=col_a)
			if and_return_filtered: return print_df
		else:
			self.logging.warning(f"Tried to print column {col_a} where column {col_b} is in {list_to_match}, but either {col_b} isn't a string so we can't match on it properly")

	def print_a_where_b_equals_this(self, polars_df, col_a, col_b, foo, alsoprint=None, valuecounts=False, header=None):
		header = header if header is not None else f"{col_a} where {col_b} is {foo}"
		if col_a not in polars_df.columns or col_b not in polars_df.columns:
			self.logging.warning(f"Tried to print column {col_a} where column {col_b} equals {foo}, but at least one of those columns aren't in the dataframe!")
			return
		if type(foo) == str:
			assert polars_df.schema[col_b] == pl.Utf8
		print_df = polars_df.with_columns(pl.when(pl.col(col_b) == foo).then(pl.col(col_a)).otherwise(None).alias(f"{col_a}_filtered")).drop_nulls(subset=f"{col_a}_filtered")
		valid_ids = self.get_valid_id_columns(polars_df)
		if col_a in valid_ids or col_b in valid_ids:  # this check avoids polars.exceptions.DuplicateError
			print_columns = [f"{col_a}_filtered", col_b] + alsoprint if alsoprint is not None else [f"{col_a}_filtered", col_b]
		else:
			print_columns = self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b] + alsoprint if alsoprint is not None else self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b]
		self.super_print_pl(print_df.select(print_columns), header)
		if valuecounts: self.print_value_counts(polars_df, only_these_columns=col_a)

	def print_a_where_b_is_null(self, polars_df, col_a, col_b, alsoprint=None, valuecounts=False):
		if col_a not in polars_df.columns or col_b not in polars_df.columns:
			self.logging.warning(f"Tried to print column {col_a} where column {col_b} is pl.Null, but at least one of those columns aren't in the dataframe!")
			return
		print_df = polars_df.with_columns(pl.when(pl.col(col_b).is_null()).then(pl.col(col_a)).otherwise(None).alias(f"{col_a}_filtered")).drop_nulls(subset=f"{col_a}_filtered")
		print_columns = self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b] + alsoprint if alsoprint is not None else self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b]
		self.super_print_pl(print_df.select(print_columns), f"{col_a} where {col_b} is pl.Null")
		if valuecounts: self.print_value_counts(print_columns, only_these_columns=col_a)

	def print_col_where(self, polars_df, column="source", equals="Coscolla", cols_of_interest=kolumns.id_columns, everything=False):
		if column not in polars_df.columns:
			self.logging.warning(f"Tried to print where {column} equals {equals}, but that column isn't in the dataframe")
			return
		
		# I am not adding all the various integer types in polars here. go away. you'll get a try/except block at best.
		elif type(equals) == list and polars_df.schema[column] != pl.List:
			self.logging.warning(f"Tried to print where {column} equals list {equals}, but that column has type {polars_df.schema[column]}")
			return
		elif type(equals) == str and polars_df.schema[column] != pl.Utf8:
			self.logging.info("This is a list column and you passed in a string -- I'm assuming you are looking for the string in the list")
			filtah = polars_df.filter(pl.col(column).list.contains(equals))
		else:
			filtah = polars_df.filter(pl.col(column) == equals)
		if not everything:
			cols_to_print = list(set([thingy for thingy in cols_of_interest if thingy in polars_df.columns] + [column]))
		else:
			cols_to_print = polars_df.columns
		with pl.Config(tbl_cols=-1, tbl_rows=40):
			print(filtah.select(cols_to_print))

	def print_only_where_col_list_is_big(self, polars_df, column_of_lists):
		if column_of_lists not in polars_df.columns:
			self.logging.warning(f"Tried to print {column_of_lists}, but that column isn't even in the dataframe!")
		elif polars_df.schema[column_of_lists] != pl.List:
			self.logging.warning(f"Tried to print where {column_of_lists} has multiple values, but that column isn't a list!")
		else:
			cols_of_interest = kolumns.id_columns + [column_of_lists]
			cols_to_print = [thingy for thingy in cols_of_interest if thingy in polars_df.columns]
			with pl.Config(tbl_cols=-1, tbl_rows=10, fmt_str_lengths=200, fmt_table_cell_list_len=10):
				print(polars_df.filter(pl.col(column_of_lists).list.len() > 1).select(cols_to_print))

	def print_only_where_col_not_null(self, polars_df, column, cols_of_interest=kolumns.id_columns):
		if column not in polars_df.columns:
			self.logging.warning(f"Tried to print where {column} is not null, but that column isn't even in the dataframe!")
		else:
			cols_to_print = list(set(cols_of_interest + [column]).intersection(polars_df.columns))
			with pl.Config(tbl_cols=-1, tbl_rows=10, fmt_str_lengths=200, fmt_table_cell_list_len=10):
				print(polars_df.filter(pl.col(column).is_not_null()).select(cols_to_print))

	def print_value_counts(self, polars_df, only_these_columns=None, skip_ids=True):
		for column in polars_df.columns:
			if skip_ids and column not in kolumns.id_columns:
				if only_these_columns is None or column in only_these_columns:
					with pl.Config(fmt_str_lengths=500, tbl_rows=50):
						counts = polars_df.select([pl.col(column).value_counts(sort=True)])
						print(counts)
				else:
					continue
			else:
				continue

	@staticmethod
	def wide_print_polars(polars_df, header, these_columns):
		assert len(these_columns) >= 3
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")
		filtered = polars_df.select(these_columns)
		filtered = filtered.filter(
			(pl.col(these_columns[1]).is_not_null()) | 
			(pl.col(these_columns[2]).is_not_null())
		)
		with pl.Config(tbl_cols=10, tbl_rows=200, fmt_str_lengths=200, fmt_table_cell_list_len=10):
			print(filtered)

	@staticmethod
	def cool_header(header):
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")

	@staticmethod
	def dfprint(polars_df, cols=10, rows=20, str_len=40, list_len=10, width=140):
		with pl.Config(tbl_cols=cols, tbl_rows=rows, fmt_str_lengths=str_len, fmt_table_cell_list_len=list_len, tbl_width_chars=width):
			print(polars_df)

	@staticmethod
	def super_print_pl(polars_df, header, select=None):
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")
		if select is not None:
			valid_selected_columns = [col for col in select if col in polars_df.columns]
			print(valid_selected_columns)
			with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=40, fmt_table_cell_list_len=10):
				print(polars_df.select(valid_selected_columns))
		else:
			with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=40, fmt_table_cell_list_len=10):
				print(polars_df)

	def print_schema(self, polars_df):
		schema_df = pl.DataFrame({
			"COLUMN": [name for name, _ in polars_df.schema.items()],
			"TYPE": [str(dtype) for _, dtype in polars_df.schema.items()]
		})
		print(schema_df)

	# --------- GENERAL FUNCTIONS --------- #

	@staticmethod
	def tempcol(polars_df, name, error=True):
		"""
		Return a string of a valid temporary column name, trying user-specified string first.
		If error, raise an error if user-specificed string isn't available.
		"""
		candidates = [name, "temp", "foo", "bar", "tmp1", "tmp2", "scratch"]
		for candidate in candidates:
			if candidate not in polars_df.columns:
				return candidate
			elif candidate == name and error:
				raise ValueError(f"Could not generate temporary column called {name} as that name is already taken")
		raise ValueError("Could not generate a temporary column")

	def replace_substring_with_col_value(self, polars_df, sample_column, output_column, template):
		"""
		template: substring SAMPLENAME will be replaced by value in that row's sample_column 
		Useful for making the 'title' string for SRA submissions.
		"""
		assert sample_column in polars_df.columns
		assert output_column not in polars_df.columns

		return polars_df.with_columns(
			pl.col(sample_column).map_elements(
				lambda sample_column: template.replace("SAMPLENAME", sample_column),
				return_dtype=pl.String
			).alias(output_column)
		)
	
	def basename_col(self, polars_df, in_col, out_col, extension='_R1_001.fastq.gz'):
		assert in_col in polars_df.columns
		assert out_col not in polars_df.columns

		if extension:
			return polars_df.with_columns(
			pl.col(in_col).map_elements(lambda f: os.path.basename(f).split(extension, 1)[0], return_dtype=pl.Utf8).alias(out_col)
		)
			

		return polars_df.with_columns(
			pl.col(in_col).map_elements(lambda f: os.path.basename(f), return_dtype=pl.Utf8).alias(out_col)
		)

	def pair_illumina_reads(self, polars_df, read_column: str, check_suffix=True):
		"""
		Try to pair everything in read_column correctly per standard Illumina paired-end
		naming conventions, which is to say:

		some_string_R1_001.fastq (or .fastq.gz)
		some_string_R2_001.fastq (or .fastq.gz)

		TODO: better way of handling no _001
		"""
		if polars_df.height % 2 != 0:
			raise ValueError("Odd number of FASTQ files provided. Cannot pair reads.")

		def extract_parts(filename):
			if check_suffix:
				match = re.match(r"(.+)_R([12])_001\.fastq(?:\.gz)?", filename)
			else:
				# NOT TESTED!! But this might be better for those without 001 at end?
				match = re.match(r"(.+)_R([12])", filename)
			if not match:
				return None, None
			return match.group(1), match.group(2)

		polars_df = polars_df.with_columns([
			pl.col(read_column).map_elements(lambda f: extract_parts(f)[0], return_dtype=pl.Utf8).alias(self.tempcol(polars_df,"pair_key")),
			pl.col(read_column).map_elements(lambda f: extract_parts(f)[1], return_dtype=pl.Utf8).alias(self.tempcol(polars_df,"read")),
		])

		if polars_df["pair_key"].null_count() > 0 or polars_df["read"].null_count() > 0:
			invalid_files = polars_df.filter(pl.col("pair_key").is_null() | pl.col("read").is_null())[read_column].to_list()
			raise ValueError(f"Invalid or unpairable FASTQ filenames: {invalid_files}")

		# we are not using pivot(on="read", index="pair_key", values=read_column) as we want to keep other metadata columns
		# unfortunately this means we have to do a costly join
		df_R1 = polars_df.filter(pl.col("read") == "1").rename({read_column: "R1"}).drop("read")
		df_R2 = polars_df.filter(pl.col("read") == "2").rename({read_column: "R2"}).drop("read")
		joined = df_R1.join(df_R2, on="pair_key", how="inner", suffix="_R2")
		other_cols = [col for col in polars_df.columns if col not in {read_column, "read", "pair_key"}]
		if other_cols:
			extras = (
				polars_df.group_by("pair_key")
				.agg([pl.col(c).unique().alias(c) for c in other_cols])
			)
			joined = joined.join(extras, on="pair_key", how="left")

		return joined.select(["R1", "R2"] + other_cols)

	def null_lists_of_len_zero(self, polars_df, just_this_column=None):
		"""skips ID columns"""
		if just_this_column is None:
			list_cols = [col for col in polars_df.columns if polars_df.schema[col] == pl.List(pl.Utf8) and col not in kolumns.id_columns]
		else:
			list_cols = just_this_column
		for column in list_cols:
			before = self.get_null_count_in_column(polars_df, column, warn=False)
			polars_df = polars_df.with_columns(pl.col(column).list.drop_nulls()) # [pl.Null] --> []
			polars_df = polars_df.with_columns([pl.when(pl.col(column).list.len() > 0).then(pl.col(column))]) # [] --> pl.Null
			after = self.get_null_count_in_column(polars_df, column, warn=False)
			self.logging.debug(f"{column}: {before} --> {after} nulls")
		return polars_df

	def nullify(self, polars_df, only_these_columns=None, no_match_NA=False, skip_ids=True):
		"""
		Turns stuff like "not collected" and "n/a" into pl.Null values, per null_values.py,
		and nulls lists that have a length of zero
		"""
		all_cols = only_these_columns if only_these_columns is not None else polars_df.columns
		if type(all_cols) == str: # idk man im tired
			all_cols = [all_cols]
		if skip_ids:
			string_cols = [col for col in all_cols if polars_df.schema[col] == pl.Utf8 and col not in kolumns.id_columns]
			list_cols = [col for col in all_cols if polars_df.schema[col] == pl.List(pl.Utf8) and col not in kolumns.id_columns]
		else:
			string_cols = [col for col in all_cols if polars_df.schema[col] == pl.Utf8]
			list_cols = [col for col in all_cols if polars_df.schema[col] == pl.List(pl.Utf8)]

		# first, null list columns of length 0
		self.logging.debug("First pass of nulling lists of len zero")
		polars_df = self.null_lists_of_len_zero(polars_df)

		# use contains_any() for the majority of checks, as it is much faster than iterating through a list + contains()
		# the downside of contains_any() is that it doesn't allow for regex
		# in either case, we do string columns first, then list columns
		self.logging.debug("Checking for null value replacements (this may take a while)")
		polars_df = polars_df.with_columns([
			pl.when(pl.col(col).str.contains_any(null_values.nulls_pl_contains_any, ascii_case_insensitive=True))
			.then(None)
			.otherwise(pl.col(col))
			.alias(col) for col in string_cols])
		polars_df = polars_df.with_columns([
			pl.col(col).list.eval(
				pl.element().filter(~pl.element().str.contains_any(null_values.nulls_pl_contains_any, ascii_case_insensitive=True))
			)
			for col in list_cols])

		contains_list = null_values.nulls_pl_contains if no_match_NA else null_values.nulls_pl_contains_plus_NA
		for null_value in contains_list:
			polars_df = polars_df.with_columns([
				pl.when(pl.col(col).str.contains(null_value))
				.then(None)
				.otherwise(pl.col(col))
				.alias(col) for col in string_cols])
			polars_df = polars_df.with_columns([
				pl.col(col).list.eval(
					pl.element().filter(~pl.element().str.contains(null_value))
				)
				for col in list_cols])
		
		# do this one more time since we may have dropped some values
		self.logging.debug("Second pass of nulling lists of len zero")
		polars_df = self.null_lists_of_len_zero(polars_df)
		return polars_df

	def mark_rows_with_value(self, polars_df, filter_func, true_value="M. avium complex", false_value='', new_column="bacterial_family", **kwargs):
		#polars_df = polars_df.with_columns(pl.lit("").alias(new_column))
		polars_df = polars_df.with_columns(
			pl.when(pl.col('organism').str.contains_any("Mycobacterium avium"))
			.then(pl.lit(true_value))
			.otherwise(pl.lit(false_value))
			.alias(new_column)
		)
		print(polars_df.select(pl.col(new_column).value_counts()))

		polars_df = polars_df.with_columns(
			pl.when(pl.col('organism').str.contains("Mycobacterium"))
			.then(pl.lit(true_value))
			.otherwise(pl.lit(false_value))
			.alias(new_column)
		)
		print(polars_df.select(pl.col(new_column).value_counts()))

	def valid_cols(self, polars_df, desired_columns: list):
		"""
		Returns the valid subset of desired_columns, "valid" in the sense of "yeah that's in the dataframe."
		Attempts to maintain order as much as possible since people like their index columns on the left.
		Will also drop duplicates (which can happen with unusual indeces or if the user messes up).
		"""
		seen = set()
		seen_uniq = [col for col in desired_columns if not (col in seen or seen.add(col))]
		return [col for col in seen_uniq if col in polars_df.columns]

	def concat_dicts_with_shared_keys(self, dict_list: list):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}

		This version is aware of primary_search showing up multiple times and will
		keep all values for primary_search.
		"""
		combined_dict, primary_search, host_info = {}, set(), set()
		for d in dict_list:
			if 'k' in d and 'v' in d:
				if d['k'] == 'primary_search':
					primary_search.add(d['v'])
				elif self.cfg.host_info_behavior != 'columns' and d['k'] in kolumns.host_info:
					host_info.add(f"{d['k']}: {str(d['v']).lstrip('host_').rstrip('_sam').rstrip('sam_s_dpl111')}")
				else:
					combined_dict[d['k']] = d['v']
		if len(primary_search) > 0:
			combined_dict.update({"primary_search": list(primary_search)}) # convert to a list to avoid the polars column becoming type object
		if self.cfg.host_info_behavior == 'dictionary' and len(host_info) > 0:
			combined_dict.update({"host_info": list(host_info)})
		elif self.cfg.host_info_behavior == 'drop':
			combined_dict = {k: v for k, v in combined_dict.items() if k not in kolumns.host_info}
		# self.cfg.host_info_behavior == 'columns' is handled automagically
		return combined_dict

	def concat_dicts_risky(dict_list: list):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}

		This version assumes 'k' and 'v' are in the dictionaries and will error otherwise,
		and doesn't support shared keys (eg, it will pick a primary_serach value at random)
		"""
		combined_dict = {}
		for d in dict_list:
			if 'k' in d and 'v' in d:
				combined_dict[d['k']] = d['v']
		return combined_dict
	
	def concat_dicts(dict_list: list):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}
		"""
		combined_dict = {}
		for d in dict_list:
			if 'k' in d and 'v' in d:
				combined_dict[d['k']] = d['v']
		return combined_dict

	def try_nullfill(self, polars_df, left_col, right_col):
		before = self.get_null_count_in_column(polars_df, left_col, warn=False)
		if polars_df.schema[left_col] is pl.List or before <= 0:
			self.logging.debug(f"{left_col} is a list or has no nulls, will not nullfill")
			return polars_df, False
		try:
			# TODO: what's the difference between this and the polars expressions we use in the fallback function?
			polars_df = polars_df.with_columns(pl.col(left_col).fill_null(pl.col(right_col)))
			after = self.get_null_count_in_column(polars_df, left_col, warn=False)
			self.logging.debug(f"Filled in {before - after} nulls in {left_col}")
			status = True
		except pl.exceptions.InvalidOperationError:
			self.logging.debug("Could not nullfill (this isn't an error, nulls will be filled if pl.Ut8 or list[str])")
			status = False
		return polars_df, status

	def cast_to_list(self, polars_df, column, allow_nulls=False):
		if polars_df[column].dtype != pl.List:
			if allow_nulls: # will break concat_list() as it propagates nulls for some reason
				polars_df = polars_df.with_columns(pl.when(pl.col(column).is_not_null().then(pl.col(column).cast(pl.List(str)))).alias("as_this_list"))
				polars_df = polars_df.drop([column]).rename({"as_this_list": column})
				return polars_df
			else:
				polars_df = polars_df.with_columns(pl.col(column).cast(pl.List(str)).alias("as_this_list"))
				polars_df = polars_df.drop([column]).rename({"as_this_list": column})
				assert polars_df.schema[column] != pl.Utf8
				return polars_df
		else:
			return polars_df
	
	def check_base_and_right_in_df(self, polars_df, left_col, right_col):
		#if left_col not in polars_df.columns and not escalate_warnings:
		#	self.logging.warning(f"Found {right_col}, but {left_col} not in dataframe")
			#we don't return this so who cares: polars_df = polars_df.drop(right_col)
		#	exit(1)
		if left_col not in polars_df.columns:
			self.logging.error(f"Found {right_col}, but {left_col} not in dataframe -- this is a sign something broke in an earlier function")
			exit(1)
		self.logging.debug(f" {polars_df[left_col].dtype} | {polars_df[right_col].dtype}")
		return 0

	def concat_columns_list(self, polars_df, left_col, right_col, uniq):
	# TODO: merge or replace this function with the concat_list_no_prop_nulls function in merge.py		
		if uniq:
			polars_df = polars_df.with_columns(
				pl.when(
					(pl.col(left_col).is_not_null())
					.and_(pl.col(right_col).is_not_null()
					.and_(pl.col(left_col) != pl.col(right_col)))       # When a row has different values for base_col and right_col,
				)                                                       # make a list of base_col and right_col, but keep only uniq values
				.then(pl.concat_list([left_col, right_col]).list.unique().list.drop_nulls()) 
				.otherwise(
					pl.when(                                            # otherwise, make list of just base_col (doesn't seem to nest if already a list)
						pl.col(left_col).is_not_null()
					)
					.then(pl.concat_list([pl.col(left_col), pl.col(left_col)]).list.unique())
					.otherwise(pl.concat_list([pl.col(right_col), pl.col(right_col)]).list.unique()) # at this point it doesn't matter if right_col is null since left is
				)
				.alias(left_col)
			).drop(right_col)
		else:
			polars_df = polars_df.with_columns(
				pl.when(
					(pl.col(left_col).is_not_null())
					.and_(pl.col(right_col).is_not_null()
					.and_(pl.col(left_col) != pl.col(right_col)))       # When a row has different values for base_col and right_col,
				)                                                       # make a list of base_col and right_col,
				.then(pl.concat_list([left_col, right_col]).drop_nulls()) 
				.otherwise(
					pl.when(                                            # otherwise, make list of just base_col (doesn't seem to nest if already a list)
						pl.col(left_col).is_not_null()
					)
					.then(pl.concat_list([pl.col(left_col), pl.col(left_col)]).list.unique())
					.otherwise(pl.concat_list([pl.col(right_col), pl.col(right_col)]).list.unique()) # at this point it doesn't matter if right_col is null since left is
				) 
				.alias(left_col)
			).drop(right_col)
		assert polars_df.select(pl.col(left_col)).dtypes == [pl.List]
		return polars_df

	def report(self, polars_df):
		print(f"Dataframe stats:")
		print(f"  𓃾 {polars_df.shape[1]} metadata columns")
		if self.is_run_indexed(polars_df):
			print(f"  𓃾 {polars_df.shape[0]} rows, each row representing 1 run")
		else:
			print(f"  𓃾 {polars_df.shape[0]} rows, each row representing 1 sample")
		print(f"  𓃾 {polars_df.estimated_size(unit='mb')} MB in memory (roughly)")

		# ideally we'd set this with a polars expression, which I think might be parallel and all that jazz, but the tuple return of
		# get_most_common seems to require handling in a for loop (and I think making it not a tuple, ergo sorting twice, may be worse)
		column_names, column_types, column_n_null, column_mode_value, column_mode_n = [], [], [], [], []
		for col in polars_df.columns:
			column_names.append(col)
			column_types.append(polars_df.schema[col])
			column_n_null.append(self.get_null_count_in_column(polars_df, col, warn=False))
			mode, count = self.get_most_common_non_null_and_its_counts(polars_df, col)
			column_mode_value.append(mode)
			column_mode_n.append(count)
		bar = pl.DataFrame({
			"column": column_names,
			"type": column_types,
			"n null": column_n_null,
			"% null": [round((n / polars_df.shape[0]) * 100, 3) for n in column_n_null],
			"mode": column_mode_value,
			"n mode": column_mode_n,
		}, strict=False)
		self.super_print_pl(bar, "per-column stats")

	def translate_HPRC_IDs(self, polars_df, col_to_translate, new_col):
		return self.translate_column(polars_df, col_to_translate, new_col, HPRC_sample_ids.HPRC_R2_isolate_to_BioSample)

	def translate_column(self, polars_df, col_to_translate, new_col, dictionary):
		if new_col not in polars_df.columns:
			polars_df = polars_df.with_columns(pl.lit(None).alias(new_col))
		for key, value in dictionary.items():
			polars_df = polars_df.with_columns(
				pl.when(pl.col(col_to_translate) == pl.lit(key))
				.then(pl.lit(value)).otherwise(pl.col(new_col)).alias(new_col)
			)
		return polars_df

	def postmerge_fallback_or_null(self, polars_df, left_col, right_col, fallback=None, dont_crash_please=0):
		if dont_crash_please >= 3:
			self.logging.error(f"We keep getting polars.exceptions.ComputeError trying to merge {left_col} (type {polars_df.schema[left_col]}) and {right_col} (type {polars_df.schema[right_col]})")
			exit(1)
		try:
			if fallback == "left":
				polars_df = polars_df.with_columns([
					pl.when((pl.col(right_col) != pl.col(left_col)).and_(pl.col(left_col).is_not_null())).then(pl.col(left_col)).otherwise(pl.col(right_col)).alias(right_col)
				])
			elif fallback == "right":
				polars_df = polars_df.with_columns([
					pl.when((pl.col(right_col) != pl.col(left_col)).and_(pl.col(right_col).is_not_null())).then(pl.col(right_col)).otherwise(pl.col(left_col)).alias(left_col)
				])
			else:
				polars_df = self.try_nullfill(polars_df, left_col, right_col)[0]
				polars_df = polars_df.with_columns([
					pl.when((pl.col(right_col) != pl.col(left_col)).and_(pl.col(right_col).is_not_null()).and_(pl.col(left_col).is_not_null())).then(pl.col(right_col)).otherwise(None).alias(right_col),
				])
			return polars_df.drop(right_col) # nullfill operates on the left column, so we want that one even if fallback on right
		except pl.exceptions.ComputeError:
			polars_df = polars_df.with_columns([
				pl.col(right_col).cast(pl.Utf8),
				pl.col(left_col).cast(pl.Utf8)
			])
			return self.postmerge_fallback_or_null(polars_df, left_col, right_col, fallback, dont_crash_please=dont_crash_please+1)

	def merge_right_columns(self, polars_df, fallback_on_left=True, escalate_warnings=True, force_index=None):
		"""
		Takes in a polars_df with some number of columns ending in "_right", where each _right column has
		a matching column with the same basename (ie, "foo_right" matches "foo"), and merges each base:right
		pair's columns. The resulting merged columns will inherit the base columns name.

		Generally, we want to avoid creating columns of type list whenever possible.

		If column in kolumns.rancheroize__warn... and fallback_on_left, keep only left value(s)
		If column in kolumns.rancheroize__warn... and !fallback_on_left, keep only right values(s)

		Additional special handling for taxoncore columns... kind of
		"""
		right_columns = [col for col in polars_df.columns if col.endswith("_right")]
		if force_index is None:
			index_column = self.get_index_column(polars_df)
		else:
			index_column = force_index
		assert index_column not in right_columns
		for right_col in right_columns:
			self.logging.debug(f"\n[{right_columns.index(right_col)}/{len(right_columns)}] Trying to merge {right_col} (type: {polars_df.schema[right_col]}...")
			base_col, nullfilled = right_col.replace("_right", ""), False
			self.check_base_and_right_in_df(polars_df, base_col, right_col)
			
			# match data types
			if polars_df.schema[base_col] != pl.List and polars_df.schema[right_col] != pl.List and polars_df.schema[base_col] != polars_df.schema[right_col]:
				try:
					polars_df = polars_df.with_columns(pl.col(right_col).cast(polars_df.schema[base_col]).alias(right_col))
					self.logging.debug(f"Cast right column {right_col} to {polars_df.schema[base_col]}")
				except Exception:
					polars_df = polars_df.with_columns([
						pl.col(base_col).cast(pl.Utf8).alias(base_col),
						pl.col(right_col).cast(pl.Utf8).alias(right_col)
					])
					self.logging.debug("Cast both columns to pl.Utf8")

			# singular-singular merge -- this breaks the schema as-is, but maybe we can make the strings into single-element lists? is that even worth it?
			"""
			if polars_df.schema[base_col] == pl.Utf8 and polars_df.schema[right_col] == pl.Utf8:
				self.logging.debug(f"Merging two string columns into {base_col}")
				polars_df = polars_df.with_columns([
					pl.when(pl.col(base_col).is_not_null() | pl.col(right_col).is_not_null())
					.then(
						pl.when(pl.col(base_col).is_not_null() & pl.col(right_col).is_null())
						.then(pl.col(base_col))
						.otherwise(
							pl.when(pl.col(base_col).is_null()) # and right is not null
							.then(pl.col(right_col))
							.otherwise(pl.concat_list([base_col, right_col])) # neither are null
						) 
					)
					# otherwise null, since both are null anyway
					.alias("silliness"),
					])

				print(polars_df.select(['silliness', base_col, right_col]))
				polars_df = polars_df.drop([base_col, right_col]).rename({"silliness": base_col})
				continue
			"""

			# in all other cases, try nullfilling
			#else:
			if polars_df.schema[base_col] == pl.List(pl.Boolean) or polars_df.schema[base_col] == pl.List(pl.Boolean):
				polars_df = self.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=[base_col, right_col], force_index=index_column)
				if polars_df.schema[base_col] == pl.List(pl.Boolean) or polars_df.schema[base_col] == pl.List(pl.Boolean):
					self.logging.warning("List of booleans detected and cannot be flattened! Nulls may propagate!")
			else:
				polars_df, nullfilled = self.try_nullfill(polars_df, base_col, right_col)
			try:
				# TODO: this breaks in situations like when we add Brites before Bos, since Brites has three run accessions with no sample_index,
				# resulting in assertionerror but no printed conflicts
				assert_series_equal(polars_df[base_col], polars_df[right_col].alias(base_col))
				polars_df = polars_df.drop(right_col)
				self.logging.debug(f"All values in {base_col} and {right_col} are the same after an filling in each other's nulls. Dropped {right_col}.")
				continue
			except AssertionError:
				self.logging.debug(f"Not equal after filling in nulls (or nullfill errored so they're definitely not equal)")
		
			# everything past this point in this for loop only fires if the assertion error happened!
			if base_col in kolumns.list_throw_error:
				self.logging.error(f"[kolumns.list_throw_error] {base_col} --> Fatal error. There should never be lists in this column.")
				print_cols = [base_col, right_col, index_column, self.cfg.indicator_column] if self.cfg.indicator_column in polars_df.columns else [base_col, right_col, index_column]
				self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select(print_cols), f"conflicts")
				exit(1)

			elif base_col in kolumns.special_taxonomic_handling:
				# same as kolumns.list_fallback_or_null, only different in logging output
				if escalate_warnings:
					self.logging.error(f"[kolumns.special_taxonomic_handling] {base_col} --> Fatal error due to escalate_warnings=True")
					self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select([base_col, right_col, index_column]), f"conflicts")
					exit(1)
				else:
					self.logging.warning(f"[kolumns.special_taxonomic_handling] {base_col} --> Conflicts fall back on {'left' if fallback_on_left else 'right'}")
					polars_df = self.postmerge_fallback_or_null(polars_df, base_col, right_col, fallback='left' if fallback_on_left else 'right')
			
			elif base_col in kolumns.list_fallback_or_null:
				if escalate_warnings:
					self.logging.error(f"[kolumns.list_fallback_or_null] {base_col} --> Fatal error due to escalate_warnings=True")
					self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select([base_col, right_col, index_column]), f"conflicts")
					exit(1)
				else:
					self.logging.warning(f"[kolumns.list_fallback_or_null] {base_col} --> Conflicts fall back on {'left' if fallback_on_left else 'right'}")
					polars_df = self.postmerge_fallback_or_null(polars_df, base_col, right_col, fallback='left' if fallback_on_left else 'right')
			
			elif base_col in kolumns.list_to_null:
				self.logging.debug(f"[kolumns.list_to_null] {base_col} --> Conflicts turned to null")
				polars_df = self.postmerge_fallback_or_null(polars_df, base_col, right_col, fallback=None)
			
			elif base_col in kolumns.list_to_float_sum:
				self.logging.error("TODO NOT IMPLEMENTED")
				exit(1)

			elif base_col in kolumns.list_to_list_silent:
				self.logging.debug(f"[kolumns.list_to_list_silent] {base_col} --> concat_list")
				if not nullfilled:
					polars_df = self.cast_to_list(polars_df, base_col)
					polars_df = self.cast_to_list(polars_df, right_col)
				polars_df = self.concat_columns_list(polars_df, base_col, right_col, False)

			elif base_col in kolumns.list_to_set_uniq:
				self.logging.debug(f"[kolumns.list_to_set_uniq] {base_col} --> concat_list only uniq")
				if not nullfilled:
					polars_df = self.cast_to_list(polars_df, base_col)
					polars_df = self.cast_to_list(polars_df, right_col)
				polars_df = self.concat_columns_list(polars_df, base_col, right_col, True)

			else:
				self.logging.debug(f"[not in kolumns] {base_col} --> concat_list only uniq")
				if not nullfilled:
					polars_df = self.cast_to_list(polars_df, base_col)
					polars_df = self.cast_to_list(polars_df, right_col)
				polars_df = self.concat_columns_list(polars_df, base_col, right_col, True)
				#self.logging.debug(self.get_rows_where_list_col_more_than_one_value(polars_df, base_col).select([self.get_index_column(polars_df), base_col]))

			assert base_col in polars_df.columns
			assert right_col not in polars_df.columns

		right_columns = [col for col in polars_df.columns if col.endswith("_right")]
		if len(right_columns) > 0:
			self.logging.error(f"Failed to remove some _right columns: {right_columns}")
			exit(1)
		# non-unique rows might be dropped here, fyi
		return polars_df

	def drop_nulls_from_possible_list_column(self, polars_df, column):
		assert column in polars_df.columns
		if polars_df.schema[column] == pl.List:
			if self.logging.getEffectiveLevel() == 10:
				nulls = polars_df.filter(pl.col(column).list.eval(pl.element().is_null()).list.any())
				if len(nulls) > 0:
					self.logging.debug("Found lists with null values:")
					print(polars_df.select(self.get_valid_id_columns(polars_df) + [column]))
			return polars_df.with_columns(pl.col(column).list.drop_nulls())
		return polars_df

	
	def iteratively_merge_these_columns(self, polars_df, merge_these_columns: list, equivalence_key=None, recursion_depth=0):
		"""
		Merges columns named in merged_these_columns.

		When all is said and done, the final merged column will be named equivalene_key's value if not None.
		"""
		assert len(merge_these_columns) > 1
		assert all(col in polars_df.columns for col in merge_these_columns)
		assert all(not col.endswith("_right") for col in polars_df.columns)
		if recursion_depth != 0:
			self.logging.debug(f"Intending to merge:\n\t{merge_these_columns}")
		left_col, right_col = merge_these_columns[0], merge_these_columns[1]
		polars_df = self.drop_nulls_from_possible_list_column(self.drop_nulls_from_possible_list_column(polars_df, left_col), right_col)
		
		self.logging.debug(f"\n\t\tIteration {recursion_depth}\n\t\tLeft: {left_col}\n\t\tRight: {right_col} (renamed to {left_col}_right)")
		polars_df = polars_df.rename({right_col: f"{left_col}_right"})
		polars_df = self.merge_right_columns(polars_df)

		del merge_these_columns[1] # NOT ZERO!!!

		if len(merge_these_columns) > 1:
			#self.logging.debug(f"merge_these_columns is {merge_these_columns}, which we will pass in to recurse")
			polars_df = self.iteratively_merge_these_columns(polars_df, merge_these_columns, recursion_depth=recursion_depth+1)
		return polars_df.rename({left_col: equivalence_key}) if equivalence_key is not None else polars_df

	def unique_bioproject_per_center_name(self, polars_df: pl.DataFrame, center_name="FZB"):
		return (
			polars_df.filter(pl.col("center_name") == center_name)
			.select("BioProject").unique().to_series().to_list()
		)
	
	def rancheroize_polars(self, polars_df, drop_non_mycobact_columns=True, nullify=True, flatten=True, disallow_right=True, check_index=True, norename=False, drop_unwanted_columns=True, index=None):
		self.logging.debug(f"Dataframe shape before rancheroizing: {polars_df.shape[0]}x{polars_df.shape[1]}")
		if drop_unwanted_columns:
			polars_df = self.drop_known_unwanted_columns(polars_df)
		if drop_non_mycobact_columns:
			polars_df = self.drop_non_tb_columns(polars_df)
		if nullify:
			polars_df = self.drop_null_columns(self.nullify(polars_df))
			
			# check we didn't mess with the index
			if check_index and index is not None:
				assert index in polars_df.columns
				assert self.get_null_count_in_column(polars_df, index) == 0
			elif check_index:
				assert self.get_null_count_in_column(polars_df, self.get_index_column(polars_df)) == 0

		print(index)
		if flatten:
			polars_df = self.flatten_all_list_cols_as_much_as_possible(polars_df, force_strings=False) # this makes merging better for "geo_loc_name_sam"
		if disallow_right:
			assert len([col for col in polars_df.columns if col.endswith("_right")]) == 0, "Found columns with _right in their name, indicating a merge failure"
		if self.cfg.paired_illumina_only:
			polars_df = self.get_paired_illumina(polars_df)

		# check date columns, our arch-nemesis
		for column in polars_df.columns:
			if column in kolumns.equivalence['date_collected']:
				if polars_df[column].dtype is not pl.Date:
					self.logging.debug(f"Found likely date column {column}, but it has type {polars_df[column].dtype}")
				else:
					self.logging.debug(f"Likely date column {column} has pl.Date type")

		if not norename:
			for key, value in kolumns.equivalence.items():
				merge_these_columns = [v_col for v_col in value if v_col in polars_df.columns and v_col not in sum(kolumns.special_taxonomic_handling.values(), [])]
				if len(merge_these_columns) > 0:
					self.logging.debug(f"Discovered {key} in column via:")
					for some_column in merge_these_columns:
						self.logging.debug(f"  * {some_column}: {polars_df.schema[some_column]}")

					if len(merge_these_columns) > 1:
						#polars_df = polars_df.with_columns(pl.implode(merge_these_columns)) # this gets sigkilled; don't bother!
						if key in drop_zone.silly_columns:
							polars_df = polars_df.drop(col)
						elif key in kolumns.list_fallback_or_null or key in kolumns.list_to_null:
							self.logging.info(f"  Coalescing these columns into {key}: {merge_these_columns}")
							polars_df = polars_df.with_columns(pl.coalesce(merge_these_columns).alias("TEMPTEMPTEMP"))
							polars_df = polars_df.drop(merge_these_columns)
							polars_df = polars_df.rename({"TEMPTEMPTEMP": key})
						#don't add kolumns.list_to_float_sum here, that's not what it's made for and it'll cause errors
						else:
							self.logging.info(f"  Merging these columns: {merge_these_columns}")
							polars_df = self.iteratively_merge_these_columns(polars_df, merge_these_columns, equivalence_key=key)
					else:
						self.logging.debug(f"  Renamed {merge_these_columns[0]} to {key}")
						polars_df = polars_df.rename({merge_these_columns[0]: key})
					assert key in polars_df.columns
		
		# do not flatten list cols again, at least not yet. use the equivalence columns for standardization.
		self.logging.debug("Checking index...")
		polars_df = self.check_index(polars_df)
		self.logging.debug(f"Dataframe shape after rancheroizing: {polars_df.shape[0]}x{polars_df.shape[1]}")



		# DEBUG REMOVE
		"""
		if 'clade' in polars_df.columns:
			null_clade = self.get_count_of_x_in_column_y(polars_df, None, 'clade')
			if null_clade > 0:
				print("Found null values for clade at bottom of rancheroize")
				self.print_value_counts(polars_df, ['clade'])
				exit(1)
		else:
			print("clade not in polars df at end of rancheroize")
		"""



		return polars_df

	def is_sample_indexed(self, polars_df):
		index = self.get_index_column(polars_df)
		return True if index in kolumns.equivalence['sample_index'] else False

	def is_run_indexed(self, polars_df):
		index = self.get_index_column(polars_df)
		return True if index in kolumns.equivalence['run_index'] else False

	def add_list_len_col(self, polars_df, list_col, new_col):
		return polars_df.with_columns(pl.col(list_col).list.len().alias(new_col))

	def coerce_to_not_list_if_possible(self, polars_df, column, index_column=None, prefix_arrow=False):
		arrow = '-->' if prefix_arrow else ''
		if index_column is not None:
			assert column != index_column
		if polars_df.schema[column] == pl.List:
			if len(self.get_rows_where_list_col_more_than_one_value(polars_df, column)) == 0:
				print(f"{arrow}Can delist") if self.logging.getEffectiveLevel() == 10 else None
				return polars_df.with_columns(pl.col(column).list.first().alias(column))
			else:
				if self.logging.getEffectiveLevel() == 10:
					debug_print = self.get_rows_where_list_col_more_than_one_value(polars_df, column)
					print(f"{arrow}{len(debug_print)} multi-element lists in {column}")
					print(debug_print.select(index_column, column))
				return polars_df
		else:
			self.logging.debug(f"Tried to coerce {column} into a non-list, but it's already a non-list")
			return polars_df

	def flatten_list_col_as_set(self, polars_df, column):
		polars_df = self.flatten_one_nested_list_col(polars_df, column) # recursive
		polars_df = polars_df.with_columns(pl.col(column).list.unique().alias(f"{column}"))
		polars_df = self.coerce_to_not_list_if_possible(polars_df, column, index_column=self.get_index_column(polars_df))
		return polars_df

	def flatten_all_list_cols_as_much_as_possible(self, polars_df, hard_stop=False, force_strings=False, just_these_columns=None,
		force_index=None):
		"""
		Flatten list columns as much as possible. If a column is just a bunch of one-element lists, for
		instance, then just take the 0th value of that list and make a column that isn't a list.

		If force_strings, any remaining columns that are still lists are forced into strings.
		"""
		# Do not run check index first, as it will break when this is run right after run-to-sample conversion
		if force_index is None:
			index_column = self.get_index_column(polars_df)
		else:
			index_column = force_index

		null_counts_before = polars_df.filter(pl.col(col).null_count() > 0 for col in polars_df.columns)
		if null_counts_before.shape[0] == 0:
			self.logging.debug("Dataframe already seems to have no nulls")
		else:
			self.logging.debug("Dataframe has some nulls")
			self.logging.debug(null_counts_before)

		self.logging.debug("Recursively unnesting lists...")
		polars_df = self.flatten_nested_list_cols(polars_df)
		self.logging.debug("Unnested all list columns. Index seems okay.")

		null_counts_after = polars_df.filter(pl.col(col).null_count() > 0 for col in polars_df.columns)
		if null_counts_after.shape[0] == 0:
			self.logging.debug("After recursively unnesting lists, dataframe seems to have no nulls")
		else:
			self.logging.debug("After recursively unnesting lists, dataframe has some nulls")
			self.logging.debug(null_counts_after)

		what_was_done = []

		if just_these_columns is None:
			col_dtype = polars_df.schema
		else:
			col_dtype = dict()
			for col in just_these_columns:
				assert col in polars_df
				dtype = polars_df.schema[col]
				col_dtype[col] = dtype
		
		for col, datatype in col_dtype.items(): # TYPES DO NOT UPDATE AUTOMATICALLY!
			
			if col in drop_zone.silly_columns:
				polars_df.drop(col)
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': pl.Null, 'result': 'dropped'})
				continue
			
			if datatype == pl.List and datatype.inner != datetime.datetime:

				if polars_df.schema[col] != pl.List:
					# fixes issues with the 'strain' column previously being a list
					self.logging.debug(f"{col} was previously a list, but isn't one any longer (this should happen with taxoncore columns as they are delisted all at once)")
					continue

				try:
					# since already handled stuff that were already delisted earlier, this should only fire if it's a list of nulls
					polars_df = polars_df.with_columns(pl.col(col).list.drop_nulls())
				except Exception:
					self.logging.error(f"{col} has type {datatype} but is acting like it isn't a list -- is it full of nulls?")
					self.logging.error(polars_df.select(col))
					exit(1) # might be overkill

				if col in kolumns.equivalence['run_index'] and index_column in kolumns.equivalence['sample_index']:
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (runs in samp-indexed df)'})
					continue
				
				elif polars_df[col].drop_nulls().shape[0] == 0:
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (empty/nulls)'})
					continue

				elif col in kolumns.special_taxonomic_handling:
					
					# First attempt to flatten ALL taxoncore stuff (yes, this will get repeated per col in kolumns.special_taxonomic_handling, too bad)
					for kolumn in kolumns.special_taxonomic_handling:
						if kolumn in polars_df.columns and polars_df.schema[kolumn] == pl.List:
							polars_df = polars_df.with_columns(pl.col(kolumn).list.unique())
							dataframe_height = polars_df.shape[1]
							polars_df = self.drop_nulls_from_possible_list_column(polars_df, kolumn)
							current_dataframe_height = polars_df.shape[1]
							assert dataframe_height == current_dataframe_height
							polars_df = self.coerce_to_not_list_if_possible(polars_df, kolumn, index_column, prefix_arrow=True)
					
					if polars_df.schema[col] == pl.List: # since it might not be after coerce_to_not_list_if_possible()
						long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(['sample_index', 'clade', 'organism', 'lineage', 'strain'])
						#long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(['sample_index', 'clade', 'organism', 'lineage']) # TODO: BAD WORKAROUND
						if len(long_boi) > 0:
							# TODO: more rules could be added, and this is a too TB specific, but for my purposes it's okay for now
							if col == 'organism' and polars_df.schema['organism'] == pl.List:
								# check lineage column first for consistency
								# TODO: these polars expressions are hilariously ineffecient but I want them explict for the time being
								if polars_df.schema['lineage'] == pl.List:
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L1')).list.all())).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L2')).list.all())).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L3')).list.all())).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L4')).list.all())).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L5')).list.all())).then(pl.lit(["Mycobacterium africanum"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L6')).list.all())).then(pl.lit(["Mycobacterium africanum"])).otherwise(pl.col("clade")).alias('organism'))
								polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() == 2).and_(pl.col('organism').list.contains("Mycobacterium tuberculosis complex sp."))).then(pl.lit(["Mycobacterium tuberculosis complex sp."])).otherwise(pl.col("organism")).alias("organism"))
								polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() == 2).and_(pl.col('organism').list.contains("Mycobacterium tuberculosis"))).then(pl.lit(["Mycobacterium tuberculosis complex sp."])).otherwise(pl.col("organism")).alias("organism"))
								# unnecessary
								#elif polars_df.schema['lineage'] == pl.Utf8:
								#	polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').str.starts_with('L')).and_(~pl.col('lineage').str.starts_with('L5')).and_(~pl.col('lineage').str.starts_with('L6')).and_(~pl.col('lineage').str.starts_with('La'))).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("organism")).alias('organism'))
							
							elif col == 'clade' and polars_df.schema['clade'] == pl.List:
								polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('clade').list.contains('MTBC')).and_(~pl.col('clade').list.contains('NTM'))).then(pl.lit(["MTBC"])).otherwise(pl.col("clade")).alias('clade'))
								
								if polars_df.schema['lineage'] == pl.List:
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L1')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L2')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L3')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L4')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L5')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L6')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
								elif polars_df.schema['lineage'] == pl.Utf8:
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').str.starts_with('L')).and_(~pl.col('lineage').str.starts_with('La'))).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))

								# We'll treat every remaining conflict as tuberculosis
								# TODO: this is probably not how we should be handling this, but we need to delist this somehow and it works for my dataset
								polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1)).then(['tuberculosis: unclassified']).otherwise(pl.col("clade")).alias('clade'))
							
							elif col == 'lineage' and polars_df.schema['lineage'] == pl.List:
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L1')).list.all())).then(pl.lit(["L1"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L2')).list.all())).then(pl.lit(["L2"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L3')).list.all())).then(pl.lit(["L3"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L4')).list.all())).then(pl.lit(["L4"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L5')).list.all())).then(pl.lit(["L5"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L6')).list.all())).then(pl.lit(["L6"])).otherwise(pl.col("lineage")).alias('lineage'))

								# We'll treat every remaining conflict as invalid and null it 
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1)).then(None).otherwise(pl.col("lineage")).alias('lineage'))
							
							if self.logging.getEffectiveLevel() == 10:
								long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(self.valid_cols(long_boi, ['sample_index', 'clade', 'organism', 'lineage', 'strain']))
								self.logging.debug(f"Non-1 {col} values after attempting to de-long them")
								self.logging.debug(long_boi)
							polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
						else:
							self.logging.debug(f"Taxoncore column {col} will not be adjusted further")
				
				elif col in kolumns.list_to_float_sum:
					# TODO: use logger adaptors instead of this print cringe
					print(f"{col}\n-->[kolumns.list_to_float_sum]") if self.logging.getEffectiveLevel() == 10 else None
					if datatype.inner == pl.String:
						print(f"-->Inner type is string, casting to pl.Int32 first") if self.logging.getEffectiveLevel() == 10 else None
						polars_df = polars_df.with_columns(
							pl.col(col).list.eval(
								pl.when(pl.element().is_not_null())
								.then(pl.element().cast(pl.Int32))
								.otherwise(None)
							).alias(f"{col}_sum")
						)
					else:
						polars_df = polars_df.with_columns(pl.col(col).list.sum().alias(f"{col}_sum"))
					polars_df = polars_df.drop(col)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[f"{col}_sum"], 'result': 'namechange + summed'})
					continue
				
				elif col in kolumns.list_to_list_silent:
					print(f"{col}\n-->[kolumns.list_to_list_silent]") if self.logging.getEffectiveLevel() == 10 else None
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': '.'})
					continue

				elif col in kolumns.list_to_null:
					print(f"{col}\n-->[kolumns.list_to_null]") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = polars_df.with_columns([
						pl.when(pl.col(col).list.len() <= 1).then(pl.col(col)).otherwise(None).alias(col)
					])
					print(f"-->Set null in conflicts, now trying to delist") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'null conflicts'})
					continue
				
				elif col in kolumns.list_to_set_uniq: 
					print(f"{col}\n-->[kolumns.list_to_set_uniq]") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					print("-->Used uniq, now trying to delist") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set-and-shrink'})
					continue
					
				elif col in kolumns.list_fallback_or_null:
					# If this had happened during a merge of two dataframes, we would be falling back on one df or the other. But here, we
					# don't know what value to fall back upon, so it's better to just null this stuff.
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					bad_ones = polars_df.filter(pl.col(col).list.len() > 1)
					if len(bad_ones) > 1:
						self.logging.warning(f"{col}\n-->[kolumns.list_fallback_or_null] Expected {col} to only have one non-null per sample, but found {bad_ones.shape[0]} conflicts (will be nulled).")
						if self.logging.getEffectiveLevel() == 10:
							print_cols = self.valid_cols(bad_ones, ['sample_index', 'run_index', col, 'continent' if col != 'continent' else 'country'])
							self.super_print_pl(bad_ones.select(print_cols), "Conflicts")
						polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
						polars_df = polars_df.with_columns(
							pl.when(pl.col(col).list.len() <= 1).then(pl.col(col)).otherwise(None).alias(col)
						)
						#assert len(self.get_rows_where_list_col_more_than_one_value(polars_df, col, False)) == 0 # beware: https://github.com/pola-rs/polars/issues/19987
						if hard_stop:
							exit(1)
						else:
							what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set-and-shrink (!!!WARNING!!)'})
							continue
					else:
						self.logging.debug(f"{col}\n-->[kolumns.list_fallback_or_null] {col} is type list, but it seems all lists have a len of 1 or 0")
						non_nulls_in_this_column = polars_df.select(pl.count(col)).item() # only counts non-nulls, see https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.count.html
						# do not run pl.when(col).list.len() <= 1 expression here, that doesn't work for some reason
						polars_df = polars_df.with_columns(
							pl.when(pl.col(col).list.len() <= 1).then(pl.col(col).first()).otherwise(None).alias(col)
						)
						polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
						assert polars_df.select(pl.count(col)).item() == non_nulls_in_this_column

				else:
					self.logging.warning(f"{col}-->Not sure how to handle, will treat it as a set")
					polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set (no rules)'})
					continue

			elif datatype == pl.List and datatype.inner == datetime.date:
				self.logging.warning(f"{col} is a list of datetimes. Datetimes break typical handling of lists, so this column will be left alone.")
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (date.date)'})
			
			else:
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': '-'})
		
		if force_strings:
			if just_these_columns is None:
				polars_df = self.stringify_all_list_columns(polars_df)
			else:
				for column in just_these_columns:
					polars_df = self.encode_as_str(polars_df, column)
		
		report = pl.DataFrame(what_was_done)
		if self.logging.getEffectiveLevel() <= 10:
			NeighLib.super_print_pl(report, "Finished flattening list columns. Results:")
		self.logging.debug("Returning flattened dataframe")
		return polars_df

	def rstrip(self, polars_df, column, strip_char=" "):
		return polars_df.with_columns([
			pl.when(pl.col(column).str.ends_with(" "))
			.then(pl.col(column).str.slice(-1))
			.otherwise(pl.col(column))
			.alias(column)
		])
	
	def recursive_rstrip(self, polars_df, column, strip_char=" "):
		while polars_df[column].str.ends_with(" ").any():
			self.logging.info("Recursing...")
			polars_df = self.rstrip(polars_df, column, strip_char)
		return polars_df

	def check_index(self, polars_df, 
			manual_index_column=None, 
			force_NCBI_runs=_cfg_force_SRR_ERR_DRR_run_index, 
			force_BioSamples=_cfg_force_SAMN_SAME_SAMD_sample_index,
			rstrip=False, # VERY SLOW AND NOT WELL TESTED!
			dupe_index_handling=_cfg_dupe_index_handling
			):
		"""
		Check a polars dataframe's apparent index, which is expected to be either run accessions or sample accessions, for the following issues:
		* pl.null/None values
		* duplicates
		* incompatiable index columns (eg, two run index columns)

		Unless manual_index_column is not none, this function will use kolumns.equivalence to figure out what your index column(s) are.
		"""
		dupe_index_handling = self._sentinal_handler(dupe_index_handling)
		force_NCBI_runs = self._sentinal_handler(force_NCBI_runs)
		force_BioSamples = self._sentinal_handler(force_BioSamples)

		# if index column is manually set, it's okay if get_index() fails
		if manual_index_column is not None:
			try:
				apparent_index_column = self.get_index_column(polars_df)
				if manual_index_column not in polars_df.columns:
					self.logging.error(f"Manual index column set to {manual_index_column}, but that column isn't in the dataframe!")
					raise ValueError
				elif manual_index_column != apparent_index_column:
					self.logging.debug(f"Manual index column set to {manual_index_column}, which is in the dataframe, but there's additional index columns too: {apparent_index_column}")
					index_column = manual_index_column
				else:
					index_column = manual_index_column
			except ValueError:
				self.logging.debug(f"Index manually set to {manual_index_column}, no other valid indeces found")
				index_column = manual_index_column
		else:
			index_column = self.get_index_column(polars_df)

		if rstrip:
			polars_df = self.recursive_rstrip(polars_df, index_column, strip_char=" ")
		
		# handle get_index_column() error cases
		if type(index_column) == list:
			if index_column[0] == 2:
				self.logging.error(f"Dataframe has multiple possible sample based indeces: {index_column[1]}")
				raise ValueError
			elif index_column[0] == 3:
				# in theory you could get away with this, since there is a sample index, but I won't support that
				self.logging.error(f"Dataframe has multiple possible run indeces: {index_column[1]}")
				raise ValueError
			elif index_column[0] == 4:
				self.logging.error(f"Dataframe has multiple possible run indeces: {index_column[1]}")
				raise ValueError
			elif index_column[0] == 5:
				self.logging.error(f"Could not find any valid index column. You can set valid index columns in kolumns.py's equivalence dictionary.")
				self.logging.error(f"Current possible run index columns (key for kolumns.equivalence['run_index']): {kolumns.equivalence['run_index']}")
				self.logging.error(f"Current possible sample index columns (key for kolumns.equivalence['sample_index']): {kolumns.equivalence['sample_index']}")
				self.logging.error(f"Your dataframe's columns: {polars_df.columns}")
				raise ValueError
			else:
				raise ValueError
		assert polars_df.schema[index_column] != pl.List # just to be super-extra-double sure

		# drop any nulls in the index column -- these needs to be before checking for duplicates
		nulls = self.get_null_count_in_column(polars_df, index_column, warn=False, error=False)
		if nulls > 0:
			self.logging.warning(f"Dropped {nulls} row(s) with null value(s) in index column {index_column}")
			polars_df = polars_df.filter(pl.col(index_column).is_not_null())
			nulls = self.get_null_count_in_column(polars_df, index_column, warn=False, error=False)
			if nulls > 0:
				self.logging.error(f"Failed to remove null values from index column {index_column}")
				raise ValueError
		
		# check for duplicates
		assert polars_df.schema[index_column] == pl.Utf8
		
		duplicate_df = polars_df.filter(polars_df[index_column].is_duplicated())
		n_dupe_indeces = len(duplicate_df)
		#if len(polars_df) != len(polars_df.unique(subset=[index_column], keep="any")):
		if n_dupe_indeces > 0:
			self.logging.debug(f"Found {n_dupe_indeces} dupes in {index_column}, will handle according to prefernces")
			if dupe_index_handling == 'allow':
				self.logging.warning(f"Reluctantly keeping {n_dupe_indeces} duplicate values in index {index_column} as per dupe_index_handling")
			elif dupe_index_handling in ['error', 'verbose_error']:
				self.logging.error("Duplicates in index found!") # print above and below verbose dataframe
				if dupe_index_handling == 'error':
					raise ValueError(f"Found {n_dupe_indeces} duplicates in index column")
				else: # verbose_error
					self.dfprint(duplicate_df)
					self.polars_to_tsv(duplicate_df, "dupes_in_index.tsv")
					raise ValueError(f"Found {n_dupe_indeces} duplicate indeces in index column (dumped to dupes_in_index.tsv)")
			elif dupe_index_handling in ['warn', 'verbose_warn', 'silent']:
				subset = polars_df.unique(subset=[index_column], keep="any")
				if dupe_index_handling == 'warn':
					self.logging.warning(f"Found {n_dupe_indeces} duplicate indeces in index {index_column}, "
						"will keep one instance per dupe")
				elif dupe_index_handling == 'verbose_warn':
					duplicate_df = duplicate_df.select(self.valid_cols(duplicate_df, [index_column, 'run_index', 'sample_index', 'submitted_files_bytes']))
					self.dfprint(duplicate_df.sort(index_column))
					self.polars_to_tsv(duplicate_df, "dupes_in_index.tsv")
					self.logging.warning(f"Found {n_dupe_indeces} duplicate indeces in index {index_column} (dumped to dupes_in_index.tsv), "
						"will keep one instance per dupe")
				polars_df = subset
			elif dupe_index_handling == 'dropall':
				subset = polars_df.unique(subset=[index_column], keep="none")
				self.logging.warning(f"Found {n_dupe_indeces} duplicate indeces in index {index_column}, will drop all of them")
				polars_df = subset
			else:
				raise ValueError(f"Unknown value provided for dupe_index_handling: {dupe_index_handling}")
		else:
			self.logging.debug(f"Did not find any duplicates in {index_column}")
		# if applicable, make sure there's no nonsense in our index columns -- also, we're checking run AND sample columns if both are present,
		# to prevent issues if we do a run-to-sample conversion later
		# also, thanks to earlier checks, we know there should only be a maximum of one sample index and one run index.
		for column in polars_df.columns:
			if column in kolumns.equivalence['sample_index'] and force_BioSamples and polars_df.schema[column] != pl.List:
				good = (
					polars_df[column].str.starts_with("SAMN") |
					polars_df[column].str.starts_with("SAME") |
					polars_df[column].str.starts_with("SAMD")
				)
				invalid_rows = polars_df.filter(~good).drop([col for col in polars_df.columns if col not in (kolumns.equivalence['sample_index'] + kolumns.equivalence['run_index'])])
				valid_rows = polars_df.filter(good)
				if len(invalid_rows) > 0:
					self.logging.warning(f"Out of {len(polars_df)} samples, found {len(invalid_rows)} samples that don't start with SAMN/SAME/SAMD (will be dropped, leaving {len(valid_rows)} afterwards):")
					print(invalid_rows)
					return valid_rows
			elif column in kolumns.equivalence['run_index'] and force_NCBI_runs and polars_df.schema[column] != pl.List:
				good = (
					polars_df[column].str.starts_with("SRR") |
					polars_df[column].str.starts_with("ERR") |
					polars_df[column].str.starts_with("DRR")
				)
				invalid_rows = polars_df.filter(~good).drop([col for col in polars_df.columns if col not in (kolumns.equivalence['sample_index'] + kolumns.equivalence['run_index'])])
				valid_rows = polars_df.filter(good)
				if len(invalid_rows) > 0:
					self.logging.warning(f"Out of {len(polars_df)} runs, found {len(invalid_rows)} runs that don't start with SRR/ERR/DRR (will be dropped, leaving {len(valid_rows)} afterwards):")
					print(invalid_rows)
					return valid_rows
			else:
				continue
		# double check no funny business
		duplicates = polars_df.filter(polars_df[index_column].is_duplicated())
		assert polars_df.filter(polars_df[index_column].is_duplicated()).shape[0] == 0
		return polars_df

	def drop_non_tb_columns(self, polars_df):
		dont_drop_these = [col for col in polars_df.columns if col not in drop_zone.clearly_not_tuberculosis]
		return polars_df.select(dont_drop_these)

	def drop_known_unwanted_columns(self, polars_df):
		return polars_df.select([col for col in polars_df.columns if col not in drop_zone.silly_columns])

	def flatten_one_nested_list_col(self, polars_df, column):
		polars_df = self.drop_nulls_from_possible_list_column(polars_df, col)
		if polars_df[column].schema == pl.List(pl.List):
			polars_df = polars_df.with_columns(pl.col(column).list.eval(pl.element().flatten().drop_nulls()))
			#polars_df = polars_df.with_columns(pl.col(col).list.eval(pl.element().flatten())) # might leave hanging nulls (does earlier drop_nulls fix this though?)
			#polars_df = polars_df.with_columns(pl.col(col).flatten().list.drop_nulls()) # polars.exceptions.ShapeError: unable to add a column of length x to a Dataframe of height y
		if polars_df[column].schema == pl.List(pl.List):
			polars_df = self.flatten_one_nested_list_col(polars_df, column) # this recursion should, in theory, handle list(list(list(str))) -- but it's not well tested
		return polars_df

	def flatten_nested_list_cols(self, polars_df):
		"""There are other ways to do this, but this one doesn't break the schema, so we're sticking with it"""
		nested_lists = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.List)]
		for col in nested_lists:
			self.logging.debug(f"Unnesting {col}")
			polars_df = self.drop_nulls_from_possible_list_column(polars_df, col)
			polars_df = self.flatten_one_nested_list_col(polars_df, col) # this is already recursive
		return(polars_df)

	def cast_to_string(self, polars_df, column):
		"""
		''Cast'' a list column into a string. Unlike encode_as_str(), brackets will not be added, nor will elements besides
		the first (0th) member of a list be perserved (unless that member is a null, because we drop nulls from lists first)

		* [] --> null
		* [null] --> null
		* ["bizz"] --> "bizz"
		* ["foo", "bar"] --> "foo"
		"""


	def encode_as_str(self, polars_df, column, L_bracket='[', R_bracket=']'):
		""" Unnests list/object data (but not the way explode() does it) so it can be writen to CSV format
		Originally based on deanm0000's code, via https://github.com/pola-rs/polars/issues/17966#issuecomment-2262903178

		LIMITATIONS: This may not work as expected on pl.List(pl.Null). You may also see oddities on some pl.Object types.
		"""
		self.logging.debug(f"Forcing column {column} into a string")
		assert column in polars_df.columns # throws an error because it's a series now?
		datatype = polars_df.schema[column]

		if datatype == pl.List(pl.String):
			polars_df = polars_df.with_columns(
				pl.when(pl.col(column).list.len() <= 1) # don't add brackets if longest list is 1 or 0 elements
				.then(pl.col(column).list.eval(pl.element()).list.join(""))
				.otherwise(
					pl.lit(L_bracket)
					+ pl.col(column).list.eval(pl.lit("'") + pl.element() + pl.lit("'")).list.join(",")
					+ pl.lit(R_bracket)
				).alias(column)
			)
			return polars_df
		
		elif datatype in [pl.List(pl.Int8), pl.List(pl.Int16), pl.List(pl.Int32), pl.List(pl.Int64), pl.List(pl.Float64)]:
			polars_df = polars_df.with_columns((
				pl.lit(L_bracket)
				+ pl.col(column).list.eval(pl.lit("'") + pl.element().cast(pl.String) + pl.lit("'")).list.join(",")
				+ pl.lit(R_bracket)
			).alias(column))
			return polars_df
		
		# This makes assumptions about the structure of the object and may not be universal
		elif datatype == pl.Object:
			polars_df = polars_df.with_columns((
				pl.col(col).map_elements(lambda s: "{" + ", ".join(f"{item}" for item in sorted(s)) + "}" if isinstance(s, set) else str(s), return_dtype=str)
			).alias(col))
			return polars_df

		elif datatype == pl.List(pl.Datetime(time_unit='us', time_zone='UTC')):
			polars_df = polars_df.with_columns((
				pl.col(col).map_elements(lambda s: "[" + ", ".join(f"{item}" for item in sorted(s)) + "]" if isinstance(s, set) else str(s), return_dtype=str)
			).alias(col))

		elif datatype == pl.Utf8:
			self.logging.debug(f"Called encode_as_str() on {column}, which already has pl.Utf8 type. Doing nothing...")
			return polars_df

		else:
			raise ValueError(f"Tried to make {column} into a string column, but we don't know what to do with type {datatype}")

	def stringify_all_list_columns(self, polars_df):
		self.logging.debug(f"Forcing ALL list columns into strings")
		for col, datatype in polars_df.schema.items():
			polars_df = self.encode_as_str(polars_df, col)
		return polars_df

	def add_column_of_just_this_value(self, polars_df, column, value):
		assert column not in polars_df.columns
		return polars_df.with_columns(pl.lit(value).alias(column))

	def drop_column(self, polars_df, column):
		assert column in polars_df.columns
		return polars_df.drop(column)

	def drop_null_columns(self, polars_df, and_non_null_type_full_of_nulls=False):
		polars_df = polars_df.drop(cs.by_dtype(pl.Null))
		polars_df = polars_df.drop(cs.by_dtype(pl.List(pl.Null)))
		if and_non_null_type_full_of_nulls:
			cols_to_keep = [col for col in polars_df.schema
				if polars_df.select(pl.col(col)).null_count().item() < polars_df.height
			]
			polars_df = polars_df.select(cols_to_keep)
		return polars_df

	def tsv_value_counts(self, polars_df, vcount_column, path):
		self.polars_to_tsv(polars_df.select([pl.col(vcount_column).value_counts(sort=True)]).unnest(vcount_column), path, null_value='null')

	def multiply_and_trim(self, col: str) -> pl.Expr:
		return (pl.col(col) * 100).round(3).cast(pl.Float64)

	def polars_to_tsv(self, polars_df, path: str, null_value=''):
		df_to_write = self.drop_null_columns(polars_df)
		columns_with_type_list_or_obj = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if (dtype == pl.List or dtype == pl.Object)]
		if len(columns_with_type_list_or_obj) > 0:
			self.logging.warning("Went to write a TSV file but detected column(s) with type list or object. Due to polars limitations, the TSVs will attempt to encode these as strings.")
			df_to_write = self.stringify_all_list_columns(df_to_write)
		try:
			if self.logging.getEffectiveLevel() == 10:
				debug = pl.DataFrame({col: [dtype1, dtype2] for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if dtype1 not in [pl.String, pl.Int32, pl.UInt32]})
				if debug.height > 0:
					self.logging.debug(f"Non-string types, and what they converted to: {debug}")
			df_to_write.write_csv(path, separator='\t', include_header=True, null_value=null_value)
			self.logging.info(f"Wrote dataframe to {path}")
		except pl.exceptions.ComputeError:
			print("WARNING: Failed to write to TSV due to ComputeError. This is likely a data type issue.")
			debug = pl.DataFrame({col:  f"Was {dtype1}, now {dtype2}" for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if col in df_to_write.columns and dtype2 != pl.String and dtype2 != pl.List(pl.String)})
			self.super_print_pl(debug, "Potentially problematic that may have caused the TSV write failure:")
			exit(1)
	
	def assert_unique_columns(self, pandas_df):
		"""Assert all columns in a !!!PANDAS!!! dataframe are unique -- useful if converting to polars """
		if len(pandas_df.columns) != len(set(pandas_df.columns)):
			dupes = []
			not_dupes = set()
			for column in pandas_df.columns:
				if column in not_dupes:
					dupes.append(column)
				else:
					not_dupes.add(column)
			raise AssertionError(f"Pandas df has duplicate columns: {dupes}")
	
	def cast_politely(self, polars_df):
		""" 
		polars_df.cast({k: v}) just doesn't cut it, and casting is not in-place, so
		this does a very goofy full column replacement
		"""
		for k, v in kolumns.not_strings.items():
			try:
				to_replace_index = polars_df.get_column_index(k)
			except pl.exceptions.ColumnNotFoundError:
				continue
			casted = polars_df.select(pl.col(k).cast(v))
			polars_df.replace_column(to_replace_index, casted.to_series())
			#print(f"Cast {k} to type {v}")
		return polars_df
