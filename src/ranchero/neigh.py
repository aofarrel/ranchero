import os
import re
import polars as pl
import datetime
from .statics import kolumns, drop_zone, null_values, HPRC_sample_ids
from polars.testing import assert_series_equal
import polars.selectors as cs
from .config import RancheroConfig
INDEX_PREFIX = "__index__"

# https://peps.python.org/pep-0661/
_DEFAULT_TO_CONFIGURATION = object()

class NeighLib:
	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger

	def _default_fallback(self, cfg_var, value):
		if value == _DEFAULT_TO_CONFIGURATION:
			return self.cfg.get_config(cfg_var)
		return value

	# --------- INDEX FUNCTIONS --------- #

	def mark_index(self, polars_df: pl.DataFrame, wannabe_index_column: str, rm_existing_index=False) -> pl.DataFrame:
		"""Attempts to mark wannabe_index_column as an index using INDEX_PREFIX, checking
		beforehand that there isn't already a marked index

		when wannabe_index_column == 'file', hypothetical_marked_index == '__INDEX__file'
		when wannabe_index_column == '__INDEX__file', hypothetical_unmarked_index == 'file'

		"""
		hypothetical_marked_index = self.get_hypothetical_index_fullname(wannabe_index_column)
		hypothetical_unmarked_index = self.get_hypothetical_index_basename(wannabe_index_column)

		if hypothetical_marked_index == wannabe_index_column:
			self.logging.info("Index is already marked!")
			return polars_df

		# is this column (or some hypothetical iteration of it) in the dataframe?
		if wannabe_index_column not in polars_df.columns:
			if hypothetical_marked_index not in polars_df.columns:
				if hypothetical_unmarked_index not in polars_df.columns:
					raise ValueError(f"{wannabe_index_column} nor {hypothetical_marked_index} nor {hypothetical_unmarked_index} in DataFrame")
				else:
					# this should never happen
					raise ValueError(f"FAILURE: {wannabe_index_column} absent, {hypothetical_marked_index} absent, {hypothetical_unmarked_index} present")
			# wannabe_index_column = 'file' and not in df, '__INDEX__file' is in df
			if self.has_one_index_column(polars_df):
				self.logging.debug(f"Tried to mark non-existent column {wannabe_index_column} as index, but {hypothetical_marked_index} already in dataframe")
				return polars_df
			else:
				raise ValueError(f"More than one index in dataframe! Columns: {sort(polars_df.columns)}")
		
		# the actually normal situation - wannabe is in the df and there is currently no index
		elif self.has_zero_index_columns(polars_df):
			polars_df = polars_df.rename({wannabe_index_column: hypothetical_marked_index})
			assert self.get_index(polars_df, guess=False) == self.get_hypothetical_index_fullname(hypothetical_marked_index)
			return polars_df
		
		# wannabe is in polars_df, but we already have precisely one existing index
		elif self.has_one_index_column(polars_df):

			# is there a marked and unmarked version of the wannabe_index_column?
			# we already ensured wannabe_index_column is in polars_df, and already handled hypothetical == wannabe
			if hypothetical_marked_index in polars_df.columns:
				assert_series_equal(polars_df[wannabe_index_column].rename(hypothetical_marked_index), polars_df[hypothetical_marked_index])
				self.logging.warning(f"Somehow {wannabe_index_column} and {hypothetical_marked_index} are both in the dataframe, but they're equal, so we'll just remove {wannabe_index_column}")
				return polars_df.drop(wannabe_index_column)
			elif rm_existing_index:
				polars_df = self.strip_index_markers(polars_df)
				polars_df = polars_df.rename({wannabe_index_column: hypothetical_marked_index})
				assert self.get_index(polars_df, guess=False) == self.get_hypothetical_index_fullname(hypothetical_marked_index)
				return polars_df
			else:
				self.logging.error("Another index already eixsts in the dataframe (set rm_existing_index to True if you want to remove automatically)")
				raise ValueError("Multiple indeces detected, and rm_existing_index is not True")
		elif rm_existing_index:
			polars_df = self.strip_index_markers(polars_df).rename({wannabe_index_column: hypothetical_marked_index})
			assert self.get_index(polars_df, guess=False) == self.get_hypothetical_index_fullname(hypothetical_marked_index)
			return polars_df
		else:
			self.logging.error("Multiple indeces detected, and rm_existing_index is not True")
			raise ValueError("Multiple indeces detected, and rm_existing_index is not True")


	def get_index(self, polars_df: pl.DataFrame, guess=False) -> str | None:
		if self.has_one_index_column(polars_df):
			for col in polars_df.columns:
				if col.startswith(INDEX_PREFIX):
					return col
			raise ValueError("We thought there was an index, but can't find a column with the index prefix!")
		if guess:
			return self.guess_index_column(polars_df)
		return None

	def get_index_subname(self, polars_df: pl.DataFrame) -> str | None:
		for col in polars_df.columns:
			if col.startswith(INDEX_PREFIX):
				return col[len(INDEX_PREFIX):]
		return None

	def get_hypothetical_index_fullname(self, wannabe_index_column: str) -> str:
		# DOES NO CHECKING (hence hypothetical)
		if wannabe_index_column.startswith(INDEX_PREFIX):
			return wannabe_index_column
		return str(INDEX_PREFIX + wannabe_index_column)

	def get_hypothetical_index_basename(self, wannabe_index_column: str) -> str:
		# DOES NO CHECKING (hence hypothetical)
		return str(wannabe_index_column.lstrip(INDEX_PREFIX))

	def strip_index_markers(self, polars_df: pl.DataFrame) -> pl.DataFrame:
		return polars_df.rename({
			col: col[len(INDEX_PREFIX):]
			for col in polars_df.columns if col.startswith(INDEX_PREFIX)
		})

	def has_multiple_index_columns(self, polars_df: pl.DataFrame):
		if len([col for col in polars_df.columns if col.startswith(INDEX_PREFIX)]) > 1:
			return True
		return False

	def has_zero_index_columns(self, polars_df: pl.DataFrame) -> bool:
		if len([col for col in polars_df.columns if col.startswith(INDEX_PREFIX)]) == 0:
			return True
		return False

	def has_one_index_column(self, polars_df: pl.DataFrame) -> bool:
		# Quick check of whether a df has one index column. That index may still be invalid -- we don't want to do
		# multiple expensive checks when not necessary, so this is a very basic check.
		if len([col for col in polars_df.columns if col.startswith(INDEX_PREFIX)]) == 1:
			return True
		return False

	def rstrip_whitespace_from_index(self, polars_df: pl.DataFrame) -> pl.DataFrame:
		# WARNING: THIS IS VERY SLOW!
		if self.has_one_index_column(polars_df):
			return self.recursive_rstrip(polars_df, self.get_index(polars_df), strip_char=" ")
		else:
			self.logging.error("Couldn't find index column in dataframe; set it via NeighLib.mark_index() before calling this function")
			raise ValueError("Couldn't find index column in dataframe; set it via NeighLib.mark_index() before calling this function")

	def check_index(self,
			polars_df, 
			guess=True,
			try_to_fix=True,
			manual_index_column=None,
			force_INSDC_runs=_DEFAULT_TO_CONFIGURATION,
			force_INSDC_samples=_DEFAULT_TO_CONFIGURATION,
			dupe_index_handling=_DEFAULT_TO_CONFIGURATION,
			allow_bad_name=False,  # for merge_upon checks, etc
			df_name=None           # for logging
			):
		"""
		Check a polars dataframe's apparent index, which is expected to be either run accessions or sample accessions, for the following issues:
		* pl.null/None values
		* duplicates
		* incompatiable index columns (eg, two run index columns)

		Unless manual_index_column is not none, this function will use kolumns.equivalence to figure out what your index column(s) are.
		"""
		df_name = "Dataframe" if df_name is None else df_name
		dupe_index_handling = self._default_fallback("dupe_index_handling", dupe_index_handling)
		force_INSDC_runs = self._default_fallback("force_INSDC_runs", force_INSDC_runs)
		force_INSDC_samples = self._default_fallback("force_INSDC_samples", force_INSDC_samples)

		assert polars_df.shape[0] != 0
		assert polars_df.shape[1] != 0

		# 1st check: Are there multiple marked __INDEX__ columns?
		if self.has_multiple_index_columns(polars_df):
			if try_to_fix:
				# to avoid messing up checks below, this will just strip the multiple indexes and continue
				self.logging.warning(f"Found multiple index columns in {df_name}, but will try to fix...")
				polars_df = self.strip_index_markers(polars_df)
			else:
				self.logging.error(f"Found multiple index columns in {df_name} (to try to fix, run with try_to_fix = True)")
				raise ValueError(f"Found multiple index columns in {df_name} (to try to fix, run with try_to_fix = True)")

		# 2nd check: What actually is the index column? (a lot of checking happens in mark_index())
		# Option A: User defined an index column manually
		if manual_index_column is not None:
			not_guessed_current_index = self.get_index(polars_df, guess=False)
			if not manual_index_column.startswith(INDEX_PREFIX) and not allow_bad_name:
				if try_to_fix:
					polars_df = self.mark_index(polars_df, manual_index_column)
					manual_index_column = self.get_index(polars_df, guess=False)
					index_to_check = manual_index_column
			
			if manual_index_column not in polars_df.columns:
				if try_to_fix: # whether we allow bad names or not, we gotta fix this!
					polars_df = self.mark_index(polars_df, manual_index_column)
					index_to_check = self.get_index(polars_df, guess=False)
				else:
					raise ValueError(f"Manual index column set to {manual_index_column}, but that column isn't in {df_name}! (it may already be marked as an index column though, try running with try_to_fix=True)")
			
			# manual_index_column is in polars_df, but there is also a marked __INDEX__ that doesn't match it
			elif manual_index_column != not_guessed_current_index and not_guessed_current_index is not None:
				if not allow_bad_name: # for merge_upon checks, etc
					self.logging.error(f"manual_index_column={manual_index_column} but {df_name} already has supposed index {not_guessed_current_index}")
					raise ValueError(f"manual_index_column={manual_index_column} but {df_name} already has supposed index {not_guessed_current_index}")
				self.logging.warning(f"You're checking {manual_index_column} as if it were {df_name}'s index, but index column {not_guessed_current_index} also exists. I'll allow it, reluctantly.")
				index_to_check = manual_index_column # no need to mark it, it's already marked... er, right? well, we'll check later anyway			
			else:
				index_to_check = manual_index_column
			not_guessed_current_index = None
		# Option B: User did not define an index column manually
		else:
			if self.has_one_index_column(polars_df):
				index_to_check = self.get_index(polars_df, guess=False)
			else:
				if guess:
					could_make_that_an_index = self.get_index(polars_df, guess=True)
					assert could_make_that_an_index is not None
					polars_df = self.mark_index(polars_df, could_make_that_an_index)
					index_to_check = self.get_hypothetical_index_fullname(could_make_that_an_index)
				else:
					index_to_check = self.get_index(polars_df, guess=False)
					assert index_to_check is not None

		assert index_to_check is not None
		assert polars_df.schema[index_to_check] == pl.Utf8
		if not allow_bad_name:
			assert index_to_check.startswith(INDEX_PREFIX)
			assert self.get_index(polars_df) == index_to_check

		# check for leading and lagging whitespace -- note that this check is a little slow, and the fix is VERY slow
		#if polars_df.filter(pl.col(index_to_check).str.starts_with(" ")).size[0] != 0:
		#	if try_to_fix:
		#		polars_df = self.recursive_rstrip(polars_df, index_to_check)
		#if polars_df.filter(pl.col(index_to_check).str.ends_with(" ")).size[0] != 0:
		#	self.logging.error("Found lagging whitespace in column")
		#	self.super_print_pl(polars_df.filter(pl.col(index_to_check).str.ends_with(" ")).select(index_to_check), "")
		#	raise ValueError(f"Found lagging whitespace in index column {index_to_check}")
		# TODO: doesn't fix leading, doesn't fix or check for tabs/carriage return (can polars even have those in a str column?), rstrip function should be replaced...
		
		# drop any nulls in the index column -- these needs to be before checking for duplicates
		nulls = self.get_null_count_in_column(polars_df, index_to_check, warn=False, error=False)
		if nulls > 0:
			self.logging.warning(f"Dropped {nulls} row(s) with null value(s) in {df_name}'s index column {index_to_check}")
			polars_df = polars_df.filter(pl.col(index_to_check).is_not_null())
			nulls = self.get_null_count_in_column(polars_df, index_to_check, warn=False, error=False)
			if nulls > 0:
				self.logging.error(f"Failed to remove null values from {df_name}'s index column {index_to_check}")
				raise ValueError
		
		# check for duplicates
		# TODO: dupe_index_handling should probably align with try_to_fix behavior
		assert polars_df.schema[index_to_check] == pl.Utf8  # in case entire column got nulled and datatype became pl.Null
		duplicate_df = polars_df.filter(polars_df[index_to_check].is_duplicated())
		n_dupe_indeces = len(duplicate_df)
		#if len(polars_df) != len(polars_df.unique(subset=[index_to_check], keep="any")):
		if n_dupe_indeces > 0:
			self.logging.debug(f"Found {n_dupe_indeces} dupes in {df_name}'s {index_to_check}, will handle according to dupe_index_handling: {dupe_index_handling}")
			if dupe_index_handling == 'allow':
				self.logging.warning(f"Reluctantly keeping {n_dupe_indeces} duplicate values in {df_name}'s {index_to_check} as per dupe_index_handling")
			elif dupe_index_handling in ['error', 'verbose_error']:
				if dupe_index_handling == 'error':
					raise ValueError(f"Found {n_dupe_indeces} duplicates in index column")
				else: # verbose_error
					self.logging.error(f"Duplicates in {df_name}'s index found!") # not in non-verbose error so testing module doesn't print "ERROR" (yeah yeah logging handlers would fix it but i dont wanna)
					self.polars_to_tsv(duplicate_df, "dupes_in_index.tsv")
					self.dfprint(duplicate_df.select(self.valid_cols(duplicate_df, [index_to_check, 'run_id', 'sample_id', 'submitted_files_bytes'])), str_len=120, width=120)
					raise ValueError(f"Found {n_dupe_indeces} duplicate indeces in {df_name}'s index column (dumped to dupes_in_index.tsv)")
			elif dupe_index_handling in ['warn', 'verbose_warn', 'silent']:
				subset = polars_df.unique(subset=[index_to_check], keep="any")
				if dupe_index_handling == 'warn':
					self.logging.warning(f"Found {n_dupe_indeces} duplicate indeces in {df_name}'s index {index_to_check}, "
						"will keep one instance per dupe")
				elif dupe_index_handling == 'verbose_warn':
					self.polars_to_tsv(duplicate_df, "dupes_in_index.tsv")
					self.logging.warning(f"Found {n_dupe_indeces} duplicate indeces in {df_name}'s index {index_to_check} (dumped to dupes_in_index.tsv), "
						"will keep one instance per dupe")
					self.dfprint(duplicate_df.select(self.valid_cols(duplicate_df, [index_to_check, 'run_id', 'sample_id', 'submitted_files_bytes'])).sort(index_to_check), str_len=120, width=120, loglevel=30)
				polars_df = subset
			elif dupe_index_handling == 'dropall':
				subset = polars_df.unique(subset=[index_to_check], keep="none")
				self.logging.warning(f"Found {n_dupe_indeces} duplicate indeces in {df_name}'s index {index_to_check}, will drop all of them")
				polars_df = subset
			elif dupe_index_handling == 'keep_most_data':
				# TODO: is it faster to do this with just the subset of columns with dupes and then concat?
				# maybe swap strategies based on the shape of duplicate_df and polars_df relative to each other
				self.logging.info(f"Found {n_dupe_indeces} duplicate indeces in {df_name}'s index {index_to_check}, will keep rows with the most non-nulls")
				
				# POLARS VERSION DIFFERENCE: polars=1.1.16 will not sort the same way as polars==1.27.0, see testing module for an example
				polars_df = polars_df.with_columns(
					pl.sum_horizontal(
						*[pl.col(c).is_not_null().cast(pl.Int64) for c in polars_df.columns if c != index_to_check]
					).alias("_non_null_count")
				)
				polars_df = (
					polars_df.sort(by=[index_to_check, "_non_null_count"], descending=[False, True])
					.unique(subset=[index_to_check], keep="first")
					.drop("_non_null_count")
				)
			else:
				raise ValueError(f"Unknown value provided for dupe_index_handling: {dupe_index_handling}")
		else:
			self.logging.debug(f"Did not find any duplicates in {df_name}'s {index_to_check}")
		
		# if applicable, make sure there's no nonsense in our index columns -- also, we're checking run AND sample columns if both are present,
		# to prevent issues if we do a run-to-sample conversion later
		# also, thanks to earlier checks, we know there should only be a maximum of one sample index and one run index.
		for column in polars_df.columns:
			if column in kolumns.equivalence['sample_id'] and force_INSDC_samples and polars_df.schema[column] != pl.List:
				good = (
					polars_df[column].str.starts_with("SAMN") |
					polars_df[column].str.starts_with("SAME") |
					polars_df[column].str.starts_with("SAMD") |
					polars_df[column].is_null() # we already dropped nulls from the index, so even if sample_id is the index column this is okay
				)
				invalid_rows = polars_df.filter(~good)
				valid_rows = polars_df.filter(good)
				columns_not_to_print = list(set([col for col in polars_df.columns if col not in (kolumns.equivalence['sample_id'] + kolumns.equivalence['run_id'] + [index_to_check])]))
				if len(invalid_rows) > 0:
					msg_1 = f"Out of {len(polars_df)} samples, found {len(invalid_rows)} samples that don't start with SAMN/SAME/SAMD, "
					msg_2 = f"whose rows will be dropped, leaving {len(valid_rows)} rows"
					self.logging.warning(msg_1+msg_2)
					self.dfprint(invalid_rows.drop(columns_not_to_print), loglevel=30)
					return valid_rows
			elif column in kolumns.equivalence['run_id'] and force_INSDC_runs and polars_df.schema[column] != pl.List:
				good = (
					polars_df[column].str.starts_with("SRR") |
					polars_df[column].str.starts_with("ERR") |
					polars_df[column].str.starts_with("DRR") |
					polars_df[column].is_null() # we already dropped nulls from the index, so even if run_id is the index column this is okay
				)
				invalid_rows = polars_df.filter(~good)
				valid_rows = polars_df.filter(good)
				columns_not_to_print = list(set([col for col in polars_df.columns if col not in (kolumns.equivalence['sample_id'] + kolumns.equivalence['run_id'] + [index_to_check])]))
				if len(invalid_rows) > 0:
					msg_1 = f"Out of {len(polars_df)} runs, found {len(invalid_rows)} runs that don't start with SRR/ERR/DRR, "
					msg_2 = f"whose rows will be dropped, leaving {len(valid_rows)} rows"
					self.logging.warning(msg_1+msg_2)
					self.dfprint(invalid_rows.drop(columns_not_to_print), loglevel=30)
					return valid_rows
			else:
				continue
		
		# double check no funny business
		duplicates = polars_df.filter(polars_df[index_to_check].is_duplicated())
		assert polars_df.filter(polars_df[index_to_check].is_duplicated()).shape[0] == 0
		self.logging.debug(f"Finished all index checks for {df_name}")
		return polars_df


	def guess_index_column(self, polars_df, angry=True):
		"""
		A last resort check for an index column, based on values in kolumns.equivalence
		"""
		already_known_index = self.get_index(polars_df, guess=False)
		if already_known_index is not None:
			return str(already_known_index)
		
		sample_matches = [col for col in kolumns.equivalence['sample_id'] if (col in polars_df.columns and polars_df.schema[col] == pl.Utf8)]
		run_matches = [col for col in kolumns.equivalence['run_id'] if (col in polars_df.columns and polars_df.schema[col] == pl.Utf8)]

		if len(sample_matches) > 1:
			if angry:
				self.logging.error(f"Tried to find dataframe index, but there's multiple possible sample indeces: {sample_matches}")
				raise ValueError(f"Tried to find dataframe index, but there's multiple possible sample indeces: {sample_matches}")
	
		elif len(sample_matches) == 1:
			if len(run_matches) > 1:
				if angry:
					raise ValueError(f"Tried to find dataframe index, but there's multiple possible run indeces (may indicate failed run->sample conversion):  {run_matches}")
				else:
					return None
			
			elif len(run_matches) == 1:
				if polars_df.schema[run_matches[0]] == pl.List:
					return str(sample_matches[0])
				else:
					return str(run_matches[0])

			else:
				return str(sample_matches[0])  # no run indeces, just one sample index

		# no sample index, multiple run indeces
		elif len(run_matches) > 1:
			if angry:
				self.logging.error(f"Dataframe has multiple possible run indeces: {index_column[1]}")
				raise ValueError(f"Tried to find dataframe index, but there's multiple possible run indeces: {run_matches}")
			else:
				return [4, run_matches]
		
		# no sample index, one run index
		elif len(run_matches) == 1:
			return str(run_matches[0])

		else:
			if angry:
				raise ValueError(f"No valid index column found in polars_df! Columns available: {polars_df.columns}")
			else:
				return [5]

		if angry:
			raise ValueError("No idea what the index column is!")
		return None



		if type(index_column) == list:
			if index_column[0] == 2:
				
				raise ValueError
			elif index_column[0] == 3:
				# in theory you could get away with this, since there is a sample index, but I won't support that
				
				raise ValueError
			elif index_column[0] == 4:
				self.logging.error(f"Dataframe has multiple possible run indeces: {index_column[1]}")
				raise ValueError
			elif index_column[0] == 5:
				self.logging.error(f"Could not find any valid index column. You can set valid index columns in kolumns.py's equivalence dictionary.")
				self.logging.error(f"Current possible run index columns (key for kolumns.equivalence['run_id']): {kolumns.equivalence['run_id']}")
				self.logging.error(f"Current possible sample index columns (key for kolumns.equivalence['sample_id']): {kolumns.equivalence['sample_id']}")
				self.logging.error(f"Your dataframe's columns: {polars_df.columns}")
				raise ValueError
			else:
				raise ValueError



	# --------- GET FUNCTIONS --------- #

	def get_number_of_x_in_column(self, polars_df, x, column):
		return len(polars_df.filter(pl.col(column) == x))

	def get_a_where_b_is_null(self, polars_df, col_a, col_b):
		if col_a not in polars_df.columns or col_b not in polars_df.columns:
			self.logging.warning(f"Tried to get column {col_a} where column {col_b} is pl.Null, but at least one of those columns aren't in the dataframe!")
			return
		get_df = polars_df.with_columns(pl.when(pl.col(col_b).is_null()).then(pl.col(col_a)).otherwise(None).alias(f"{col_a}_filtered")).drop_nulls(subset=f"{col_a}_filtered")
		return get_df

	def get_most_common_non_null_and_its_counts(self, polars_df, column, and_its_counts=True):
		counts = polars_df.select(
			pl.col(column)
			.filter(pl.col(column).is_not_null())
			.value_counts(sort=True) # creates struct[2] column named col, sorted in descending order
		)
		counts = counts.unnest(column) # splits col into col and "counts" columns
		try:
			return tuple(counts.row(0))
		except Exception:
			self.logging.warning(f"Could not calculate mode for {column} -- is it full of nulls?")
			return ('ERROR', 'N/A')

	def get_null_count_in_column(self, polars_df, column, warn=True, error=False):
		series = polars_df.get_column(column)
		null_count = series.null_count()
		if null_count > 0 and warn:
			self.logging.warning(f"Found {null_count} nulls in column {column}")
		elif null_count > 0 and error:
			self.logging.error(f"Found {null_count} nulls in column {column}")
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
		print_columns = self.get_valid_id_columns(print_df) + [f"{col_a}_filtered", col_b] + alsoprint if alsoprint is not None else [self.get_index(print_df, guess=True)] + [f"{col_a}_filtered", col_b]
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
					with pl.Config(fmt_str_lengths=500, tbl_rows=50, set_tbl_hide_column_data_types=True):
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

	def dfprint(self, polars_df, cols=10, rows=20, str_len=40, list_len=10, width=140, loglevel=None):
		with pl.Config(tbl_cols=cols, tbl_rows=rows, fmt_str_lengths=str_len, fmt_table_cell_list_len=list_len, tbl_width_chars=width):
			if loglevel is None or loglevel >= self.logging.getEffectiveLevel():
				print(polars_df)
			else:
				pass

	@staticmethod
	def super_print_pl(polars_df, header, select=None, str_len=45):
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")
		try:
			polars_df = polars_df.fill_null("-")
		except Exception: # TODO: be more specific, it's some kind of polars type error
			self.logging.warning("Cannot fill null values with strings; print below may have empty row")
		if select is not None:
			valid_selected_columns = [col for col in select if col in polars_df.columns]
			with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=str_len, fmt_table_cell_list_len=10):
				print(polars_df.select(valid_selected_columns))
		else:
			with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=str_len, fmt_table_cell_list_len=10):
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

	def null_list_of_len_zero(self, polars_df, column):
		before = self.get_null_count_in_column(polars_df, column, warn=False)
		polars_df = polars_df.with_columns(pl.col(column).list.drop_nulls()) # [pl.Null] --> []
		polars_df = polars_df.with_columns([pl.when(pl.col(column).list.len() > 0).then(pl.col(column))]) # [] --> pl.Null
		after = self.get_null_count_in_column(polars_df, column, warn=False)
		self.logging.debug(f"{column}: {before} --> {after} nulls")
		if after == polars_df.shape[0]:
			self.logging.warning(f"Column {column} is now entirely null")
		return polars_df

	def null_lists_of_len_zero(self, polars_df, just_this_column=None, skip_ids=True, skip_index=True, index=None):
		list_cols = self.get_columns_by_type(polars_df, pl.List, skip_ids=skip_ids, skip_index=skip_index, index=index)
		for column in list_cols:
			polars_df = self.null_list_of_len_zero(polars_df, column)
		return polars_df

	def get_columns_by_type(self, polars_df, polars_type, subset=None, skip_ids=True, skip_index=True, index=None):
		all_cols = subset if subset is not None else polars_df.columns
		if type(all_cols) == str:
			all_cols = [all_cols]
		if skip_index:
			if index is None:
				all_cols.remove(self.get_index(polars_df))
			else:
				all_cols.remove(index)
		if skip_ids:
			return [col for col in all_cols if polars_df.schema[col] == polars_type and col not in kolumns.id_columns]
		else:
			return [col for col in all_cols if polars_df.schema[col] == polars_type]

	def nullify(self, polars_df, only_these_columns=None, no_match_NA=False, skip_ids=True, skip_index=True, index=None):
		"""
		Turns stuff like "not collected" and "n/a" into pl.Null values, per null_values.py,
		and nulls lists that have a length of zero
		"""
		self.logging.debug("First pass of nulling lists of len zero...")
		polars_df = self.null_lists_of_len_zero(polars_df)

		string_cols = self.get_columns_by_type(polars_df, pl.Utf8, 
			subset=only_these_columns, skip_ids=skip_ids, skip_index=skip_index, index=index)
		list_cols = self.get_columns_by_type(polars_df, pl.List(pl.Utf8), 
			subset=only_these_columns, skip_ids=skip_ids, skip_index=skip_index, index=index)

		self.logging.debug("Performing string replacements for null values (this may take a while)...")
		# Here's the fun part -- string replacements!
		# contains_any():
		# * pretty fast, compared to for-looping a list + contains()
		# * anywhere-in-string matching
		# * case insensitive
		# * does not support regex
		self.logging.debug("Running contains_any() on columns of type string...")
		polars_df = polars_df.with_columns([
			pl.when(pl.col(col).str.contains_any(null_values.nulls_pl_contains_any, ascii_case_insensitive=True))
			.then(None)
			.otherwise(pl.col(col))
			.alias(col) for col in string_cols])
		
		self.logging.debug("Running contains_any() on columns of type list...")
		polars_df = polars_df.with_columns([
			pl.col(col).list.eval(
				pl.element().filter(~pl.element().str.contains_any(null_values.nulls_pl_contains_any, ascii_case_insensitive=True))
			)
			for col in list_cols])

		# At this point it's possible for a column's type to have changed into null or list(null),
		# so we need to regenerate string_cols and list_cols
		string_cols = self.get_columns_by_type(polars_df, pl.Utf8, 
			subset=only_these_columns, skip_ids=skip_ids, skip_index=skip_index, index=index)
		list_cols = self.get_columns_by_type(polars_df, pl.List(pl.Utf8), 
			subset=only_these_columns, skip_ids=skip_ids, skip_index=skip_index, index=index)

		# Now we use a for loop and contains() (booooooo) because that allows us to use regex
		self.logging.debug("Looping with contains()...")
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
		self.logging.debug("Second pass of nulling lists of len zero...")
		polars_df = self.null_lists_of_len_zero(polars_df)
		self.logging.debug("Finished nullify()")
		return polars_df

	def assert_no_list_columns(self, polars_df: pl.DataFrame):
		list_cols = [name for name, dtype in polars_df.schema.items() if isinstance(dtype, pl.List)]
		assert not list_cols, f"Found list columns: {list_cols}"

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
				elif self.cfg.host_info_handling != 'columns' and d['k'] in kolumns.host_info:
					host_info.add(f"{d['k']}: {str(d['v']).lstrip('host_').rstrip('_sam').rstrip('sam_s_dpl111')}")
				else:
					combined_dict[d['k']] = d['v']
		if len(primary_search) > 0:
			combined_dict.update({"primary_search": list(primary_search)}) # convert to a list to avoid the polars column becoming type object
		if self.cfg.host_info_handling == 'dictionary' and len(host_info) > 0:
			combined_dict.update({"host_info": list(host_info)})
		elif self.cfg.host_info_handling == 'drop':
			combined_dict = {k: v for k, v in combined_dict.items() if k not in kolumns.host_info}
		# self.cfg.host_info_handling == 'columns' is handled automagically
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

	def try_nullfill_left(self, polars_df, left_col, right_col):
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
				polars_df = polars_df.with_columns(pl.when(pl.col(column).is_not_null()).then(pl.col(column).cast(pl.List(str))).alias("as_this_list"))
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
				polars_df = self.try_nullfill_left(polars_df, left_col, right_col)[0]
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
			index_column = self.guess_index_column(polars_df)
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
			if polars_df.schema[base_col] == pl.List(pl.Boolean) or polars_df.schema[right_col] == pl.List(pl.Boolean):
				polars_df = self.flatten_all_list_cols_as_much_as_possible(polars_df, just_these_columns=[base_col, right_col], force_index=index_column)
				if polars_df.schema[base_col] == pl.List(pl.Boolean) or polars_df.schema[base_col] == pl.List(pl.Boolean):
					self.logging.warning("List of booleans detected and cannot be flattened! Nulls may propagate!")
			else:
				polars_df, nullfilled = self.try_nullfill_left(polars_df, base_col, right_col)
			try:
				# TODO: this breaks in situations like when we add Brites before Bos, since Brites has three run accessions with no sample_id,
				# resulting in assertionerror but no printed conflicts

				# BE AWARE THAT THIS WILL FIRE IF ONE OF THEM HAS NULL VALUES WHERE THE OTHER DOES NOT
				assert_series_equal(polars_df[base_col], polars_df[right_col].alias(base_col))
				polars_df = polars_df.drop(right_col)
				self.logging.debug(f"All values in {base_col} and {right_col} are the same after an filling in each other's nulls. Dropped {right_col}.")
				continue
			except AssertionError:
				self.logging.debug(f"Not equal after filling in nulls (or nullfill errored so they're definitely not equal)")
		
			# everything past this point in this for loop only fires if the assertion error happened!
			if base_col in kolumns.list_throw_error_strict:
				self.logging.error(f"[kolumns.list_throw_error_strict] {base_col} --> Fatal error. There should never be lists in this column.")
				print_cols = [base_col, right_col, index_column]
				#print_cols = [base_col, right_col, index_column, self.cfg.indicator_column] if self.cfg.indicator_column in polars_df.columns else [base_col, right_col, index_column]
				if len(polars_df.filter(pl.col(base_col) != pl.col(right_col))) == 0:
					self.logging.error("Conflict seems to be from null values only -- consider using kolumns.list_throw_error instead of kolumns.list_throw_error_strict")
					assert_series_equal(polars_df[base_col], polars_df[right_col].alias(base_col)) # this will provide more helpful output than super_print_pl
				else:
					self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select(print_cols), f"conflicts")
				exit(1)

			elif base_col in kolumns.list_throw_error:
				print_cols = [base_col, right_col, index_column]
				#print_cols = [base_col, right_col, index_column, self.cfg.indicator_column] if self.cfg.indicator_column in polars_df.columns else [base_col, right_col, index_column]
				if len(polars_df.filter(pl.col(base_col) != pl.col(right_col))) == 0:
					self.logging.debug("[kolumns.list_throw_error] Found conflicts, but they're nulls, so who cares?")
					polars_df = polars_df.drop(right_col) # TODO: is this right?
				else:
					self.logging.error(f"[kolumns.list_throw_error] {base_col} --> Fatal error. There should never be lists in this column.")
					self.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select(print_cols), f"conflicts")
					assert_series_equal(polars_df[base_col], polars_df[right_col].alias(base_col)) # this will provide more helpful output than super_print_pl

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
				#self.logging.debug(self.get_rows_where_list_col_more_than_one_value(polars_df, base_col).select([self.guess_index_column(polars_df), base_col]))

			assert base_col in polars_df.columns
			assert right_col not in polars_df.columns, f"Caught {right_col} in dataframe after it should have been removed"

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
				if len(nulls) > 0 and self.logging.getEffectiveLevel() == 10:
					self.logging.debug("Found lists with null values:")
					self.dfprint(nulls.select(self.get_valid_id_columns(polars_df) + [column]), loglevel=10)
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
	
	def rancheroize_polars(self,
			polars_df:  pl.DataFrame,
			drop_non_mycobact_columns=False,
			nullify=True,
			flatten=True,
			disallow_right=True,
			check_index=True,
			norename=False,
			drop_unwanted_columns=False,
			input_index=None,
			output_index=None,
			standardize_index=True,
			name="Dataframe"):
		"""
		Rancheroize a dataframe into a semi-standardized format. This standardization is focused on more consistent column names rather than
		standardizing actual values within said columns (ie, this won't convert your country names to ISO codes). The goal of this is to
		reduce the number of extraneous columns and provide a basic framework for dataframe merges.

		polars_df:
			Your polars dataframe. Cannot be pl.Series nor can it be pandas.
		drop_non_mycobact_columns:
			Drop columns that are largely useless for dealing with non-Mycobacteria.
		nullify:
			Attempt to convert values like "unknown" and "not applicable" into pl.Null as per null_values.py -- this is within the design
			goals of rancheroize() as a function because making sure nulls exist where they should is important for cross-dataframe merges.
			Also attempts to drop columns of type pl.Null.
		disallow_right:
			Do not allow columns with names ending in "_right" as they may indicate a failed merge.
		check_index:
			Run check_index() on dataframe (will take into account input_index, output_index, and standardize_index)
		norename:
			Skip standardizing columns as per kolumns.equivalence.items(). If output_index == True, index will still be renamed. If
			standardize_index == True, index will still be standardized as per kolumns.equivalence.items() if possible.
		drop_unwanted_columns:
			Drop columns present in drop_zone.py
		input_index:
			Name of a column currently IN dataframe that you want to treat as the index. If not provided, will guess according to get_index().
		output_index:
			Name of a column currently NOT IN dataframe that you want to rename the index to. If you provide a value that doesn't have INDEX_PREFIX, it
			will be added on for compatibility purposes. For example:
			rancheroize_polars(index="run", output_index="runacc")          --> renames "run" to "__index__runacc" (assuming INDEX_PREFIX is __index__)
			rancheroize_polars(index="run", output_index="__index__runacc") --> renames "run" to "__index__runacc" (assuming INDEX_PREFIX is __index__)
		standardize_index:
			If index column seems to match something in kolumns.id_columns, rename it to its standardized name in the same way you would rename anything
			else per kolumns.equivalence.items(). For more examples, see documentation in kolumns.py
		name:
			An arbitrary name given to the dataframe, used only for debug prints.
		

		Also affected by paired_illumina_only
		"""
		df_name = "Dataframe" if name is None else name
		if self.logging.getEffectiveLevel() <= 10:
			debug_args = {**locals()}
			del debug_args['self']
			del debug_args['polars_df']
			self.logging.debug(f"Args: {debug_args}")
			self.logging.debug(f"{df_name} shape before rancheroizing: {polars_df.shape[0]}x{polars_df.shape[1]}")
			self.logging.debug(f"{df_name} has these columns before rancheroizing: {polars_df.columns}")
		if output_index is not None and standardize_index is not None:
			warnL1 = f"You set output_index={output_index} but also {standardize_index}={standardize_index}. "
			warnL2 = "output_index will take priority."
			self.logging.warning(warnL1+warnL2)

		# 1: Do we know what the index actually is?
		
		# Dataframe does not have a marked index (or it has multiple marked indeces)
		if not self.has_one_index_column(polars_df):
			self.logging.debug("[!has_one_index_column] Stripping any and all index markers")
			polars_df = self.strip_index_markers(polars_df) # strips ALL marked index columns (in case there's multiple)
			
			# We have not-1 index, and user didn't define an index
			if input_index is None:
				self.logging.debug("[!has_one_index_column] User did not provide an index so we have to guess the correct one")
				self.logging.warning(f"Guessing {df_name}'s index...")
				current_index = self.get_index(polars_df, guess=True)
			
			# We have not-1 index, but user defined an index
			else:
				self.logging.debug(f"[!has_one_index_column] User did provide index={input_index}")
				# Because we cleared all INDEX_PREFIX-marked columns, if user included INDEX_PREFIX, then
				# input_index will not be in the dataframe at the moment
				current_index = self.get_hypothetical_index_basename(input_index)
				assert current_index in polars_df
			
			expected_index = self.get_hypothetical_index_fullname(current_index)
			polars_df = self.mark_index(polars_df, current_index)
			marked_index = self.get_index(polars_df, guess=False)
			assert marked_index == expected_index
			index = marked_index
			current_index, marked_index, expected_index = None, None, None
			self.logging.debug(f"[!has_one_index_column] Index is now {index}")

		# Dataframe seems to have one marked index, and there is an input_index
		elif input_index is not None:
			current_index = self.get_index(polars_df, guess=False)
			if self.get_hypothetical_index_basename(current_index) == input_index:
				index = current_index
			elif current_index != input_index: # and it's not just a prefix difference 
				errorL1 = f"Attempted to rancheroize {df_name} with pre-existing index {current_index}, but was told "
				errorL2 = f"input_index = {input_index}.\nHint: If you want to swap from a run-based index to a "
				errorL3 = "sample-based index, use run_to_sample_index()."
				self.logging.error(errorL1+errorL2+errorL3)
				raise ValueError
			else:
				index = input_index

		# No input_index provided but there is a marked index (has_one_index_column() is only true if one marked index col)
		else:
			self.logging.debug(f"No input_index provided, but there is a marked index already at {self.get_index(polars_df, guess=False)}")
			index = self.get_index(polars_df, guess=False)

		# 2. Okay, we know what the index is, but do we have to change it?
		index_could_be_standardized = any(self.get_hypothetical_index_basename(index) in v for v in kolumns.equivalence_id_columns.values())
		self.logging.debug(f"index_could_be_standardized: {index_could_be_standardized}")

		# Rename per standardize_index (if applicable)
		if standardize_index and output_index is None and index_could_be_standardized:
			standardized_index = next( # next() returns only one value b/c kolumns.py asserted equivalence_id_columns.values() are unique
				k for k, v in kolumns.equivalence_id_columns.items() if self.get_hypothetical_index_basename(index) in v
			)
			if standardized_index != self.get_hypothetical_index_basename(index):
				assert standardized_index not in polars_df.columns
				assert self.get_hypothetical_index_fullname(standardized_index) not in polars_df.columns
				polars_df = polars_df.rename({index: standardized_index})
				polars_df = self.mark_index(polars_df, standardized_index)
				self.logging.debug(f"Renamed index from {index} to {self.get_hypothetical_index_fullname(standardized_index)}")
				index = self.get_index(polars_df, guess=False)
				assert index == self.get_hypothetical_index_fullname(standardized_index)
		
		# Rename per output_index (if applicable)
		if output_index is not None:
			self.logging.debug(f"User set output_index={output_index}, will rename according to that")
			assert output_index not in polars_df.columns        # we do this check here in case earlier stuff caused issues
			polars_df = polars_df.rename({index: output_index})
			
			# Warn users if their desired index output doesn't have INDEX_PREFIX
			if not output_index.startswith(INDEX_PREFIX):
				self.logging.debug(f"output_index={output_index} does not start with INDEX_PREFIX")
				polars_df = self.mark_index(polars_df, output_index)
				warnL1=f"You set output_index={output_index} but final index will be "
				warnL2=f"{self.get_hypothetical_index_fullname(output_index)} for maximum "
				warnL3="compatibility.\nHint: If you're okay with ranchero having a harder time "
				warnL4="automatically detecting your index column, you can simply call polars' "
				warnL5=f"rename() after rancheroize to force the column name to be literally {output_index}"
				self.logging.warning(warnL1+warnL2+warnL3+warnL4+warnL5)
				index = self.get_hypothetical_index_fullname(output_index)
			else:
				self.logging.debug(f"output_index={output_index} does start with INDEX_PREFIX")
				index = output_index
				assert index in polars_df.columns
				self.logging.debug(f"Renamed index {index} to {output_index}")

		assert index in polars_df.columns

		if drop_unwanted_columns:
			polars_df = self.drop_known_unwanted_columns(polars_df)
			assert index in polars_df.columns
		if drop_non_mycobact_columns:
			polars_df = self.drop_non_tb_columns(polars_df)
			assert index in polars_df.columns
		if nullify:
			polars_df = self.drop_null_columns(self.nullify(polars_df))
			# check we didn't mess with the index, which can happen with null stuff
			if check_index and index is not None:
				assert index in polars_df.columns
				assert self.get_null_count_in_column(polars_df, index) == 0
			elif check_index:
				assert self.get_null_count_in_column(polars_df, self.guess_index_column(polars_df)) == 0
		if flatten:
			polars_df = self.flatten_all_list_cols_as_much_as_possible(polars_df, force_strings=False, skip_taxoncore_entirely=True) # this makes merging better for "geo_loc_name_sam"
		if disallow_right:
			assert len([col for col in polars_df.columns if col.endswith("_right")]) == 0, "Found columns with _right in their name, indicating a merge failure"
		if self.cfg.paired_illumina_only:
			polars_df = self.get_paired_illumina(polars_df)

		# check date columns, our arch-nemesis
		for column in polars_df.columns:
			if column in kolumns.equivalence['date_collected']:
				if polars_df[column].dtype is not pl.Date:
					self.logging.warning(f"Found likely date column {column}, but it has type {polars_df[column].dtype} (will take no action)")
				else:
					self.logging.debug(f"Likely date column {column} has pl.Date type")

		if not norename:
			for key, value in kolumns.equivalence.items():
				merge_these_columns = [v_col for v_col in value if v_col in polars_df.columns and v_col not in sum(kolumns.special_taxonomic_handling.values(), []) and v_col != index]
				self.logging.debug(f"values associated with {key} that appear in dataframe: {merge_these_columns}")
				if len(merge_these_columns) > 0:
					self.logging.debug(f"Discovered {key} in column via:")
					for some_column in merge_these_columns:
						self.logging.debug(f"  * {some_column}: {polars_df.schema[some_column]}")

					if len(merge_these_columns) > 1:
						#polars_df = polars_df.with_columns(pl.implode(merge_these_columns)) # this gets sigkilled; don't bother!
						if key in drop_zone.silly_columns:
							polars_df = polars_df.drop(col)
							self.logging.info(f"{key} will be dropped thanks to being in drop_zone.silly_columns")
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
		if check_index:
			self.logging.debug(f"Checking {df_name}'s index one last time...")
			polars_df = self.check_index(polars_df, manual_index_column=index)
		else:
			assert index in polars_df.columns
		self.logging.debug(f"{df_name}'s shape after rancheroizing: {polars_df.shape[0]}x{polars_df.shape[1]}")
		self.logging.debug(f"{df_name} has these columns after rancheroizing: {polars_df.columns}")

		return polars_df

	@staticmethod
	def sort_list_str_col(polars_df: pl.DataFrame, col: str, safe=True) -> pl.DataFrame:
		"""
		Sort lists of strings in a List(Utf8) column alphabetically.
		"""
		if safe:
			return polars_df.with_columns(pl.col(col).list.eval(
				pl.element()
			)
			.map_elements(
				lambda lst: sorted(lst, key=lambda x: (x is not None, x)),
				return_dtype=pl.List(pl.Utf8)
			)
			.alias(col)
		)
		else: # way faster but might explode
			return polars_df.with_columns(pl.col(col).list.sort().alias(col))

	def is_sample_indexed(self, polars_df):
		index = self.guess_index_column(polars_df)
		return True if index in kolumns.equivalence['sample_id'] else False

	def is_run_indexed(self, polars_df):
		index = self.guess_index_column(polars_df)
		return True if index in kolumns.equivalence['run_id'] else False

	def add_list_len_col(self, polars_df, list_col, new_col):
		return polars_df.with_columns(pl.col(list_col).list.len().alias(new_col))

	def coerce_to_not_list_if_possible(self, polars_df, column, prefix_arrow=False):
		# TODO: I don't like this prefix arrow thing
		# TODO: should this try to look for uniques? --> that's moreso flatten_list_col_as_set()... really gotta make better f(x) names
		arrow = '-->' if prefix_arrow else ''
		if self.get_index_subname(polars_df) is not None:
			assert column != self.get_index_subname(polars_df)
		
		if polars_df.schema[column] == pl.List:
			polars_df = self.null_list_of_len_zero(polars_df, column)
			if len(self.get_rows_where_list_col_more_than_one_value(polars_df, column)) == 0:
				if self.get_null_count_in_column(polars_df, column) == polars_df.shape[0]:
					self.logging.error(f"{column} seems to already be full of nulls?")
					self.dfprint(polars_df.select(column), rows=20, loglevel=40)
					exit(1)
				self.logging.debug(f"{arrow}Can delist {column} (and it's not currently full of nulls)")
				self.dfprint(polars_df.select(column), rows=200, loglevel=10)
				polars_df = polars_df.with_columns(pl.col(column).list.first().alias(column))
				if self.get_null_count_in_column(polars_df, column) == polars_df.shape[0]:
					self.logging.error(f"Oops, we converted everything in {column} to null")
					self.dfprint(polars_df.select(column), rows=20, loglevel=40)
					exit(1)
				return polars_df
			else:
				if self.logging.getEffectiveLevel() == 10:
					debug_print = self.get_rows_where_list_col_more_than_one_value(polars_df, column)
					self.logging.debug(f"{arrow}{len(debug_print)} multi-element lists in {column}")
					self.dfprint(debug_print.select(self.get_index(debug_print), column), loglevel=10)
				return polars_df
		else:
			self.logging.debug(f"Tried to coerce {column} into a non-list, but it's already a non-list")
			return polars_df

	def flatten_list_col_as_set(self, polars_df, column):
		self.logging.debug(f"Calling flatten_one_nested_list_col(polars_df, {column}) (recursive)")
		polars_df = self.flatten_one_nested_list_col(polars_df, column) # recursive
		self.logging.debug(f"Keeping only unique values in {column}")
		polars_df = polars_df.with_columns(pl.col(column).list.unique().alias(f"{column}"))
		self.logging.debug(f"Attempting to coerce {column} into non-list type (this will null lists of len zero but won't run full nullify)")
		polars_df = self.coerce_to_not_list_if_possible(polars_df, column)
		if polars_df.schema[column] != pl.List:
			self.logging.debug(f"Successfully converted {column} into a non-list")
		else:
			self.logging.debug(f"Couldn't convert {column} into a non-list")
		return polars_df

	def uniq_flat(self, polars_df, column, keep_list=False, err_on_list=False):
		"""Basically a cleaner way to call those various flatten tasks for lists"""
		if polars_df.schema[column] != pl.List:
			# TODO: check if Object or whatever that other weird polars type is
			self.logging.debug("Column isn't a list, returning without any changes")
			return polars_df
		self.logging.debug(f"{column} is a list")
		polars_df = self.flatten_list_col_as_set(polars_df, column) # drops any duplicates
		if not keep_list:
			polars_df = self.coerce_to_not_list_if_possible(polars_df, column)
			if polars_df.schema[column] != pl.List:
				self.logging.debug(f"{column} was a list, but after filtering by unique values, it's not a list anymore")
			else:
				assert len(polars_df.filter(pl.col(column).list.len() > 1)) != 0 # if this ever fires it means flatten_list_col_as_set() is fecked
				self.logging.debug(f"{column} seems to still have some conflicts even after flatten_list_col_as_set() and coerce_to_not_list_if_possible()")
				self.dfprint(polars_df.filter(pl.col(column).list.len() > 1).select([self.get_index(polars_df, guess=True), column]), loglevel=10)
				if err_on_list:
					self.logging.error(f"Failed to convert {column} out of a list type as there's at least one row with two or more unique values")
					self.logging.error("Hint: Run this function with err_on_list=False if you want to return the dataframe without an error.")
					self.logging.error("Hint: Force this list into a string with brackets using encode_as_str().")
					raise TypeError
		return polars_df


	def handle_inconsistent_taxoncore_TB(self,
		polars_df,
		index=None,
		clade_col='clade',
		organism_col='organism',
		lineage_col='lineage',
		strain_col='strain',
		force_strings=True,
		#propagate_up='always', # ['always', 'conflict', 'never']
		):
		if index is None:
			index = self.get_index(polars_df, guess=True)
		assert {"clade_conflict", "organism_conflict", "lineage_conflict", "strain_conflict"}.isdisjoint(set(polars_df.columns))
		assert {clade_col, organism_col, lineage_col, strain_col}.issubset(set(polars_df.columns))

		# The general approach here is start from most specific (strain) and check to make sure everything above that is cool, eventually
		# going up the ranks to clade.
		# Is everything at this level in agreement?
		# --> Yes: Go up a level
		# --> No: Is the disagreement meaningful? (ie, is it between two mutually exclusive categories)
		#     --> Yes: Null this and everything BELOW it
		# TODO--> No: Is the level below specific enough that we can decide?
		#         --> Yes: Cool, do that
		#         --> No: Choose the least specific one, do not touch levels above and below

		# Strain
		if polars_df.schema[strain_col] == pl.List:
			polars_df = self.uniq_flat(polars_df, column=strain_col, keep_list=False, err_on_list=True)

		# Lineage
		if polars_df.schema[lineage_col] == pl.List:
			polars_df = self.uniq_flat(polars_df, column=lineage_col, keep_list=False, err_on_list=False)
		if polars_df.schema[lineage_col] == pl.List:
			# TODO: although it would be cringe, it might actually be faster to use otherwise-chaining 
			# to put all this into one expression (we probably can't do with without otherwise-chaining
			# without a really painful merge afterwards).
			polars_df = polars_df.with_columns(
				pl.when((pl.col(lineage_col).list.eval(pl.element().str.starts_with('L1')).list.all()))
				.then(pl.lit(["L1"])).otherwise(pl.col(lineage_col)).alias(lineage_col))
			polars_df = polars_df.with_columns(
				pl.when((pl.col(lineage_col).list.eval(pl.element().str.starts_with('L2')).list.all()))
				.then(pl.lit(["L2"])).otherwise(pl.col(lineage_col)).alias(lineage_col))
			polars_df = polars_df.with_columns(
				pl.when((pl.col(lineage_col).list.eval(pl.element().str.starts_with('L3')).list.all()))
				.then(pl.lit(["L3"])).otherwise(pl.col(lineage_col)).alias(lineage_col))
			polars_df = polars_df.with_columns(
				pl.when((pl.col(lineage_col).list.eval(pl.element().str.starts_with('L4')).list.all()))
				.then(pl.lit(["L4"])).otherwise(pl.col(lineage_col)).alias(lineage_col))
			polars_df = polars_df.with_columns(
				pl.when((pl.col(lineage_col).list.eval(pl.element().str.starts_with('L5')).list.all()))
				.then(pl.lit(["L5"])).otherwise(pl.col(lineage_col)).alias(lineage_col))
			polars_df = polars_df.with_columns(
				pl.when((pl.col(lineage_col).list.eval(pl.element().str.starts_with('L6')).list.all()))
				.then(pl.lit(["L6"])).otherwise(pl.col(lineage_col)).alias(lineage_col))
			polars_df = self.uniq_flat(polars_df, column=lineage_col, keep_list=False, err_on_list=False)

				#pl.when((pl.col('lineage').list.eval(pl.element().str.starts_with('L1')).list.all()))
				#.then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade')),

		# Organism
		#if propagate_up == 'always':
		#	# fill organism based on lineage
		#	pass
		if polars_df.schema[organism_col] == pl.List:
			polars_df = self.uniq_flat(polars_df, column=organism_col, keep_list=False, err_on_list=False)
		#if polars_df.schema[organism_col] == pl.List and propagate_up == 'conflict':
		#	# fill organism based on lineage
		#	pass
				

		# Clade
		#if propagate_up == 'always':
		#	# fill clade based on organism
		#	pass
		if polars_df.schema[clade_col] == pl.List:
			polars_df = self.uniq_flat(polars_df, column=clade_col, keep_list=False, err_on_list=False)

		#if polars_df.schema[clade_col] == pl.List and propagate_up == 'conflict':
		#	# fill clade based on lineage... except actually this should be AFTER clade conflict, i think,
		#	# because clade has some mutually exclusive stuff... so does organism i guess....
		#	pass

		if polars_df.schema[clade_col] == pl.List:
			polars_df = polars_df.with_columns(
				pl.when(
					(pl.col(clade_col).list.len() > 1)
					.and_(
						(
							(pl.col(clade_col).list.contains("tuberculosis: human-adapted"))
							.and_(pl.col(clade_col).list.contains("tuberculosis: animal-adapted"))
						)
						.or_(
							(pl.col(clade_col).list.contains("NTM: abscessus complex"))
							.and_(pl.col(clade_col).list.contains("NTM: avium complex"))
						)
						.or_(
								(
									(pl.col(clade_col).list.contains("NTM: unclassified"))
									.or_(pl.col(clade_col).list.contains("NTM: avium complex"))
									.or_(pl.col(clade_col).list.contains("NTM: abscessus complex"))
								)
								.and_(
									(pl.col(clade_col).list.contains("tuberculosis: unclassified"))
									.or_(pl.col(clade_col).list.contains("tuberculosis: human-adapted"))
									.or_(pl.col(clade_col).list.contains("tuberculosis: animal-adapted"))
								)
						)
						# TODO: there should also be handling for leprosy, mycolicibacterium, etc but those ones usually don't show up
						# and may not be worth really the computation time? hmmm

					)
				)
				.then(True).otherwise(False).alias("clade_conflict")
			)
			clade_conflicts = polars_df.filter(pl.col("clade_conflict") == True)
			if len(clade_conflicts) > 0:
				self.logging.debug(f"[clade] found {len(clade_conflicts)} rows where {clade_col} had conflicts; will null them + all levels below")
				self.dfprint(clade_conflicts.select([index, clade_col, organism_col, lineage_col, strain_col]))
				polars_df.with_columns([
					pl.when(pl.col("clade_conflict") == True).then(None).otherwise(pl.col(clade_col)).alias(clade_col),
					pl.when(pl.col("clade_conflict") == True).then(None).otherwise(pl.col(organism_col)).alias(organism_col),
					pl.when(pl.col("clade_conflict") == True).then(None).otherwise(pl.col(lineage_col)).alias(lineage_col),
					pl.when(pl.col("clade_conflict") == True).then(None).otherwise(pl.col(strain_column)).alias(strain_column)
				])

				if len(polars_df.filter(pl.col(clade_col).list.len() > 1)) == 0:
					self.logging.debug(f"[clade] {clade_col} now only has 0 or 1 values, can flatten")
					polars_df = self.coerce_to_not_list_if_possible(polars_df, clade_col)
					assert polars_df.schema[clade_col] != pl.List
				else:
					self.logging.debug("[clade] we still have conflicts but they likely don't matter")
					still_clade_conflicts = polars_df.filter(pl.col(clade_col).list.len() > 1)
					self.dfprint(still_clade_conflicts.select([index, clade_col, organism_col, lineage_col, strain_col]))
					if force_strings:
						self.logging.debug(f'[clade] because force_strings, remaining conflicts will turn {clade_col} into "unclassified mycobacteria"')
						polars_df = polars_df.with_columns(
							pl.when(pl.col(clade_col).list.len() > 1)
							.then(pl.lit("unclassified mycobacteria"))
							.otherwise(pl.col(clade_col))
							.alias(clade_col)
						)
						polars_df = self.coerce_to_not_list_if_possible(polars_df, clade_col)
						assert polars_df.schema[clade_col] != pl.List
			else:
				self.logging.debug(f"[clade] did not find any meaningful conflicts in {clade_col}")
				still_clade_conflicts = polars_df.filter(pl.col(clade_col).list.len() > 1)
				self.dfprint(still_clade_conflicts.select([index, clade_col, organism_col, lineage_col, strain_col]))
				if force_strings:
					self.logging.debug(f'[clade] because force_strings, remaining conflicts will turn {clade_col} into "unclassified mycobacteria"')
					polars_df = polars_df.with_columns(
						pl.when(pl.col(clade_col).list.len() > 1)
						.then(pl.lit(["unclassified mycobacteria"]))
						.otherwise(pl.col(clade_col))
						.alias(clade_col)
					)
					polars_df = self.coerce_to_not_list_if_possible(polars_df, clade_col)
					assert polars_df.schema[clade_col] != pl.List

			polars_df = polars_df.drop(['clade_conflict']) # TODO: make this drop optional?
			self.logging.debug(f"[clade] Finished dealing with {clade_col}")
		
		# TODO: At the end, optionally verify everything (ie, if lineage is L1, clade is tuberculosis: human-adapted, etc --> maybe better fit in Standardizer taxoncore zone?
		return polars_df
		

	def flatten_all_list_cols_as_much_as_possible(self, 
		polars_df, 
		hard_stop=False, 
		force_strings=False, 
		just_these_columns=None,
		force_index=None, 
		new_taxoncore_handling=True, 
		skip_taxoncore_entirely=False):
		"""
		Flatten list columns as much as possible. If a column is just a bunch of one-element lists, for
		instance, then just take the 0th value of that list and make a column that isn't a list.

		If force_strings, any remaining columns that are still lists are forced into strings.
		"""
		# Do not run check index first, as it will break when this is run right after run-to-sample conversion
		taxoncore_flag = False
		if force_index is None:
			index_column = self.get_index(polars_df)
			if index_column is None:
				raise ValueError("Dataframe doesn't have an index column. Set it using NeighLib.mark_index().")
		else:
			index_column = force_index

		null_counts_before = polars_df.filter(pl.col(col).null_count() > 0 for col in polars_df.columns)
		if null_counts_before.shape[0] == 0:
			self.logging.debug("Dataframe already seems to have no nulls")
		else:
			self.logging.debug("Dataframe has some nulls:")
			self.dfprint(null_counts_before, loglevel=10)

		self.logging.debug("Recursively unnesting lists...")
		polars_df = self.flatten_nested_list_cols(polars_df)
		polars_df = self.check_index(polars_df, manual_index_column=index_column) # already run in flatten_nested_list_cols?!
		self.logging.debug("Unnested all list columns. Index seems okay.")

		null_counts_after = polars_df.filter(pl.col(col).null_count() > 0 for col in polars_df.columns)
		if null_counts_after.shape[0] == 0:
			self.logging.debug("After recursively unnesting lists, dataframe seems to have no nulls")
		else:
			self.logging.debug("After recursively unnesting lists, dataframe has some nulls")
			self.dfprint(null_counts_after, loglevel=10)

		what_was_done = []

		if just_these_columns is None:
			col_dtype = dict(polars_df.schema)
			if new_taxoncore_handling:
				for taxoncore_column in kolumns.special_taxonomic_handling:
					if taxoncore_column in col_dtype:
						taxoncore_flag = True
						del col_dtype[taxoncore_column]
		else:
			col_dtype = dict()
			for col in just_these_columns:
				assert col in polars_df
				dtype = polars_df.schema[col]
				col_dtype[col] = dtype

		if new_taxoncore_handling and taxoncore_flag and not skip_taxoncore_entirely and {'clade', 'oragnism', 'lineage', 'strain'}.issubset(set(polars_df.columns)):
			polars_df = self.handle_inconsistent_taxoncore_TB(polars_df, index=index_column)
		
		for col, datatype in col_dtype.items(): # TYPES DO NOT UPDATE AUTOMATICALLY!

			self.logging.debug(f"->col {col} has stored datatype {datatype}, current datatype {polars_df.schema[col]}")
			
			if col in drop_zone.silly_columns:
				polars_df = polars_df.drop(col)
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': pl.Null, 'result': 'dropped'})
				continue
			
			if datatype == pl.List and datatype.inner != datetime.datetime and not self.is_nested_list_dtype(polars_df.schema[col]):

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

				if col in kolumns.equivalence['run_id'] and index_column in kolumns.equivalence['sample_id']: # TODO: replace with issampleindex()? what about when index is specificed as something else
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (runs in samp-indexed df)'})
					continue
				
				elif polars_df[col].drop_nulls().shape[0] == 0:
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (empty/nulls)'})
					continue

				elif col in kolumns.special_taxonomic_handling and not skip_taxoncore_entirely:
					assert new_taxoncore_handling is False
					
					# First attempt to flatten ALL taxoncore stuff (yes, this will get repeated per col in kolumns.special_taxonomic_handling, too bad)
					for kolumn in kolumns.special_taxonomic_handling:
						if kolumn in polars_df.columns and polars_df.schema[kolumn] == pl.List:
							polars_df = polars_df.with_columns(pl.col(kolumn).list.unique())
							dataframe_height = polars_df.shape[1]
							polars_df = self.drop_nulls_from_possible_list_column(polars_df, kolumn)
							current_dataframe_height = polars_df.shape[1]
							assert dataframe_height == current_dataframe_height
							polars_df = self.coerce_to_not_list_if_possible(polars_df, kolumn, prefix_arrow=True)
					
					if polars_df.schema[col] == pl.List: # since it might not be after coerce_to_not_list_if_possible()
						long_boi = polars_df.filter(pl.col(col).list.len() > 1).select([index_column, 'clade', 'organism', 'lineage', 'strain'])
						#long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(['sample_id', 'clade', 'organism', 'lineage']) # TODO: BAD WORKAROUND
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
								long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(self.valid_cols(long_boi, [index_column, 'clade', 'organism', 'lineage', 'strain']))
								self.logging.debug(f"Non-1 {col} values after attempting to de-long them")
								self.dfprint(long_boi, loglevel=10)
							polars_df = self.coerce_to_not_list_if_possible(polars_df, col, prefix_arrow=True)
						else:
							self.logging.debug(f"Taxoncore column {col} will not be adjusted further")
				
				elif col in kolumns.list_to_float_sum:
					# TODO: use logger adaptors instead of this print cringe
					print(f"{col}\n-->[kolumns.list_to_float_sum]") if self.logging.getEffectiveLevel() == 10 else None
					if datatype.inner == pl.String:
						print(f"-->Inner type is string, casting to pl.Int64 first") if self.logging.getEffectiveLevel() == 10 else None
						polars_df = polars_df.with_columns(
							pl.col(col).list.eval(
								pl.when(pl.element().is_not_null())
								.then(pl.element().cast(pl.Int64))
								.otherwise(None)
							).alias(f"{col}_int64")
						)
						polars_df = polars_df.with_columns(pl.col(f"{col}_int64").list.sum().alias(f"{col}_sum"))
						polars_df = polars_df.drop(f"{col}_int64")
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
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'null conflicts'})
					continue
				
				elif col in kolumns.list_to_set_uniq: 
					print(f"{col}\n-->[kolumns.list_to_set_uniq]") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					print("-->Used uniq, now trying to delist") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, prefix_arrow=True)
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
							print_cols = self.valid_cols(bad_ones, ['sample_id', 'run_id', col, 'continent' if col != 'continent' else 'country'])
							self.super_print_pl(bad_ones.select(print_cols), "Conflicts")
						polars_df = self.coerce_to_not_list_if_possible(polars_df, col, prefix_arrow=True)
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
						# Previously we would use polars_df.select(pl.count(col)).item() to get the number of non-nulls, then after the polars expression
						# that selects the first value, we'd then compare against that older non-nulls value with the current non-null value. Not sure
						# why, because currently polars_df.select(pl.count(col)).item() counts empty lists as non-null.
						# ...but should we even be getting empty lists at all here?
						# DON'T DO THIS:
						#polars_df = polars_df.with_columns(
						#	pl.when(pl.col(col).list.len() <= 1).then(pl.col(col).first()).otherwise(None).alias(col)
						#)
						polars_df = self.coerce_to_not_list_if_possible(polars_df, col, prefix_arrow=True) # this will also null lists of len zero
						self.logging.debug(f"Column ran through coerce, is now type {polars_df.schema[col]}")
				else:
					# list.unique() does not work on nested lists so you better hope you removed them earlier!
					self.logging.warning(f"{col} (type {type(polars_df.schema[col])})-->Not sure how to handle, will treat it as a set")
					assert polars_df.schema[col] == pl.List
					polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, prefix_arrow=True)
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

		# TODO: REPLACE WITH POLARS RSTRIP, WHICH ALREADY STRIPS ALL WHITESPACE


		return polars_df.with_columns([
			pl.when(pl.col(column).str.ends_with(" "))
			.then(pl.col(column).str.slice(-1))
			.otherwise(pl.col(column))
			.alias(column)
		])
	
	def recursive_rstrip(self, polars_df, column, strip_char=" "):

		# TODO: REPLACE WITH POLARS RSTRIP, WHICH ALREADY STRIPS ALL WHITESPACE

		while polars_df[column].str.ends_with(" ").any():
			self.logging.info("Recursing...")
			polars_df = self.rstrip(polars_df, column, strip_char)
		return polars_df

	def drop_non_tb_columns(self, polars_df):
		dont_drop_these = [col for col in polars_df.columns if col not in drop_zone.clearly_not_tuberculosis]
		return polars_df.select(dont_drop_these)

	def drop_known_unwanted_columns(self, polars_df):
		return polars_df.select([col for col in polars_df.columns if col not in drop_zone.silly_columns])

	def drop_low_cardinality_cols(self, polars_df, minimum=3, index=None):
		"""
		Drop columns that have less than cutoff unique elements.
		Ex: If polars_df has 300 rows and col "librarysource" is "GENOMIC" across all 300 of them,
		genomic would be dropped if cutoff > 0.
		"""
		dropped = []
		starting_columns = polars_df.shape[1]
		if index is None:
			index = self.get_index(polars_df, guess=True)
		for column in polars_df.columns:
			if column == index:
				continue
			counts = polars_df.select([pl.col(column).value_counts(sort=True)])
			if len(counts) <= minimum:
				dropped.append(column)
				polars_df = polars_df.drop(column)
		self.logging.info(f"Removed {starting_columns - polars_df.shape[1]} columns with less than {cutoff} unique values")
		self.logging.debug(f"Dropped columns: {dropped}")
		return polars_df

	def drop_mostly_null_cols(self, polars_df, minimum_count=0, minimum_pct=None, index=None):
		"""
		Drop columns that have less than minimum_count non-null values. If minimum_pct is not None, it will also be applied
		as a minimum percentage (as float 0 - 1) of the column is non-null.

		Examples on polars_df of 100 rows where col "organism" is null for 90 of those columns:
		minimum_count=0, minimum_pct=None --> kept
		minimum_count=40, minimum_pct=None --> kept
		minimum_count=90, minimum_pct=None --> kept
		minimum_count=95, minimum_pct=None --> dropped
		minimum_count=0, minimum_pct=0.9 --> kept
		minimum_count=91, minimum_pct=0.9 --> dropped
		"""
		dropped = []
		total_rows, starting_columns = polars_df.shape[0], polars_df.shape[1]
		if index is None:
			index = self.get_index(polars_df, guess=True)
		if minimum_count == 0 and minimum_pct is None:
			self.logging.warning("Minimum value of non-nulls set to zero, no minimum_pct set. Returning unchanged dataframe.")
			return polars_df
		elif minimum_count == 0: # and minimum_pct is not None
			if minimum_pct > 1 or minimum_pct < 0:
				self.logging.error("minimum_pct should be a float between 0 and 1 (or None)")
				raise ValueError
		
		for column in polars_df.columns:
			if column == index:
				continue
			null_counts = self.get_null_count_in_column(polars_df, column, warn=False, error=False)
			non_null_counts = total_rows - null_counts
			
			if non_null_counts <= minimum_count:
				dropped.append(column)
				polars_df = polars_df.drop(column)
				continue

			if minimum_pct is not None:
				if non_null_counts / total_rows <= minimum_pct:
					dropped.append(column)
					polars_df = polars_df.drop(column)
		
		self.logging.info(f"Removed {starting_columns - polars_df.shape[1]} columns")
		self.logging.debug(f"Dropped columns: {dropped}")
		return polars_df

	@staticmethod
	def list_nesting_depth(dtype: pl.DataType):
		depth, cur = 0, dtype
		while isinstance(cur, pl.List):
			depth += 1
			cur = cur.inner
		return depth

	def is_nested_list_dtype(self, dtype: pl.DataType) -> bool:
		return self.list_nesting_depth(dtype) >= 2

	def get_nested_list_cols(self, polars_df: pl.DataFrame) -> list[str]:
		#return [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.List)]
		nested_list_columns = [name for name, datatype in polars_df.schema.items() if self.is_nested_list_dtype(datatype)]
		self.logging.debug(f"Found {len(nested_list_columns)} nested list columns")
		return nested_list_columns

	def flatten_one_nested_list_col(self, polars_df, column):
		polars_df = self.drop_nulls_from_possible_list_column(polars_df, column)
		if self.is_nested_list_dtype(polars_df.schema[column]):
			i = 1
			while i < self.list_nesting_depth(polars_df.schema[column]):
				#polars_df = polars_df.with_columns(pl.col(column).list.eval(pl.element().flatten())) # might leave hanging nulls (does earlier drop_nulls fix this though?)
				#polars_df = polars_df.with_columns(pl.col(column).flatten().list.drop_nulls()) # polars.exceptions.ShapeError: unable to add a column of length x to a Dataframe of height y
				polars_df = polars_df.with_columns(pl.col(column).list.eval(pl.element().flatten().drop_nulls()))
				i+=1
		assert not self.is_nested_list_dtype(polars_df.select(column).schema)
		return polars_df

	def flatten_nested_list_cols(self, polars_df):
		"""There are other ways to do this, but this one doesn't break the schema, so we're sticking with it"""
		nested_lists = self.get_nested_list_cols(polars_df)
		for col in nested_lists:
			self.logging.debug(f"Unnesting {col}")
			polars_df = self.drop_nulls_from_possible_list_column(polars_df, col)
			polars_df = self.flatten_one_nested_list_col(polars_df, col) # this is already recursive
		return polars_df

	def cast_to_string(self, polars_df, column, strip_dquotes=True, null_empty_strs=True):
		"""
		''Cast'' a list column into a string. Unlike encode_as_str(), brackets will not be added, nor will elements besides
		the first (0th) member of a list be perserved (unless that member is a null, because we drop nulls from lists first)

		* [] --> null
		* [null] --> null
		* [""] --> null, unless !null_empty_strs
		* ["bizz"] --> "bizz"
		* ["foo", "bar"] --> "foo"
		* ["\"buzz\""] --> "buzz" (extra "" removed)

		This is mostly useful for dealing with Terra data tables, where it's not uncommon to get one-element lists
		"""
		start_shape = polars_df.shape
		polars_df = polars_df.with_columns(
			pl.col(column)
			.list.drop_nulls()
			.list.first()                 # take the 0th element, or null if list empty
			.str.strip_chars('"')         # strip any rouge dquotes
			.cast(pl.Utf8)                # cast to string
			.alias(column)
		)

		# remove empty strings (or not)
		if null_empty_strs:
			polars_df = polars_df.with_columns(pl.when(pl.col(column) == pl.lit("")).then(None).otherwise(pl.col(column)).alias(column))
		assert polars_df.shape == start_shape
		return polars_df

	def encode_as_str(self, polars_df, column, L_bracket='[', R_bracket=']', list_bracket_style=_DEFAULT_TO_CONFIGURATION):
		""" Unnests list/object data (but not the way explode() does it) so it can be writen to CSV format
		Originally based on deanm0000's code, via https://github.com/pola-rs/polars/issues/17966#issuecomment-2262903178

		LIMITATIONS: This may not work as expected on pl.List(pl.Null). You may also see oddities on some pl.Object types.
		"""
		list_bracket_style = self._default_fallback('list_bracket_style', list_bracket_style)
		self.logging.debug(f"Forcing column {column} into a string")
		assert column in polars_df.columns # throws an error because it's a series now?
		datatype = polars_df.schema[column]

		if datatype == pl.List(pl.String):

			# No, we can't json_encode() here, at least I couldn't get that to work...

			if list_bracket_style == 'always':
				polars_df = polars_df.with_columns(
					#pl.when(pl.col(column).list.drop_nulls().list.first().is_not_null())
					pl.when(pl.col(column).is_not_null())
					.then(
						pl.lit(L_bracket)
						#+ pl.col(column).list.eval(pl.lit("'") + pl.element().cast(pl.Utf8).str.replace_all("'", r"\'") + pl.lit("'")).list.join(", ")
						+ pl.col(column).list.eval(pl.lit('"') + pl.element() + pl.lit('"')).list.join(",")
						+ pl.lit(R_bracket)
					)
					.otherwise(None)
					.alias(column)
				)
			elif list_bracket_style == 'len_gt_one':
				# This may be less compatiable with other TSV readers
				polars_df = polars_df.with_columns(
					pl.when(pl.col(column).list.len() <= 1) # don't add brackets if longest list is 1 or 0 elements
					.then(pl.col(column).list.eval(pl.element()).list.join(""))
					.otherwise(
						pl.lit(L_bracket)
						+ pl.col(column).list.eval(pl.lit("'") + pl.element() + pl.lit("'")).list.join(",")
						+ pl.lit(R_bracket)
					)
					.alias(column)
				)
			else:
				raise TypeError(f"Unrecognized argument for list_bracket_style: {list_bracket_style}")
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

		elif datatype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
			raise TypeError(f"Datatype is {datatype} but encode_as_str() doesn't work on that type yet")
		else:
			raise ValueError(f"Tried to make {column} into a string column, but we don't know what to do with type {datatype}")

	def stringify_all_list_columns(self, polars_df):
		self.logging.debug(f"Forcing ALL list columns into strings")
		for col, datatype in polars_df.schema.items():
			if datatype == pl.List or datatype == pl.Object:
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

	def polars_to_tsv(self, polars_df, path: str, null_value='', quote_style=_DEFAULT_TO_CONFIGURATION):
		quote_style = self._default_fallback("quote_style", quote_style)
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
			df_to_write.write_csv(path, separator='\t', include_header=True, null_value=null_value, quote_style=quote_style)
			self.logging.info(f"Wrote dataframe to {path}")
		except pl.exceptions.ComputeError:
			self.logging.error("Failed to write to TSV due to ComputeError. This is likely a data type issue.")
			debug = pl.DataFrame({col:  f"Was {dtype1}, now {dtype2}" for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if col in df_to_write.columns and dtype2 != pl.String and dtype2 != pl.List(pl.String)})
			self.super_print_pl(debug, "Potentially problematic that may have caused the TSV write failure:")
			exit(1)

	def col_to_list(self, polars_df, col):
		return pl.Series(polars_df.select(col)).to_list()
	
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


	# Here be dragons...
	def _testcfg_mycobact_is_false(self, via_another_module=None):
		if via_another_module is None:
			print("❌ Not called correctly!")
			exit(1)
		elif not via_another_module:
			assert self.cfg.mycobacterial_mode == False
			print("✅ Successfully updated mycobacterial_mode in NeighLib")
		else:
			assert self.cfg.mycobacterial_mode == False
			print("✅ Successfully updated mycobacterial_mode in NeighLib via another module")

	def _testcfg_logger_is_debug(self, via_another_module=None):
		if via_another_module is None:
			self.logging.debug("❌ Not called correctly!")
			exit(1)
		elif not via_another_module:
			self.logging.debug("✅ Successfully updated loglevel in NeighLib")
		else:
			self.logging.debug("✅ Successfully updated loglevel in NeighLib via another module")
