# general purpose functions

import polars as pl
import datetime
from src.statics import kolumns, drop_zone, null_values
from polars.testing import assert_series_equal
from .config import RancheroConfig

import logging
logging.basicConfig(format='%(levelname)s:%(funcName)s:%(message)s', level=logging.DEBUG)


class NeighLib:

	def __init__(cls, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			cls._actually_set_config(configuration=configuration)

	def _actually_set_config(cls, configuration: RancheroConfig):
		cls.cfg = configuration

	@classmethod
	def nullify(cls, polars_df):
		return polars_df.with_columns(pl.col(pl.Utf8).replace(null_values.null_values_dictionary))

	def print_col_where(polars_df, column="source", equals="Coscolla", cols_of_interest=['acc', 'run_index', 'source', 'literature_lineage', 'Biosample', 'sample_index', 'concat_list', 'coscolla_lineage']+kolumns.equivalence['date_collected']):
		if column not in polars_df.columns:
			logging.warning(f"Tried to print where {column} equals {equals}, but that column isn't in the dataframe")
			return
		
		# I am not adding all the various integer types in polars here. go away. you'll get a try/except block at best.
		elif type(equals) == list and polars_df.schema[column] != pl.List:
			logging.warning(f"Tried to print where {column} equals list {equals}, but that column has type {polars_df.schema[column]}")
			return
		elif type(equals) == str and polars_df.schema[column] != pl.Utf8:
			logging.info("This is a list column and you passed in a string -- I'm assuming you are looking for the string in the list")
			filtah = polars_df.filter(pl.col(column).list.contains(equals))
		else:
			filtah = polars_df.filter(pl.col(column) == equals)
		cols_to_print = [thingy for thingy in cols_of_interest if thingy in polars_df.columns]
		with pl.Config(tbl_cols=-1):
			logging.info(filtah.select(cols_to_print))

	def print_only_where_col_not_null(polars_df, column, possible_index_columns=['acc', 'run_index', 'BioSample', 'sample_index']):
		if column not in polars_df.columns:
			logging.warning(f"Tried to print where {column} is not null, but that column isn't even in the dataframe!")
		else:
			cols_to_print = [thingy for thingy in possible_index_columns if thingy in polars_df.columns].append(column)
			with pl.Config(tbl_cols=-1):
				print(polars_df.filter(pl.col(column).is_not_null()).select(cols_to_print))

	def mark_rows_with_value(polars_df, filter_func, true_value="M. avium complex", false_value='', new_column="bacterial_family", **kwargs):
		#polars_df = polars_df.with_columns(pl.lit("").alias(new_column))
		polars_df = polars_df.with_columns(
			pl.when(pl.col('organism').str.contains_any("Mycobacterium avium"))  # contains should return boolean
			.then(pl.lit(true_value))  # Set true_value where condition is True
			.otherwise(pl.lit(false_value))  # Set false_value otherwise
			.alias(new_column)  # Alias as the new column
		)
		#polars_df.with_columns(pl.when(pl.col("organism").str.contains_any(["Mycobacterium avium", "lalala"])).then(pl.lit(true_value)).alias(new_column))
		print(polars_df.select(pl.col(new_column).value_counts()))

		polars_df = polars_df.with_columns(
			pl.when(pl.col('organism').str.contains("Mycobacterium"))
			.then(pl.lit(true_value))
			.otherwise(pl.lit(false_value))
			.alias(new_column)
		)

		#polars_df = polars_df.with_columns(
		#   pl.when(filter_func(polars_df, **kwargs))
		#   .then(pl.lit(true_value))
		#   .otherwise(pl.lit(false_value))
		#   .alias(new_column)
		#)
		print(polars_df.select(pl.col(new_column).value_counts()))

	def likely_is_run_indexed(polars_df):
		# TODO: make more robust
		singular_runs = (
			("run_index" in polars_df.schema and polars_df.schema["run_index"] == pl.String) or
			("run_accession" in polars_df.schema and polars_df.schema["run_accession"] == pl.String) or
			("acc" in polars_df.schema and polars_df.schema["acc"] == pl.String)
		)
		if singular_runs:
			return True
		else:
			return False

	def print_value_counts(polars_df, skip_ids=True):
		ids = ['sample_index', 'biosample', 'BioSample', 'acc', 'acc_1', 'run_index', 'run_accession'] + kolumns.addl_ids
		for column in polars_df.columns:
			if skip_ids and column not in ids:
				counts = polars_df.select([pl.col(column).value_counts(sort=True)])
				print(counts)
			else:
				continue

	def get_valid_columns_list_from_arbitrary_list(polars_df, desired_columns: list):
		return [col for col in desired_columns if col in polars_df.columns]

	def check_columns_exist(polars_df, column_list: list, err=False, verbose=False):
		missing_columns = [col for col in column_list if col not in polars_df.columns]
		if len(missing_columns) == 0:
			#if cls.cfg.verbose: print("All requested columns exist in dataframe")
			return True
		else:
			#if cls.cfg.verbose: print(f"Missing these columns: {missing_columns}")
			if err: exit(1)
			return False

	def concat_dicts_with_shared_keys(dict_list: list):
		"""
		Takes in a list of dictionaries with literal 'k' and 'v' values and
		flattens them. For instance, this:
		[{'k': 'bases', 'v': '326430182'}, {'k': 'bytes', 'v': '141136776'}]
		becomes:
		{'bases': '326430182', 'bytes': '141136776'}

		This version is aware of primary_search showing up multiple times and will
		keep all values for primary_search.
		"""
		combined_dict = {}
		primary_search = set()
		for d in dict_list:
			if 'k' in d and 'v' in d:
				if d['k'] == 'primary_search':
					primary_search.add(d['v'])
				else:
					combined_dict[d['k']] = d['v']
		combined_dict.update({"primary_search": list(primary_search)}) # convert to a list to avoid the polars column becoming type object
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
	
	@staticmethod
	def super_print_pl(polars_df, header):
		print(f"┏{'━' * len(header)}┓")
		print(f"┃{header}┃")
		print(f"┗{'━' * len(header)}┛")
		with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=500, fmt_table_cell_list_len=10):
			print(polars_df)

	@classmethod
	def sort_out_taxoncore_columns(cls, polars_df):
		"""
		Some columns in polars_df will be in list all_taxoncore_columns. We want to use these taxoncore columns to create three new columns:
		* imputed_organism should be of form "Mycobacterium" plus one more word, with no trailing "subsp." or "variant", if a specific organism can be imputed from a taxoncore column, else null
		* imputed_lineage should be of form "L" followed by a float if a specific lineage can be imputed from a taxoncore column, else null
		* imputed_strain are strings if a specific strain can be imputed from a taxoncore column, else null

		Rules:
		* Any column with value "Mycobacterium tuberculosis H37Rv" sets imputed_organism to "Mycobacterium tuberculosis", imputed_lineage to "L4.8", and imputed_strain to "H37Rv"
		* Any column with value "Mycobacterium variant bovis" sets imputed_organism to "Mycobacterium bovis"
		* Any column with "lineage" followed by numbers sets imputed_lineage to "L" plus the numbers, minus any whitespace (there may be periods between the numbers, keep them)



		"""
		polars_df = polars_df.with_columns(
			pl.when(pl.col(base_col) != pl.col(right_col))
			.then(
				pl.when(pl.col(base_col).str.contains_any)
			)
			.otherwise(pl.col(base_col))
			.alias('temp')
		)
		print(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select([base_col, right_col, 'temp']))
		exit(2)

	#.str.contains_any("Mycobacterium avium")
	@classmethod
	def merge_right_columns(cls, polars_df, fallback_on_left=True, err_on_matching_failure=True):
		"""
		Takes in a polars_df with some number of columns ending in "_right", where each _right column has
		a matching column with the same basename (ie, "foo_right" matches "foo"), and merges each base:right
		pair's columns. The resulting merged columns will inherit the base columns name.

		Generally, we want to avoid creating columns of type list whenever possible.

		If column in kolumns.rancheroize__warn... and fallback_on_left, keep only left value(s)
		If column in kolumns.rancheroize__warn... and !fallback_on_left, keep only right values(s)

		Additional special handling for:
		* organism and lineage (kolumns.organism_and_lineage_combined)
		"""
		right_columns = [col for col in polars_df.columns if col.endswith("_right")]
		for right_col in right_columns:
			nullfilled = False
			logging.debug(f"[{right_columns.index(right_col)}/{len(right_columns)}] Trying to merge {right_col}...")
			base_col = right_col.replace("_right", "")

			if base_col not in polars_df.columns and not err_on_matching_failure:
				logging.warning(f"Found {right_col}, but {base_col} not in dataframe -- will drop that column and continue, but this may break things later")
				polars_df = polars_df.drop(right_col)
				continue
			elif base_col not in polars_df.columns:
				logging.error(f"Found {right_col}, but {base_col} not in dataframe -- this is a sign something broke in an earlier function")
				exit(1)
			else:
				pass # intentional

			logging.debug(f"\n\n{base_col}: {polars_df[base_col].dtype}\n{right_col}: {polars_df[right_col].dtype}")
			
			try:
				polars_df = polars_df.with_columns(pl.col(base_col).fill_null(pl.col(right_col)))
				polars_df = polars_df.with_columns(pl.col(right_col).fill_null(pl.col(base_col)))
				nullfilled = True
			except pl.exceptions.InvalidOperationError:
				logging.debug("Could not nullfill (this isn't an error, nulls will be filled if pl.Ut8 or list[str])")
				nullfilled = False
			
			try:
				# if they are equal after filling in nulls, we don't need to turn anything into a list
				assert_series_equal(polars_df[base_col], polars_df[right_col].alias(base_col))
				polars_df = polars_df.drop(right_col)
				logging.debug(f"All values in {base_col} and {right_col} are the same after an attempted nullfill, so no lists are necessary. Dropped {right_col}.")
			
			except AssertionError:
				# not equal after filling in nulls (or nullfill errored)

				
				if base_col in kolumns.merge__special_taxonomic_handling:
					
					if fallback_on_left:
						logging.info(f"Found conflicting metadata in columns that appear to have organism or lineage metadata, but special handling should have happened earlier. Falling back on {base_col}.")
						polars_df = polars_df.drop(right_col)
					else:
						logging.info(f"Found conflicting metadata in columns that appear to have organism or lineage metadata, but special handling should have happened earlier. Falling back on {right_col}.")
						polars_df = polars_df.drop(base_col).rename({right_col: base_col})
				elif base_col in kolumns.merge__error:
					logging.error("Found conflicting metadata in columns that should not have conflicting metadata!")
					cls.super_print_pl(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select([base_col, right_col]), f"conflicts")
					exit(1)
				elif base_col in kolumns.merge__warn_then_pick_arbitrarily_to_keep_singular:
					if fallback_on_left:
						logging.debug(f"Not all values in {base_col} and {right_col} are the same, but we want to avoid creating lists. Falling back on {base_col}.")
						if {base_col} == 'date_collected':
							logging.debug(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select([base_col, right_col]))
						polars_df = polars_df.drop(right_col)
					else:
						logging.debug(f"Not all values in {base_col} and {right_col} are the same, but we want to avoid creating lists. Falling back on {right_col}.")
						if base_col == 'date_collected':
							logging.warning("We found date conflicts!!!")
							logging.debug(polars_df.filter(pl.col(base_col) != pl.col(right_col)).select([base_col, right_col]))
							exit(1)
						polars_df = polars_df.drop(base_col).rename({right_col: base_col})
				
				elif base_col in kolumns.merge__sum:
					logging.error("TODO NOT IMPLEMENTED")
					exit(1)
				elif base_col in kolumns.merge__drop:
					logging.error("TODO NOT IMPLEMENTED")
					exit(1)
				
				else:
					if nullfilled:
						# previous nullfilled succceeded, and therefore (hopefully) both columns are list[str] or pl.Ut8
						# this is known to work with base_col and right_col are both pl.Ut8, or when both are list[str]

						# TODO: differenciate between merge__make_list and merge__make_set
						
						logging.debug("Columns seem compatiable with concat_list, will try to merge with that")
						polars_df = polars_df.with_columns(
							pl.when(pl.col(base_col) != pl.col(right_col))             # When a row has different values for base_col and right_col,
							.then(pl.concat_list([base_col, right_col]).list.unique()) # make a list of base_col and right_col, but keep only uniq values
							.otherwise(pl.concat_list([base_col]))                     # otherwise, make list of just base_col (doesn't seem to nest if already a list, thankfully)
							.alias(base_col)
						).drop(right_col)
						#if veryverbose: NeighLib.super_print_pl(polars_df.select(base_col), f"after merging to make {base_col} to a list")
						assert polars_df.select(pl.col(base_col)).dtypes == [pl.List]
					else:
						if polars_df[base_col].dtype != pl.List(pl.Utf8):
							logging.debug(f"{base_col} is not a list, but we will make it one")
							polars_df = polars_df.with_columns(pl.col(base_col).cast(pl.List(str)))
						if polars_df[right_col].dtype != pl.List(pl.Utf8):
							logging.debug(f"{right_col} is not a list, but we will make it one")
							polars_df = polars_df.with_columns(pl.col(right_col).cast(pl.List(str)))
						polars_df = polars_df.with_columns(pl.col(right_col).fill_null(pl.col(base_col)).alias(base_col))
						polars_df = polars_df.drop(right_col)
						# TODO: double check we're not dropping what we just nullfilled. check alias!!

			assert base_col in polars_df.columns
			assert right_col not in polars_df.columns

		right_columns = [col for col in polars_df.columns if col.endswith("_right")]
		if len(right_columns) > 0:
			logging.error(f"Failed to remove some _right columns: {right_columns}")
			exit(1)
		# non-unique rows might be dropped here, fyi
		return polars_df

	@classmethod
	def iteratively_merge_these_columns(cls, polars_df, merge_these_columns: list, equivalence_key=None, forbid_index_merge=False):
		"""
		Merges columns named in merged_these_columns.

		When all is said and done, the final merged column will be named equivalene_key's value if not None.
		"""
		assert len(merge_these_columns) > 1
		assert all(col in polars_df.columns for col in merge_these_columns)
		assert all(not col.endswith("_right") for col in polars_df.columns)
		
		left_col, right_col = merge_these_columns[0], merge_these_columns[1]

		logging.debug(f"Intending to merge:\n\t{merge_these_columns}\n\t\tLeft:{left_col}\n\t\tRight:{right_col}")

		logging.debug(f"Merging {left_col} and {right_col} by renaming {right_col} to {left_col}_right")
		polars_df = polars_df.rename({right_col: f"{left_col}_right"})
		polars_df = cls.merge_right_columns(polars_df)

		del merge_these_columns[1] # NOT ZERO!!!

		if len(merge_these_columns) > 1:
			logging.debug(f"merge_these_columns is {merge_these_columns}, which we will pass in to recurse")
			polars_df = cls.iteratively_merge_these_columns(polars_df, merge_these_columns)
		return polars_df.rename({left_col: equivalence_key}) if equivalence_key is not None else polars_df

	@classmethod
	def get_rows_where_list_col_more_than_one_value(cls, polars_df, list_col, force_uniq=False):
		assert polars_df.schema[list_col] == pl.List
		if force_uniq:
			polars_df = polars_df.with_columns(pl.col(list_col).list.unique().alias(f"{list_col}_uniq"))
			return polars_df.filter(pl.col(f"{list_col}_uniq").list.len() > 1)
		else:
			return polars_df.filter(pl.col(list_col).list.len() > 1)
	
	@classmethod
	def rancheroize_polars(cls, polars_df):
		polars_df = cls.drop_known_unwanted_columns(polars_df)
		polars_df = cls.nullify(polars_df)
		polars_df = cls.drop_null_columns(polars_df)

		# check date columns, our arch-nemesis
		for column in polars_df.columns:
			if column in kolumns.equivalence['date_collected']:
				if polars_df[column].dtype is not pl.Date:
					logging.warning(f"Found likely date column {column}, but it has type {polars_df[column].dtype}")
				else:
					logging.debug(f"Likely date column {column} has date type")

		for key, value in kolumns.equivalence.items():
			merge_these_columns = [v_col for v_col in value if v_col in polars_df.columns and v_col not in sum(kolumns.merge__special_taxonomic_handling.values(), [])]
			if len(merge_these_columns) > 0:
				logging.debug(f"Discovered {key} in column via {merge_these_columns}")

				if len(merge_these_columns) > 1:
					#polars_df = polars_df.with_columns(pl.implode(merge_these_columns)) # this gets sigkilled; don't bother!
					if key in kolumns.rts__drop:
						polars_df = polars_df.drop(col)
					#don't add kolumns.rts__list_to_float_via_sum here, that's not what it's made for and it'll cause errors
					else:
						logging.info(f"Merging these columns: {merge_these_columns}")
						polars_df = cls.iteratively_merge_these_columns(polars_df, merge_these_columns, equivalence_key=key)
				else:
					logging.debug(f"Renamed {merge_these_columns[0]} to {key}")
					polars_df = polars_df.rename({merge_these_columns[0]: key})
			
		return polars_df

	@classmethod
	def get_index_column(cls, polars_df):
		sample_indeces = kolumns.equivalence['sample_index']
		sample_matches = [col for col in sample_indeces if col in polars_df.columns]
		run_indeces = kolumns.equivalence['run_index']
		run_matches = [col for col in run_indeces if col in polars_df.columns]

		if len(sample_matches) > 1:
			raise ValueError(f"Tried to find dataframe index, but there's multiple possible sample indeces: {sample_matches}")
	
		if len(sample_matches) == 1:
			if len(run_matches) > 1:
				raise ValueError(f"Tried to find dataframe index, but there's multiple possible run indeces (indicates failed run->sample conversion):  {run_matches}")
			
			if len(run_matches) == 1:
				if isinstance(run_indeces, list):
					return sample_matches[0]
				elif len(set(sample_indeces)) == len(sample_indeces):  # check samples are unique
					return sample_matches[0]
				else:
					return run_matches[0]
			
			return sample_matches[0]  # no run accessions

		if len(run_matches) == 1:
			return run_matches[0]
	
		raise ValueError(f"No valid index column found in polars_df! Columns available: {polars_df.columns}")

	@classmethod
	def flatten_all_list_cols_as_much_as_possible(cls, polars_df, hard_stop=False, force_strings=False):
		"""
		Flatten list columns as much as possible. If a column is just a bunch of one-element lists, for
		instance, then just take the 0th value of that list and make a column that isn't a list.

		If force_strings, any remaining columns that are still lists are forced into strings.
		"""

		# unnest nested lists (recursive)
		polars_df = cls.flatten_nested_list_cols(polars_df)
		number_of_columns = len(polars_df.schema)

		index_column = cls.get_index_column(polars_df)
		for col, datatype in polars_df.schema.items():
			logging.debug(f"Evaulating on {col} of type {datatype}")
			if col in kolumns.rts__drop:
				polars_df.drop(col)
				logging.debug(f"-->Dropped {col} per kolumns rules")
			assert number_of_columns == len(polars_df.schema), f"There should be {number_of_columns} columns, but there's now {len(polars_df.schema)}"
			if datatype == pl.List and datatype.inner != datetime.datetime:
				
				if polars_df[col].drop_nulls().shape[0] == 0:
					logging.warning(f"{col} has datatype {datatype} but seems empty or contains only nulls, skipping...")
					continue

				if col in kolumns.rts__list_to_float_via_sum:
					logging.debug(f"-->Summing {col}")
					if datatype.inner == pl.String:
						polars_df = polars_df.with_columns(
							pl.col(col).list.eval(
								pl.when(pl.element().is_not_null())
								.then(pl.element().cast(pl.Int32))
								.otherwise(pl.lit("null"))
							).alias(f"{col}_sum")
						)
					else:
						polars_df = polars_df.with_columns(pl.col(col).list.sum().alias(f"{col}_sum"))
					polars_df = polars_df.drop(col)
					continue
				elif col in kolumns.rts__keep_as_list:
					logging.debug(f"-->Handling {col} as a list (ie doing nothing)")
					continue
				
				n_rows_prior = polars_df.shape[0]
				exploded = polars_df.explode(col)
				assert col in exploded.columns
				if col != index_column:
					exploded = exploded.select([col, index_column]).drop_nulls().unique()
				else:
					exploded = exploded.select([col]).drop_nulls().unique()
				n_rows_now = exploded.shape[0]


				if col in ['run_date_run', 'pacbio_rs_binding_kit_barcode_exp', 'pacbio_rs_binding_kit_barcode', 'coscolla_lineage', 'coscolla_sublineage']:
					debug_print = cls.get_rows_where_list_col_more_than_one_value(polars_df, col, False)
					cls.super_print_pl(debug_print.select([index_column, col]).head(15), f"polars_df, non-uniq (true len {len(debug_print)})")


				if col not in kolumns.rts__keep_as_list and col not in kolumns.rts__drop and col not in kolumns.rts__list_to_float_via_sum:
					logging.debug(f"-->Treating {col} as a set")
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					# check if this can now be turned into not-a-list
					if len(cls.get_rows_where_list_col_more_than_one_value(polars_df, col, False)) == 0:
						logging.debug(f"-->After removing non-uniques, we can turn {col} into single-value")
						polars_df = polars_df.with_columns(pl.col(col).list.first().alias(col))
					else:
						logging.debug(f"-->After removing non-uniques, we still have {len(cls.get_rows_where_list_col_more_than_one_value(polars_df, col, False))} multi-element lists in {col}")
						debug_print = cls.get_rows_where_list_col_more_than_one_value(polars_df, col, False)
						cls.super_print_pl(debug_print.select([index_column, f"{col}"]).head(10), f"polars_df, after set treatment (true len {len(debug_print)})")
					continue

				if n_rows_now > n_rows_prior:
					# not sure why, but this does not seem to work as expected
					#non_unique_rows = exploded.filter(pl.col(col).is_duplicated()).sort(index_column)
					#logging.debug(f"-->Non-unique values in column {col}: {non_unique_rows}")
					#logging.debug(f"-->Number of non-unique rows in {col}: {non_unique_rows.shape[0]}")
						
					if col in kolumns.rts__list_to_float_via_sum:
						logging.debug(f"-->Summing {col}")
						if datatype.inner == pl.String:
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
					elif col in kolumns.rts__keep_as_set:  # because we exploded with unique(), we now have a set (sort of), but I think this is better than trying to do a column merge
						logging.debug(f"-->Handling {col} as a set")
						polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
					elif col in kolumns.rts__warn_if_list_with_unique_values:
						logging.warning(f"Expected {col} to only have one non-null per sample, but that's not the case. Will keep as a set.")
						
						#filtered_df = polars_df.filter(
						#	pl.col(col).list.eval(pl.element().n_unique()) > 1
						#)
						#NeighLib.super_print_pl(filtered_df.select([index_column, col]), "more than one non-null per sample")
						
						polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
						if hard_stop:
							exit(1)
						else:
							continue
					else:
						logging.warning(f"-->Not sure how to handle {col}, will treat it as a set")
						polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
				else:
					# all list columns are either one element, or effectively one element after dropping nulls
					polars_df = polars_df.with_columns(pl.col(col).list.first().alias(col))
					# this creates a one column df
					#polars_df = polars_df.with_columns(temp_df[col].alias(col))
					# this doesn't error but it's hella slow and I don't trust it
					#unique_values = temp_df[col].to_list()
					#polars_df = polars_df.with_columns(
					#	pl.when(pl.col(col).is_null()).then(None).otherwise(pl.lit(unique_values)).alias(col)
			elif datatype == pl.List and datatype.inner == datetime.date:
				logging.warning(f"{col} is a list of datetimes. Datetimes break typical handling of lists, so this column will be left alone.")
			else:
				logging.debug(f"-->Leaving {col} as-is")
		if force_strings:
			polars_df = cls.stringify_all_list_columns(polars_df)
		return polars_df

	@classmethod
	def drop_non_tb_columns(cls, polars_df):
		dont_drop_these = [col for col in polars_df.columns if col not in drop_zone.clearly_not_tuberculosis]
		return polars_df.select(dont_drop_these)

	@classmethod
	def drop_known_unwanted_columns(cls, polars_df):
		return polars_df.select([col for col in polars_df.columns if col not in drop_zone.silly_columns])

	@staticmethod
	def flatten_nested_list_cols(polars_df):
		"""Flatten nested list columns"""

		# This version seems to breaking the schema:
		#polars_df = polars_df.with_columns(
		#   [pl.col(x).list.eval(pl.lit("'") + pl.element() + pl.lit('"')).list.join(",").alias(x) for x, y in polars_df.schema.items() if isinstance(y, pl.List) and isinstance(y.inner, pl.List)]
		#)

		nested_lists = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.List)]
		for col in nested_lists:
			#new_col = pl.col(col).list.eval(pl.element().cast(pl.Utf8).map_elements(lambda s: f"'{s}'")).alias(f"{col}_flattened")
			#new_col = pl.col(col).list.eval(pl.element().cast(pl.Utf8).map_elements(lambda s: f"'{s}'", return_dtype=str)).list.join(",").alias(f"{col}_flattened")
			polars_df = polars_df.with_columns(pl.col(col).list.eval(pl.element().flatten().drop_nulls()))
		
		# this recursion should, in theory, handle list(list(list(str))) -- but it's not well tested
		remaining_nests = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.List)]
		if len(remaining_nests) != 0:
			polars_df = flatten_nested_list_cols(polars_df)
		return(polars_df)

	@staticmethod
	def stringify_one_list_column(polars_df, column):
		for col, datatype in polars_df.schema.items():
			if col==column and datatype == pl.List(pl.String):
				polars_df = polars_df.with_columns(
					pl.when(pl.col(col).list.len() <= 1) # don't add brackets if longest list is 1 or 0 elements
					.then(pl.col(col).list.eval(pl.element()).list.join(""))
					.otherwise(
						pl.lit("[")
						+ pl.col(col).list.eval(pl.lit("'") + pl.element() + pl.lit("'")).list.join(",")
						+ pl.lit("]")
					).alias(col)
				)
				return polars_df
			
			# pl.Int doesn't exist and pl.List(int) doesn't seem to work, so we'll take the silly route
			elif col==column and (datatype == pl.List(pl.Int8) or datatype == pl.List(pl.Int16) or datatype == pl.List(pl.Int32) or datatype == pl.List(pl.Int64)):
				polars_df = polars_df.with_columns((
					pl.lit("[")
					+ pl.col(col).list.eval(pl.lit("'") + pl.element().cast(pl.String) + pl.lit("'")).list.join(",")
					+ pl.lit("]")
				).alias(col))
				return polars_df
			
			elif col==column and datatype == pl.Object:
				polars_df = polars_df.with_columns((
					pl.col(col).map_elements(lambda s: "{" + ", ".join(f"{item}" for item in sorted(s)) + "}" if isinstance(s, set) else str(s), return_dtype=str)
				).alias(col))
				return polars_df

			elif col==column:
				logging.error(f"Tried to make {col} into a string column, but we don't know what to do with type {datatype}")
				exit(1)

			else:
				continue
		print(f"Could not find {col} in dataframe")
		exit(1)
	

	@staticmethod
	def stringify_all_list_columns(polars_df):
		""" Unnests list/object data (but not the way explode() does it) so it can be writen to CSV format
		Heavily based on deanm0000 code, via https://github.com/pola-rs/polars/issues/17966#issuecomment-2262903178

		LIMITATIONS: This may not work as expected on pl.List(pl.Null). You may also see oddities on some pl.Object types.
		"""
		for col, datatype in polars_df.schema.items():
			if datatype == pl.List(pl.String):
				polars_df = polars_df.with_columns(
					pl.when(pl.col(col).list.len() <= 1) # don't add brackets if longest list is 1 or 0 elements
					.then(pl.col(col).list.eval(pl.element()).list.join(""))
					.otherwise(
						pl.lit("[")
						+ pl.col(col).list.eval(pl.lit("'") + pl.element() + pl.lit("'")).list.join(",")
						+ pl.lit("]")
					).alias(col)
				)

			# pl.Int doesn't exist and pl.List(int) doesn't seem to work, so we'll take the silly route
			elif (datatype == pl.List(pl.Int8) or datatype == pl.List(pl.Int16) or datatype == pl.List(pl.Int32) or datatype == pl.List(pl.Int64)):
				polars_df = polars_df.with_columns((
					pl.lit("[")
					+ pl.col(col).list.eval(pl.lit("'") + pl.element().cast(pl.String) + pl.lit("'")).list.join(",")
					+ pl.lit("]")
				).alias(col))
			
			elif datatype == pl.Object:
				polars_df = polars_df.with_columns((
					pl.col(col).map_elements(lambda s: "{" + ", ".join(f"{item}" for item in sorted(s)) + "}" if isinstance(s, set) else str(s), return_dtype=str)
				).alias(col))

		return polars_df

	@classmethod
	def print_polars_cols_and_dtypes(cls, polars_df):
		[print(f"{col}: {dtype}") for col, dtype in zip(polars_df.columns, polars_df.dtypes)]

	@classmethod
	def drop_null_columns(cls, polars_df):
		""" Drop columns of type null or list(null) """
		import polars.selectors as cs
		polars_df = polars_df.drop(cs.by_dtype(pl.Null))
		polars_df = polars_df.drop(cs.by_dtype(pl.List(pl.Null)))
		return polars_df

	@classmethod
	def polars_to_tsv(cls, polars_df, path: str):
		print("Writing to TSV. Lists and objects will converted to strings, and columns full of nulls will be dropped.")
		df_to_write = cls.drop_null_columns(polars_df)
		columns_with_type_list_or_obj = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if (dtype == pl.List or dtype == pl.Object)]
		if len(columns_with_type_list_or_obj) > 0:
			df_to_write = cls.stringify_all_list_columns(df_to_write)
		try:
			### DEBUG ###
			debug = pl.DataFrame({col: [dtype1, dtype2] for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if dtype2 != pl.String})
			logging.debug(f"Non-string types, and what they converted to: {debug}")
			df_to_write.write_csv(path, separator='\t', include_header=True, null_value='')
			logging.info(f"Wrote dataframe to {path}")
		except pl.exceptions.ComputeError:
			print("WARNING: Failed to write to TSV due to ComputeError. This is likely a data type issue.")
			debug = pl.DataFrame({col:  f"Was {dtype1}, now {dtype2}" for col, dtype1, dtype2 in zip(polars_df.columns, polars_df.dtypes, df_to_write.dtypes) if col in df_to_write.columns and dtype2 != pl.String and dtype2 != pl.List(pl.String)})
			cls.super_print_pl(debug, "Potentially problematic that may have caused the TSV write failure:")
			exit(1)

	def get_dupe_columns_of_two_polars(polars_df_a, polars_df_b, assert_shared_cols_equal=False):
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
	
	def assert_unique_columns(pandas_df):
		"""Assert all columns in a pandas df are unique -- useful if converting to polars """
		if len(pandas_df.columns) != len(set(pandas_df.columns)):
			dupes = []
			not_dupes = set()
			for column in pandas_df.columns:
				if column in not_dupes:
					dupes.append(column)
				else:
					not_dupes.add(column)
			raise AssertionError(f"Pandas df has duplicate columns: {dupes}")
	
	def cast_politely(polars_df):
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
