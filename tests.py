import os
import re
import polars as pl
import datetime
import polars as pl
from polars.testing import assert_series_equal
import polars.selectors as cs
import traceback

verbose = False
pl.Config.set_tbl_rows(15)
pl.Config.set_tbl_cols(15)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_fmt_table_cell_list_len(10)
pl.Config.set_tbl_width_chars(200)

import ranchero as ranchero

### Configuration ###
def hellish_cfg_tests():

	# Do not change the order any of these functions are run in!
	def cfg_default_mycobact():
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True
		print("✅ mycobacterial_mode is True by default")
	def cfg_change_intrafunction():
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True
		ranchero.Configuration.set_config({"mycobacterial_mode": False}) # also sets up interfunction test
		assert ranchero.Configuration.get_config("mycobacterial_mode") == False
		print("✅ cfg.mycobacterial_mode can be changed by set_config")
	def cfg_change_interfunction():
		assert ranchero.Configuration.get_config("mycobacterial_mode") == False
		print("✅ cfg.mycobacterial_mode retains changes across functions")
		ranchero.Configuration.set_config({"mycobacterial_mode": True})
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True # back to default
		print("✅ ...and changes back")
	def cfg_change_NeighLib_intrafunction():
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True
		ranchero.Configuration.set_config({"mycobacterial_mode": False}) # also sets up interfunction test
		ranchero.NeighLib._testcfg_mycobact_is_false(via_another_module=False)
	def cfg_change_NeighLib_interfunction():
		ranchero.NeighLib._testcfg_mycobact_is_false(via_another_module=False)
		print("✅ cfg.mycobacterial_mode in NeighLib context retains changes across functions")
		ranchero.Configuration.set_config({"mycobacterial_mode": True})
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True # back to default
	def cfg_change_NeighLib_intrafunction_via_Standardizer():
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True
		ranchero.Configuration.set_config({"mycobacterial_mode": False})
		ranchero.Standardizer.test_neighlib_cfg_update_mycobact(via_another_module=True)
	def cfg_change_NeighLib_interfunction_via_Standardizer():
		ranchero.NeighLib._testcfg_mycobact_is_false(via_another_module=True)
		print("✅ cfg.mycobacterial_mode in NeighLib context via Standardizer retains changes across functions")
		ranchero.Configuration.set_config({"mycobacterial_mode": True})
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True # back to default
	def cfg_change_Standardizer_intrafunction():
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True
		ranchero.Configuration.set_config({"mycobacterial_mode": False})
		ranchero.Standardizer._testcfg_mycobact_is_false()
	def cfg_change_Standardizer_interfunction():
		ranchero.Standardizer._testcfg_mycobact_is_false()
		print("✅ cfg.mycobacterial_mode in Standardizer context retains changes across functions")
		ranchero.Configuration.set_config({"mycobacterial_mode": True})
		assert ranchero.Configuration.get_config("mycobacterial_mode") == True # back to default
	cfg_default_mycobact()
	cfg_change_intrafunction()
	cfg_change_interfunction()
	cfg_change_NeighLib_intrafunction()
	cfg_change_NeighLib_interfunction()
	cfg_change_NeighLib_intrafunction_via_Standardizer()
	cfg_change_NeighLib_interfunction_via_Standardizer()
	cfg_change_Standardizer_intrafunction()
	cfg_change_Standardizer_interfunction()
	
	# Logging is tested seperately because we handle it via the very silly method of destroying all other logger handlers
	# Change loglevel on NeighLib
	def change_loglevel_NeighLib():
		assert ranchero.Configuration.get_config("loglevel") != 10
		ranchero.Configuration.set_config({"loglevel": 10})
		ranchero.NeighLib._testcfg_logger_is_debug(via_another_module=False)
		ranchero.Configuration.set_config({"loglevel": 40})
		assert ranchero.Configuration.get_config("loglevel") == 40

	# Change loglevel on NeighLib imported module, where the module calls a NeighLib function
	# TODO: consider adding a logging handler to make sure this printed the right version (but logger init destroys handlers so... maybe not)
	def change_loglevel_NeighLib_via_Standardizer():
		assert ranchero.Configuration.get_config("loglevel") != 10
		ranchero.Configuration.set_config({"loglevel": 10})
		ranchero.Standardizer.test_neighlib_cfg_update(via_another_module=True)
		ranchero.Configuration.set_config({"loglevel": 40})
		assert ranchero.Configuration.get_config("loglevel") == 40

	# Change loglevel on NeighLib-imported module, where the module does its own logging
	def change_loglevel_Standardizer():
		assert ranchero.Configuration.get_config("loglevel") != 10
		ranchero.Configuration.set_config({"loglevel": 10})
		ranchero.Standardizer._testcfg_logger_is_debug()
		ranchero.Configuration.set_config({"loglevel": 40})
		assert ranchero.Configuration.get_config("loglevel") == 40
	
	change_loglevel_NeighLib()
	change_loglevel_NeighLib_via_Standardizer()
	change_loglevel_Standardizer()
	
### Polars assumptions ###
def polars_null_handling():
	# check for https://github.com/pola-rs/polars/issues/20069 and/or related regressions

	def dtype_null__len_of_null():
		just_null = pl.DataFrame({'x': [None]}).with_columns(x_len=pl.col('x').len())
		if verbose: print(just_null)
		assert just_null[0,1] == 1
		print("✅ len(`pl.Null` in column of type `pl.Null`) = 1")

	def dtype_list_null__len_of_empty_list():
		null_list = pl.DataFrame({'x': [[]]}).with_columns(x_len=pl.col('x').len(), x_list_len=pl.col('x').list.len())
		if verbose: print(null_list)
		assert null_list[0,1] == 1
		print("✅ len(`[]` in column of type `list[null]`) = 1")

	def dtype_listlen_null__len_of_empty_list():
		null_list = pl.DataFrame({'x': [[]]}).with_columns(x_len=pl.col('x').len(), x_list_len=pl.col('x').list.len())
		if verbose: print(null_list)
		assert null_list[0,2] == 0
		print("✅ list.len(`[]` in column of type `list[null]`) = 0")
	
	def dtype_list_null__len_of_nullnull():
		null_list = pl.DataFrame({'x': [[None, None]]}).with_columns(x_len=pl.col('x').len(), x_list_len=pl.col('x').list.len())
		if verbose: print(null_list)
		assert null_list[0,1] == 1
		print("✅ len(`[pl.Null, pl.Null]` in column of type `list[null]`) = 1")
	
	def dtype_list_null__listlen_of_nullnull():
		null_list = pl.DataFrame({'x': [[None, None]]}).with_columns(x_len=pl.col('x').len(), x_list_len=pl.col('x').list.len())
		if verbose: print(null_list)
		assert null_list[0,2] == 2
		print("✅ list.len(`[pl.Null, pl.Null]` in column of type `list[null]`) = 2")


	dtype_null__len_of_null()
	dtype_list_null__len_of_empty_list()
	dtype_listlen_null__len_of_empty_list()
	dtype_list_null__len_of_nullnull()
	dtype_list_null__listlen_of_nullnull()

	nullframe = pl.DataFrame({'x': [
		None,
		[],
		[None],
		[None, None],
		["foo", None, "bar"],
		["a", "b"]
	]}).with_columns(x_list_len=pl.col('x').list.len())
	if verbose: print(nullframe)

	def schema_guessing_isnt_whack_nullframe(nullframe):
		assert nullframe.dtypes == [pl.List(pl.Utf8), pl.UInt32]
		print("✅ nullframe has correct schema")

	def dtype_list_str__listlen_of_unlisted_null(nullframe):
		if verbose: print(nullframe[0])
		assert nullframe[0,0] == None
		assert nullframe[0,1] == None
		print("✅ list.len(`pl.Null` in column of type `list[str]`) = pl.Null")

	def dtype_list_str__listlen_of_empty_list(nullframe):
		if verbose: print(nullframe[1])
		assert list(nullframe[1,0]) == []
		assert nullframe[1,1] == 0
		print("✅ list.len(`[]` in column of type `list[str]`) = 0")

	def dtype_list_str__listlen_of_listed_null(nullframe):
		if verbose: print(nullframe[2])
		assert list(nullframe[2,0]) == [None]
		assert nullframe[2,1] == 1
		print("✅ list.len(`[pl.Null]` in column of type `list[str]`) = 1")

	def dtype_list_str__listlen_of_listed_nullnull(nullframe):
		if verbose: print(nullframe[3])
		assert list(nullframe[3,0]) == [None, None]
		assert nullframe[3,1] == 2
		print("✅ list.len(`[pl.Null, pl.Null]` in column of type `list[str]`) = 2")

	def dtype_list_str__listlen_of_aNullb(nullframe):
		if verbose: print(nullframe[4])
		assert list(nullframe[4,0]) == ["foo", None, "bar"]
		assert nullframe[4,1] == 3
		print("✅ list.len(`['foo', pl.Null, 'bar']` in column of type `list[str]`) = 3")

	def dtype_list_str__listlen_of_ab(nullframe):
		if verbose: print(nullframe[5])
		assert list(nullframe[5,0]) == ["a", "b"]
		assert nullframe[5,1] == 2
		print("✅ list.len(`['a', 'b']` in column of type `list[str]`) = 2")

	schema_guessing_isnt_whack_nullframe(nullframe)
	dtype_list_str__listlen_of_unlisted_null(nullframe)
	dtype_list_str__listlen_of_empty_list(nullframe)
	dtype_list_str__listlen_of_listed_null(nullframe)
	dtype_list_str__listlen_of_listed_nullnull(nullframe)
	dtype_list_str__listlen_of_aNullb(nullframe)
	dtype_list_str__listlen_of_ab(nullframe)

### utilities ###
def general_utilities():
	def sort_list_str_col():
		df = pl.DataFrame({
			"important words": [["lorem", "ipsum"],["Lorem", "ipsum"],["dolor", "sit", "amet"]]
		})
		df_goal = pl.DataFrame({
			"important words": [["ipsum", "lorem"],["Lorem", "ipsum"],["amet", "dolor", "sit"]]
		})
		Ranchero.NeighLib.sort_list_str_col(df)
		assert_frame_equal(df, df_goal)

### Index stuff ###
def miscellanous_index_stuff():

	def removing_nulls_from_an_index_column():
		df = pl.DataFrame({"__index__file": ["foo.fq", "bar.fq", None, "bizz.fq"]})
		df = ranchero.check_index(df)
		assert df.shape[0] == 3
		print("✅ Removing nulls from an index column")

	removing_nulls_from_an_index_column()

def dupe_index_handling():
	dupe_index_df = pl.DataFrame({
		"__index__file": ["foo.fq", "bar.fq", "bar.fq", "bizz.fq"],
		"int_or_null": [1, None, 2, 4],
		"list_data": [[1,2], [1,2], [1,2], [1,2]],
		"list_or_null": [[1], None, [2], [3]],
		"str_or_null": ["foo", "bar", None, "bizz"]
	})
	non_null_counts = pl.Series(dupe_index_df.with_columns(
		pl.sum_horizontal(
			*[pl.col(c).is_not_null().cast(pl.Int64) for c in dupe_index_df.columns if c != '__index__file']
		).alias("_non_null_count")
	).select('_non_null_count')).to_list()
	assert non_null_counts == [4,2,3,4]

	def dupe_index_handling__error(dupe_index_df):
		ranchero.Configuration.set_config({"dupe_index_handling": 'error'})
		df = dupe_index_df
		try:
			df = ranchero.check_index(df)
		except ValueError:
			print("✅ Dupe index handling: error (threw ValueError)")
	
	def dupe_index_handling__keep_most_data(dupe_index_df):
		"""
		Apparent polars version difference crops up here:
		polars==1.1.16 --> df stays in foo, bar, bizz order
		polars==1.1.27 --> df alphabetized as bar, bizz, foo
		"""
		ranchero.Configuration.set_config({"dupe_index_handling": 'keep_most_data'})
		df_goal = pl.DataFrame({
			"__index__file": ["bar.fq", "bizz.fq", "foo.fq"],
			"int_or_null": [2, 4, 1],
			"list_data": [[1,2], [1,2], [1,2]],
			"list_or_null": [[2], [3], [1]],
			"str_or_null": [None, "bizz", "foo"]
		})
		df = dupe_index_df
		df = ranchero.check_index(df)
		pl.testing.assert_frame_equal(df, df_goal)
		print("✅ Dupe index handling: keep_most_data (kept the one with the least number of nulls)")
	
	def dupe_index_handling__verbose_warn(dupe_index_df):
		ranchero.Configuration.set_config({"dupe_index_handling": 'verbose_warn'})
		df = dupe_index_df
		df = ranchero.check_index(df)
		assert df.shape[0] == 3
		print("✅ Dupe index handling: verbose_warn (removed one)")
	
	def dupe_index_handling__warn(dupe_index_df):
		ranchero.Configuration.set_config({"dupe_index_handling": 'warn'})
		df = dupe_index_df
		df = ranchero.check_index(df)
		assert df.shape[0] == 3
		print("✅ Dupe index handling: warn (removed one)")

	def remove_dupes_after_concat():
		# Test dataframes need two columns or else pl.concat() will drop the repeated one automatically.
		ranchero.Configuration.set_config({"dupe_index_handling": 'warn'}) # TODO: set to default?
		df1 = pl.DataFrame({"__index__file": ["foo.fq", "bar.fq", "bizz.fq"], "host": ["human", "dog", "cat"]})
		df2 = pl.DataFrame({"__index__file": ["loreum.fq", "bar.fq", "ipsum.fq"], "host": ["llama", "chicken", "boar"]})
		df3 = pl.concat([df1, df2], how="align")
		assert df3.shape[0] == 6
		df3 = ranchero.check_index(df3)
		assert df3.shape[0] == 5
		print("✅ Removing dupes from an index column created by pl.concat")

	dupe_index_handling__error(dupe_index_df)
	dupe_index_handling__keep_most_data(dupe_index_df)
	dupe_index_handling__verbose_warn(dupe_index_df)
	dupe_index_handling__warn(dupe_index_df)
	remove_dupes_after_concat()

def run_to_sample_index_swap():
	df = pl.DataFrame({
		"__index__run": ["SRR13684378", "SRR30310804", "SRR30310805", "SRR9291314"],
		"organism": ["Homo sapiens", "Homo sapiens", "Homo sapiens", "Homo sapiens"],
		"purposely_conflicting_metadata": ["foo", "bar", "bizz", "buzz"],
		"sample_index": ["SAMN17861658", "SAMN41021645", "SAMN41021645", "SAMN12046450"]
	})
	flipped = ranchero.run_index_to_sample_index(df)
	flipped_goal = pl.DataFrame({
		"__index__sample_index": ["SAMN17861658", "SAMN41021645", "SAMN12046450"],
		"organism": ["Homo sapiens", "Homo sapiens", "Homo sapiens"],
		"purposely_conflicting_metadata": [["foo"], ["bar", "bizz"], ["buzz"]],
		"run": [["SRR13684378"], ["SRR30310804", "SRR30310805"], ["SRR9291314"]],
	})
	ranchero.check_index(flipped)
	flipped = ranchero.NeighLib.sort_list_str_col(flipped, "purposely_conflicting_metadata", safe=False)
	flipped = ranchero.NeighLib.sort_list_str_col(flipped, "run", safe=False)
	flipped_goal = ranchero.NeighLib.sort_list_str_col(flipped_goal, "purposely_conflicting_metadata", safe=False)
	flipped_goal = ranchero.NeighLib.sort_list_str_col(flipped_goal, "run", safe=False)
	pl.testing.assert_frame_equal(
		flipped.sort("__index__sample_index").select(['__index__sample_index', "organism", "purposely_conflicting_metadata", "run"]),
		flipped_goal.sort("__index__sample_index").select(['__index__sample_index', "organism", "purposely_conflicting_metadata", "run"])
	)
	print("✅ Flipping run-indexed dataframe to sample-indexed dataframe")



### File parsing ###
def file_parsing(folder="./inputs/test"):

	# Read TSV

	# Read CSV via from_tsv(delimiter=","), and that CSV has internal commas within dquotes

	# Read efetch XML

	# Read SRA webview XML

	# Read generic JSON
	def read_json(folder):
		left = ranchero.from_bigquery(f"{folder}/left.json", auto_standardize=False)

	# Read BigQuery NJSON on sra metadata table

	# Read BigQuery NJSON on sra metadata table and that one taxonomic table

	read_json(folder)

### Merge stuff ###
def merge_stuff():

	# Every column in a polars dataframe has a datatype. Nulls can be stored in any arbitrary datatype column,
	# and in theory could act differently depending on the column's datatype, so we are going to create a bunch
	# of different test dataframes focused on one specific column datatype.

	dtype_null = pl.DataFrame({
		"null": [None, None, None]
	})
	assert dtype_null.dtypes == [pl.Null]
	print("✅ dtype_null has correct schema")

	dtype_str = pl.DataFrame({
		"null": [None, None, None],
		"str": ["foo", "foo", "foo"]
	},schema_overrides={"null": pl.Utf8})
	assert dtype_str.dtypes == [pl.Utf8, pl.Utf8]
	print("✅ dtype_str has correct schema")

	dtype_int = pl.DataFrame({
		"int": [3, 3, 3],
	})
	assert dtype_int.dtypes == [pl.Int64]
	print("✅ dtype_int has correct schema")

	dtype_float = pl.DataFrame({
		"pi": [3.1415, 3.1415, 3.1415],
	})
	assert dtype_float.dtypes == [pl.Float64]
	print("✅ dtype_float has correct schema")

	dtype_list_null = pl.DataFrame({
		"[]": [[], [], []],
		"[pl.Null]": [[None], [None], [None]],
	})
	assert dtype_list_null.dtypes == [pl.List(pl.Null), pl.List(pl.Null)]
	print("✅ dtype_list_null has correct schema")

	dtype_list_str = pl.DataFrame({
		"[]": [[], [], []],
		"[pl.Null]": [[None], [None], [None]],
		"one empty string": [[""], [""], [""]],
		"two words": [["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"]],
		"word, None, word": [["hello", None, "world"],["hello", None, "world"],["hello", None, "world"]],
		"two repeats one uniq": [["duck", "duck", "goose"], ["duck", "duck", "goose"],["duck", "duck", "goose"]],
	}, schema_overrides={"[]": pl.List(pl.Utf8), "[pl.Null]": pl.List(pl.Utf8)})
	assert set(dtype_list_str.dtypes) == set([pl.List(pl.Utf8)])
	print("✅ dtype_list_str has correct schema")

	# TODO: add empty lists, etc
	dtype_list_float = pl.DataFrame({
		"two floats": [[2.19, 7.3],[2.19, 7.3],[2.19, 7.3]],
		"one float and None": [[2.19, None],[2.19, None],[2.19, None]]
	})
	assert set(dtype_list_float.dtypes) == set([pl.List(pl.Float64)])
	print("✅ dtype_list_float has correct schema")

	# TODO: add empty lists, etc
	dtype_list_int = pl.DataFrame({
		"three ints": [[8, 9, 10],[8, 9, 10],[8, 9, 10]],
	})
	assert set(dtype_list_int.dtypes) == set([pl.List(pl.Int64)])
	print("✅ dtype_list_int has correct schema")


	# Blocking a merge due to either of the dataframes having dupes in the merge_upon column

hellish_cfg_tests()
polars_null_handling()
general_utilities()
miscellanous_index_stuff()
dupe_index_handling()
run_to_sample_index_swap()
file_parsing()
merge_stuff()

