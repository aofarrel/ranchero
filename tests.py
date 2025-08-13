import os
import re
import polars as pl
import datetime
import polars as pl
from polars.testing import assert_series_equal
import polars.selectors as cs
import traceback
import src as Ranchero

pl.Config.set_tbl_rows(15)
pl.Config.set_tbl_cols(15)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_fmt_table_cell_list_len(10)
pl.Config.set_tbl_width_chars(200)

df = pl.DataFrame({
	"right": [pl.Null, 5, 7.8, "bar", [], [pl.Null], ["hello", pl.Null, "world"], ["lorem", "ipsum"], ["duck", "duck", "goose"], [8, 9, 10], [2.19, 7.3]],
	"pure null": [pl.Null, pl.Null, pl.Null, pl.Null, pl.Null, pl.Null, pl.Null, pl.Null, pl.Null, pl.Null, pl.Null],
	"int": [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
	"float": [3.1415, 3.1415, 3.1415, 3.1415, 3.1415, 3.1415, 3.1415, 3.1415, 3.1415, 3.1415, 3.1415],
	"str": ["foo", "foo", "foo", "foo", "foo", "foo", "foo", "foo", "foo", "foo", "foo"],
	"list: empty": [[], [], [], [], [], [], [], [], [], [], []],
	"list: empty str": [[""], [""], [""], [""], [""], [""], [""], [""], [""], [""], [""]],
	"list: null": [[pl.Null], [pl.Null], [pl.Null], [pl.Null], [pl.Null], [pl.Null], [pl.Null], [pl.Null], [pl.Null], [pl.Null], [pl.Null]],
	"list: str + null": [["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"],["hello", pl.Null, "world"]],
	"list: str": [["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"],["lorem", "ipsum"]],
}, strict=False, schema_overrides={"list: str + null": pl.Object})

verbose = False


### Configuration ###


### File parsing ###

# Read TSV

# Read CSV via from_tsv(delimiter=","), and that CSV has internal commas within dquotes

# Read efetch XML

# Read SRA webview XML

# Read BigQuery NJSON on sra metadata table

# Read BigQuery NJSON on sra metadata table and that one taxonomic table

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

	def schema_guessing_isnt_whack(nullframe):
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

	schema_guessing_isnt_whack(nullframe)
	dtype_list_str__listlen_of_unlisted_null(nullframe)
	dtype_list_str__listlen_of_empty_list(nullframe)
	dtype_list_str__listlen_of_listed_null(nullframe)
	dtype_list_str__listlen_of_listed_nullnull(nullframe)
	dtype_list_str__listlen_of_aNullb(nullframe)
	dtype_list_str__listlen_of_ab(nullframe)


### Index stuff ###
def miscellanous_index_stuff():

	def removing_nulls_from_an_index_column():
		df = pl.DataFrame({"__index__file": ["foo.fq", "bar.fq", None, "bizz.fq"]})
		df = Ranchero.check_index(df)
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
		Ranchero.Configuration.set_config({"dupe_index_handling": 'error'})
		df = dupe_index_df
		try:
			df = Ranchero.check_index(df)
		except ValueError:
			print("✅ Dupe index handling: error (threw ValueError)")
	
	def dupe_index_handling__keep_most_data(dupe_index_df):
		Ranchero.Configuration.set_config({"dupe_index_handling": 'keep_most_data'})
		df_goal = pl.DataFrame({
			"__index__file": ["bar.fq", "bizz.fq", "foo.fq"],
			"int_or_null": [2, 4, 1],
			"list_data": [[1,2], [1,2], [1,2]],
			"list_or_null": [[2], [3], [1]],
			"str_or_null": [None, "bizz", "foo"]
		})
		df = dupe_index_df
		df = Ranchero.check_index(df)
		pl.testing.assert_frame_equal(df, df_goal)
		print("✅ Dupe index handling: keep_most_data (kept the one with the least number of nulls)")
	
	def dupe_index_handling__verbose_warn(dupe_index_df):
		Ranchero.Configuration.set_config({"dupe_index_handling": 'verbose_warn'})
		df = dupe_index_df
		df = Ranchero.check_index(df)
		assert df.shape[0] == 3
		print("✅ Dupe index handling: verbose_warn (removed one)")
	
	def dupe_index_handling__warn(dupe_index_df):
		Ranchero.Configuration.set_config({"dupe_index_handling": 'warn'})
		df = dupe_index_df
		df = Ranchero.check_index(df)
		assert df.shape[0] == 3
		print("✅ Dupe index handling: warn (removed one)")

	def remove_dupes_after_concat():
		# Test dataframes need two columns or else pl.concat() will drop the repeated one automatically.
		Ranchero.Configuration.set_config({"dupe_index_handling": 'warn'}) # TODO: set to default?
		df1 = pl.DataFrame({"__index__file": ["foo.fq", "bar.fq", "bizz.fq"], "host": ["human", "dog", "cat"]})
		df2 = pl.DataFrame({"__index__file": ["loreum.fq", "bar.fq", "ipsum.fq"], "host": ["llama", "chicken", "boar"]})
		df3 = pl.concat([df1, df2], how="align_full")
		assert df3.shape[0] == 6
		df3 = Ranchero.check_index(df3)
		assert df3.shape[0] == 5
		print("✅ Removing dupes from an index column created by pl.concat")

	
	dupe_index_handling__error(dupe_index_df)
	dupe_index_handling__keep_most_data(dupe_index_df)
	dupe_index_handling__verbose_warn(dupe_index_df)
	dupe_index_handling__warn(dupe_index_df)
	remove_dupes_after_concat()



### Merge stuff ###
# Blocking a merge due to either of the dataframes having dupes in the merge_upon column



polars_null_handling()
miscellanous_index_stuff()
dupe_index_handling()

