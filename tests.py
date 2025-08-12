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


### Configuration ###


### File parsing ###

# Read TSV

# Read CSV via from_tsv(delimiter=","), and that CSV has internal commas within dquotes

# Read efetch XML

# Read SRA webview XML

# Read BigQuery NJSON on sra metadata table

# Read BigQuery NJSON on sra metadata table and that one taxonomic table


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



dupe_index_handling()

