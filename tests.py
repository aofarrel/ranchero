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



### File parsing ###

# Read TSV

# Read CSV via from_tsv(delimiter=","), and that CSV has internal commas within dquotes

# Read efetch XML

# Read SRA webview XML

# Read BigQuery NJSON on sra metadata table

# Read BigQuery NJSON on sra metadata table and that one taxonomic table


### Index stuff ###

# Removing nulls from an index column
def removing_nulls_from_an_index_column():
	df = pl.DataFrame({"__index__file": ["foo.fq", "bar.fq", None, "bizz.fq"]})
	df = Ranchero.check_index(df)
	assert df.shape[0] == 3
	print("✅ Removing nulls from an index column")

# Removing dupes from an index column
def removing_dupes_from_an_index_column():
	df = pl.DataFrame({"__index__file": ["foo.fq", "bar.fq", "bar.fq", "bizz.fq"]})
	df = Ranchero.check_index(df)
	assert df.shape[0] == 3
	print("✅ Removing dupes from an index column")

# Concatenating two dataframes that will result in dupe indeces, and making sure dupes are dropped
# Test dataframes need two columns or else pl.concat() will drop the repeated one automatically.
def remove_dupes_after_concat():
	df1 = pl.DataFrame({"__index__file": ["foo.fq", "bar.fq", "bizz.fq"], "host": ["human", "dog", "cat"]})
	df2 = pl.DataFrame({"__index__file": ["loreum.fq", "bar.fq", "ipsum.fq"], "host": ["llama", "chicken", "boar"]})
	df3 = pl.concat([df1, df2], how="align_full")
	assert df3.shape[0] == 6
	df3 = Ranchero.check_index(df3)
	assert df3.shape[0] == 5
	print("✅ Removing dupes from an index column created by pl.concat")

# Blocking a merge due to either of the dataframes having dupes in the merge_upon column


### Merge stuff ###


removing_nulls_from_an_index_column()
removing_dupes_from_an_index_column()
remove_dupes_after_concat()
