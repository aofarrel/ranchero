import sys
import src as Ranchero
import polars as pl

edirect = Ranchero.from_efetch(sys.argv[1], index_by_file=True)
edirect = edirect.rename({'filename': 'most_likely_filename', 'alias': 'alternative_filename'}).drop(['notes'])
edirect = edirect.group_by("most_likely_filename").agg(
	[pl.col('run_index'), pl.col('submitted_files_gibytes'), pl.col('alternative_filename').unique()]
)

if edirect.schema['submitted_files_gibytes'] == pl.Float64:
	smol_files = edirect.filter(pl.col('submitted_files_gibytes') < 1.0)
elif edirect.schema['submitted_files_gibytes'] == pl.List:
	smol_files = edirect.filter(pl.col("submitted_files_gibytes").list.eval(pl.element() < 1.0).list.any())
else:
	raise TypeError("submitted_files_gibytes is neither a list nor a float?")

print("Files of a size less than one GiB")
smol_files = Ranchero.hella_flat(smol_files, force_index="most_likely_filename").sort('submitted_files_gibytes')
Ranchero.dfprint(smol_files, str_len=1000, rows=50)
Ranchero.to_tsv(smol_files, "sra_less_than_1_GiByte.tsv")
