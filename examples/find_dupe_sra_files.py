import sys
import src as Ranchero
import polars as pl

edirect = Ranchero.from_efetch(sys.argv[1], index_by_file=True, group_by_file=True)
edirect = edirect.rename({'submitted_files': 'most_likely_filename', 'alias': 'alternative_filename'}).drop(['notes'])

if edirect.schema['run_id'] != pl.List:
	print("Congrats! There's no matching filenames uploaded under different run indeces.")
	exit(0)
print(edirect.columns)
possible_dupes = edirect.filter(pl.col('run_id').list.len() > 1)
possible_dupes = possible_dupes.with_columns(pl.when(
	pl.col('submitted_files_gibytes').list.len() > 1)
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("different_file_sizes")
).select(['most_likely_filename', 'alternative_filename', 'run_id', 'sample_id', 
		'submitted_files_gibytes', 'different_file_sizes', 'total_bases', 'archive_data_bytes'])

print("Files that may have been double-uploaded")
Ranchero.dfprint(possible_dupes, str_len=1000, rows=50)
Ranchero.to_tsv(possible_dupes, "sra_possible_dupes.tsv")
