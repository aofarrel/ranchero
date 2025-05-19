# See if stuff in a metadata JSON file have already been upload to BQ by matching on filename.
# Usage: did_I_already_put_that_on_SRA_simple.py <bq_json> <metadata_tsv>
#       <bq_json>: jsonl from BigQuery search of SRA (all samples in a given BioProject, etc)
#       <metadata_tsv>: a TSV of stuff you want on SRA that, at a minimum, includes a "filename" column

import sys
import src as Ranchero
import polars as pl

# Parse BQ file to get what's already on SRA
bq = Ranchero.from_bigquery(sys.argv[1])
bq = Ranchero.extract_filename(bq, 'primary_search', 'files')
bq = Ranchero.NeighLib.add_list_len_col(bq, 'files', 'files_len')
bq = bq.select(['acc', 'files', 'sample_name', 'files_len', 'library_name'])

# Some samples got uploaded as multi-file, which requires special handling
bq = bq.with_columns(
	pl.when(pl.col("files_len") > 1)
	.then(True)
	.otherwise(False)
	.alias("multifile")
)
bq = bq.explode("files").rename({'files':'filename', 'sample_name':'sample_id', 'acc':'SRR_per_SRA', 'library_name': 'library_id'}).drop('files_len')
full_list_of_bq_SRR = set(bq.select(("SRR_per_SRA")).to_series().unique().to_list())

# We want to merge this with our own TSVs on the filename column, but there's a possibility
# of duplicate filenames on SRA, which causes issues when merging. This approach fixes that.
# See did_I_already_put_that_on_SRA_complex.py for more information.
bq = bq.group_by("filename").agg([pl.col('sample_id'), pl.col('SRR_per_SRA'), pl.col('library_id')])
bq = Ranchero.hella_flat(bq, force_index="filename")
bq = Ranchero.NeighLib.check_index(bq, manual_index_column="filename") # sanity check for duplicates

# Parse TSV file of metadata
metadata = Ranchero.from_tsv(sys.argv[2], check_index=False, auto_rancheroize=False)
metadata = Ranchero.NeighLib.check_index(metadata, manual_index_column='filename')

# Merge with BQ information
merged = Ranchero.merge_dataframes(
	left=bq, right=metadata, 
	left_name="SRA_table", right_name="metadata_table",
	merge_upon="filename", force_index="filename")

# Mark rows of files not on SRA
merged = merged.with_columns(
	pl.when(pl.col('SRR_per_SRA').is_null())
	.then(True).otherwise(False).alias("not_on_sra")
)

# Also mark rows with "fail" in filename, which we may or may not want on SRA
merged = merged.with_columns(
	pl.when(pl.col('filename').str.contains("fail"))
	.then(True).otherwise(False).alias("is_fail")
)

confirmed_not_on_sra = Ranchero.hella_flat(merged.filter(pl.col("not_on_sra") == True), force_index="filename")
confirmed_not_on_sra = Ranchero.NeighLib.drop_null_columns(confirmed_not_on_sra, and_non_null_type_full_of_nulls=True)
Ranchero.dfprint(confirmed_not_on_sra, cols=10, rows=-1, width=190, str_len=100)
Ranchero.to_tsv(confirmed_not_on_sra, "confirmed_not_on_sra.tsv")

