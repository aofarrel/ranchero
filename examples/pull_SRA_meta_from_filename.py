# Pull accession (SRR), study, BioProject, and BioSample from filename on SRA
#                          !!WARNING!!
# Typically, a run accession is attached to only one BioProject. However, there
# can be multiple run accessions in a single BioSample, and each one can have
# a different BioProject. The BigQuery SRA tables are effectively indexed by run
# accession, so every run accession will typically only report one BioProject,
# even if it is part of a BioSample that is in multiple BioProjects. This same
# logic also applies to the "study" field.
#
# Usage:
# pull_SRA_meta_from_filename.py <bq_jsonl> <tsv_out>


import sys
import src as Ranchero
import polars as pl

bq = Ranchero.from_bigquery(sys.argv[1])
bq = Ranchero.extract_filename(bq, 'primary_search', 'files')

# Special handling for "one run accession, many files"
bq = Ranchero.NeighLib.add_list_len_col(bq, 'files', 'files_len')
bq = bq.with_columns(
	pl.when(pl.col("files_len") > 1)
	.then(True)
	.otherwise(False)
	.alias("multifile")
)
bq = bq.explode("files").rename({'files':'filename', 'acc':'run_accession'}).drop('files_len')

# Special handling for duplicated filenames
bq = bq.with_columns(
	pl.when(pl.col("filename").is_duplicated())
	.then(True)
	.otherwise(False)
	.alias("SRA_dupe")
)

print(bq.columns)

# It looks goofy, but this explode-then-unexplode approach allows us to handle
# both duplicate uploads and multi-file uploads.
bq = bq.group_by("filename").agg([pl.col('sample_name'),
	pl.col('run_accession'), pl.col('sra_study'),
	pl.col('bioproject'), pl.col('biosample'),
	pl.col('multifile'), pl.col('SRA_dupe'),])
bq = Ranchero.hella_flat(bq, force_index="filename")
bq = Ranchero.NeighLib.check_index(bq, manual_index_column="filename") # sanity check

print(bq.sort('SRA_dupe'))
Ranchero.to_tsv(bq, sys.argv[2])