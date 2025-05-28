#  ** You should use did_I_already_put_that_on_SRA.py instead of this script unless ALL are true: **
#  A) you cannot use edirect, but can use BigQuery
#  B) you are absolutely certain all run accessions have PRECISELY ONE file associated with them
#  C) you are not working with PE Illumina data, sample pools, or barcodes
#
# See if stuff in a metadata TSV file have already been upload to BQ by matching on filename.
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
print(bq.filter(pl.col('SRR_per_SRA').list.contains("SRR13684378")))
print(bq.filter(pl.col('SRR_per_SRA').list.len() > 1))
print(bq.filter(pl.col('SRR_per_SRA').list.contains("SRR30310805")))

# Merge with BQ information
merged = Ranchero.merge_dataframes(
	left=metadata, right=bq, 
	left_name="metadata_table", right_name="SRA_table",
	merge_upon="filename", force_index="filename", drop_exclusive_right=True)
print(merged.filter(pl.col('SRR_per_SRA').list.contains("SRR13684378")))
print(merged.filter(pl.col('SRR_per_SRA').list.len() > 1))
print(merged.filter(pl.col('SRR_per_SRA').list.contains("SRR30310805")))

# Mark rows of files not on SRA
merged = merged.with_columns(
	pl.when(pl.col('SRR_per_SRA').is_null())
	.then(False).otherwise(True).alias("on_SRA")
)

# Also mark rows with "fail" in filename, which we may or may not want on SRA
merged = merged.with_columns(
	pl.when(pl.col('filename').str.contains("fail"))
	.then(True).otherwise(False).alias("is_fail")
)

#print("Merged dataframe")
#Ranchero.dfprint(merged, cols=10, rows=20, width=190, str_len=100)

if "production" in merged.columns:
	Ranchero.NeighLib.cool_header("Merged dataframe where production is WUSTL Y1")
	merged = merged.filter(pl.col('production') == 'WUSTL_HPRC_HiFi_Year1').sort("filename")
	Ranchero.dfprint(merged.select(['sample_id', 'filename', 'SRR_per_SRA', 'on_SRA']), rows=500)

	Ranchero.NeighLib.cool_header("Probably on SRA")
	probably_on_sra = merged.filter(pl.col("on_SRA") == True)
	probably_on_sra = probably_on_sra.filter(pl.col('production') == 'WUSTL_HPRC_HiFi_Year1')
	Ranchero.dfprint(probably_on_sra.select(['sample_id', 'filename', 'SRR_per_SRA', 'on_SRA']), rows=500)

	Ranchero.NeighLib.cool_header("Probably not on SRA")
	probably_not_on_sra = merged.filter(pl.col("on_SRA") == False)
	probably_not_on_sra = probably_not_on_sra.filter(pl.col('production') == 'WUSTL_HPRC_HiFi_Year1')
	Ranchero.dfprint(probably_not_on_sra.select(['sample_id', 'filename', 'SRR_per_SRA', 'on_SRA']), rows=500)

	Ranchero.NeighLib.cool_header("Grouped by production")
	Ranchero.dfprint(merged.group_by(pl.col("production")).agg([pl.col('sample_id'), pl.col('SRR_per_SRA'), pl.col('library_id')]))
	exit(1)

print("Stuff probably not on SRA")
probably_not_on_sra = Ranchero.hella_flat(merged.filter(pl.col("on_SRA") == False), force_index="filename")
probably_not_on_sra = Ranchero.NeighLib.drop_null_columns(probably_not_on_sra, and_non_null_type_full_of_nulls=True)
Ranchero.dfprint(probably_not_on_sra, cols=10, rows=-1, width=190, str_len=100)
Ranchero.to_tsv(probably_not_on_sra, "probably_not_on_sra.tsv")

if "production" in merged.columns:
	print("Not on SRA, production is WUSTL_HPRC_HiFi_Year1")
	probably_not_on_sra = probably_not_on_sra.filter(pl.col('production') == 'WUSTL_HPRC_HiFi_Year1')
	Ranchero.dfprint(probably_not_on_sra.select(['sample_id', 'filename', 'production']))

	print("On SRA, production is WUSTL_HPRC_HiFi_Year1")
	probably_on_sra = merged.filter(pl.col("on_SRA") == True)
	probably_on_sra = probably_on_sra.filter(pl.col('production') == 'WUSTL_HPRC_HiFi_Year1')
	Ranchero.dfprint(probably_on_sra.select(['sample_id', 'filename', 'production']))
	exit(1)

	#print("Not on SRA, grouped by production")
	#Ranchero.dfprint(probably_not_on_sra.group_by(pl.col("production")).agg([pl.col('sample_id'), pl.col('library_id')]))

	print("On SRA, grouped by production")
	probably_on_sra = Ranchero.hella_flat(merged.filter(pl.col("on_SRA") == True), force_index="filename")
	Ranchero.dfprint(probably_on_sra.group_by(pl.col("production")).agg([pl.col('sample_id'), pl.col('library_id')]))

