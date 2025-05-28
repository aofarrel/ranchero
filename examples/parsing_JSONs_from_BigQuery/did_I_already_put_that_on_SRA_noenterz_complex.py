#  ** You should use did_I_already_put_that_on_SRA.py instead of this script unless ALL are true: **
#  A) you cannot use edirect, but can use BigQuery
#  B) you are absolutely certain all run accessions have PRECISELY ONE file associated with them
#  C) you are not working with PE Illumina data, sample pools, or barcodes
#
# This is an extension of libraryIDs_and_filenames_from_bigquery.py
# In this scenario, we already uploaded a bunch of files to SRA but
# lost track of which ones were uploaded. We did a search of SRA on
# BQ to get a newline-delimited JSON of relevent samples. We're now
# using this Python script here to compare filenames to see what is
# and is not already on SRA.
#
# In terms of Ranchero, what we're mainly using here are file IO
# wrappers, BQ-specific stuff, & the merge function, the latter of
# which handles nulls in a way that is (arguably) more intutive
# than how polars usually does it.
#
# Here's what we're especially interested in:
# A) Files that got uploaded to SRA as multi-file run accessions
# B) Files that got uploaded to SRA twice
# C) Files with "fail" in their filename, and if they got onto SRA
# D) Files that made it onto SRA that we lost track of
# E) Files that we say are on SRA, but aren't actually there
# F) Specific run accession mismatches between us and SRA
#
# Additional checks:
# * No duplicates in filenames across our own TSVs

import sys
import src as Ranchero
import polars as pl
pl.Config.set_tbl_cols(10)
pl.Config.set_tbl_width_chars(160)
pl.Config.set_fmt_str_lengths(30)
pl.Config.set_fmt_table_cell_list_len(5)

# Parse BQ file to get what's already on SRA
bq = Ranchero.from_bigquery(sys.argv[1])
bq = Ranchero.extract_filename(bq, 'primary_search', 'files')
bq = Ranchero.NeighLib.add_list_len_col(bq, 'files', 'files_len')
bq = bq.select(['acc', 'files', 'sample_name', 'files_len', 'library_name',
	'population_sam_s_dpl40', 'race_sam', 'ethnicity_sam', 'platform', 'instrument'])

# Just print a lil something
Ranchero.NeighLib.cool_header("Sus samples")

# [A] Some samples got uploaded as multi-file, which requires special handling
bq = bq.with_columns(
	pl.when(pl.col("files_len") > 1)
	.then(True)
	.otherwise(False)
	.alias("multifile")
)

bq = bq.explode("files").rename({'files':'filename', 'sample_name':'sample_id', 'acc':'SRR_per_SRA', 'library_name': 'library_id'}).drop('files_len')
full_list_of_bq_SRR = set(bq.select(("SRR_per_SRA")).to_series().unique().to_list())

# [B] Check for files that may have been uploaded twice
bq = bq.with_columns(
	pl.when(pl.col("filename").is_duplicated())
	.then(True)
	.otherwise(False)
	.alias("SRA_dupe")
)

# We want to merge this with our own TSVs on the filename column, but as we now know, there's
# dupes in the filename column we want to merge upon. This causes issues, so we're going to
# un-explode the filename column. This explode-then-unexplode approach allows us to handle
# both duplicate uploads and multi-file uploads.
bq = bq.group_by("filename").agg([pl.col('sample_id'), pl.col('SRR_per_SRA'), pl.col('library_id'), pl.col('multifile'), pl.col('SRA_dupe'),
	pl.col('population_sam_s_dpl40'), pl.col('race_sam'), pl.col('ethnicity_sam'), 
	pl.col('platform'), pl.col('instrument')])
bq = Ranchero.hella_flat(bq, force_index="filename")
bq = Ranchero.NeighLib.check_index(bq, manual_index_column="filename") # sanity check for duplicates


# Now that we only have one copy of every file here, we can give an accurate count of [A] and [B]
multifile = bq.filter(pl.col("multifile") == True)
print(f"{multifile.height} samples seem to have been uploaded to SRA as multi-file run accessions (due to dupes, this count may be inaccurate)")
dupes = bq.filter(pl.col('SRA_dupe') == True)
print(f"{dupes.height} files were uploaded to SRA at least twice")

# Get TSV we use to keep track of ONT data. check_index=False because it has a non-standard index
ONT_raw = Ranchero.from_tsv("/Users/aofarrel/Downloads/WORKING R2 Sequencing Data Index - ont.tsv", check_index=False, auto_rancheroize=False)
ONT = ONT_raw.select(['filename', 'platform', 'accession', 'sample_id'])
Ranchero.NeighLib.check_index(ONT, manual_index_column='filename')
ONT = ONT.rename({'accession': 'SRR_per_us'})

# Get TSV we use to keep track of Illumina data
# This one doesn't have an accession column, so we'll add one manually
ILL_raw = Ranchero.from_tsv("/Users/aofarrel/Downloads/WORKING R2 Sequencing Data Index - ill.tsv", check_index=False, auto_rancheroize=False)
ILL = ILL_raw.select(['filename', 'platform', 'sample_id'])
Ranchero.NeighLib.check_index(ILL, manual_index_column='filename')
ILL = ILL.with_columns(SRR_per_us=None)

# Get TSV we use to keep track of HiFi data
HIF_raw = Ranchero.from_tsv("/Users/aofarrel/Downloads/WORKING R2 Sequencing Data Index - hifi.tsv", check_index=False, auto_rancheroize=False)
HIF = HIF_raw.select(['filename', 'platform', 'accession', 'sample_id'])
Ranchero.NeighLib.check_index(HIF, manual_index_column='filename')
HIF = HIF.rename({'accession': 'SRR_per_us'})

# Get TSV we use to keep track of Hi-C data
HIC_raw = Ranchero.from_tsv("/Users/aofarrel/Downloads/WORKING R2 Sequencing Data Index - hic.tsv", check_index=False, auto_rancheroize=False)
HIC = HIC_raw.select(['filename', 'platform', 'accession', 'sample_id'])
Ranchero.NeighLib.check_index(HIC, manual_index_column='filename')
HIC = HIC.rename({'accession': 'SRR_per_us'})

# Get TSV we use to keep track of DeepConsensus data
DCN_raw = Ranchero.from_tsv("/Users/aofarrel/Downloads/WORKING R2 Sequencing Data Index - dc.tsv", check_index=False, auto_rancheroize=False)
DCN = DCN_raw.select(['filename', 'platform', 'accession', 'sample_id'])
Ranchero.NeighLib.check_index(DCN, manual_index_column='filename')
DCN = DCN.rename({'accession': 'SRR_per_us'})

# Sanity check for duplicates across our own TSVs
ONT_files = pl.Series(ONT.select("filename")).to_list()
ILL_files = pl.Series(ILL.select("filename")).to_list()
HIF_files = pl.Series(HIF.select("filename")).to_list()
HIC_files = pl.Series(HIC.select("filename")).to_list()
DCN_files = pl.Series(DCN.select("filename")).to_list()
all_of_our_files = ONT_files + ILL_files + HIF_files + HIC_files + DCN_files
assert len(all_of_our_files) == len(set(all_of_our_files))

# We will do the merging one-at-a-time so we can better keep track of indicator columns
merged_ONT = Ranchero.merge_dataframes(
	left=bq, right=ONT, 
	left_name="SRA_table", right_name="ONT_table",
	merge_upon="filename", force_index="filename")
merged_ILL = Ranchero.merge_dataframes(
	left=bq, right=ILL, 
	left_name="SRA_table", right_name="ILL_table",
	merge_upon="filename", force_index="filename")
merged_HIF = Ranchero.merge_dataframes(
	left=bq, right=HIF, 
	left_name="SRA_table", right_name="HIF_table",
	merge_upon="filename", force_index="filename")
merged_HIC = Ranchero.merge_dataframes(
	left=bq, right=HIC, 
	left_name="SRA_table", right_name="HIC_table",
	merge_upon="filename", force_index="filename")
merged_DCN = Ranchero.merge_dataframes(
	left=bq, right=DCN, 
	left_name="SRA_table", right_name="DCN_table",
	merge_upon="filename", force_index="filename")

merged_ONT_ILL = Ranchero.hella_flat(Ranchero.merge_dataframes(
	left=merged_ONT, right=merged_ILL, 
	indicator=None,
	merge_upon="filename", force_index="filename"), force_index="filename")
merged_ONT_ILL_HIF = Ranchero.hella_flat(Ranchero.merge_dataframes(
	left=merged_ONT_ILL, right=merged_HIF, 
	indicator=None,
	merge_upon="filename", force_index="filename"), force_index="filename")
merged_ONT_ILL_HIF_HIC = Ranchero.hella_flat(Ranchero.merge_dataframes(
	left=merged_ONT_ILL_HIF, right=merged_HIC, 
	indicator=None,
	merge_upon="filename", force_index="filename"), force_index="filename")
merged_ONT_ILL_HIF_HIC_DCN = Ranchero.hella_flat(Ranchero.merge_dataframes(
	left=merged_ONT_ILL_HIF_HIC, right=merged_DCN, 
	indicator=None,
	merge_upon="filename", force_index="filename"), force_index="filename")
merged = merged_ONT_ILL_HIF_HIC_DCN

# [C] Mark rows that contain "fail" in their filename
merged = merged.with_columns(
	pl.when(pl.col('filename').str.contains("fail"))
	.then(True).otherwise(False).alias("is_fail")
)
# How many of those were uploaded to SRA? How many weren't?
fails_on_sra = merged.filter(
	(pl.col("is_fail") == True)
	.and_(pl.col("SRR_per_SRA").is_not_null())
)
fails_not_on_sra = merged.filter(
	(pl.col("is_fail") == True)
	.and_(pl.col("SRR_per_SRA").is_null())
)
print(f"{fails_on_sra.height} fails made it onto SRA, and an additional {fails_not_on_sra.height} "
	"fails on our own TSVs.")

# [D] Mark rows representing files that are on SRA, but we seem to lost track of (ie we have no accession for)
merged = merged.with_columns(
	pl.when(
		(pl.col('SRR_per_SRA').is_not_null())
		.and_(pl.col('SRR_per_us').is_null())
	)
	.then(True).otherwise(False).alias("fire_n_forget")
)
fire_n_forget = merged.filter(pl.col("fire_n_forget") == True)
print(f"{fire_n_forget.height} samples made it onto SRA without us noting their accessions.")


# [E] We say a file is on SRA but SRA doesn't seem to have it
# (NOTE: SRA IS KNOWN TO BE INCONSISTENT! VERIFY WITH FASTQ-DUMP!)
merged = merged.with_columns(
	pl.when(
		(pl.col('SRR_per_SRA').is_null())
		.and_(pl.col('SRR_per_us').is_not_null())
		.and_(~pl.col("SRR_per_us").is_in(full_list_of_bq_SRR)) # THIS MUST BE INCLUDED!!
	).then(True).otherwise(False).alias("MIA")
)
mia = merged.filter(pl.col("MIA") == True)
print(f"{mia.height} files we think we uploaded but !MIGHT! not be on SRA")

# [F] SRR_per_SRA and SRR_per_us columns are both non-null but not matching
merged = merged.with_columns(
	pl.when(
		(pl.col('SRR_per_SRA').is_not_null())
		.and_(pl.col('SRR_per_us').is_not_null())
		.and_(pl.col('SRR_per_SRA').list.len == 1)
		.and_(pl.col('SRR_per_SRA').list.first() != pl.col('SRR_per_us'))
	)
	.then(True).otherwise(False).alias("mismatch")
)
mismatch = merged.filter(pl.col('mismatch') == True)
print(f"{mismatch.height} files have non-null accessions between us and SRA that don't match")

hella_sus = merged.filter(
	(pl.col('multifile') == True)
	.or_(pl.col('SRA_dupe') == True)
	.or_(pl.col('is_fail') == True)
	.or_(pl.col('fire_n_forget') == True)
	.or_(pl.col('MIA') == True)
	.or_(pl.col('mismatch') == True)
)
hella_sus = Ranchero.hella_flat(hella_sus, force_index="filename")
print(f"In total, {hella_sus.height} files are weird in some way or another (files can be weird in more than one way)")
Ranchero.to_tsv(hella_sus, "HPRC_hella_sus_2025-05-09.tsv")

# Mark only rows representing files that definitely are not on SRA, and we should consider uploading
merged = merged.with_columns(
	pl.when(
		(pl.col('SRR_per_SRA').is_null())
		.and_(pl.col('SRR_per_us').is_null())
	)
	.then(True).otherwise(False).alias("not_on_sra")
)
confirmed_not_on_sra = Ranchero.hella_flat(merged.filter(pl.col("not_on_sra") == True), force_index="filename")
files_to_upload = confirmed_not_on_sra.select("filename").to_series().unique().to_list()

Ranchero.NeighLib.cool_header("Upload candidates")
print(f"{confirmed_not_on_sra.height} files are definitely not on SRA, of which:")
print(f"* {confirmed_not_on_sra.filter(pl.col("is_fail") == False).height} do NOT have fail in filename")
print(f"* {confirmed_not_on_sra.filter(pl.col("collection") == 'ONT_table').height} are ONT")
print(f"* {confirmed_not_on_sra.filter(pl.col("collection") == 'ILL_table').height} are non-Hi-C illumina (possible 1000G?)")
print(f"* {confirmed_not_on_sra.filter(pl.col("collection") == 'HIF_table').height} are HiFi")
print(f"* {confirmed_not_on_sra.filter(pl.col("collection") == 'HIC_table').height} are Hi-C")
print(f"* {confirmed_not_on_sra.filter(pl.col("collection") == 'DCN_table').height} are DeepConsensus")
print(f"This makes for {len(files_to_upload)} files to upload")
Ranchero.to_tsv(confirmed_not_on_sra, "HPRC_confirmed_not_on_sra_2025-05-09.tsv")

upload_table_ONT = ONT_raw.filter(pl.col("filename").is_in(files_to_upload))
upload_table_ILL = ILL_raw.filter(pl.col("filename").is_in(files_to_upload))
upload_table_HIF = HIF_raw.filter(pl.col("filename").is_in(files_to_upload))
upload_table_HIC = HIC_raw.filter(pl.col("filename").is_in(files_to_upload))
upload_table_DCN = DCN_raw.filter(pl.col("filename").is_in(files_to_upload))

# Drop empty (or functionally empty) columns
upload_table_ONT = Ranchero.NeighLib.drop_null_columns(upload_table_ONT, and_non_null_type_full_of_nulls=True)
upload_table_ILL = Ranchero.NeighLib.drop_null_columns(upload_table_ILL, and_non_null_type_full_of_nulls=True)
upload_table_HIF = Ranchero.NeighLib.drop_null_columns(upload_table_HIF, and_non_null_type_full_of_nulls=True)
upload_table_HIC = Ranchero.NeighLib.drop_null_columns(upload_table_HIC, and_non_null_type_full_of_nulls=True)
upload_table_DCN = Ranchero.NeighLib.drop_null_columns(upload_table_DCN, and_non_null_type_full_of_nulls=True)

# Now that we dropped functionally empty columns, let's check our tables for missing metadata
can_easily_add_later = ['notes', 'biosample_accession', 'library_id', 'accession']
missing_in_all_ONT = set(ONT_raw.filter(~pl.col("filename").is_in(files_to_upload)).columns) - set(upload_table_ONT.columns) - set(can_easily_add_later)
missing_in_all_ILL = set(ILL_raw.filter(~pl.col("filename").is_in(files_to_upload)).columns) - set(upload_table_ILL.columns) - set(can_easily_add_later)
missing_in_all_HIF = set(HIF_raw.filter(~pl.col("filename").is_in(files_to_upload)).columns) - set(upload_table_HIF.columns) - set(can_easily_add_later)
missing_in_all_HIC = set(HIC_raw.filter(~pl.col("filename").is_in(files_to_upload)).columns) - set(upload_table_HIC.columns) - set(can_easily_add_later)
missing_in_all_DCN = set(DCN_raw.filter(~pl.col("filename").is_in(files_to_upload)).columns) - set(upload_table_DCN.columns) - set(can_easily_add_later)
print(f"All ONT to-upload samples are missing the following metadata: {missing_in_all_ONT}")
print(f"All ILL to-upload samples are missing the following metadata: {missing_in_all_ILL}")
print(f"All HIF to-upload samples are missing the following metadata: {missing_in_all_HIF}")
print(f"All HIC to-upload samples are missing the following metadata: {missing_in_all_HIC}")
print(f"All DCN to-upload samples are missing the following metadata: {missing_in_all_DCN}")

# That ONT table looks pretty good... let's split it by the 'production' column to keep track of it more easily,
# and check for stuff that might be null in just a few rows
print("Checking ONT tables...")
ONT_upload_tables = upload_table_ONT.partition_by("production", as_dict=True)
for name, dataframe in ONT_upload_tables.items():
	print(f"\n{name}:")
	for col in dataframe.columns:
		if dataframe.select(pl.col(col)).null_count().item() != 0:
			print(f"-->{col} has {dataframe.select(pl.col(col)).null_count().item()} null values")

# How about the hifi data?
print("Checking HIFI tables...")
HIF_upload_tables = upload_table_HIF.partition_by("production", as_dict=True)
for name, dataframe in HIF_upload_tables.items():
	print(f"\n{name}:")
	for col in dataframe.columns:
		if dataframe.select(pl.col(col)).null_count().item() != 0:
			print(f"-->{col} has {dataframe.select(pl.col(col)).null_count().item()} null values")

