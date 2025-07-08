# Merge a "processed-okay" TSV from SRA with a data table to get just the IDs
# Usage: put_SRA_ids_on_data_table.py <processed-okay_tsv> <metadata_tsv> <verbose>
#       <processed-okay_tsv>: TSV from SRA submission portal
#       <metadata_tsv>: a TSV of stuff you want on SRA that, at a minimum, includes a "filename" column
#       <verbose>: Optional -v flag to print intermediate dataframes

import sys
import src as Ranchero
import polars as pl

if len(sys.argv) == 4:
	if sys.argv[3] not in ['-v', '-verbose', '--verbose']:
		print(f"Can't parse third argument: {sys.argv[3]}")
		exit(1)
	else:
		verbose = True
elif len(sys.argv) == 3:
	verbose = False
else:
	print("Usage: put_SRA_ids_on_data_table.py <processed-okay_tsv> <metadata_tsv> <verbose>")
	exit(1)

# Parse XML file to get what's already on SRA
okay = Ranchero.from_tsv(sys.argv[1], check_index=False, auto_rancheroize=False)
okay = Ranchero.NeighLib.check_index(okay, manual_index_column='filename')
assert okay.height > 0
assert 'accession' in okay.columns
if verbose:
	print("processed-okay dataframe:")
	Ranchero.dfprint(okay.sort("filename"), cols=5, rows=20, width=190, str_len=100)
else:
	print(f"{okay.height} files in metadata dataframe")

# Parse TSV file of metadata
# If it's not indexed by filename, you'll want to add an explode() in here
metadata = Ranchero.from_tsv(sys.argv[2], check_index=False, auto_rancheroize=False)
metadata = Ranchero.NeighLib.check_index(metadata, manual_index_column='filename')
assert metadata.height > 0
assert 'accession' not in metadata.columns
if verbose:
	print("Metadata dataframe:")
	Ranchero.dfprint(metadata.sort("filename"), cols=5, rows=20, width=190, str_len=100)
else:
	print(f"{metadata.height} files in metadata dataframe")

# To make merging easier, we're going to drop columns from okay dataframe, since we care less about its contents
okay = okay.select(["filename", "accession"])

# Merge with SRA table upon the "filename" column
upon_filename = Ranchero.merge_dataframes(
	left=metadata, right=okay, 
	left_name="metadata_table", right_name="SRA_filename",
	merge_upon="filename", force_index="filename", drop_exclusive_right=True)

merged = upon_filename.with_columns(
	pl.when(pl.col('accession').is_null())
	.then(False).otherwise(True).alias("on_SRA")
)

# Also mark rows with "fail" in filename, which we may or may not want on SRA
merged = merged.with_columns(
	pl.when(pl.col('filename').str.contains("fail"))
	.then(True).otherwise(False).alias("is_fail")
)

if verbose:
	print("Merged dataframe (metadata + SRA)")
	Ranchero.dfprint(merged.sort("on_SRA"), cols=10, rows=20, width=190, str_len=100)

probably_not_on_sra = Ranchero.hella_flat(merged.filter(pl.col("on_SRA") == False), force_index="filename")
probably_not_on_sra = Ranchero.NeighLib.drop_null_columns(probably_not_on_sra, and_non_null_type_full_of_nulls=True)
if probably_not_on_sra.height > 0:
	# Generate data table to put on SRA by merging back with input TSV
	slap_that_on_sra = Ranchero.merge_dataframes(
		left=probably_not_on_sra.select("filename"), right=metadata,
		merge_upon="filename", force_index="filename", drop_exclusive_right=True).drop('collection')
	assert slap_that_on_sra.height == probably_not_on_sra.height
	Ranchero.to_tsv(slap_that_on_sra, "upload_candidates.tsv")
	if verbose:
		print("Samples that don't seem to be on SRA (wrote a to-upload table to upload_candidates.tsv):")
		Ranchero.dfprint(probably_not_on_sra.sort("filename"), cols=10, rows=-1, width=190, str_len=100)
	else:
		print(f"{probably_not_on_sra.height} files need to be uploaded, wrote to upload_candidates.tsv")
		exit(1)
	
else:
	print("Everything seems to be on SRA!")

# no flatten or null drop here because we want it to be like input dataframe as much as possible
probably_on_sra = merged.filter(pl.col("on_SRA") == True)
if probably_on_sra.height > 0:
	assert probably_on_sra.height == metadata.height
	Ranchero.to_tsv(probably_on_sra.drop(['collection', 'on_SRA', 'is_fail']), "data_table_with_ids.tsv")
	Ranchero.to_tsv(probably_on_sra.select(['sample_ID', 'filename', 'accession']), "readme_filewise.tsv")
	if verbose:
		print("Samples that DO seem to be on SRA (wrote to data_table_with_ids.tsv and readme_filewise.tsv):")
		Ranchero.dfprint(probably_on_sra.sort("filename"), cols=10, rows=-1, width=190, str_len=100)
	else:
		print(f"All{probably_on_sra.height} files are already on SRA, wrote to data_table_with_ids.tsv and readme_filewise.tsv")
else:
	print("None of the provided files are on SRA!")

