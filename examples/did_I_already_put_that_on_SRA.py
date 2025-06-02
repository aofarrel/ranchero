# See if stuff in a metadata TSV file have already been upload to SRA by matching on filename.
# Usage: did_I_already_put_that_on_SRA_simple.py <edirect_XML> <metadata_tsv> <verbose>
#       <edirect_XML>: XML from Entrez Direct search of SRA, assuming -db sra
#       <metadata_tsv>: a TSV of stuff you want on SRA that, at a minimum, includes a "filename" column
#       <verbose>: Optional -v flag to print intermediate dataframes

import sys
import src as Ranchero
import polars as pl

if len(sys.argv) > 4:
	if sys.argv[3] not in ['-v', '-verbose', '--verbose']:
		print(f"Can't parse third argument: {sys.argv[3]}")
	else:
		verbose = True
else:
	verbose = False

# Parse XML file to get what's already on SRA
edirect = Ranchero.from_efetch(sys.argv[1], index_by_file=True)
edirect = edirect.rename({'submitted_files':'filename'}).drop(['notes'])

# We want to merge this with our own TSVs on the filename column, but there's a possibility
# of duplicate filenames on SRA, which causes issues when merging. This approach fixes that.
edirect_by_filename = edirect.group_by("filename").agg(
	[pl.col('run_index'), pl.col('submitted_files_gibytes'), pl.col('alias')]
)
edirect_by_filename = Ranchero.hella_flat(edirect_by_filename, force_index="filename")

# Unfortunately, sometimes SRA changes the name of files upon upload. It seems that when this
# happens, it saves the old filename in the "alias" column, which is typically a list at this point.
if edirect_by_filename.schema["alias"] == pl.List:
	edirect_by_alias = edirect.explode("alias")
	edirect_by_alias = edirect_by_alias.filter(pl.col('alias') != pl.col('filename'))
else:
	edirect_by_alias = edirect.filter(pl.col('alias') != pl.col('filename'))

# Like we did with the filenames, handle dupes among the aliases
edirect_by_alias = edirect_by_alias.group_by("alias").agg(
	[pl.col('run_index'), pl.col('submitted_files_gibytes')]
)
edirect_by_alias = Ranchero.hella_flat(edirect_by_alias, force_index="alias")

# Parse TSV file of metadata
# If it's not indexed by filename, you'll want to add an explode() in here
metadata = Ranchero.from_tsv(sys.argv[2], check_index=False, auto_rancheroize=False)
metadata = Ranchero.NeighLib.check_index(metadata, manual_index_column='filename')
if verbose:
	print("Metadata dataframe:")
	Ranchero.dfprint(metadata.sort("filename"), cols=5, rows=20, width=190, str_len=100)

# Merge with SRA table upon the "filename" column
upon_filename = Ranchero.merge_dataframes(
	left=metadata, right=edirect_by_filename, 
	left_name="metadata_table", right_name="SRA_table",
	merge_upon="filename", force_index="filename", drop_exclusive_right=True)

# Unfortunately we also need to merge on the "alias" column because some files get
# their name changed when uploaded to SRA!
upon_alias = Ranchero.merge_dataframes(
	left=metadata.rename({"filename": "alias"}), right=edirect_by_alias, 
	left_name="metadata_table", right_name="SRA_table",
	merge_upon="alias", force_index="alias", drop_exclusive_right=True)

Ranchero.dfprint(edirect_by_filename, cols=10, rows=20, width=190, str_len=100)
Ranchero.dfprint(edirect_by_alias, cols=10, rows=20, width=190, str_len=100)
exit(1)







# Mark rows of files not on SRA
merged = merged.with_columns(
	pl.when(pl.col('run_index').is_null())
	.then(False).otherwise(True).alias("on_SRA")
)

# Also mark rows with "fail" in filename, which we may or may not want on SRA
merged = merged.with_columns(
	pl.when(pl.col('filename').str.contains("fail"))
	.then(True).otherwise(False).alias("is_fail")
)

if verbose:
	print("Merged dataframe")
	Ranchero.dfprint(merged.sort("on_SRA"), cols=10, rows=20, width=190, str_len=100)

probably_not_on_sra = Ranchero.hella_flat(merged.filter(pl.col("on_SRA") == False), force_index="filename")
probably_not_on_sra = Ranchero.NeighLib.drop_null_columns(probably_not_on_sra, and_non_null_type_full_of_nulls=True)
if probably_not_on_sra.height > 0:
	print("Stuff probably not on SRA")
	Ranchero.dfprint(probably_not_on_sra.sort("filename").drop(
		[c for c in probably_not_on_sra.columns if c != 'filename']
	), cols=10, rows=-1, width=190, str_len=100)
	Ranchero.to_tsv(probably_not_on_sra, "probably_not_on_sra.tsv")
else:
	print("Everything seems to be on SRA!")

if verbose:
	if "production" in merged.columns:
		print("Not on SRA, grouped by production")
		Ranchero.dfprint(probably_not_on_sra.group_by(pl.col("production"))
			.agg([pl.col('filetype').unique(), pl.col('filename').len()])
			.rename({'filename': '# of files'})
			.sort("production"))

		print("On SRA, grouped by production")
		probably_on_sra = Ranchero.hella_flat(merged.filter(pl.col("on_SRA") == True), force_index="filename")
		Ranchero.dfprint(probably_on_sra.group_by(pl.col("production"))
			.agg([pl.col('filetype').unique(), pl.col('filename').len()])
			.rename({'filename': '# of files'})
			.sort("production"))

