import sys
import src as Ranchero
import polars as pl

polars_df = Ranchero.from_tsv(sys.argv[1], check_index=False, auto_rancheroize=False)

if 'biosample_accession' not in polars_df:
	if 'biosample' in polars_df:
		polars_df = polars_df.rename({"biosample": "biosample_accession"})
	if 'BioSample' in polars_df:
		polars_df = polars_df.rename({"BioSample": "biosample_accession"})

if 'biosample_accession' not in polars_df:
	# still no biosample column? add one based on sample ID
	polars_df = Ranchero.translate_HPRC_IDs(polars_df, "sample_ID", "biosample_accession")
	print("Generated biosample_accession column from known HPRC sample_IDs")
else:
	# double check prexisting HPRC sample_IDs:BioSample connection
	potential_man = Ranchero.translate_HPRC_IDs(polars_df, "sample_ID", "regenerated_biosample")
	mismatches = potential_man.filter(pl.col("regenerated_biosample") != pl.col("biosample_accession"))
	if mismatches.height > 0:
		print("ERROR - translated HPRC IDs but they don't match what's already on the dataframe!")
		Ranchero.dfprint(mismatches.select(["filename", "sample_ID", "biosample_accession", "regenerated_biosample"]))
		exit(1)
	else:
		print("Found existing BioSamples which all seem to match HPRC sample IDs")

polars_df = polars_df.with_columns(
	pl.when(pl.col("generator_facility") == "University of California, Santa Cruz")
	.then(pl.lit("UC Santa Cruz Genomics Institute"))
	.otherwise(pl.col("generator_facility"))
	.alias("generator_facility"))
print("Standardized submitter name")

if 'notes' in polars_df.columns:
	if polars_df.filter(pl.col('notes').is_not_null()).height == 0:
		print("Dropped notes column since all of its values are null")
		polars_df = polars_df.drop('notes')

sadness = False
for col in polars_df.columns:
	rows_with_null_in_col = polars_df.filter(pl.col(col).is_null())
	if rows_with_null_in_col.height != 0:
		sadness = True
		print(f"{rows_with_null_in_col.height} out of {polars_df.height} rows are null at column {col}:")
		print(rows_with_null_in_col.select(['filename', col]))
if sadness:
	exit(1)
else:
	print("No columns with null values found")

print("Wrote to upload_candidates_checked.tsv")
Ranchero.to_tsv(polars_df, "upload_candidates_checked.tsv") 