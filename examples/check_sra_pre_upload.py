import sys
import src as Ranchero
import polars as pl

polars_df = Ranchero.from_tsv("upload_candidates.tsv", check_index=False, auto_rancheroize=False)

# check/fix column names
if 'biosample_accession' not in polars_df:
	if 'biosample' in polars_df:
		polars_df = polars_df.rename({"biosample": "biosample_accession"})
	if 'BioSample' in polars_df:
		polars_df = polars_df.rename({"BioSample": "biosample_accession"})
assert 'biosample_accession' in polars_df


# check HPRC sample_IDs:BioSample connection
potential_man = Ranchero.translate_HPRC_IDs(polars_df, "sample_ID", "regenerated_biosample")
mismatches = potential_man.filter(pl.col("regenerated_biosample") != pl.col("biosample_accession"))
if mismatches.height > 0:
	print("ERROR - translated HPRC IDs but they don't match what's already on the dataframe!")
	Ranchero.dfprint(mismatches.select(["filename", "sample_ID", "biosample_accession", "regenerated_biosample"]))
	exit(1)

polars_df = polars_df.with_columns(
	pl.when(pl.col("generator_facility") == "University of California, Santa Cruz")
	.then(pl.lit("UC Santa Cruz Genomics Institute"))
	.otherwise(pl.col("generator_facility"))
	.alias("generator_facility"))

print("Wrote to upload_candidates_checked.tsv")
Ranchero.to_tsv(polars_df, "upload_candidates_checked.tsv") 