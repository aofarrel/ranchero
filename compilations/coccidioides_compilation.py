# Standardize metadata for the Coccidioides genus, best known for "valley fever"
import sys
rc = sys.argv[1]
import polars as pl
import time
start = time.time()
import ranchero
print(f"Module import time: {time.time() - start:.4f}")
ranchero.Configuration.set_config({"loglevel": 30})

start, df = time.time(),ranchero.from_bigquery("./inputs/BQ/FUNGUS/Coccidioides_BQ_2025-10-10.json",
	auto_rancheroize=False)
print(f"Parsed Coccidioides file from bigquery in {time.time() - start:.4f} seconds")

# initial rancheroize
start, df = time.time(), ranchero.rancheroize(df, drop_unwanted_columns=True)
print(f"Rancheroized in {time.time() - start:.4f} seconds")

start, df = time.time(), ranchero.standardize_everything(df)
print(f"Standardized in {time.time() - start:.4f} seconds")

start, df = time.time(), ranchero.NeighLib.drop_mostly_null_cols(df, minimum_count=5, minimum_pct=0)
print(f"Dropped mostly null columns in {time.time() - start:.4f} seconds")

ranchero.NeighLib.report(df)

ranchero.dfprint(df.filter(pl.col('isolation_source_raw').is_not_null()).select(['isolation_source_raw', 'isolation_source_cleaned']), rows=10000)
ranchero.NeighLib.print_value_counts(df, ['continent', 'country', 'region'])
ranchero.NeighLib.print_value_counts(df, ['top_taxon'])

print("Non-North-American samples (top taxon is genus-level and determined by NCBI, not submitter)")
ranchero.dfprint(df.filter(
	(pl.col('continent') != pl.lit('North America'))
	.and_(pl.col('continent').is_not_null()
)).select(['continent', 'country', 'top_taxon', 'top_pct', 'runner_up_taxa', 'submitted_organism' , 'date_sequenced']), rows=1000)

df = df.filter(pl.col("top_taxon") == pl.lit("Coccidioides"))
print(f"When filtered by top taxon, we have {df.shape[0]} samples")

ranchero.to_tsv(df, f"Coccidioides_genus_SRA_filtered_rc{rc}.tsv")
ranchero.to_tsv(df.select("__index__run_id"), f"Coccidioides_genus_SRA_filtered_rc{rc}_just_samples.tsv")