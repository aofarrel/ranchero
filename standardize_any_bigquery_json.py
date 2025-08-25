VERBOSE = False
JSON_PATH = "/Users/aofarrel/Downloads/bq-results-20250801-215940-1754085716223.json"
OUT_PATH = "./my_very_cool_c_auris_metadata.tsv"
INDEX_BY_BIOSAMPLE = True

import time
start = time.time()
import polars as pl
print(f"Imported polars in {time.time() - start:.4f} seconds")
start = time.time()
import ranchero
print(f"Imported ranchero in {time.time() - start:.4f} seconds")

if VERBOSE:
	ranchero.Configuration.set_config({"loglevel": 20})
else:
	ranchero.Configuration.set_config({"loglevel": 40})


ranchero.Configuration.set_config({"mycobacterial_mode": False})
start = time.time()
polars_df = ranchero.from_bigquery(JSON_PATH, auto_rancheroize=False, normalize_attributes=True, auto_standardize=False)
print(f"Read BigQuery JSON in {time.time() - start:.4f} seconds")

# this dumps some information that will help me make the standardization functions faster
if VERBOSE:
	ranchero.dfprint(pl.Series(polars_df.select("isolate_sam_ss_dpl100")).value_counts(sort=True, parallel=True, normalize=True))
	for kolumn in ranchero.statics.kolumns.equivalence["geoloc_info"]:
		if kolumn in polars_df.columns:
			ranchero.dfprint(pl.Series(polars_df.select(kolumn)).value_counts(sort=True, parallel=True, normalize=True))
		else:
			print(f"{kolumn} not in dataframe")
	for kolumn in ranchero.statics.kolumns.equivalence["isolation_source"]:
		if kolumn in polars_df.columns:
			ranchero.dfprint(pl.Series(polars_df.select(kolumn)).value_counts(sort=True, parallel=True, normalize=True))
		else:
			print(f"{kolumn} not in dataframe")


start, polars_df = time.time(), ranchero.rancheroize(polars_df)
print(f"Rancheroized in {time.time() - start:.4f} seconds")
start, polars_df = time.time(), ranchero.standardize_countries(polars_df)
print(f"Standardized countries in {time.time() - start:.4f} seconds")
start, polars_df = time.time(), ranchero.cleanup_dates(polars_df)
print(f"Standardized dates in {time.time() - start:.4f} seconds")
start, polars_df = time.time(), ranchero.standardize_sample_source(polars_df)
print(f"Standardized sample source in {time.time() - start:.4f} seconds")
start, polars_df = time.time(), ranchero.standardize_hosts(polars_df)
print(f"Standardized hosts in {time.time() - start:.4f} seconds")
if VERBOSE:
	ranchero.dfprint(polars_df.select(ranchero.valid_cols(polars_df, ['__index__run', 'BioProject', 'date_collected', 'host_scienname', 'isolation_source', 'isolation_source_raw', 'continent', 'country', 'region'])))

if INDEX_BY_BIOSAMPLE:
	polars_df = ranchero.run_index_to_sample_index(polars_df)
	polars_df = polars_df.select(ranchero.valid_cols(polars_df, ["__index__sample", "run_id", "mbytes", "platform", "instrument", "BioProject", "organism", "assay_type", "librarylayout",	"bases", "date_sequenced",	"date_collected", "genotype_sam_ss_dpl92", "clade_sam",	"host_disease", "strain_sam_ss_dpl139", "host_info", "country", "continent", "region", "isolation_source", "host_scienname", "host_confidence", "host_commonname"]))
	ranchero.dfprint(polars_df)

ranchero.to_tsv(polars_df, OUT_PATH)




