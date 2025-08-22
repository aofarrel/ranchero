VERBOSE = False
JSON_PATH = "/Users/aofarrel/Downloads/bq-results-20250801-215940-1754085716223.json"
OUT_PATH = "./my_very_cool_c_auris_metadata.json"

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
json = ranchero.from_bigquery(JSON_PATH, auto_rancheroize=False, normalize_attributes=True, auto_standardize=False)
print(f"Read BigQuery JSON in {time.time() - start:.4f} seconds")

# this dumps some information that will help me make the standardization functions faster
if VERBOSE:
	ranchero.dfprint(pl.Series(json.select("isolate_sam_ss_dpl100")).value_counts(sort=True, parallel=True, normalize=True))
	for kolumn in ranchero.statics.kolumns.equivalence["geoloc_info"]:
		if kolumn in json.columns:
			ranchero.dfprint(pl.Series(json.select(kolumn)).value_counts(sort=True, parallel=True, normalize=True))
		else:
			print(f"{kolumn} not in dataframe")
	for kolumn in ranchero.statics.kolumns.equivalence["isolation_source"]:
		if kolumn in json.columns:
			ranchero.dfprint(pl.Series(json.select(kolumn)).value_counts(sort=True, parallel=True, normalize=True))
		else:
			print(f"{kolumn} not in dataframe")


start, json = time.time(), ranchero.rancheroize(json)
print(f"Rancheroized in {time.time() - start:.4f} seconds")
start, json = time.time(), ranchero.standardize_countries(json)
print(f"Standardized countries in {time.time() - start:.4f} seconds")
start, json = time.time(), ranchero.cleanup_dates(json)
print(f"Standardized dates in {time.time() - start:.4f} seconds")
start, json = time.time(), ranchero.standardize_sample_source(json)
print(f"Standardized sample source in {time.time() - start:.4f} seconds")
start, json = time.time(), ranchero.standardize_hosts(json)
print(f"Standardized hosts in {time.time() - start:.4f} seconds")

if VERBOSE:
	ranchero.dfprint(json.select(ranchero.valid_cols(json, ['__index__acc', 'BioProject', 'date_collected', 'host_scienname', 'isolation_source', 'isolation_source_raw', 'continent', 'country', 'region'])))
ranchero.to_tsv(json, OUT_PATH)

