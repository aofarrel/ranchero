import sys
rc = sys.argv[1]
VERBOSE = True
JSON_PATH = "./inputs/BQ/FUNGUS/c_auris_bq-results-20250801-215940-1754085716223.json_modified.json"
OUT_PATH = f"../my_very_cool_c_auris_metadata_rc{rc}.tsv"
INDEX_BY_BIOSAMPLE = False

import time
start = time.time()
import polars as pl
print(f"Imported polars in {time.time() - start:.4f} seconds")
start = time.time()
#import ranchero
from src import ranchero
print(f"Imported ranchero in {time.time() - start:.4f} seconds")

if VERBOSE:
	ranchero.Configuration.set_config({"loglevel": 10})
else:
	ranchero.Configuration.set_config({"loglevel": 20})


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

start, polars_df = time.time(), ranchero.standardize_isolation_source(polars_df)
print(f"Standardized sample source in {time.time() - start:.4f} seconds")
if VERBOSE:
	ranchero.NeighLib.print_value_counts(polars_df.select(['isolation_source', 'isolation_source_raw']))
	print(polars_df.filter(pl.col('sample_id') == pl.lit('SAMEA117586178')).select(['sample_id', 'isolation_source', 'isolation_source_raw', 'isolate_sam_ss_dpl100_raw']))
	isolation_source_was_changed_interestingly = polars_df.filter(
		(~pl.col('isolation_source').is_in(ranchero.statics.sample_sources.standardized_values))
		.and_(pl.col('isolation_source_raw').str.len_bytes() == pl.col('isolation_source').str.len_bytes())
		#.and_(~pl.col('isolation_source').str.contains('axilla'))
	)
	ranchero.super_print(isolation_source_was_changed_interestingly.select(['sample_id', 'isolation_source', 'isolation_source_raw', 'isolate_sam_ss_dpl100_raw']), "How isolation source changed")

start, polars_df = time.time(), ranchero.standardize_host_disease(polars_df)
print(f"Standardized host_disease in {time.time() - start:.4f} seconds")

start, polars_df = time.time(), ranchero.standardize_hosts(polars_df)
print(f"Standardized hosts in {time.time() - start:.4f} seconds")
if VERBOSE:
	ranchero.dfprint(
		polars_df.select(
			ranchero.valid_cols(
				polars_df, [ranchero.get_index(polars_df), 'date_collected',
				'host_scienname', 'isolation_source', 'isolation_source_raw', 'continent',
				'country', 'region']
			)
		)
	)

if INDEX_BY_BIOSAMPLE:
	polars_df = ranchero.run_index_to_sample_index(polars_df)
	polars_df = polars_df.select(
		ranchero.valid_cols(
		polars_df, ["__index__sample", "run_id", "mbytes", "platform", "instrument", "BioProject",
		"organism", "assay_type", "librarylayout", "bases", "date_sequenced", "date_collected",
		"genotype_sam_ss_dpl92", "clade_sam", "host_disease", "strain_sam_ss_dpl139", "host_info",
		"country", "continent", "region", "host_scienname", "host_confidence", "host_commonname"]
		)
	)
	ranchero.dfprint(polars_df)

polars_df = polars_df.drop(['datastore_provider', 'jattr', 'a260_a230_sam', 'a260_a280_sam', 'total_volume_sam', 'primary_search'])
print(polars_df.sort('component_organism_sam').select('component_organism_sam'))

ranchero.to_tsv(polars_df.sort('__index__run_id'), OUT_PATH)




