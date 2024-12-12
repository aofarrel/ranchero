import time
start = time.time()
import gc
import src as Ranchero
_b_ = "\033[1m"
_bb_ = "\033[0m"
print(f"Module import time: {time.time() - start:.4f}")
slim = False
start_from_scratch = True
inject = True
do_run_index_merges = True

module_start = time.time()

def inital_file_parse():
	#we don't immediately rancheroize as this is faster (probably)
	start, tba6 = time.time(),Ranchero.from_bigquery("./inputs/tba6_no_tax_table_bq_2024-09-19.json")
	print(f"Parsed tba6 file from bigquery in {time.time() - start:.4f} seconds")  # should be under five minutes for tba5, less for tba6
	start, tba6 = time.time(), Ranchero.drop_non_tb_columns(tba6)
	print(f"Dropped non-TB-related columns in {time.time() - start:.4f} seconds")

	tba6 = tba6.drop(['center_name', 'insdc_center_name_sam']) # these are a pain in the neck to standardize and not necessary for the tree
	
	# initial rancheroize
	start, tba6 = time.time(), Ranchero.rancheroize(tba6)
	print(f"Rancheroized in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.standardize_everything(tba6)
	print(f"Standardized in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.drop_lowcount_columns(tba6)
	print(f"Removed columns with few values in {time.time() - start:.4f}s seconds") # should be done last

	Ranchero._NeighLib.print_a_where_b_is_foo(tba6, "date_collected", "sample_index", "SAMEA110052021")

	print(tba6.estimated_size(unit='mb'))

	# move to demo.py
	#print(Ranchero.unique_bioproject_per_center_name(tba6))
	#Ranchero.print_a_where_b_is_null(tba6, 'region', 'country')
	#Ranchero.print_a_where_b_is_foo(tba6, 'country', 'BioProject', 'PRJEB9680', valuecounts=True)
	#Ranchero.NeighLib.print_value_counts(tba6, ['country'])


	# READD THE RM ALL PHAGES

	#start, tba6 = time.time(), Ranchero.classify_bacterial_family(tba6)
	#print(f"Classified bacterial family in {time.time() - start:.4f} seconds")

	start = time.time()
	Ranchero.to_tsv(tba6, "tba6_no_nonsense.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f} seconds")
	return tba6

def inject_metadata(tba6):
	bioproject_injector = Ranchero.injector_from_tsv("./inputs/literature_shorthands_ACTUALLY_LEGIT - injector.tsv")
	bovis_time = Ranchero.injector_from_tsv("./inputs/overrides/PRJEB18668 - good.tsv")
	host_overrides = Ranchero.injector_from_tsv("./inputs/overrides/host_overrides.tsv")
	#norway = Ranchero.injector_from_tsv("./inputs/overrides/PRJEB12184 - good.tsv", drop_columns=["literature_lineage"])
	PRJNA575883p1 = Ranchero.injector_from_tsv("./inputs/overrides/PRJNA575883p1.tsv")
	PRJNA575883p2 = Ranchero.injector_from_tsv("./inputs/overrides/PRJNA575883p2.tsv")
	imrl = Ranchero.injector_from_tsv("./inputs/overrides/IMRL/IMRL.csv")

	# I've already verified that injecting per-biosample vs per-bioproject doesn't make a difference in output, at least
	# when it comes to BioProject PRJEB2138 getting set to Russia 
	null = Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, None, 'country')
	print(f"{_b_}Prior to injecting, we have {null} samples with no value for country{_bb_}")
	start = time.time()
	tba6 = Ranchero.inject_metadata(tba6, bioproject_injector, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, bovis_time, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, host_overrides, overwrite=True)
	#tba6 = Ranchero.inject_metadata(tba6, norway, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, PRJNA575883p1, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, PRJNA575883p2, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, imrl, overwrite=True)

	print(f"{_b_}Injected metadata in {time.time() - start:.4f} seconds{_bb_}")
	null = Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, None, 'country')
	print(f"{_b_}After injecting, we have {null} samples with no value for country{_bb_}")

	print(f"Nulls in sample_index: {Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, None, 'sample_index')}")
	start = time.time()
	Ranchero.to_tsv(tba6, "tba6_injected.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f}s seconds")

	Ranchero.NeighLib.print_a_where_b_is_foo(tba6, "host", "sample_index", "SAMN16755333")
	Ranchero.NeighLib.print_a_where_b_is_foo(tba6, "region", "sample_index", "SAMN16755333")
	Ranchero.NeighLib.print_a_where_b_is_foo(tba6, "date_collected", "sample_index", "SAMN16755333")
	Ranchero.NeighLib.print_a_where_b_is_foo(tba6, "latlon", "sample_index", "SAMN16755333")

	gc.collect()

	return tba6


def run_merges(tba6):
	merged = tba6

	# Brites and Bos
	print(f"{_b_}Processing Brites{_bb_}")
	brites = Ranchero.from_tsv("./inputs/publications/run_indexed/Brites_2018/brites_cleaned.tsv", 
		auto_rancheroize=True, explode_upon=";", 
		drop_columns=['country']) # several incorrect countries
	#brites = brites.with_columns(sample_index=None)
	#brites = brites.with_columns(country=None)
	#brites = brites.with_columns(region=None)
	#seals = Ranchero.injector_from_tsv("./inputs/overrides/pinnipeds_and_friends.tsv")
	#bos = Ranchero.injector_from_tsv("./inputs/overrides/Bos_2015/ancient.tsv")
	#brites = Ranchero.inject_metadata(Ranchero.inject_metadata(brites, bos, overwrite=True), seals, overwrite=True)
	#brites = Ranchero.standardize_countries(brites)
	merged = Ranchero.merge_dataframes(merged, brites, merge_upon="run_index", left_name="tba6", right_name="Brites",
		drop_exclusive_right=True) # we have to do this or else run-to-sample breaks

	# Coll
	print(f"{_b_}Processing Coll{_bb_}")
	coll = Ranchero.from_tsv("./inputs/publications/run_indexed/Coll_2018/coll_processed.tsv", auto_rancheroize=True)
	coll = Ranchero.NeighLib.add_column_of_just_this_value(coll, "pheno_source", "Coll_2018")
	merged = Ranchero.merge_dataframes(merged, coll, merge_upon="run_index", left_name="tba6", right_name="Coll", drop_exclusive_right=True)

	# Coscolla
	print(f"{_b_}Processing Coscolla{_bb_}")
	coscolla = Ranchero.standardize_countries(Ranchero.from_tsv("./inputs/publications/run_indexed/Coscolla_2021/coscolla_sans_weird.tsv", explode_upon=";", auto_rancheroize=True))
	merged = Ranchero.merge_dataframes(merged, coscolla, merge_upon="run_index", left_name="tba6", right_name="Coscolla", drop_exclusive_right=True)

	# CRyPTIC Reuse Table (NOT WALKER 2022)
	print(f"{_b_}Processing CRyPTIC reuse table{_bb_}")
	CRyPTIC = Ranchero.from_tsv("./inputs/publications/run_indexed/CRyPTIC reuse table/CRyPTIC_reuse_table_20240917.csv", delimiter=",", auto_rancheroize=True,
		explode_upon=".",
		drop_columns=["AMI_MIC","BDQ_MIC","CFZ_MIC","DLM_MIC","EMB_MIC","ETH_MIC","INH_MIC","KAN_MIC","LEV_MIC","LZD_MIC","MXF_MIC","RIF_MIC","RFB_MIC",
		"AMI_PHENOTYPE_QUALITY","BDQ_PHENOTYPE_QUALITY","CFZ_PHENOTYPE_QUALITY","DLM_PHENOTYPE_QUALITY","EMB_PHENOTYPE_QUALITY","ETH_PHENOTYPE_QUALITY",
		"INH_PHENOTYPE_QUALITY","KAN_PHENOTYPE_QUALITY","LEV_PHENOTYPE_QUALITY","LZD_PHENOTYPE_QUALITY","MXF_PHENOTYPE_QUALITY","RIF_PHENOTYPE_QUALITY",
		"RFB_PHENOTYPE_QUALITY","ENA_SAMPLE","VCF","REGENOTYPED_VCF"])
	CRyPTIC = Ranchero.NeighLib.add_column_of_just_this_value(CRyPTIC, "pheno_source", "CRyPTIC_reuse_PMC9363010")
	merged = Ranchero.merge_dataframes(merged, CRyPTIC, merge_upon="run_index", left_name="tba6", right_name="CRyPTIC", drop_exclusive_right=True)

	# Merker (two of them...)
	print(f"{_b_}Processing the run-indexed part of Merker 2022{_bb_}")
	merker = Ranchero.from_tsv("./inputs/publications/run_indexed/Merker_2022 (run)/Merker_clean_run_indeces.tsv", auto_rancheroize=True)
	merker = Ranchero.NeighLib.add_column_of_just_this_value(merker, "pheno_source", "Merker_2022_PMC9426364")
	merged = Ranchero.merge_dataframes(merged, merker, merge_upon="run_index", left_name="tba6", right_name="Merker_2022", drop_exclusive_right=True)
	# DONT FORGET THE OTHER MERKER PAPER, AND ALSO THE SAMPLE-INDEXED PART
	
	# Napier
	print(f"{_b_}Processing Napier{_bb_}")
	napier = Ranchero.from_tsv("./inputs/publications/run_indexed/Napier_2020/napier_samples_github_sans_weird.csv", delimiter=",", explode_upon="_", auto_rancheroize=True)
	napier = Ranchero.standardize_countries(napier)
	merged = Ranchero.merge_dataframes(merged, napier, merge_upon="run_index", right_name="Napier", drop_exclusive_right=True)
	print(f"Nulls in sample_index: {Ranchero._NeighLib.get_count_of_x_in_column_y(merged, None, 'sample_index')}")

	# Nimmo
	print(f"{_b_}Processing Nimmo{_bb_}")
	# Stucki
	print(f"{_b_}Processing Stucki{_bb_}")
	stucki = Ranchero.from_tsv("./inputs/publications/run_indexed/Stucki_2016 (run)/Stucki_2016 - cleaned - by run.tsv", auto_rancheroize=True)
	merged = Ranchero.merge_dataframes(merged, stucki, merge_upon="run_index", right_name="Stucki", drop_exclusive_right=True)

	# Walker (CRyPTIC cross-study pheno table, NOT THE REUSE TABLE!!)
	print(f"{_b_}Processing Walker pheno data (WHO2021, CRyPTIC-associated, PMC7612554, only the run-indexed samples){_bb_}")
	walker = Ranchero.from_tsv("./inputs/publications/run_indexed/Walker_2022-CRyPTIC-run/runindexed_Walker2022_pheno_WHO2021_CRyPTIC_PMC7612554.tsv", auto_rancheroize=True)
	walker = Ranchero.NeighLib.add_column_of_just_this_value(walker, "pheno_source", "Walker_2022_PMC7612554")
	walker = Ranchero.standardize_countries(walker)
	start, merged = time.time(), Ranchero.merge_dataframes(merged, walker, merge_upon="run_index", right_name="Walker_2022", indicator="collection", fallback_on_left=True, escalate_warnings=False)
	print(f"Merged with run-based Walker in {time.time() - start:.4f} seconds")


	# the Nextstrain tree
	print(f"{_b_}Processing Nextstrain tree{_bb_}")
	nextstrain = Ranchero.from_tsv("./inputs/nextstrain_fixed_metadata.tsv", auto_rancheroize=True, explode_upon=";",
		# dates unreliable, antibiotic data mostly null
		drop_columns=["date_collected", 'Pyrazinamide','Capreomycin','Ethambutol','Rifampicin','Isoniazid','Ethionamide','Streptomycin','Pyrazinamide','Fluoroquinolones','Kanamycin','Amikacin'])
	nextstrain = Ranchero.standardize_everything(nextstrain)
	merged = Ranchero.merge_dataframes(merged, nextstrain, merge_upon="run_index", right_name="nextstrain", drop_exclusive_right=True)
	print(f"{_b_}Finishing up run-based stuff...{_bb_}")
	merged = Ranchero.rancheroize(merged)
	start = time.time()
	Ranchero.to_tsv(merged, "./merged_by_run.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f}s seconds")

	print(merged.estimated_size(unit='mb'))

	gc.collect()
	return merged




########################################################################




if start_from_scratch:
	tba6 = inital_file_parse()
else:
	start, tba6 = time.time(), Ranchero.from_tsv("tba6_no_nonsense.tsv")
	print(f"Imported tba6 file without extremely irrelevant columns in {time.time() - start:.4f} seconds")

if inject:
	tba6 = inject_metadata(tba6)
else:
	start, tba6 = time.time(), Ranchero.from_tsv("tba6_injected.tsv")
	print(f"Imported tba6 file with injections in {time.time() - start:.4f} seconds")

if do_run_index_merges:
	merged = run_merges(tba6)
else:
	start, merged = time.time(), Ranchero.from_tsv("merged_by_run.tsv")
	print(f"Imported run-indexed tba6 file, with some merges, in {time.time() - start:.4f} seconds")

if slim:
	boooooo = merged.columns
	merged = merged.drop([boo for boo in boooooo if boo not in ['collection', 'run_index', 'sample_index']])

merged = merged.drop(['atc_sam', 'bdq_mic_sam', 'total_bases_run'])

print(merged.estimated_size(unit='mb'))

print("Columns so far:")
print(merged.columns)

print("By type:")
Ranchero.NeighLib.print_value_counts(merged, ['mycobact_type'])

print("Checking sample source...")
Ranchero.NeighLib.print_value_counts(tba6, ['sample_source'])

#Ranchero.print_col_where(tba6, 'sample_index', 'SAMN02360560') # TODO: don't remember why this is here or why it is empty




# merged with sample-indexed data
start, merged_flat = time.time(), Ranchero.hella_flat(merged)
print(f"Flattened everything in {time.time() - start:.4f} seconds")

gc.collect()
start, merged_by_sample = time.time(),Ranchero.run_index_to_sample_index(merged_flat)

gc.collect()
print(f"Converted run indeces to sample indeces in {time.time() - start:.4f} seconds")
#Ranchero.to_tsv(merged_by_sample, "./merged_per_sample_not_flat.tsv")

start, merged_by_sample = time.time(), Ranchero.hella_flat(merged_by_sample)
print(f"Flattened samples in {time.time() - start:.4f} seconds")
Ranchero.to_tsv(merged_by_sample, "./merged_per_sample.tsv")

# atypical
#print(f"{_b_}Processing atypical samples{_bb_}")
#atypical = Ranchero.from_tsv("./inputs/atypical.tSV", auto_rancheroize=True)
#merged = Ranchero.merge_dataframes(merged, atypical, merge_upon="sample_index", left_name="tba6", right_name="atypical_genotypes", drop_exclusive_right=False)

print(merged_by_sample.estimated_size(unit='mb'))
merged = merged_by_sample
gc.collect()

# Andres
# Bateson
# Eldholm 
# Finci
print(f"{_b_}Processing Finci{_bb_}")
Finci_pheno = Ranchero.from_tsv("./inputs/publications/sample_indexed/Finci_2022 (PRJEB48275)/PRJEB48275_Finci_plus_pheno.tsv", 
	drop_columns=["pheno_WHO_resistance","pheno_INH_MIC","pheno_RMP_MIC","pheno_EMB_MIC","pheno_PZA_MIC","pheno_AMK_MIC","pheno_CAP_MIC",
	"pheno_KAN_MIC","pheno_MFX_MIC","pheno_LFX_MIC"], auto_rancheroize=True)
Finci_pheno = Ranchero.standardize_countries(Finci_pheno)
Finci_pheno = Ranchero.NeighLib.add_column_of_just_this_value(Finci_pheno, "pheno_source", "Finci_2022_PMC9436784")
start, merged = time.time(), Ranchero.merge_dataframes(merged, Finci_pheno, merge_upon="sample_index", right_name="finci_2022", indicator="collection")


# Menardo (two of them...)
print(f"{_b_}Processing Menardo (two of them){_bb_}")
start = time.time()
menardo_2018 = Ranchero.from_tsv("./inputs/publications/sample_indexed/Menardo_2018/menardo_2018_processed.csv", delimiter=',', auto_rancheroize=True)
menardo_2021 = Ranchero.from_tsv("./inputs/publications/sample_indexed/Menardo_2021/menardo_REAL.tsv", auto_rancheroize=True)
merged = Ranchero.merge_dataframes(merged_by_sample, menardo_2018, merge_upon="sample_index", right_name="menardo_2018", indicator="collection")
merged = Ranchero.merge_dataframes(merged_by_sample, menardo_2021, merge_upon="sample_index", right_name="menardo_2021", indicator="collection", escalate_warnings=False)
print(f"Merged with menardo in {time.time() - start:.4f} seconds")

# Merker (sample side of 2022)
# Shuaib

# standford data
print(f"{_b_}Processing Standford{_bb_}")
standford_1 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/max_standford_YYYY-MM.tsv", auto_rancheroize=True)))
standford_2 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/max_standford_YYYY-MM-DD.tsv", auto_rancheroize=True)))
standford_3 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/max_standford_DD-MM-YYYY.tsv", auto_rancheroize=True))) # this one should NOT overwrite left
standford_4 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/max_standford_slashdates.tsv", auto_rancheroize=True))) # ditto
start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_1, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=False, escalate_warnings=False)
print(f"Merged with standford1 in {time.time() - start:.4f} seconds")
start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_2, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=False, escalate_warnings=False)
print(f"Merged with standford2 in {time.time() - start:.4f} seconds")
start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_3, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=True, escalate_warnings=False)
print(f"Merged with standford3 in {time.time() - start:.4f} seconds")
start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_4, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=True, escalate_warnings=False)
print(f"Merged with standford4 in {time.time() - start:.4f} seconds")
start, merged = time.time(), Ranchero.cleanup_dates(merged)
print(f"Cleaned up dates so far in {time.time() - start:.4f} seconds")

# Walker (CRyPTIC big pheno table)
print(f"{_b_}Processing Walker pheno data (WHO2021, CRyPTIC, PMC7612554, only the sample-indexed samples){_bb_}")
walker = Ranchero.from_tsv("./inputs/publications/sample_indexed/Walker_2022-CRyPTIC-samp/sampleindexed_Walker2022_pheno_WHO2021_CRyPTIC_PMC7612554.tsv", auto_rancheroize=True, explode_upon=" ")
walker = Ranchero.NeighLib.add_column_of_just_this_value(walker, "pheno_source", "CRyPTIC_WHO2021_PMC7612554")
start, merged = time.time(), Ranchero.merge_dataframes(merged_by_sample, walker, merge_upon="sample_index", right_name="Walker_2022", indicator="collection")
print(f"Merged with sample-based Walker in {time.time() - start:.4f} seconds")


# input lists
start = time.time()
print(f"{_b_}Processing inputs{_bb_}")
tba3 = Ranchero.from_tsv("./inputs/tba3_redo.tsv", auto_rancheroize=True)
july_2024_valid = Ranchero.from_tsv("./inputs/2024-06-25-valid-samples-with-diff.tsv", auto_rancheroize=True)
merged = Ranchero.merge_dataframes(merged, tba3, merge_upon="sample_index", right_name="input_tba3", indicator="collection")
merged = Ranchero.merge_dataframes(merged, july_2024_valid, merge_upon="sample_index", right_name="input_others", indicator="collection")
print(f"Merged with tba3 and july_2024_valid in {time.time() - start:.4f} seconds")



print(f"{_b_}Processing denylist{_bb_}")
denylist = Ranchero.from_tsv("./inputs/denylist_2024-07-23_lessdupes.tsv", auto_rancheroize=True)
start, merged = time.time(), Ranchero.merge_dataframes(merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection")
print(f"Merged with denylist in {time.time() - start:.4f} seconds")

#PRJEB9680 = Ranchero.from_tsv"./inputs/PRJEB9680 superset.tsv", auto_rancheroize=True)
#start = time.time()
#merged = Ranchero.merge_dataframes(merged_by_sample, PRJEB9680, merge_upon="sample_index", right_name="PRJEB9680_superset", indicator="collection")
#print(f"Merged with PRJEB9680 (just to mark that superset BioProject in the indicator column) in {time.time() - start:.4f} seconds")


Ranchero.to_tsv(merged, "./ranchero_partial_rc4.tsv")
Ranchero.print_schema(merged)
Ranchero.NeighLib.print_value_counts(merged, ['libraryselection'])
Ranchero.NeighLib.print_value_counts(merged, ['host_scienname', 'host_confidence', 'host_streetname'])
Ranchero.NeighLib.big_print_polars(merged, "merged hosts and dates", ['sample_index', 'date_collected', 'host_scienname', 'lineage'])
#Ranchero.NeighLib.big_print_polars(merged.filter(pl.col("date_collected").str.contains(r"\d{2}/\d{2}/\d{2}")), "merged has date slashes in 2 2 2 format", ['sample_index', 'date_collected'])
#Ranchero.NeighLib.big_print_polars(merged.filter(pl.col("date_collected").str.contains(r"\d{2}/\d{2}/\d{4}")), "merged has date slashes in 2 2 4 format", ['sample_index', 'date_collected'])


exit(1)

tree_metadata_v8_rc10 = Ranchero.from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
tree_metadata_v8_rc10 = Ranchero.rancheroize(tree_metadata_v8_rc10)
tree_metadata_v8_rc10.drop(['BioProject', 'isolation_source', 'host']) # we are parsing these directly from SRA now
print(f"Finished reading a bunch more metadata in  {time.time() - start:.4f} seconds")
start = time.time()
merged = Ranchero.merge_dataframes(merged, tree_metadata_v8_rc10, merge_upon="sample_index", right_name="tree_metadata_v8_rc10", indicator="collection", fallback_on_left=False)
print(f"Merged with old tree metadata file in {time.time() - start:.4f} seconds")
Ranchero.NeighLib.big_print_polars(tree_metadata_v8_rc10, "v8rc10 hosts and dates", ['sample_index', 'date_collected', 'host'])

print(f"Finished entire module in {time.time() - module_start} seconds")


