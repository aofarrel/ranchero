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
sample_merges = True

module_start = time.time()

def inital_file_parse():
	#we don't immediately rancheroize as this is faster (probably)
	start, tba6 = time.time(),Ranchero.from_bigquery("./inputs/BQ/tba6_no_tax_table_bq_2024-09-19.json_modified.json")
	print(f"Parsed tba6 file from bigquery in {time.time() - start:.4f} seconds")  # should be under five minutes for tba5, less for tba6
	start, tba6 = time.time(), Ranchero.drop_non_tb_columns(tba6)
	tba6 = tba6.drop(['center_name', 'insdc_center_name_sam']) # these are a pain in the neck to standardize and not necessary for the tree
	print(f"Dropped non-TB-related columns in {time.time() - start:.4f} seconds")

	# initial rancheroize
	start, tba6 = time.time(), Ranchero.rancheroize(tba6)
	print(f"Rancheroized in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.standardize_everything(tba6)
	print(f"Have {Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, None, 'date_collected')} nulls for date_collected")
	Ranchero.NeighLib.print_value_counts(tba6, ['date_collected'])
	print(f"Standardized in {time.time() - start:.4f} seconds")
	print(f"Have {Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, None, 'date_collected')} nulls for date_collected")

	start, tba6 = time.time(), Ranchero.drop_lowcount_columns(tba6)
	print(f"Removed columns with few values in {time.time() - start:.4f}s seconds") # should be done last

	print(tba6.estimated_size(unit='mb'))

	# move to demo.py
	#print(Ranchero.unique_bioproject_per_center_name(tba6))
	Ranchero.print_a_where_b_is_null(tba6, 'region', 'country')
	if Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, 'Ivory Coast', 'country') > 0:
		exit(1)
	#Ranchero.print_a_where_b_is_foo(tba6, 'country', 'BioProject', 'PRJEB9680', valuecounts=True)
	#Ranchero.NeighLib.print_value_counts(tba6, ['country'])

	start = time.time()
	Ranchero.to_tsv(tba6, "tba6_no_nonsense.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f} seconds")
	Ranchero.NeighLib.report(tba6)
	return tba6

def inject_metadata(tba6):
	Ranchero.NeighLib.print_value_counts(tba6, ['country', 'region'])

	bioproject_injector = Ranchero.injector_from_tsv("./inputs/literature_shorthands_ACTUALLY_LEGIT - injector.tsv")
	bovis_time = Ranchero.injector_from_tsv("./inputs/overrides/PRJEB18668 - good.tsv")
	host_overrides = Ranchero.injector_from_tsv("./inputs/overrides/host_overrides.tsv")
	norway = Ranchero.injector_from_tsv("./inputs/overrides/PRJEB12184 - good.tsv", drop_columns=["literature_lineage"])
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
	tba6 = Ranchero.inject_metadata(tba6, norway, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, PRJNA575883p1, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, PRJNA575883p2, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, imrl, overwrite=True)

	Ranchero.NeighLib.print_value_counts(tba6, ['country', 'region'])
	if Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, 'Ivory Coast', 'country') > 0:
		exit(1)

	print(f"{_b_}Injected metadata in {time.time() - start:.4f} seconds{_bb_}")
	null = Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, None, 'country')
	
	print(f"{_b_}After injecting, we have {null} samples with no value for country{_bb_}")
	print(f"Nulls in sample_index: {Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, None, 'sample_index')}")
	Ranchero.NeighLib.report(tba6)


	start = time.time()
	Ranchero.to_tsv(tba6, "tba6_injected.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f}s seconds")
	gc.collect()
	return tba6


def run_merges(tba6):
	merged = tba6

	# Bos
	#bos = Ranchero.from_tsv("./inputs/publications/Bos_2015/ancient.tsv")
	#merged = Ranchero.merge_dataframes(merged, bos, merge_upon="run_index", left_name="tba6", right_name="Bos (ancient)")

	if Ranchero._NeighLib.get_count_of_x_in_column_y(merged, 'Ivory Coast', 'country') > 0:
		exit(1)

	# Brites
	print(f"{_b_}Processing Brites{_bb_}")
	brites = Ranchero.from_tsv("./inputs/publications/run_indexed/Brites_2018/brites_cleaned.tsv", 
		auto_rancheroize=True, explode_upon=";", 
		drop_columns=['country']) # several incorrect countries
	merged = Ranchero.merge_dataframes(merged, brites, merge_upon="run_index", left_name="tba6", right_name="Brites",
		drop_exclusive_right=True) # we have to do this or else run-to-sample breaks

	# Coll
	print(f"{_b_}Processing Coll{_bb_}")
	coll = Ranchero.from_tsv("./inputs/publications/run_indexed/Coll_2018/coll_processed.tsv", auto_standardize=True)
	coll = Ranchero.NeighLib.add_column_of_just_this_value(coll, "pheno_source", "Coll_2018")
	merged = Ranchero.merge_dataframes(merged, coll, merge_upon="run_index", left_name="tba6", right_name="Coll", drop_exclusive_right=True)

	# Coscolla
	print(f"{_b_}Processing Coscolla{_bb_}")
	coscolla = Ranchero.from_tsv("./inputs/publications/run_indexed/Coscolla_2021/coscolla_sans_weird.tsv", explode_upon=";", auto_standardize=True)
	merged = Ranchero.merge_dataframes(merged, coscolla, merge_upon="run_index", left_name="tba6", right_name="Coscolla", drop_exclusive_right=True)

	# CRyPTIC Reuse Table (NOT WALKER 2022)
	print(f"{_b_}Processing CRyPTIC reuse table{_bb_}")
	CRyPTIC = Ranchero.from_tsv("./inputs/publications/run_indexed/CRyPTIC reuse table/CRyPTIC_reuse_table_20240917.csv", delimiter=",", explode_upon=".",
		drop_columns=["AMI_MIC","BDQ_MIC","CFZ_MIC","DLM_MIC","EMB_MIC","ETH_MIC","INH_MIC","KAN_MIC","LEV_MIC","LZD_MIC","MXF_MIC","RIF_MIC","RFB_MIC",
		"AMI_PHENOTYPE_QUALITY","BDQ_PHENOTYPE_QUALITY","CFZ_PHENOTYPE_QUALITY","DLM_PHENOTYPE_QUALITY","EMB_PHENOTYPE_QUALITY","ETH_PHENOTYPE_QUALITY",
		"INH_PHENOTYPE_QUALITY","KAN_PHENOTYPE_QUALITY","LEV_PHENOTYPE_QUALITY","LZD_PHENOTYPE_QUALITY","MXF_PHENOTYPE_QUALITY","RIF_PHENOTYPE_QUALITY",
		"RFB_PHENOTYPE_QUALITY","ENA_SAMPLE","VCF","REGENOTYPED_VCF"], auto_standardize=True)
	CRyPTIC = Ranchero.NeighLib.add_column_of_just_this_value(CRyPTIC, "pheno_source", "CRyPTIC_reuse_PMC9363010")
	merged = Ranchero.merge_dataframes(merged, CRyPTIC, merge_upon="run_index", left_name="tba6", right_name="CRyPTIC", drop_exclusive_right=True)

	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])
	Ranchero.NeighLib.print_value_counts(merged, ['date_collected'])
	if Ranchero._NeighLib.get_count_of_x_in_column_y(merged, 'Ivory Coast', 'country') > 0:
		exit(1)

	# Merker (two of them...)
	print(f"{_b_}Processing the run-indexed part of Merker 2022{_bb_}")
	merker = Ranchero.from_tsv("./inputs/publications/run_indexed/Merker_2022 (run)/Merker_clean_run_indeces.tsv", auto_standardize=True)
	merker = Ranchero.NeighLib.add_column_of_just_this_value(merker, "pheno_source", "Merker_2022_PMC9426364")
	merged = Ranchero.merge_dataframes(merged, merker, merge_upon="run_index", left_name="tba6", right_name="Merker_2022", drop_exclusive_right=True)
	# DONT FORGET THE OTHER MERKER PAPER, AND ALSO THE SAMPLE-INDEXED PART

	# Napier
	print(f"{_b_}Processing Napier{_bb_}")
	napier = Ranchero.from_tsv("./inputs/publications/run_indexed/Napier_2020/napier_samples_github_sans_weird.csv", delimiter=",", explode_upon="_", auto_standardize=True)
	merged = Ranchero.merge_dataframes(merged, napier, merge_upon="run_index", right_name="Napier", drop_exclusive_right=True)
	
	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])
	if Ranchero._NeighLib.get_count_of_x_in_column_y(merged, 'Ivory Coast', 'country') > 0:
		exit(1)

	# Nimmo
	print(f"{_b_}Processing Nimmo{_bb_}")
	nimmo_L2 = Ranchero.from_tsv("./inputs/publications/run_indexed/Nimmo_2024/L2.tsv", auto_standardize=False)
	nimmo_L2 = Ranchero.NeighLib.add_column_of_just_this_value(nimmo_L2, "lineage", "L2")
	nimmo_L2 = Ranchero.standardize_everything(nimmo_L2) # AFTER adding lineage column
	nimmo_L4 = Ranchero.from_tsv("./inputs/publications/run_indexed/Nimmo_2024/L4.tsv", auto_standardize=False)
	nimmo_L4 = Ranchero.NeighLib.add_column_of_just_this_value(nimmo_L4, "lineage", "L4")
	nimmo_L4 = Ranchero.standardize_everything(nimmo_L4) # AFTER adding lineage column
	merged = Ranchero.merge_dataframes(merged, nimmo_L2, merge_upon="run_index", right_name="Nimmo", drop_exclusive_right=True, fallback_on_left=True)
	merged = Ranchero.merge_dataframes(merged, nimmo_L4, merge_upon="run_index", right_name="Nimmo", drop_exclusive_right=True, fallback_on_left=True)
	
	# Stucki
	print(f"{_b_}Processing Stucki{_bb_}")
	stucki = Ranchero.from_tsv("./inputs/publications/run_indexed/Stucki_2016 (run)/Stucki_2016 - cleaned - by run.tsv", auto_standardize=True)
	merged = Ranchero.merge_dataframes(merged, stucki, merge_upon="run_index", right_name="Stucki", drop_exclusive_right=True)

	# Walker (CRyPTIC cross-study pheno table, NOT THE REUSE TABLE!!)
	print(f"{_b_}Processing Walker pheno data (WHO2021, CRyPTIC-associated, PMC7612554, only the run-indexed samples){_bb_}")
	walker = Ranchero.from_tsv("./inputs/publications/run_indexed/Walker_2022-CRyPTIC-run/runindexed_Walker2022_pheno_WHO2021_CRyPTIC_PMC7612554.tsv")
	walker = Ranchero.NeighLib.add_column_of_just_this_value(walker, "pheno_source", "Walker_2022_PMC7612554")
	start, merged = time.time(), Ranchero.merge_dataframes(merged, walker, merge_upon="run_index", right_name="Walker_2022", indicator="collection", fallback_on_left=True, escalate_warnings=False)
	print(f"Merged with run-based Walker in {time.time() - start:.4f} seconds")

	if Ranchero._NeighLib.get_count_of_x_in_column_y(merged, 'Ivory Coast', 'country') > 0:
		exit(1)

	# the Nextstrain tree
	print(f"{_b_}Processing Nextstrain tree{_bb_}")
	nextstrain = Ranchero.from_tsv("./inputs/nextstrain_fixed_metadata.tsv", explode_upon=";", auto_standardize=True,
		# dates unreliable, antibiotic data mostly null
		drop_columns=["date_collected", 'Pyrazinamide','Capreomycin','Ethambutol','Rifampicin','Isoniazid','Ethionamide','Streptomycin','Pyrazinamide','Fluoroquinolones','Kanamycin','Amikacin','nextstrain_drug_resistance'])
	merged = Ranchero.merge_dataframes(merged, nextstrain, merge_upon="run_index", right_name="nextstrain", drop_exclusive_right=True)
	print(f"Merged with Nextstrain tree in {time.time() - start:.4f} seconds")
	
	Ranchero.NeighLib.print_value_counts(merged, ['date_collected'])

	print(f"{_b_}Finishing up run-based stuff...{_bb_}")
	merged = Ranchero.rancheroize(merged)
	start = time.time()
	Ranchero.to_tsv(merged, "./merged_by_run.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f}s seconds")

	print(merged.estimated_size(unit='mb'))

	gc.collect()
	return merged

def sample_index_merges(merged_runs):
	merged = merged_runs
	merged = merged.drop(['atc_sam', 'bdq_mic_sam', 'total_bases_run'])

	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])

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
	#atypical = Ranchero.from_tsv("./inputs/atypical.tSV")
	#merged = Ranchero.merge_dataframes(merged, atypical, merge_upon="sample_index", left_name="tba6", right_name="atypical_genotypes", drop_exclusive_right=False)

	print(merged_by_sample.estimated_size(unit='mb'))
	merged = merged_by_sample
	gc.collect()

	# Andres
	print(f"{_b_}Processing Andres{_bb_}")
	Andres_pheno = Ranchero.from_tsv("./inputs/publications/sample_indexed/Andres_2019/Andres_cleaned.tsv", drop_columns=['Ethambutol_MIC'], auto_standardize=True)
	Andres_pheno = Ranchero.NeighLib.add_column_of_just_this_value(Andres_pheno, "pheno_source", "Andres_2019_PMC6355586")
	start, merged = time.time(), Ranchero.merge_dataframes(merged, Andres_pheno, merge_upon="sample_index", right_name="Andres_2019", indicator="collection")
	print(f"Merged with Andres in {time.time() - start:.4f} seconds")

	# Bateson
	# Eldholm 
	# Finci
	print(f"{_b_}Processing Finci{_bb_}")
	Finci_pheno = Ranchero.from_tsv("./inputs/publications/sample_indexed/Finci_2022 (PRJEB48275)/PRJEB48275_Finci_plus_pheno.tsv", auto_standardize=True,
		drop_columns=["pheno_WHO_resistance","pheno_INH_MIC","pheno_RMP_MIC","pheno_EMB_MIC","pheno_PZA_MIC","pheno_AMK_MIC","pheno_CAP_MIC",
		"pheno_KAN_MIC","pheno_MFX_MIC","pheno_LFX_MIC"])
	Finci_pheno = Ranchero.NeighLib.add_column_of_just_this_value(Finci_pheno, "pheno_source", "Finci_2022_PMC9436784")
	start, merged = time.time(), Ranchero.merge_dataframes(merged, Finci_pheno, merge_upon="sample_index", right_name="Finci_2022", indicator="collection")

	# Menardo (two of them...)
	print(f"{_b_}Processing Menardo (two of them){_bb_}")
	start = time.time()
	menardo_2018 = Ranchero.from_tsv("./inputs/publications/sample_indexed/Menardo_2018/menardo_2018_processed.csv", delimiter=',', auto_standardize=True)
	menardo_2021 = Ranchero.from_tsv("./inputs/publications/sample_indexed/Menardo_2021/menardo_REAL.tsv", auto_standardize=True)
	merged = Ranchero.merge_dataframes(merged, menardo_2018, merge_upon="sample_index", right_name="Menardo_2018", indicator="collection")
	merged = Ranchero.merge_dataframes(merged, menardo_2021, merge_upon="sample_index", right_name="Menardo_2021", indicator="collection", escalate_warnings=False)
	print(f"Merged with menardos in {time.time() - start:.4f} seconds")

	if Ranchero._NeighLib.get_count_of_x_in_column_y(merged, 'Ivory Coast', 'country') > 0:
		exit(1)

	# Merker (sample side of 2022)
	print(f"{_b_}Processing Merker's sample-indexed stuff{_bb_}")
	merker_samples = Ranchero.from_tsv("./inputs/publications/sample_indexed/Merker_2022 [XRS]/Merker_cleaned_XRS_id.tsv", auto_rancheroize=False, glob=False, check_index=False)
	merker_ids = Ranchero.from_tsv("./inputs/publications/sample_indexed/Merker_2022 [XRS]/ERS-to-SAME-incomplete.txt", auto_rancheroize=False, glob=False, check_index=False)
	merker_samples = Ranchero.NeighLib.add_column_of_just_this_value(merker_samples, "pheno_source", "Merker_2022_PMC9426364")
	merker_sample_fixed = Ranchero.merge_dataframes(merker_samples, merker_ids, merge_upon="XRS_id")
	merker_sample_fixed = Ranchero.standardize_everything(merker_sample_fixed)
	merged = Ranchero.merge_dataframes(merged, merker_sample_fixed, merge_upon="sample_index", right_name="Merker_2022", indicator="collection")

	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])


	# Shuaib

	# standford data
	print(f"{_b_}Processing Standford{_bb_}")
	standford_1 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/max_standford_YYYY-MM.tsv")))
	standford_2 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/max_standford_YYYY-MM-DD.tsv")))
	standford_3 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/max_standford_DD-MM-YYYY.tsv"))) # this one should NOT overwrite left
	standford_4 = Ranchero.standardize_everything(Ranchero.from_tsv("./inputs/max_standford_slashdates.tsv")) # ditto
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

	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])


	#PRJEB9680 = Ranchero.from_tsv"./inputs/PRJEB9680 superset.tsv")
	#start = time.time()
	#merged = Ranchero.merge_dataframes(merged, PRJEB9680, merge_upon="sample_index", right_name="PRJEB9680_superset", indicator="collection")
	#print(f"Merged with PRJEB9680 (just to mark that superset BioProject in the indicator column) in {time.time() - start:.4f} seconds")


	# Walker (CRyPTIC big pheno table)
	print(f"{_b_}Processing Walker pheno data (WHO2021, CRyPTIC, PMC7612554, only the sample-indexed samples){_bb_}")
	walker = Ranchero.from_tsv("./inputs/publications/sample_indexed/Walker_2022-CRyPTIC-samp/sampleindexed_Walker2022_pheno_WHO2021_CRyPTIC_PMC7612554.tsv", explode_upon=" ")
	walker = Ranchero.NeighLib.add_column_of_just_this_value(walker, "pheno_source", "CRyPTIC_WHO2021_PMC7612554")
	walker = Ranchero.standardize_countries(walker)
	start, merged = time.time(), Ranchero.merge_dataframes(merged, walker, merge_upon="sample_index", right_name="Walker_2022", indicator="collection")
	print(f"Merged with sample-based Walker in {time.time() - start:.4f} seconds")

	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])
	Ranchero.NeighLib.print_a_where_b_is_null(merged, 'region', 'country')


	# input lists
	start = time.time()
	print(f"{_b_}Processing inputs, outputs, denylist, and what's on the tree{_bb_}")
	inputs = Ranchero.from_tsv("./inputs/pipeline/probable_inputs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, inputs, merge_upon="sample_index", right_name="input", indicator="collection", drop_exclusive_right=False)
	
	diffs = Ranchero.from_tsv("./inputs/pipeline/probable_inputs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, diffs, merge_upon="sample_index", right_name="diff", indicator="collection", drop_exclusive_right=False)
	
	tree = Ranchero.from_tsv("./inputs/pipeline/samples on tree 2024-12-12.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, tree, merge_upon="sample_index", right_name="tree", indicator="collection", drop_exclusive_right=False)

	tbprofiler = Ranchero.from_tsv("./inputs/TBProfiler/tbprofiler_basically_everything_rancheroized.tsv")
	tbprofiler = tbprofiler.drop(['tbprof_main_lin', 'tbprof_family', 'superbatch'])
	merged = Ranchero.merge_dataframes(merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=False)
	
	denylist = Ranchero.from_tsv("./inputs/pipeline/denylist_2024-07-23_lessdupes.tsv", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=False)
	
	print(f"Merged with pipeline information in {time.time() - start:.4f} seconds")

	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])


	return merged


########################################################################




if start_from_scratch:
	tba6 = inital_file_parse()
else:
	start, tba6 = time.time(), Ranchero.from_tsv("tba6_no_nonsense.tsv")
	print(f"Imported tba6 file without extremely irrelevant columns in {time.time() - start:.4f} seconds")

if inject:
	tba6_injected = inject_metadata(tba6)
else:
	start, tba6_injected = time.time(), Ranchero.from_tsv("tba6_injected.tsv")
	print(f"Imported tba6 file with injections in {time.time() - start:.4f} seconds")

if do_run_index_merges:
	merged_runs = run_merges(tba6_injected)
else:
	start, merged_runs = time.time(), Ranchero.from_tsv("merged_by_run.tsv")
	print(f"Imported run-indexed tba6 file, with some merges, in {time.time() - start:.4f} seconds")

if sample_merges:
	merged_samps = sample_index_merges(merged_runs)
#else:
#	start, merged_samps = time.time(), Ranchero.from_tsv("ranchero_partial.tsv")
#	print(f"Imported run-indexed tba6 file, with some merges, in {time.time() - start:.4f} seconds")


if slim:
	boooooo = merged.columns
	merged = merged.drop([boo for boo in boooooo if boo not in ['collection', 'run_index', 'sample_index']])




merged = merged_samps
merged = merged.drop(['lat', 'lon', 'date_collected_year', 'date_collected_month', 'reason', 'host_info', 'geoloc_name'], strict=False)
merged = Ranchero.hella_flat(merged)
Ranchero.print_schema(merged)
Ranchero.NeighLib.print_value_counts(merged, ['libraryselection'])
Ranchero.NeighLib.print_value_counts(merged, ['host_scienname', 'host_confidence', 'host_streetname'])
Ranchero.NeighLib.print_value_counts(merged, ['date_collected'])
Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage', 'strain'])
Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])
Ranchero.NeighLib.report(merged)
Ranchero.to_tsv(merged, "./ranchero_rc8.tsv")




#Ranchero.NeighLib.big_print_polars(merged, "merged hosts and dates", ['sample_index', 'date_collected', 'host_scienname', 'lineage'])
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


