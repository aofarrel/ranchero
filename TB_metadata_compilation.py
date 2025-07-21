rc = "rc17½½"

import polars as pl

import time
start = time.time()
import gc
import src as Ranchero
_b_ = "\033[1m"
_bb_ = "\033[0m"
_c_ = "\033[0;36m"
print(f"Module import time: {time.time() - start:.4f}")
start_from_scratch = True
inject = True
do_run_index_merges = True

module_start = time.time()

def check_stuff(polars_df, name=None):
	if name is not None:
		print(f"{_b_}...Performing checks on {name}...{_bb_}")
	else:
		print(f"{_b_}...Performing checks...{_bb_}")
	print(f"{_c_}")
	Ranchero.NeighLib.check_index(polars_df)
	print(f"Estimated size: {polars_df.estimated_size(unit='mb')} MB")

	if 'collection' in polars_df.columns:
		assert polars_df.schema['collection'] is not Ranchero.pl.List(Ranchero.pl.List(Ranchero.pl.Utf8))
		#assert Ranchero.NeighLib.get_null_count_in_column(polars_df, 'collection') != 0
	else:
		print("collection not in polars_df")

	if 'isolation_source' in polars_df.columns:
		assert polars_df.schema['isolation_source'] is not Ranchero.pl.Utf8
		print(f"isolation_source has type {polars_df.schema['isolation_source']}")
		Ranchero.NeighLib.print_value_counts(polars_df, ['isolation_source'])
	else:
		print("isolation_source not in polars_df")

	if 'primary_search' in polars_df.columns:
		assert polars_df.schema['primary_search'] is not Ranchero.pl.Utf8
		print(f"primary_search has type {polars_df.schema['primary_search']}")
	else:
		print("primary_search not in polars_df")

	Ranchero.NeighLib.print_a_where_b_equals_these(polars_df, 'pheno_ETHAMBUTOL', 'sample_index', ['SAMEA2534071', 'SAMEA104600139', 'SAMEA2535090'], alsoprint=['pheno_ETHAMBUTOL', 'pheno_source'])

	# location
	assert 'geoloc_info' not in polars_df.columns
	assert 'geoloc_name' not in polars_df.columns
	#Ranchero.NeighLib.print_a_where_b_equals_these(polars_df, col_a='country', col_b='run_index', list_to_match=['ERR046972', 'ERR2884698', 'ERR841442', 'ERR5908244', 'SRR23310897', 'SRR12380906', 'SRR18054772', 'SRR10394499', 'SRR9971324', 'ERR732681', 'SRR23310897'], alsoprint=['region', 'continent'])
	if Ranchero.NeighLib.get_a_where_b_is_null(polars_df, 'country', 'continent').shape[0] > 0:
		print("Found countries without continents")
		Ranchero.NeighLib.print_a_where_b_is_null(polars_df, 'country', 'continent')
		exit(1)
	# this isn't a good check, because some samples genuinely are ambiguous
	#if Ranchero.NeighLib.get_a_where_b_is_null(polars_df, 'region', 'country').shape[0] > 0:
	#	print("Found regions without countries")
	#	Ranchero.NeighLib.print_a_where_b_is_null(polars_df, 'region', 'country')
	#	exit(1)
	if 'country' in polars_df.columns:
		if Ranchero._NeighLib.get_count_of_x_in_column_y(polars_df, 'Ivory Coast', 'country') > 0:
			print("Found non-ISO Ivory Coast in country column")
			exit(1)
		null_continent = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'continent')
		null_country = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'country')
		null_region = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'region')
		print(f"{null_continent} nulls in continent")
		print(f"{null_country} nulls in country")
		print(f"{null_region} nulls in region")
		Ranchero.NeighLib.print_value_counts(polars_df, ['continent'])
		Ranchero.NeighLib.print_value_counts(polars_df, ['country'])
	
	# host
	if 'host_commonname' in polars_df.columns:
		if Ranchero.NeighLib.get_a_where_b_is_null(polars_df, 'host_commonname', 'host_confidence').shape[0] > 0:
			print("Found host common names with no host confidence")
			Ranchero.NeighLib.print_a_where_b_is_null(polars_df, 'host_commonname', 'host_confidence')
			exit(1)
		if Ranchero.NeighLib.get_a_where_b_is_null(polars_df, 'host_scienname', 'host_confidence').shape[0] > 0:
			print("Found host scientific names with no host confidence")
			Ranchero.NeighLib.print_a_where_b_is_null(polars_df, 'host_scienname', 'host_confidence')
			exit(1)
		print(f"Found {polars_df.filter(pl.col('host_commonname') == pl.lit('human')).shape[0]} human hosts")
	
	# taxoncore
	if 'clade' in polars_df.columns:
		assert polars_df.schema['clade'] is not Ranchero.pl.List
		null_clade = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'clade')
		if null_clade > 0:
			print("Found null values for clade!")
			Ranchero.NeighLib.print_value_counts(polars_df, ['clade'])
			Ranchero.NeighLib.print_a_where_b_is_null(polars_df, 'sample_index', 'clade')
			#exit(1) # handled at end
	else:
		print('clade not in polars_df')
	
	if 'organism' in polars_df.columns:
		assert polars_df.schema['organism'] is not Ranchero.pl.List
		null_organism = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'organism')
		if null_organism > 0:
			print("Found null values for organism!")
			Ranchero.NeighLib.print_value_counts(polars_df, ['organism'])
			Ranchero.NeighLib.print_a_where_b_is_null(polars_df, 'clade', 'organism')
			#exit(1) # handled at end
	else:
		print('organism not in polars_df')

	if 'lineage' in polars_df.columns:
		assert polars_df.schema['lineage'] is not Ranchero.pl.List
		null_lineage = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'lineage')
		print(f"{null_lineage} nulls in lineage")
		
	if 'strain' in polars_df.columns:
		assert polars_df.schema['strain'] is not Ranchero.pl.List
		null_strain = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'strain')
		print(f"{null_strain} nulls in strain")
	
	# date
	if 'date_sequenced' in polars_df.columns:
		null_dateseq = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'date_sequenced')
		print(f"{null_dateseq} nulls in date_sequenced")
	else:
		print('date_sequenced not in polars_df')
	
	if 'date_collected' in polars_df.columns:
		null_datecoll = Ranchero.NeighLib.get_count_of_x_in_column_y(polars_df, None, 'date_collected')
		print(f"{null_datecoll} nulls in date_collected")
	else:
		print('date_collected not in polars_df')

	Ranchero.NeighLib.report(polars_df)
	print(f"{_b_}...Done checking...{_bb_}")
	

def inital_file_parse():
	#we don't immediately rancheroize as this is faster (probably)
	start, tba6 = time.time(),Ranchero.from_bigquery("./inputs/BQ/tba6_no_tax_table_bq_2024-09-19.json_modified.json")
	print(f"Parsed tba6 file from bigquery in {time.time() - start:.4f} seconds")
	start, tba6 = time.time(), Ranchero.drop_non_tb_columns(tba6)
	tba6 = tba6.drop(['insdc_center_name_sam']) # this and 'center_name' are a nightmare to standardize so we'll just leave one of them as-is
	print(f"Dropped non-TB-related columns in {time.time() - start:.4f} seconds")

	start = time.time()
	Ranchero.to_tsv(tba6, f"Mycobacterium_genus_BigQuery_raw_{rc}_p1.tsv") # previously "tba6 no nonsense"
	print(f"Wrote to disk in {time.time() - start:.4f} seconds")

	# initial rancheroize
	start, tba6 = time.time(), Ranchero.rancheroize(tba6)
	print(f"Rancheroized in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.standardize_everything(tba6)
	print(f"Standardized in {time.time() - start:.4f} seconds")

	start, tba6 = time.time(), Ranchero.drop_lowcount_columns(tba6)
	print(f"Removed columns with few values in {time.time() - start:.4f}s seconds") # should be done last

	# move to demo.py
	#print(Ranchero.unique_bioproject_per_center_name(tba6))
	#Ranchero.print_a_where_b_equals_this(tba6, 'region', 'country', 'CIV', valuecounts=True)
	#Ranchero.NeighLib.print_value_counts(tba6, ['country'])

	check_stuff(tba6)
	Ranchero.NeighLib.report(tba6)

	start = time.time()
	Ranchero.to_tsv(tba6, f"Mycobacterium_genus_BigQuery_standardized_{rc}_p2.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f} seconds")
	check_stuff(tba6, "tba6 in memory")
	print("Reading disk:")
	what_we_just_wrote = Ranchero.from_tsv(f"Mycobacterium_genus_BigQuery_standardized_{rc}_p2.tsv", auto_standardize=False)
	check_stuff(what_we_just_wrote, "tba6 on disk")
	what_we_just_wrote = None
	gc.collect()
	return tba6

def inject_metadata(tba6):
	check_stuff(tba6)
	tba6 = Ranchero.add_column_with_this_value(tba6, "collection", None)

	bioproject_injector = Ranchero.injector_from_tsv("./inputs/overrides/injector_with_shorthands.tsv")
	bovis_time = Ranchero.injector_from_tsv("./inputs/overrides/PRJEB18668 - good.tsv")
	host_overrides = Ranchero.injector_from_tsv("./inputs/overrides/host_overrides.tsv")
	norway = Ranchero.injector_from_tsv("./inputs/overrides/PRJEB12184 - good.tsv", drop_columns=["literature_lineage"])
	PRJNA575883p1 = Ranchero.injector_from_tsv("./inputs/overrides/PRJNA575883p1 no host disease.tsv")
	PRJNA575883p2 = Ranchero.injector_from_tsv("./inputs/overrides/PRJNA575883p2.tsv")
	imrl = Ranchero.injector_from_tsv("./inputs/overrides/IMRL/IMRL.csv")
	fran_SRA_dates = Ranchero.injector_from_tsv("./inputs/overrides/fran_ERR_date_only.tsv")
	PRJEB7056 = Ranchero.injector_from_tsv("./inputs/overrides/PRJEB7056.tsv")

	# PRJNA235618 is goofy because its samples are definitely in the dataset but its accession doesn't show up in the BQ metadata at all
	PRJNA235618 = Ranchero.injector_from_tsv("./inputs/overrides/PRJNA235618.tsv")

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
	tba6 = Ranchero.inject_metadata(tba6, fran_SRA_dates, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, PRJEB7056, overwrite=True)
	tba6 = Ranchero.inject_metadata(tba6, PRJNA235618, overwrite=True)

	print(f"{_b_}Injected metadata in {time.time() - start:.4f} seconds{_bb_}")
	null = Ranchero._NeighLib.get_count_of_x_in_column_y(tba6, None, 'country')
	print(f"{_b_}After injecting, we have {null} samples with no value for country{_bb_}")
	tba6 = Ranchero.NeighLib.nullify(tba6)
	tba6 = Ranchero.standardize_everything(tba6) # give continents to injected countries and fix taxoncore
	check_stuff(tba6, "tba6 after injectors in memory")

	start = time.time()
	Ranchero.to_tsv(tba6, f"Mycobacterium_genus_injections_{rc}_p3.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f}s seconds")
	return tba6

def run_merges(tba6):
	merged = tba6
	tba6 = None # avoid copy-paste mistakes
	check_stuff(merged, 'tba6 at top of run_merges()')

	# Shauib
	print(f"{_b_}Processing Shuaib's run-indexed samples{_bb_}")
	#shuaib_XRS = Ranchero.from_tsv("./inputs/publications/sample_indexed/Shuaib_2022 (PMC9222951)/shuaib_2022_XRS.tsv", auto_rancheroize=False, glob=False, check_index=False)
	#merged = Ranchero.standardize_countries(Ranchero.merge_dataframes(merged, shuaib_XRS, merge_upon="XRS_id", right_name="Shuaib_2022", indicator="collection"))
	shuaib_SRR = Ranchero.from_tsv("./inputs/publications/sample_indexed/Shuaib_2022 (PMC9222951)/shuaib_2022_SRR.tsv", auto_rancheroize=False, glob=False, check_index=False)
	merged = Ranchero.standardize_countries(Ranchero.merge_dataframes(merged, shuaib_SRR, merge_upon="run_index", right_name="Shuaib_2022", indicator="collection"))

	# Bos
	#bos = Ranchero.from_tsv("./inputs/publications/Bos_2015/ancient.tsv")
	#merged = Ranchero.merge_dataframes(merged, bos, merge_upon="run_index", left_name="tba6", right_name="Bos (ancient)")

	# Brites
	print(f"{_b_}Processing Brites{_bb_}")
	brites = Ranchero.from_tsv("./inputs/publications/run_indexed/Brites_2018/brites_cleaned.tsv", 
		auto_rancheroize=True, explode_upon=";", 
		drop_columns=['country']) # several incorrect countries
	merged = Ranchero.merge_dataframes(merged, brites, merge_upon="run_index", left_name="tba6", right_name="Brites_2018",
		drop_exclusive_right=True) # we have to do this or else run-to-sample breaks
	check_stuff(merged)

	# Cancino-Muñoz
	print(f"{_b_}Processing Cancino-Muñoz{_bb_}")
	cancinomunoz = Ranchero.from_tsv("./inputs/publications/run_indexed/Cancino-Muñoz_2022/cancino-munoz_2022.tsv")
	assert 'geoloc_info' not in cancinomunoz.columns
	assert 'geoloc_info' not in merged.columns
	merged = Ranchero.merge_dataframes(merged, cancinomunoz, merge_upon="run_index", left_name="tba6", right_name="Cancino-Muñoz_2022",
		drop_exclusive_right=True)
	check_stuff(merged)

	# Coll
	print(f"{_b_}Processing Coll{_bb_}")
	coll = Ranchero.from_tsv("./inputs/publications/run_indexed/Coll_2018/coll_processed.tsv", auto_standardize=True)
	coll = Ranchero.add_column_with_this_value(coll, "pheno_source", "Coll_2018")
	merged = Ranchero.merge_dataframes(merged, coll, merge_upon="run_index", left_name="tba6", right_name="Coll_2018", drop_exclusive_right=True)

	# Coscolla
	print(f"{_b_}Processing Coscolla{_bb_}")
	coscolla = Ranchero.from_tsv("./inputs/publications/run_indexed/Coscolla_2021/coscolla_sans_weird.tsv", explode_upon=";", auto_standardize=True)
	merged = Ranchero.merge_dataframes(merged, coscolla, merge_upon="run_index", left_name="tba6", right_name="Coscolla_2021", drop_exclusive_right=True)

	# CRyPTIC Reuse Table (NOT WALKER 2022)
	print(f"{_b_}Processing CRyPTIC reuse table{_bb_}")
	CRyPTIC = Ranchero.from_tsv("./inputs/publications/run_indexed/CRyPTIC reuse table/CRyPTIC_reuse_table_20240917.csv", delimiter=",", explode_upon=".",
		drop_columns=["AMI_MIC","BDQ_MIC","CFZ_MIC","DLM_MIC","EMB_MIC","ETH_MIC","INH_MIC","KAN_MIC","LEV_MIC","LZD_MIC","MXF_MIC","RIF_MIC","RFB_MIC",
		"AMI_PHENOTYPE_QUALITY","BDQ_PHENOTYPE_QUALITY","CFZ_PHENOTYPE_QUALITY","DLM_PHENOTYPE_QUALITY","EMB_PHENOTYPE_QUALITY","ETH_PHENOTYPE_QUALITY",
		"INH_PHENOTYPE_QUALITY","KAN_PHENOTYPE_QUALITY","LEV_PHENOTYPE_QUALITY","LZD_PHENOTYPE_QUALITY","MXF_PHENOTYPE_QUALITY","RIF_PHENOTYPE_QUALITY",
		"RFB_PHENOTYPE_QUALITY","ENA_SAMPLE","VCF","REGENOTYPED_VCF"], auto_standardize=True)
	CRyPTIC = Ranchero.add_column_with_this_value(CRyPTIC, "pheno_source", "CRyPTIC_reuse_table") #PMC9363010
	merged = Ranchero.merge_dataframes(merged, CRyPTIC, merge_upon="run_index", left_name="tba6", right_name="CRyPTIC_reuse_table", drop_exclusive_right=True)
	check_stuff(merged)

	# Merker (two of them...)
	#print(f"{_b_}Processing the run-indexed part of Merker 2022{_bb_}")
	#merker = Ranchero.from_tsv("./inputs/publications/run_indexed/Merker_2022 (run)/Merker_clean_run_indeces.tsv", auto_standardize=True, 
	#	drop_columns=["pheno_STREPTOMYCIN","pheno_ISONIAZID","pheno_RIFAMPICIN","pheno_ETHAMBUTOL","pheno_PYRAZINAMIDE",
	#	"pheno_AMIKACIN","pheno_KANAMYCIN","pheno_CAPREOMYCIN","pheno_CYCLOSERINE","pheno_CLOFAZIMINE","pheno_OFLOXACIN",
	#	"pheno_PAS","pheno_RIFABUTIN","pheno_MOXIFLOXACIN","pheno_LINEZOLID","pheno_ETHIONAMIDE","pheno_BEDAQUILINE"]) # turns out these are genotypic
	#merker = Ranchero.add_column_with_this_value(merker, "pheno_source", "Merker_2022") #PMC9426364
	#merged = Ranchero.merge_dataframes(merged, merker, merge_upon="run_index", left_name="tba6", right_name="Merker_2022", drop_exclusive_right=True)
	# DONT FORGET THE OTHER MERKER PAPER, AND ALSO THE SAMPLE-INDEXED PART
	#check_stuff(merged)

	# Napier
	print(f"{_b_}Processing Napier{_bb_}")
	napier = Ranchero.from_tsv("./inputs/publications/run_indexed/Napier_2020/napier_samples_github_sans_weird_no_US.csv", delimiter=",", explode_upon="_", auto_standardize=True)
	merged = Ranchero.merge_dataframes(merged, napier, merge_upon="run_index", right_name="Napier_2020", drop_exclusive_right=True)
	check_stuff(merged)

	# Nimmo
	print(f"{_b_}Processing Nimmo{_bb_}")
	nimmo_L2 = Ranchero.from_tsv("./inputs/publications/run_indexed/Nimmo_2024/L2.tsv", auto_standardize=False)
	nimmo_L2 = Ranchero.add_column_with_this_value(nimmo_L2, "lineage", "L2")
	nimmo_L2 = Ranchero.standardize_everything(nimmo_L2) # AFTER adding lineage column
	nimmo_L4 = Ranchero.from_tsv("./inputs/publications/run_indexed/Nimmo_2024/L4.tsv", auto_standardize=False)
	nimmo_L4 = Ranchero.add_column_with_this_value(nimmo_L4, "lineage", "L4")
	nimmo_L4 = Ranchero.standardize_everything(nimmo_L4) # AFTER adding lineage column
	merged = Ranchero.merge_dataframes(merged, nimmo_L2, merge_upon="run_index", right_name="Nimmo_2024", drop_exclusive_right=True, fallback_on_left=True)
	merged = Ranchero.merge_dataframes(merged, nimmo_L4, merge_upon="run_index", right_name="Nimmo_2024", drop_exclusive_right=True, fallback_on_left=True)
	check_stuff(merged)
	
	# Stucki
	print(f"{_b_}Processing Stucki{_bb_}")
	stucki = Ranchero.from_tsv("./inputs/publications/run_indexed/Stucki_2016 (run)/Stucki_2016 - cleaned - by run.tsv", auto_standardize=True)
	merged = Ranchero.merge_dataframes(merged, stucki, merge_upon="run_index", right_name="Stucki_2016", drop_exclusive_right=True)
	check_stuff(merged)

	# Walker (CRyPTIC cross-study pheno table, NOT THE REUSE TABLE!!)
	print(f"{_b_}Processing Walker pheno data (WHO2021, CRyPTIC-associated, PMC7612554, only the run-indexed samples){_bb_}")
	walker = Ranchero.from_tsv("./inputs/publications/run_indexed/Walker_2022-CRyPTIC-run/runindexed_Walker2022_pheno_WHO2021_CRyPTIC_PMC7612554.tsv")
	walker = Ranchero.add_column_with_this_value(walker, "pheno_source", "Walker_2022") #PMC7612554
	start, merged = time.time(), Ranchero.merge_dataframes(merged, walker, merge_upon="run_index", right_name="Walker_2022", indicator="collection", fallback_on_left=True, escalate_warnings=False)
	print(f"Merged with run-based Walker in {time.time() - start:.4f} seconds")
	check_stuff(merged)

	# the Nextstrain tree
	print(f"{_b_}Processing Nextstrain tree{_bb_}")
	nextstrain = Ranchero.from_tsv("./inputs/nextstrain_fixed_metadata.tsv", explode_upon=";", auto_standardize=True,
		# dates unreliable, antibiotic data mostly null
		drop_columns=["date_collected", 'Pyrazinamide','Capreomycin','Ethambutol','Rifampicin','Isoniazid','Ethionamide','Streptomycin','Pyrazinamide','Fluoroquinolones','Kanamycin','Amikacin','nextstrain_drug_resistance'])
	merged = Ranchero.merge_dataframes(merged, nextstrain, merge_upon="run_index", right_name="nextstrain_global_tree", drop_exclusive_right=True)
	print(f"Merged with Nextstrain tree in {time.time() - start:.4f} seconds")
	check_stuff(merged)

	print(f"{_b_}Finishing up run-based stuff...{_bb_}")
	merged = Ranchero.standardize_everything(merged) # TODO: this loses about 200 lineages!!!
	check_stuff(merged)

	start = time.time()
	Ranchero.to_tsv(merged, f"./Mycobacterium_genus_run-based_metadata_{rc}_p4.tsv")
	print(f"Wrote to disk in {time.time() - start:.4f}s seconds")
	return merged

def sample_index_merges(merged_runs):
	merged = merged_runs
	merged = merged.drop(['atc_sam', 'bdq_mic_sam', 'total_bases_run'])
	check_stuff(merged)

	# merged with sample-indexed data
	print(f"{_b_}Preparing to swap index{_bb_}")
	start = time.time()
	merged_flat = Ranchero.hella_flat(merged)
	merged_by_sample = Ranchero.run_index_to_sample_index(merged_flat)
	merged_by_sample = Ranchero.hella_flat(merged_by_sample)
	print(f"{_b_}Converted run indeces to sample indeces in {time.time() - start:.4f} seconds{_bb_}")
	Ranchero.to_tsv(merged_by_sample, f"./Mycobacterium_genus_swapped_index_{rc}_p5.tsv")
	
	Ranchero.NeighLib.print_value_counts(merged_by_sample, ['clade', 'organism', 'lineage'])
	check_stuff(merged_by_sample)

	merged = merged_by_sample

	# atypical
	#print(f"{_b_}Processing atypical samples{_bb_}")
	#atypical = Ranchero.from_tsv("./inputs/atypical.tSV")
	#merged = Ranchero.merge_dataframes(merged, atypical, merge_upon="sample_index", left_name="tba6", right_name="atypical_genotypes", drop_exclusive_right=False)

	# Andres
	print(f"{_b_}Processing Andres{_bb_}")
	Andres_pheno = Ranchero.from_tsv("./inputs/publications/sample_indexed/Andres_2019/Andres_cleaned.tsv", drop_columns=['Ethambutol_MIC'], auto_standardize=True)
	Andres_pheno = Ranchero.add_column_with_this_value(Andres_pheno, "pheno_source", "Andres_2019") # PMC6355586
	start, merged = time.time(), Ranchero.merge_dataframes(merged, Andres_pheno, merge_upon="sample_index", right_name="Andres_2019", indicator="collection")
	print(f"Merged with Andres in {time.time() - start:.4f} seconds")
	check_stuff(merged)

	# Bateson
	# Eldholm 
	# Finci
	print(f"{_b_}Processing Finci{_bb_}")
	Finci_pheno = Ranchero.from_tsv("./inputs/publications/sample_indexed/Finci_2022 (PRJEB48275)/PRJEB48275_Finci_plus_pheno.tsv", auto_standardize=False, # needs to be false or the merge gets hella cringe
		drop_columns=["pheno_WHO_resistance"])
	Finci_pheno = Ranchero.add_column_with_this_value(Finci_pheno, "pheno_source", "Finci_2022") #PMC9436784
	#Finci_pheno = Finci_pheno.drop(['country', 'pheno_AMIKACIN', 'pheno_CAPREOMYCIN', 'pheno_ETHAMBUTOL', 'pheno_ISONIAZID', 'pheno_KANAMYCIN', 'pheno_MOXIFLOXACIN', 'pheno_PYRAZINAMIDE', 'pheno_RIFAMPICIN', 'pheno_LEVOFLOXACIN', 'lineage'])
	start, merged = time.time(), Ranchero.standardize_everything(Ranchero.merge_dataframes(merged, Finci_pheno, merge_upon="sample_index", right_name="Finci_2022", indicator="collection", fallback_on_left=True), skip_sample_source=True)
	check_stuff(merged)

	# Menardo (two of them...)
	print(f"{_b_}Processing Menardo (two of them){_bb_}")
	start = time.time()
	menardo_2018 = Ranchero.from_tsv("./inputs/publications/sample_indexed/Menardo_2018/menardo_2018_processed.csv", delimiter=',')
	menardo_2021 = Ranchero.from_tsv("./inputs/publications/sample_indexed/Menardo_2021/menardo_REAL.tsv", auto_standardize=True)
	merged = Ranchero.merge_dataframes(merged, menardo_2018, merge_upon="sample_index", right_name="Menardo_2018", indicator="collection")
	merged = Ranchero.merge_dataframes(merged, menardo_2021, merge_upon="sample_index", right_name="Menardo_2021", indicator="collection", escalate_warnings=False)
	print(f"Merged with menardos in {time.time() - start:.4f} seconds")
	check_stuff(merged)

	# Merker (sample side of 2022) AAAAAAAAAAAA
	#print(f"{_b_}Processing Merker's sample-indexed stuff{_bb_}")
	#merker_samples = Ranchero.from_tsv("./inputs/publications/sample_indexed/Merker_2022 [ERS]/Merker_cleaned_ERS_id.tsv", auto_rancheroize=False, glob=False, check_index=False,
	#	drop_columns=["pheno_STREPTOMYCIN","pheno_ISONIAZID","pheno_RIFAMPICIN","pheno_ETHAMBUTOL","pheno_PYRAZINAMIDE",
	#	"pheno_AMIKACIN","pheno_KANAMYCIN","pheno_CAPREOMYCIN","pheno_CYCLOSERINE","pheno_CLOFAZIMINE","pheno_OFLOXACIN",
	#	"pheno_PAS","pheno_RIFABUTIN","pheno_MOXIFLOXACIN","pheno_LINEZOLID","pheno_ETHIONAMIDE","pheno_BEDAQUILINE"]) # turns out these are genotypic
	#merker_ids = Ranchero.from_tsv("./inputs/publications/sample_indexed/Merker_2022 [ERS]/ERS-to-SAME-incomplete.txt", auto_rancheroize=False, glob=False, check_index=False)
	#merker_sample_fixed = Ranchero.merge_dataframes(merker_samples, merker_ids, merge_upon="SRS_id") # needed for the samples indeces that aren't in the other dataframe
	#merker_sample_fixed = Ranchero.standardize_everything(merker_sample_fixed)
	#merged = Ranchero.merge_dataframes(merged, merker_samples, merge_upon="SRS_id", right_name="Merker_2022", indicator="collection")
	#check_stuff(merged)

	print(f"{_b_}Processing CSISP ref set{_bb_}")
	ref_set_CSISP = Ranchero.from_tsv("./inputs/publications/sample_indexed/ref_set_CSISP.tsv", auto_rancheroize=False, glob=False, check_index=False)
	merged = Ranchero.merge_dataframes(merged, ref_set_CSISP, merge_upon="sample_index", right_name="ref_set_CSISP", indicator="collection")
	check_stuff(merged)

	# standford data
	print(f"{_b_}Processing Standford{_bb_}")
	standford_1 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/standford/max_standford_YYYY-MM.tsv")))
	standford_2 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/standford/max_standford_YYYY-MM-DD.tsv")))
	standford_3 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/standford/max_standford_MM-DD-YYYY.tsv"), in_format="MM/DD/YYYY")) # this one should NOT overwrite left
	standford_4 = Ranchero.standardize_everything(Ranchero.from_tsv("./inputs/standford/max_standford_slashdates.tsv")) # ditto
	standford_5 = Ranchero.standardize_countries(Ranchero.cleanup_dates(Ranchero.from_tsv("./inputs/standford/max_standford_DD.MM.YYYY.tsv"), in_format="DD.MM.YYYY")) # this one should NOT overwrite left
	start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_1, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=False, escalate_warnings=False)
	print(f"Merged with standford1 in {time.time() - start:.4f} seconds")
	start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_2, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=False, escalate_warnings=False)
	print(f"Merged with standford2 in {time.time() - start:.4f} seconds")
	start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_3, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=True, escalate_warnings=False)
	print(f"Merged with standford3 in {time.time() - start:.4f} seconds")
	start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_4, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=True, escalate_warnings=False)
	print(f"Merged with standford4 in {time.time() - start:.4f} seconds")
	start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_5, merge_upon="sample_index", right_name="standford", indicator="collection", fallback_on_left=True, escalate_warnings=False)
	print(f"Merged with standford5 in {time.time() - start:.4f} seconds")
	start, merged = time.time(), Ranchero.cleanup_dates(merged)
	print(f"Cleaned up dates so far in {time.time() - start:.4f} seconds")

	#Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage', 'strain'])
	Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage'])
	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])
	check_stuff(merged)

	#PRJEB9680 = Ranchero.from_tsv"./inputs/PRJEB9680 superset.tsv")
	#start = time.time()
	#merged = Ranchero.merge_dataframes(merged, PRJEB9680, merge_upon="sample_index", right_name="PRJEB9680_superset", indicator="collection")
	#print(f"Merged with PRJEB9680 (just to mark that superset BioProject in the indicator column) in {time.time() - start:.4f} seconds")


	# Walker (CRyPTIC big pheno table)
	print(f"{_b_}Processing Walker pheno data (WHO2021, CRyPTIC, PMC7612554, only the sample-indexed samples){_bb_}")
	walker = Ranchero.from_tsv("./inputs/publications/sample_indexed/Walker_2022-CRyPTIC-samp/sampleindexed_Walker2022_pheno_WHO2021_CRyPTIC_PMC7612554.tsv", explode_upon=" ")
	walker = Ranchero.add_column_with_this_value(walker, "pheno_source", "CRyPTIC_WHO2021_PMC7612554")
	walker = Ranchero.standardize_everything(walker)
	start, merged = time.time(), Ranchero.merge_dataframes(merged, walker, merge_upon="sample_index", right_name="Walker_2022", indicator="collection")
	print(f"Merged with sample-based Walker in {time.time() - start:.4f} seconds")
	#Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage', 'strain'])
	Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage'])
	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])
	check_stuff(merged)

	# Fran's data -- has to be last since it adds new sample IDs
	print(f"{_b_}Processing TGU{_bb_}")
	Fran_non_sra = Ranchero.from_tsv("./inputs/fran_not_SRA.tsv").drop(['pheno_STREPTOMYCIN', 'pheno_ISONIAZID', 'pheno_RIFAMPICIN', 'pheno_ETHAMBUTOL', 'pheno_PYRAZINAMIDE', 'pheno_PAS', 'pheno_LINEZOLID', 'pheno_KANAMYCIN', 'pheno_CAPREOMYCIN', 'pheno_PROTHIONAMIDE', 'pheno_FLUOROQUINOLONES'])
	#Fran_non_sra = Ranchero.add_column_with_this_value(Fran_non_sra, "pheno_source", "TGU")
	Fran_sra = Ranchero.from_tsv("./inputs/fran_SRA.tsv").drop(['pheno_STREPTOMYCIN', 'pheno_ISONIAZID', 'pheno_RIFAMPICIN', 'pheno_ETHAMBUTOL', 'pheno_PYRAZINAMIDE', 'pheno_PAS', 'pheno_LINEZOLID', 'pheno_KANAMYCIN', 'pheno_CAPREOMYCIN', 'pheno_PROTHIONAMIDE', 'pheno_FLUOROQUINOLONES'])
	#Fran_sra = Ranchero.add_column_with_this_value(Fran_sra, "pheno_source", "TGU")
	start, merged = time.time(), Ranchero.merge_dataframes(merged, Fran_non_sra, merge_upon="sample_index", right_name="TGU", indicator="collection", drop_exclusive_right=False)
	start, merged = time.time(), Ranchero.merge_dataframes(merged, Fran_sra, merge_upon="sample_index", right_name="TGU", indicator="collection", drop_exclusive_right=False)
	print(f"Merged with TGU in {time.time() - start:.4f} seconds")
	#Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage', 'strain'])
	Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage'])
	check_stuff(merged)

	# input lists
	start = time.time()
	print(f"{_b_}Processing inputs, outputs, denylist, and what's on the tree{_bb_}")
	inputs = Ranchero.from_tsv("./inputs/pipeline/probable_inputs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, inputs, merge_upon="sample_index", right_name="input", indicator="collection", drop_exclusive_right=False)
	print(f"Merged with inputs list in {time.time() - start:.4f} seconds")

	diffs = Ranchero.from_tsv("./inputs/pipeline/probable_diffs.txt", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, diffs, merge_upon="sample_index", right_name="diff", indicator="collection", drop_exclusive_right=False)
	print(f"Merged with diffs list in {time.time() - start:.4f} seconds")

	tree = Ranchero.from_tsv("./ranchero_output_archive/2025-07-08-FINAL_ranchero_rc17.subset.annotated.tsv", auto_rancheroize=False, list_columns=['run_index'])
	for column in tree.columns:
		if column in merged and column not in ['sample_index']:
			# coinfection and tbprofiler_lineage_usher will get added too
			tree = tree.drop(column)
	merged = Ranchero.merge_dataframes(merged, tree, merge_upon="sample_index", right_name="tree", indicator="collection", drop_exclusive_right=False)
	print(f"Merged with pipeline information in {time.time() - start:.4f} seconds")

	tbprofiler = Ranchero.from_tsv("./inputs/tbprofiler/tbprofiler_basically_everything_rancheroized_lesscolumns_FIXMISSINGDATA.tsv")
	tbprofiler = tbprofiler.drop(['tbprof_family', 'superbatch'], strict=False)
	merged = Ranchero.merge_dataframes(merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=False)
	
	denylist = Ranchero.from_tsv("./inputs/pipeline/denylist_2024-07-23_lessdupes.tsv", auto_rancheroize=False)
	merged = Ranchero.merge_dataframes(merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=True)
	
	print(f"Merged with pipeline information in {time.time() - start:.4f} seconds")
	#Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage', 'strain'])
	Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage'])
	Ranchero.NeighLib.print_a_where_b_equals_these(merged, col_a='country', col_b='run_index', list_to_match=['ERR046972', 'ERR2884698', 'ERR841442', 'ERR5908244', 'SRR23310897', 'SRR12380906', 'SRR18054772', 'SRR10394499', 'SRR9971324', 'ERR732681', 'SRR23310897'], alsoprint=['region', 'continent'])
	Ranchero.NeighLib.print_value_counts(merged, ['country', 'region'])
	check_stuff(merged)

	# this will take a while, but will fix missing clade columns since we added some taxoncore information over time,
	# and will fix goofy mistakes I missed (hopefully)
	print(f"{_b_}One last standardization pass{_bb_}")
	merged = Ranchero.standardize_everything(merged, force_strings=True, organism_fallback="Mycobacterium tuberculosis complex sp.", clade_fallback="MTBC", skip_sample_source=True)
	print(f"Re-standardized in {time.time() - start:.4f} seconds")

	#Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage', 'strain'])
	Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'lineage'])
	check_stuff(merged)

	return merged


########################################################################

if start_from_scratch:
	tba6_standardized = inital_file_parse()
	tba6_injected = inject_metadata(tba6_standardized)
	merged_runs = run_merges(tba6_injected)
	merged_samps = sample_index_merges(merged_runs)
else:
	if inject:
		print("Reading from tba6_standardized.tsv")
		tba6_standardized = Ranchero.from_tsv("tba6_standardized.tsv", auto_standardize=False)
		tba6_injected = inject_metadata(tba6_standardized)
		merged_runs = run_merges(tba6_injected)
		merged_samps = sample_index_merges(merged_runs)
	else:
		if do_run_index_merges:
			print("Reading from tba6_injected.tsv")
			tba6_injected = Ranchero.from_tsv("tba6_injected.tsv", auto_standardize=False)
			merged_runs = run_merges(tba6_injected)
			merged_samps = sample_index_merges(merged_runs)
		else:
			print(f"Reading from Mycobacterium_genus_run-based_metadata_{rc}_p4.tsv")
			merged_runs = Ranchero.from_tsv(f"Mycobacterium_genus_run-based_metadata_{rc}_p4.tsv", auto_standardize=False)
			merged_samps = sample_index_merges(merged_runs)

# fix a BioProject-level injection
print(f"Found {merged_samps.filter(pl.col('host_commonname') == pl.lit('human')).shape[0]} human hosts at start of main")
merged_samps = Ranchero.inject_metadata(merged_samps, Ranchero.injector_from_tsv("./inputs/overrides/caprae_injector.tsv"), overwrite=True)
print(f"Found {merged_samps.filter(pl.col('host_commonname') == pl.lit('human')).shape[0]} human hosts after bioproject injection")

# final nonsense
merged_samps = merged_samps.rename({'lineage': 'literature_lineage'})
merged_samps = merged_samps.rename({'tbprofiler_lineage_usher': 'usher_lineage_from_tbprofiler'})
merged_samps = merged_samps.drop(['lat', 'lon', 'date_collected_year', 'date_collected_month', 'reason', 'host_info', 'geoloc_info', 'mbytes_sum_sum', 'geoloc_name'], strict=False)
merged_samps = merged_samps.drop(['tbprof_rd', 'tbprof_spoligotype', 'tbprof_frac'], strict=False) # seem to be from the main lineage only, not the sublineage
merged_samps = merged_samps.drop(['mbases_sum', 'bases_sum', 'bytes_sum'], strict=False) # often inaccurate


print("---------------------- Whole-genus value counts ----------------------")
merged = Ranchero.hella_flat(merged_samps)
check_stuff(merged)
print(f"Found {merged.filter(pl.col('host_commonname') == pl.lit('human')).shape[0]} human hosts in genus")

Ranchero.NeighLib.print_value_counts(merged, ['sample_source'])
Ranchero.NeighLib.print_value_counts(merged, ['host_scienname', 'host_confidence', 'host_streetname'])
Ranchero.NeighLib.print_value_counts(merged, ['date_collected'])
Ranchero.NeighLib.print_value_counts(merged, ['clade', 'organism', 'literature_lineage', 'strain'])
Ranchero.NeighLib.print_value_counts(merged, ['country', 'continent', 'region'])

Ranchero.to_tsv(merged, f"./Mycobacterium_genus_metadata_ranchero_{rc}-verbose.tsv")
concise = merged.drop(['primary_search', 'mbases_sum', 'bases_sum', 'bytes_sum', 'libraryselection', 'librarysource', 'instrument', 'host_info', 'collection'], strict=False)
Ranchero.to_tsv(concise, f"./Mycobacterium_genus_metadata_ranchero_{rc}-concise.tsv")

print("---------------------- Tree-only value counts ----------------------")
tree_only_merged = Ranchero.hella_flat(merged_samps.filter(pl.col('collection').list.contains('tree')))
check_stuff(tree_only_merged)
print(f"Found {merged_samps.filter(pl.col('host_commonname') == pl.lit('human')).shape[0]} human hosts on the tree")

Ranchero.NeighLib.print_value_counts(tree_only_merged, ['sample_source'])
Ranchero.NeighLib.print_value_counts(tree_only_merged, ['host_scienname', 'host_confidence', 'host_streetname'])
Ranchero.NeighLib.print_value_counts(tree_only_merged, ['date_collected'])
Ranchero.NeighLib.print_value_counts(tree_only_merged, ['clade', 'organism', 'literature_lineage', 'strain'])
Ranchero.NeighLib.print_value_counts(tree_only_merged, ['country', 'continent', 'region'])

Ranchero.to_tsv(tree_only_merged, f"./2025-07-08_tree_metadata_ranchero_{rc}-verbose.tsv")
concise = tree_only_merged.drop(['primary_search', 'mbases_sum', 'bases_sum', 'bytes_sum', 'libraryselection', 'librarysource', 'instrument', 'host_info', 'collection'], strict=False)
Ranchero.to_tsv(concise, f"./2025-07-08_tree_metadata_ranchero_{rc}-concise.tsv")




print(f"{_b_}Host information{_bb_}")
has_host = tree_only_merged.select(pl.col('host_commonname').filter(pl.col('host_commonname').is_not_null()))
human = has_host.filter(pl.col('host_commonname') == pl.lit('human'))
cowish = has_host.filter([
	pl.col('host_commonname').str.contains_any(['domestic cattle', 'bovine', 'gaur', 'buffalo', 'bison'])
])
badger = has_host.filter([
	pl.col('host_commonname').str.contains(pl.lit('badger'))
])
deer = has_host.filter([
	pl.col('host_commonname').str.contains_any(['deer', 'elk', 'moose'])
])
pig = has_host.filter([
	pl.col('host_commonname').str.contains_any(['pig', 'boar'])
])
with pl.Config(tbl_cols=-1, tbl_rows=100):
	print(has_host.select(pl.col('host_commonname').value_counts(sort=True)))
	print(cowish.select(pl.col('host_commonname').value_counts(sort=True)))
	print(deer.select(pl.col('host_commonname').value_counts(sort=True)))
	print(pig.select(pl.col('host_commonname').value_counts(sort=True)))
print(f"{has_host.shape[0]} ({has_host.shape[0] / tree_only_merged.shape[0] * 100 :.2f}%) of samples on the tree have any host annotation at all")
print(f"->{human.shape[0]} ({human.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly human")
print(f"->{cowish.shape[0]} ({cowish.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly cattle")
print(f"->{badger.shape[0]} ({badger.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly badger")
print(f"->{deer.shape[0]} ({deer.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly deer")
print(f"->{pig.shape[0]} ({pig.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly pig")

print(f"{_b_}date collected{_bb_}")
has_date_collected = tree_only_merged.select(pl.col('date_collected').filter(pl.col('date_collected').is_not_null()))
print(f"There's {has_date_collected.shape[0]} ({has_date_collected.shape[0] / tree_only_merged.shape[0] * 100 :.2f}%) samples on the final tree with a date_collected value.")

print(f"{_b_}TBProfiler{_bb_}")
print("Out of the samples on the tree, what is median and mean of the median coverage?")
print(tree_only_merged.filter(pl.col('tbprof_median_coverage').is_not_null()).select(pl.median('tbprof_median_coverage')))
print(tree_only_merged.filter(pl.col('tbprof_median_coverage').is_not_null()).select(pl.mean('tbprof_median_coverage')))

print("What's on the tree WITHOUT TBProfiler lineage information? (sometimes tbprofiler can't assign a lineage or may assign two)")
with pl.Config(tbl_cols=-1, tbl_rows=100):
	lacks_tbprof_main =  tree_only_merged.filter(~pl.col('tbprof_main_lin').is_not_null()).select(['sample_index', 'tbprof_main_lin', 'usher_lineage_from_tbprofiler', 'literature_lineage'])
	print(lacks_tbprof_main.sort('literature_lineage'))
print("What's on the tree WITH TBProfiler lineage information? (sometimes tbprofiler can't assign a lineage or may assign two)")
has_tbprof_main_lin = tree_only_merged.filter(pl.col('tbprof_main_lin').is_not_null()).select(['tbprof_main_lin', 'usher_lineage_from_tbprofiler', 'literature_lineage'])
has_one_tbprof_main_lin = has_tbprof_main_lin.filter(~pl.col('tbprof_main_lin').str.contains(';'))
print(f"A total of {has_tbprof_main_lin.shape[0]} ({has_tbprof_main_lin.shape[0] / tree_only_merged.shape[0] * 100 :.2f}%) samples have TBProf lineage info")
print(f"->Of which {has_one_tbprof_main_lin.shape[0]} ({has_one_tbprof_main_lin.shape[0] / has_tbprof_main_lin.shape[0] * 100 :.2f}% of have main lineage is True) samples have just one TBProf lineage main lineage")

four = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage4')])
two = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage2')])
one = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage1')])
three = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage3')])
la1 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('La1')])
six = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage6')])
five = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage5')])
la3 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('La3')])
la2 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('La2')])
lineage7 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage7')])
lineage9 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage9')])
lineage8 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage8')])

print(f"->{four.shape[0]} ({four.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage4")
print(f"->{two.shape[0]} ({two.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage2")
print(f"->{one.shape[0]} ({one.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage1")
print(f"->{three.shape[0]} ({three.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage3")
print(f"->{la1.shape[0]} ({la1.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are La1")
print(f"->{six.shape[0]} ({six.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage6")
print(f"->{five.shape[0]} ({five.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage5")
print(f"->{la3.shape[0]} ({la3.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are la3")
print(f"->{la2.shape[0]} ({la2.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are la2")
print(f"->{lineage7.shape[0]} ({lineage7.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage7")
print(f"->{lineage9.shape[0]} ({lineage9.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage9")
print(f"->{lineage8.shape[0]} ({lineage8.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage8")

has_tbprof_dr = tree_only_merged.filter(pl.col('tbprof_drtype').is_not_null())
print(f"{has_tbprof_dr.shape[0]}  ({has_tbprof_dr.shape[0] / tree_only_merged.shape[0] * 100 :.2f}%) samples have a tbprof dr type")

sense = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('Sensitive')])
mdr = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('MDR-TB')])
other = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('Other')])
prexdrtb = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('Pre-XDR-TB')])
hr = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('HR-TB')])
rr = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('RR-TB')])
xdr = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('XDR-TB')])

print(f"->{sense.shape[0]} ({sense.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are Sensitive")
print(f"->{other.shape[0]} ({other.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are explictly other")
print(f"->{mdr.shape[0]} ({mdr.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are MDR-TB")
print(f"->{prexdrtb.shape[0]} ({prexdrtb.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are Pre-XDR-TB")
print(f"->{hr.shape[0]} ({hr.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are MDR-TB")
print(f"->{rr.shape[0]} ({rr.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are RR")
print(f"->{xdr.shape[0]} ({xdr.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are XDR-TB")


print(f"Finished entire module in {time.time() - module_start} seconds")
