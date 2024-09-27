import polars as pl

# Generally speaking, NCBI Run Selector columns are the same as
# the columns you get from BigQuery, but with capitalization.
# Double-quotes are NCBI run selector specific, single-quote
# are from BQ or elsewhere. No difference at runtime, just 
# marking for the sake of whoever is looking at this later
# when something breaks. :-)

# TODO: Double check these!
# TODO: Get NCBI Run Selc equivalent of genotype_sam_ss_dpl92

# rancheroized columns that can be squished into one by standardize.py
# TODO: add a verification step that everything here is a value in everything_worth_keeping
addl_ids = ['primary_search', 'other_id', 'XRS_id']
organism = ['organism', 'organism_common', 'organism_sciname']
host = ['host', 'host_common', 'host_sciname']
strain = ['strain_geno', 'strain']
mtbc_lineage = ['strain_geno', 'strain', 'organism', 'organism_common', 'organism_sciname', 'genotype', 'lineage_mtb']

# when going from run-level to sample-level, how should we treat these columns?
rts__list_to_float_via_sum = ['mbytes', 'mybytes', 'bases', 'bytes']
rts__drop = ['release_date', 'create_date', 'library_name', 'avgspotlen', 'datastore_region', 'napier_type'] # napier_type causes issues due to duplicates in the Napier dataset
rts__keep_as_list = ['librarylayout', 'libraryselection', 'librarysource', 'instrument', 'platform', 'assay_type', 'run_file_version', 'isolate_info'] # non-unique values will be kept
rts__keep_as_set = ['BioProject', 'datastore_filetype', 'datastore_provider', 'primary_search', 'run_index', 'SRX_id', 'sra_study'] # non-unique values will be dropped
rts__warn_if_list_with_unique_values = ['center_name', 'center_name_insdc', 'isolation_source', 'date_collected', 'geoloc_country_calc', 'geoloc_country_or_sea', 'host_sciname', 'organism_common', 'release_date']

rancheroize__warn_if_list_with_unique_values = rts__warn_if_list_with_unique_values + rts__list_to_float_via_sum

date = ['samplingday_sam', 'collection_year_sam', 'collection_month_sam', 'collectiondateym_sam',  'year_isolated_sam']
organism = ['phenotype_sam', 'organism_sciname', 'organism_common']
source_information = ['specimen_sam', 'isolation_source_sam_ss_dpl261', 'sample_type_exp', 'host_body_product_sam', 'tissue_source_sam', 'env_biome_sam', 'env_feature_sam', 'env_material_sam', 'sample_type_sam_ss_dpl131', 'isolation_source_sam', 'isolate_sam_ss_dpl100', 'source_name_sam', 'sample_type_sam_ss_dpl131']
host_information = ['patient_year_of_birth_sam', 'patient_sex_sam', 'patient_has_hiv_sam', 'host_status_sam', 'patientid_sam', 'patient_finished_treatment_sam', 'patient_has_hiv_sam', 'patient_sex_sam', 'patient_year_of_birth_sam', 'anonymised_badger_id_sam', 'patient_number_sam_s_dpl111', 'age_sam', 'host_disease_outcome_sam', 'host_disease_stage_sam', 'host_sex_sam', 'env_broad_scale_sam', 'env_local_scale_sam', 'env_medium_sam', 'mouse_strain_sam', 'host_age_sam', 'host_disease_sam', 'host_health_state_sam', 'host_subject_id_sam', 'host_description_sam', 'age_at_death_sam', 'age_at_death_units_sam', 'age_atdeath_weeks_sam']
location = ['doi_location_sam', 'geoloc_country_or_sea_region', 'patient_country_of_birth_sam', 'country_sam', 'geoloc_name', 'altitude_sam_s_dpl11', 'isolation_country_sam', 'latitude_and_longitude_sam', 'lat_lon_sam_s_dpl34', 'geo_accession_exp', 'geoloc_country_calc', 'geoloc_country_or_sea']
geno = ['genotype_variation_sam', 'strain_name_alias_sam', 'orgmod_note_sam_s_dpl305', 'atpe_mutation_sam', 'rv0678_mutation_sam', 'subsource_note_sam', 'mutant_sam', 'lineage_sam','note_sam','linege_sam',
'subspecf_gen_lin_sam', 'species_sam', 
'spoligotype_sam', 'vntr_sam', 'organism_sam', 'subtype_sam', 'pathotype_sam', 'serotype_sam', 'serovar_sam', 'genotype', 'subgroup_sam', 'arrayexpress_species_sam', 'strain', 'organism', 'organism_common', 'organism_sciname', 'mlva___spoligotype_sam']

# these columns are considered "equivalent" and nullfill each other
equivalence = {
		'assay_type': ['assay_type', 'Assay Type'],
		'assay_type': ['assay_type', 'assay_type_sam', 'assay_type_run'],
		'attributes': ['attributes'], # excluding j_attr on purpose
		'avgspotlen': ['avgspotlen', 'AvgSpotLen'],
		'bases': ['bases', 'total_bases_run', 'Bases'],
		'BioProject': ['BioProject', 'bioproject', 'Bioproject'],
		'BioSampleModel': ['BioSampleModel', 'biosamplemodel', 'biosamplemodel_sam'],
		'bytes': ['bytes', 'Bytes'],
		'center_name': ['center_name', 'Center Name', 'center_name_insdc', 'insdc_center_name_sam'],
		'date_collected': ['date_collected', 'Collection_Date', 'collection_date_sam', 'sample_collection_date_sam_s_dpl127', 'collection_date_orig_sam', 'collection_date_run', 'date_coll', 'date', 'colection_date_sam'],
		'geoloc_latlon': ['geoloc_latlon', 'lat_lon_sam_s_dpl34', 'lat_lon', 'lat_lon_run'],
		'geoloc_name': ['geoloc_name', 'geo_loc_name_country', 'geo_loc_name_country_calc', 'geo_loc_name_country_continent', 'geo_loc_name_sam', 'geographic_location__country_and_or_sea__sam', 'region_sam', 'geo_loc_name_run', 'geographic_location__country_and_or_sea__run'],
		'host': ['host', 'host_sciname', 'host_sam', 'specific_host_sam', 'host_common', 'host_run', 'host_scientific_name_sam', 'host_common_name_run', 'host_scientific_name_run'],
		'instrument': ['instrument', 'Instrument'],
		'isolation_source': ['isolation_source', 'sample_type_sam_ss_dpl131', 'isolation_source_sam', 'isolation_source_sam_ss_dpl261', 'isolation_source_host_associated_sam_s_dpl264', 'isolation_source_host_associated_sam_s_dpl263', 'isolate_sam_ss_dpl100', 'isolation_source_run', 'sample_type_run_s_dpl517', 'isolate_run'],
		'librarylayout': ['librarylayout', 'LibraryLayout'], # no uderscore to match BQ format
		'libraryselection': ['libraryselection', 'LibrarySelection'],
		'librarysource': ['librarysource', 'LibrarySource'],
		'literature_lineage': ['literature_lineage', 'lineage', 'mtb_lineage_sam', 'strain', 'strain_sam_ss_dpl139', 'genotype', 'cell_line_sam', 'genotype_sam_ss_dpl92', 'strain_genotype_sam_s_dpl382', 'cell_line_run'],
		'organism': ['organism', 'Organism', 'scientific_name_sam', 'common_name_sam', 'scientific_name_run', 'common_name_run'],
		'platform': ['platform', 'Platform'], # platform_sam and platform_run seem to be something else
		'primary_search': ['primary_search'],
		'run_index': ['run_index', 'acc', 'run', 'Run', 'run_accession', 'run_acc'],
		'sample_index': ['sample_index', 'biosample', 'BioSample', 'Biosample', 'sample'],
		'sra_study': ['sra_study', 'SRA Study'], # SRP ID
		'SRX_id': ['SRX_id', 'experiment', 'Experiment'], # DO NOT USE experiment_sam! that is something totally different! 
		'XRS_id': ['XRS_id', 'sample_acc'], # SRS/ERS/DRS accession
	}
columns_to_keep = equivalence.keys()

# not used, but I'm leaving this here for people who want it
equivalence_extended = {
		'datastore_filetype': ['datastore_filetype', 'DATASTORE filetype'],
		'datastore_provider': ['datastore_provider', 'DATASTORE provider'],
		'datastore_region': ['datastore_region', 'DATASTORE region'],
		'library_name': ['library_name', 'Library Name'],
		'mycobacteriaceae_family_sam': ['mycobacteriaceae_family_sam'],
		'mycobacterium_genus_sam': ['mycobacterium_genus_sam'],
		'release_date': ['release_date', 'ReleaseDate', 'releasedate'],
}


common_col_to_ranchero_col = {
	"Sample Name": "other_id",  # *sometimes* SAME/SAMN/SAMD but often something else
	
}

# some real and hypothetical confusing fields that should be dropped
purposeful_exclusions = [
	"additional_instrument_model_run",
	"experimental_factor__genotype_exp", 
	"MB", "mb", "mB",  # ambigious - megabytes or megabases?
	"quality_control_method_version_run",
	"sampling_platform_sam",
	"strain_background_sam_s_dpl381" # seems to be mouse strain
]

not_strings = {
	"avgspotlen": pl.Int32(),  # tba5 is fine with Int16, but tba6 needs Int32
	"bases": pl.Int64(),
	"bytes": pl.Int64(),
	"date_collected": pl.Date,
	"ileft": pl.Int16(),
	"ilevel": pl.Int16(),
	"iright": pl.Int16(),
	"mbases": pl.Int32(),
	"run_file_version": pl.Int16(),
	"self_count": pl.Int32(),
	"tax_id": pl.Int32(),
	"total_count": pl.Int32()
}
