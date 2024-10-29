import polars as pl

# Generally speaking, NCBI Run Selector columns are the same as
# the columns you get from BigQuery, but with capitalization.
# Double-quotes are NCBI run selector specific, single-quote
# are from BQ or elsewhere. No difference at runtime, just 
# marking for the sake of whoever is looking at this later
# when something breaks. :-)

# these columns are considered "equivalent" and nullfill each other
equivalence = {
		'assay_type': ['assay_type', 'Assay Type', 'assay_type_sam', 'assay_type_run'],
		'attributes': ['attributes'], # excluding j_attr on purpose
		'avgspotlen': ['avgspotlen', 'AvgSpotLen'],
		'bases': ['bases', 'Bases'],
		'BioProject': ['BioProject', 'bioproject', 'Bioproject'],
		'BioSampleModel': ['BioSampleModel', 'biosamplemodel', 'biosamplemodel_sam'],
		'bytes': ['bytes', 'Bytes'],
		'center_name': ['center_name', 'Center Name', 'center_name_insdc', 'insdc_center_name_sam'],
		'collection': ['collection'],
		'date_collected': ['date_collected', 'date_of_collection_sam', 'date_isolation', 'Collection_Date', 'collection_date_sam', 'sample_collection_date_sam_s_dpl127', 'collection_date_orig_sam', 'collection_date_run', 'date_coll', 'date', 'colection_date_sam', 'collectiondateym_sam'],
		'date_collected_year': ['date_collected_year', 'collection_year_sam', 'year_isolated_sam'],
		'date_collected_month': ['date_collected_month', 'collection_month_sam'],
		'date_collected_day': ['date_collected_day', 'samplingday_sam'],
		'genotype': ['genotype', 'genotype_sam_ss_dpl92', 'genotype_variation_sam', 'spoligotype_sam',  'mlva___spoligotype_sam', 'vntr_sam', 'serotype_sam', 'serovar_sam', 'orgmod_note_sam_s_dpl305', 'atpe_mutation_sam', 'rv0678_mutation_sam', 'mutant_sam', 'subtype_sam', 'pathotype_sam', 'subgroup_sam', 'arrayexpress_species_sam'],
		'geoloc_latlon': ['geoloc_latlon', 'lat_lon_sam_s_dpl34', 'lat_lon', 'latitude_and_longitude_sam', 'lat_lon_run'],
		'geoloc_name': ['geoloc_name', 'geo_loc_name_country', 'geo_loc_name_country_calc', 'geoloc_country_calc', 'geo_loc_name_country_continent', 'geo_loc_name_sam', 'geographical_location_sam', 'geo_loc_name_sam_s_dpl209', 'isolation_country_sam', 'country_sam', 'geographic_location__region_and_locality__sam', 'geographic_location__country_and_or_sea__region__sam', 'geographic_location__countryand_orsea_region__sam', 'geographic_location__country_and_or_sea__sam', 'region_sam', 'geoloc_country_or_sea', 'geoloc_country_or_sea_region', 'geo_loc_name_run', 'geographic_location__country_and_or_sea__run', 'doi_location_sam',  'geo_accession_exp'], # doi_location_sam and geo_accession_exp should be lowest priority
		'host': ['host', 'host_sciname', 'host_sam', 'specific_host_sam', 'host_common', 'host_common_name_sam', 'host_run', 'host_scientific_name_sam', 'host_taxon_id_sam', 'host_common_name_run', 'host_scientific_name_run'],
		'host_disease': ['host_disease', 'disease', 'disease_sam', 'host_disease_sam'],
		'host_info': ['patient_country_of_birth_sam', 'host_status_sam', 'patient_year_of_birth_sam', 'patientid_sam', 'patient_finished_treatment_sam', 'patient_has_hiv_sam', 'patient_sex_sam', 'patient_number_sam_s_dpl111', 'age_sam', 'host_disease_outcome_sam', 'host_disease_stage_sam', 'host_sex_sam', 'env_broad_scale_sam', 'env_local_scale_sam', 'env_medium_sam', 'host_age_sam', 'host_health_state_sam', 'host_subject_id_sam', 'host_description_sam', 'age_at_death_sam', 'age_at_death_units_sam', 'age_atdeath_weeks_sam'],
		'host_scienname': ['host_scienname'],
		'host_confidence': ['host_confidence'],
		'host_commonname': ['host_commonname', 'host_streetname'],
		'instrument': ['instrument', 'Instrument'],
		'isolation_source': ['isolation_source', 'sample_type_sam_ss_dpl131', 'sample_source', 'tissue_sam_ss_dpl145', 'isolation_source_sam', 'isolation_type_sam', 'isolation_source_sam_ss_dpl261', 'isolation_source_host_associated_sam_s_dpl264', 'specimen_sam', 'host_body_product_sam', 'bio_material_sam', 'tissue_source_sam', 'subsource_note_sam', 'env_biome_sam', 'env_feature_sam', 'env_material_sam', 'source_name_sam', 'isolation_source_host_associated_sam_s_dpl263', 'isolate_sam_ss_dpl100', 'plant_product_sam', 'isolation_source_run', 'sample_type_run_s_dpl517', 'isolate_run', 'sample_type_exp'],
		'librarylayout': ['librarylayout', 'LibraryLayout'], # no uderscore to match BQ format
		'libraryselection': ['libraryselection', 'LibrarySelection'],
		'librarysource': ['librarysource', 'LibrarySource'],
		'lineage': ['lineage', 'lineage_sam', 'linege_sam', 'mtb_lineage_sam', 'subspecf_gen_lin_sam', 'literature_lineage'],
		'mycobact_type': ['mycobact_type'],
		'organism': ['organism', 'organism_sciname', 'organism_common', 'organism_sam', 'tax_id_sam_ss_dpl29', 'subspecies_sam', 'Organism', 'scientific_name_sam', 'species_sam', 'common_name_sam', 'phenotype_sam', 'scientific_name_run', 'common_name_run'],
		'platform': ['platform', 'Platform'], # platform_sam and platform_run seem to be something else
		'primary_search': ['primary_search'],
		'run_index': ['run_index', 'acc', 'run', 'Run', 'run_accession', 'run_acc'],
		'run_file_create_date': ['run_file_create_date'],
		'sample_index': ['sample_index', 'biosample', 'BioSample', 'Biosample', 'sample'],
		'sra_study': ['sra_study', 'SRA Study'], # SRP ID
		'strain': ['strain', 'strain_sam_ss_dpl139', 'strain_name_alias_sam', 'strain_geno', 'sub_strain_sam_s_dpl389', 'strain_genotype_sam_s_dpl382', 'cell_line_sam', 'cell_line_run'],
		'SRX_id': ['SRX_id', 'experiment', 'Experiment'], # DO NOT USE experiment_sam! that is something totally different! 
		'XRS_id': ['XRS_id', 'sample_acc'], # SRS/ERS/DRS accession
	}
columns_to_keep = equivalence.keys()
assert len(set(sum(equivalence.values(), []))) == len(sum(equivalence.values(), []))  # effectively asserts no shared values (both within a key's value-lists, and across all other value-lists)


# when going from run-level to sample-level, or flattening existing lists as much as possible, how should we treat these columns?
rts__list_to_float_via_sum = ['bytes', 'bases']
rts__NEVER_list = ['isolation_source']
rts__drop = ['eight_pac_barcode_run', 'pacbio_rs_binding_kit_barcode_exp', 'pacbio_rs_sequencing_kit_barcode_run', 'run_id_run', 'release_date', 'library_name', 'avgspotlen', 'datastore_region', 'mybytes', 'run_file_version', 'napier_type'] # napier_type causes issues due to duplicates in the Napier dataset
rts__keep_as_list = ['coscolla_mean_depth', 'coscolla_percent_not_covered', 'avgspotlen', 'run_file_create_date', 'mbytes', 'mbases', 'librarylayout', 'libraryselection', 'instrument', 'platform', 'isolate_info'] # non-unique values will be kept
rts__keep_as_set = ['assay_type', 'center_name', 'center_name_insdc', 'BioProject', 'datastore_filetype', 'datastore_provider', 'primary_search', 'run_index', 'SRX_id', 'sra_study'] # non-unique values will be dropped
rts__warn_if_list_with_unique_values = [
	'librarysource',
	'napier_lineage', 'coscolla_sublineage',
	'date_collected', 
	'geoloc_latlon', 'geoloc_name', 'geoloc_country_calc', 'geoloc_country_or_sea', 'coscolla_country', 'napier_country',
	'host', 'host_sciname', 
	'organism_common', 
	'release_date']

# when merging existing datasets, how should we treat these columns, if (not equivalent after nullfill AND neither are already lists)?
# if either are already lists, will use rts behavior
merge__error = ['BioSample', 'sample_index', 'librarylayout', 'libraryselection', 'librarysource', 'instrument', 'platform', 'assay_type', 'run_file_version', 'isolate_info']
merge__sum = []
merge__drop = ['eight_pac_barcode_run', 'pacbio_rs_binding_kit_barcode_exp', 'pacbio_rs_sequencing_kit_barcode_run', 'run_id_run', 'datastore_filetype', 'datastore_provider', 'release_date', 'library_name', 'avgspotlen', 'datastore_region', 'napier_type'] # napier_type causes issues due to duplicates in the Napier dataset
merge__make_list = [] # non-unique values will be kept
merge__make_set = ['run_index',  'primary_search'] # non-unique values will be dropped
merge__warn_then_pick_arbitrarily_to_keep_singular = [
	'BioProject', 
	'SRX_id', 'sra_study',
	'center_name', 'center_name_insdc', 
	'isolation_source',
	'date_collected', 
	'geoloc_latlon', 'geoloc_name', 'geoloc_country_calc', 'geoloc_country_or_sea', 
	'host', 'host_sciname', 
	'organism_common', 
	'release_date']
merge__special_taxonomic_handling = {key: value for key, value in equivalence.items() if key in ['genotype', 'lineage', 'organism', 'strain']}
all_taxoncore_columns = sum(merge__special_taxonomic_handling.values(), [])

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
