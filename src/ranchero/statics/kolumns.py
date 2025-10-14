import polars as pl

# Generally speaking, NCBI Run Selector columns are the same as
# the columns you get from BigQuery, but with capitalization.
# Double-quotes are NCBI run selector specific, single-quote
# are from BQ or elsewhere. No difference at runtime, just 
# marking for the sake of whoever is looking at this later
# when something breaks. :-)

# these columns are considered "equivalent" and nullfill each other
equivalence_standard = {
		'collection': ['collection'], # default indicator column -- change this if you change cfg.indicator_column!

		'assay_type': ['assay_type', 'Assay Type', 'assay_type_sam', 'assay_type_run'],
		'attributes': ['attributes'], # excluding j_attr on purpose
		'avgspotlen': ['avgspotlen', 'AvgSpotLen'],
		'bases': ['bases', 'Bases'],
		'BioProject': ['BioProject', 'bioproject', 'Bioproject'],
		'bytes': ['bytes', 'Bytes'],
		'center_name': ['center_name', 'Center Name', 'center_name_insdc', 'insdc_center_name_sam'],
		'clade': ['clade'],
		'continent': ['continent'],
		'country': ['country'],
		'date_collected': ['date_collected', 'date_collection', 'collection_date_sam', 'date_of_collection_sam', 'date_isolation', 'Collection_Date', 'sample_collection_date_sam_s_dpl127', 'collection_date_orig_sam', 'collection_date_run', 'date_coll', 'date', 'colection_date_sam', 'collectiondateym_sam'],
		'date_collected_year': ['date_collected_year', 'collection_year_sam', 'year_isolated_sam'],
		'date_collected_month': ['date_collected_month', 'collection_month_sam'],
		'date_collected_day': ['date_collected_day', 'samplingday_sam'],
		'date_sequenced': ['run_file_create_date', 'run_date_run'],
		'genotype': ['genotype', 'genotype_sam_ss_dpl92', 'genotype_variation_sam', 'spoligotype_sam',  'mlva___spoligotype_sam', 'vntr_sam', 'serotype_sam', 'serovar_sam', 'orgmod_note_sam_s_dpl305', 'atpe_mutation_sam', 'rv0678_mutation_sam', 'mutant_sam', 'subtype_sam', 'pathotype_sam', 'subgroup_sam', 'arrayexpress_species_sam'],
		'geoloc_info': ['geo_loc_name_country', 'geo_loc_name_country_calc', 'geoloc_country_calc', 'isolation_country_sam', 'country_sam', 'geographic_location__country_and_or_sea__sam', 'geoloc_country_or_sea', 'geo_loc_name_country_continent', 'geographic_location_sam_s_dpl93', 'geo_loc_name_country_continent_calc', 'geo_loc_name_sam', 'geographical_location_sam', 'geo_loc_name_sam_s_dpl209', 'geographic_location__region_and_locality__sam', 'geographic_location__country_and_or_sea__region__sam', 'geographic_location__countryand_orsea_region__sam', 'region_sam', 'geoloc_country_or_sea_region', 'isolation_site_sam', 'geo_loc_name_run', 'geographic_location__country_and_or_sea__run'], # doi_location_sam and geo_accession_exp should be lowest priority
		'host': ['host', 'host_sciname', 'host_sam', 'host_taxid_sam', 'specific_host_sam', 'host_common', 'host_common_name_sam', 'host_run', 'host_scientific_name_sam', 'host_taxon_id_sam', 'host_common_name_run', 'host_scientific_name_run'],
		'host_disease': ['host_disease', 'disease', 'disease_sam', 'host_disease_sam'],
		'host_confidence': ['host_confidence'],
		'host_commonname': ['host_commonname', 'host_streetname'],
		'host_scienname': ['host_scienname'],
		'instrument': ['instrument', 'Instrument'],
		'isolation_source': ['isolation_source', 'sample_type_sam_ss_dpl131', 'sample_source', 'tissue_sam_ss_dpl145', 'env_medium_sam', 'host_tissue_sampled_sam_s_dpl239', 'isolation_source_sam', 'isolation_type_sam', 'isolation_source_sam_ss_dpl261', 'isolation_source_host_associated_sam_s_dpl264', 'specimen_sam', 'culture_collection_sam_ss_dpl468', 'host_body_product_sam', 'bio_material_sam', 'tissue_source_sam', 'subsource_note_sam', 'env_biome_sam', 'env_feature_sam', 'env_material_sam', 'source_name_sam', 'isolation_source_host_associated_sam_s_dpl263', 'plant_product_sam', 'isolation_source_run', 'sample_type_run_s_dpl517', 'isolate_run', 'sample_type_exp'],
		'isolation_source_cleaned': ['isolation_source_cleaned'],
		'isolation_source_raw': ['isolation_source_raw'],
		'isolate_sam_ss_dpl100': ['isolate_sam_ss_dpl100'], # this has special handling due to usually being a sample name, but sometimes being actually useful isolation source information
		'latlon': ['latlon', 'lat_lon_sam_s_dpl34', 'lat_lon', 'latitude_and_longitude_sam', 'lat_lon_run'],
		'lat': ['lat', 'geographic_location__latitude__sam'],
		'lon': ['lon', 'geographic_location__longitude__sam'],
		'library_name': ['library_name', 'libraryname', 'library_id', 'library_ID'],
		'librarylayout': ['librarylayout', 'LibraryLayout'], # no underscore to match BQ format
		'libraryselection': ['libraryselection', 'LibrarySelection'],
		'librarysource': ['librarysource', 'LibrarySource'],
		'lineage': ['lineage', 'lineage_sam', 'linege_sam', 'mtb_lineage_sam', 'subspecf_gen_lin_sam', 'literature_lineage', 'note_sam'],
		'organism': ['organism', 'sub_species_sam', 'organism_sciname', 'organism_common', 'organism_sam', 'tax_id_sam_ss_dpl29', 'subspecies_sam', 'Organism', 'scientific_name_sam', 'species_sam', 'common_name_sam', 'type_material_sam', 'mycobacterium_type_result_result_sam', 'phenotype_sam', 'scientific_name_run', 'common_name_run'],
		'platform': ['platform', 'Platform'], # platform_sam and platform_run seem to be something else
		'primary_search': ['primary_search'],
		'region': ['region'],
		'sra_study': ['sra_study', 'SRA Study'], # SRP ID
		'strain': ['strain', 'strain_sam_ss_dpl139', 'strain_name_alias_sam', 'strain_geno', 'sub_strain_sam_s_dpl389', 'strain_genotype_sam_s_dpl382', 'cell_line_sam', 'cell_line_run'],
}
equivalence_id_columns = {
	'run_id': ['run_id', 'run_index', 'acc', 'run', 'Run', 'run_accession', 'run_acc'],
	'sample_id': ['sample_id', 'sample_index', 'biosample', 'BioSample', 'Biosample', 'sample'],
	'SRX_id': ['SRX_id', 'experiment', 'Experiment'], # DO NOT USE experiment_sam! that is something totally different! 
	'SRS_id': ['SRS_id', 'sample_acc'], # SRS/ERS/DRS accession
	'file': ['file', 'filename']
}
id_columns = equivalence_id_columns['run_id'] + equivalence_id_columns['sample_id'] + equivalence_id_columns['SRS_id'] + equivalence_id_columns['SRX_id']
equivalence = equivalence_standard | equivalence_id_columns
assert len(set(sum(equivalence.values(), []))) == len(sum(equivalence.values(), []))  # effectively asserts no shared values (both within a key's value-lists, and across all other value-lists)

# Once columns are merged, the "equivalence" columns are dropped since they have redundant information.
columns_to_keep_after_rancheroize = equivalence.keys()
columns_to_drop_after_rancheroize = [item for value in equivalence.values() for item in value[1:]]

# primary_search tends to include values from these columns (this intentionally includes pre-rancheroized columns.)
common_primary_search_values = [
	# rancheroized
	'BioProject', 'isolation_source', 'library_name', 'run_id', 'sample_id', 'sra_study', 'SRX_id', 'SRS_id',
	# not rancheroized but common
	'acc', 'bioproject', 'biosample', 'experiment', 'isolate_run', 'isolate_sam_ss_dpl100', 'library_id', 'sample_name', 'sample_acc', 'sample_id_run']

# Special handling for taxoninomic columns -- don't add them to any of the list-to-x stuff below.
special_taxonomic_handling = {key: value for key, value in equivalence.items() if key in ['clade', 'genotype', 'lineage', 'organism', 'strain']}
all_taxoncore_columns = sum(special_taxonomic_handling.values(), [])

# Special handling for host information columns, depending on config settings
host_info = ['host_info', 'host_disease_stat_sam', 'host_life_stage_sam', 'pathogenicity_sam', 'passaged_in_sam', 'patient_year_of_arrival_to_israel_sam', 'passage_species_sam', 
'patient_country_of_birth_sam', 'subsrc_note_sam_s_dpl392', 'host_status_sam', 'patient_year_of_birth_sam', 'patient_finished_treatment_sam', 'host_disease_stage_sam',
'patient_has_hiv_sam', 'patient_sex_sam', 'age_sam', 'host_disease_outcome_sam', 'host_sex_sam', 'pulmonary_disord_sam', 'host_age_sam', 'host_health_state_sam', 
'host_subject_id_sam', 'host_description_sam', 'age_at_death_sam', 'age_at_death_units_sam', 'age_atdeath_weeks_sam']

# Sometimes we end up with list columns thanks to merges, or when going from run-to-sample, or because the original data was in list format.
# Nested lists will be flattened automatically. But how should we deal with these flat list columns? Default behavior: list_to_set_uniq

# In: pl.List() of floats or integers
# Out: Float
list_to_float_sum = ['bytes', 'bases', 'mbases', 'mbytes', 'mbytes_sum', 'mbases_sum', 'bytes_sum', 'bases_sum']

# In: pl.List() of pl.Utf8
# Out: pl.Utf8
list_to_string_join = ['isolate_info']

# In: pl.List() of any type
# Out: pl.List() if any row contains at least two unique values, inner type otherwise
list_to_set_uniq = [
	'assay_type', 
	'BioProject', 
	'BioSampleModel',
	'center_name', 
	'center_name_insdc', 
	'collection', # typical indicator column
	'country_1', # intermediate column used in metadata standardization
	'datastore_filetype', 
	'datastore_provider',
	'instrument',
	'host_info',
	'pheno_source',
	'primary_search',
	'geoloc_info_unhandled',
	'run_id',
	'sra_study',
	'libraryselection',
	'librarylayout',
	'librarysource',
	'SRX_id',
	'platform'
]

# Unchanged
list_to_list_silent = [
	'avgspotlen',
	'geo_loc_name_sam',
	'geoloc_country_calc',
	'geoloc_country_or_sea', 
	'geoloc_info'
]

# Throw an error... unless the mismatch is caused by the columns simply having null differences
list_throw_error = []

# Throw an error, even if the mismatch is just a null thing
list_throw_error_strict = ["BioSample", "sample_id"]

# In: pl.List() of any type
# Out: Inner type if flattening existing list, falling back on left or right if merge
list_fallback_or_null = [
	'host_disease',
	'host',
	'host_commonname',
	'host_confidence',
	'host_scienname',
	'country',
	'continent',
	'isolation_source',
	'isolation_source_raw',
	'isolation_source_cleaned',
	'region',
]

# In: pl.List() of any type
# Out: Inner type, but rows that previously had lists of 2+ values turn to pl.Null
# Additionally, columns in list_to_null will be coalesced when merging equivalent columns, 
# rather than turned into a list and then re-processed later.
list_to_null = [
	'center_name',
	'center_name_insdc',
	'coscolla_country', 
	'coscolla_mean_depth',
	'coscolla_percent_not_covered',
	'coscolla_sublineage',
	'date_collected',
	'date_collected_day',
	'date_collected_month',
	'date_collected_year',
	'date_sequenced',
	'latlon',
	'pheno_AMIKACIN',
	'pheno_BEDAQUILINE',
	'pheno_CAPREOMYCIN',
	'pheno_CIPROFLOXACIN',
	'pheno_CLOFAZIMINE',
	'pheno_CYCLOSERINE',
	'pheno_DELAMANID',
	'pheno_ETHAMBUTOL',
	'pheno_ETHIONAMIDE',
	'pheno_LEVOFLOXACIN',
	'pheno_ISONIAZID',
	'pheno_KANAMYCIN',
	'pheno_LINEZOLID',
	'pheno_MOXIFLOXACIN',
	'pheno_OFLOXACIN',
	'pheno_PAS',
	'pheno_PYRAZINAMIDE',
	'pheno_RIFABUTIN',
	'pheno_RIFAMPICIN',
	'pheno_STREPTOMYCIN',
	'release_date',
	'sra_study'
]



# not used, but I'm leaving this here for people who want it
equivalence_extended = {
		'BioSampleModel': ['BioSampleModel', 'biosamplemodel', 'biosamplemodel_sam'],
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
