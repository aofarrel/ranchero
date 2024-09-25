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
rts__drop = ['release_date', 'create_date', 'library_name', 'avgspotlen', 'datastore_region', 'release_date', 'napier_type'] # napier_type causes issues due to duplicates in the Napier dataset
rts__keep_as_list = ['librarylayout', 'libraryselection', 'librarysource', 'instrument', 'platform', 'assay_type', 'run_file_version', 'isolate_info'] # non-unique values will be kept
rts__keep_as_set = ['BioProject', 'datastore_filetype', 'datastore_provider', 'primary_search', 'run_index', 'SRX_id', 'sra_study'] # non-unique values will be dropped
rts__warn_if_list_with_unique_values = ['center_name', 'center_name_insdc', 'isolation_source', 'date_collected', 'geoloc_country_calc', 'geoloc_country_or_sea', 'host_sciname', 'organism_common', 'release_date']

date = ['collection_year_sam', 'collection_month_sam', 'collectiondateym_sam',  'year_isolated_sam']
organism = ['phenotype_sam', 'organism_sciname', 'organism_common']
source_information = ['specimen_sam', 'isolation_source_sam_ss_dpl261', 'sample_type_exp', 'host_body_product_sam', 'tissue_source_sam', 'env_biome_sam', 'env_feature_sam', 'env_material_sam', 'sample_type_sam_ss_dpl131', 'isolation_source_sam', 'isolate_sam_ss_dpl100', 'source_name_sam', 'sample_type_sam_ss_dpl131']
host_information = ['patient_year_of_birth_sam', 'patient_sex_sam', 'patient_has_hiv_sam', 'host_status_sam', 'patientid_sam', 'patient_finished_treatment_sam', 'patient_has_hiv_sam', 'patient_sex_sam', 'patient_year_of_birth_sam', 'specific_host_sam', 'anonymised_badger_id_sam', 'patient_number_sam_s_dpl111', 'age_sam', 'host_disease_outcome_sam', 'host_disease_stage_sam', 'host_sex_sam', 'env_broad_scale_sam', 'env_local_scale_sam', 'env_medium_sam', 'mouse_strain_sam', 'host_age_sam', 'host_disease_sam', 'host_sam', 'host_sciname', 'host_health_state_sam', 'host_subject_id_sam', 'host_description_sam', 'age_at_death_sam', 'age_at_death_units_sam', 'age_atdeath_weeks_sam']
location = ['doi_location_sam', 'geoloc_country_or_sea_region', 'patient_country_of_birth_sam', 'country_sam', 'geoloc_name', 'altitude_sam_s_dpl11', 'isolation_country_sam', 'latitude_and_longitude_sam', 'lat_lon_sam_s_dpl34', 'geo_accession_exp', 'geoloc_country_calc', 'geoloc_country_or_sea']
geno = ['genotype_variation_sam', 'strain_name_alias_sam', 'orgmod_note_sam_s_dpl305', 'atpe_mutation_sam', 'rv0678_mutation_sam', 'subsource_note_sam', 'mutant_sam', 'lineage_sam','note_sam','linege_sam',
'subspecf_gen_lin_sam', 'species_sam', 
'spoligotype_sam', 'vntr_sam', 'organism_sam', 'subtype_sam', 'pathotype_sam', 'serotype_sam', 'serovar_sam', 'genotype', 'subgroup_sam', 'arrayexpress_species_sam', 'strain', 'organism', 'organism_common', 'organism_sciname', 'mlva___spoligotype_sam']

# these columns are considered "equivalent" and nullfill each other
equivalence = {
		'collected_by': 'collected_by',
		'collected_by_sam': 'collected_by',
		'collected_by_run': 'collected_by',

		'isolation_source_sam': 'isolation_source',
		'isolation_source_sam_ss_dpl261': 'isolation_source',
		'isolation_source_host_associated_sam_s_dpl264': 'isolation_source',
		'isolation_source_host_associated_sam_s_dpl263': 'isolation_source',
		'isolation_source_run': 'isolation_source',

		'collection_date_sam': 'date_collected',
		'collection_date_run': 'date_collected',
		'collection_date_orig_sam': 'date_collected',
		'date_collected': 'date_collected',

		'lat_lon_sam_s_dpl34': 'geoloc_latlon',
		'lat_lon_run': 'geoloc_latlon',

		'geo_loc_name_sam': 'geoloc_name',
		'geo_loc_name_run': 'geoloc_name',
		'region_sam': 'geoloc_name',

		'host': 'host',
		'host_sam': 'host',
		'host_run': 'host',

		'host_disease': 'host_disease',
		'host_disease_sam': 'host_disease',
		'host_disease_run': 'host_disease',
  		
  		 # this one is all over the place - info, sample ids...
		'isolate_info': 'isolate_info',
		'isolate_sam_ss_dpl100': 'isolate_info',
		'isolate_run': 'isolate_info',

		'assay_type_sam': 'assay_type',
		'assay_type_run': 'assay_type',

		'cell_line_sam': 'cell_line',
		'cell_line_run': 'cell_line',

		'common_name_sam': 'organism_common',
		'common_name_run': 'organism_common',

		'geographic_location__country_and_or_sea__sam': 'geoloc_country_or_sea',
		'geographic_location__country_and_or_sea__run': 'geoloc_country_or_sea',

		'geographic_location__country_and_or_sea__region__sam': 'geoloc_country_or_sea_region',
		'geographic_location__country_and_or_sea__region__run': 'geoloc_country_or_sea_region',

		'host_common_name_sam': 'host_common',
		'host_common_name_run': 'host_common',

		'host_scientific_name_sam': 'host_sciname',
		'host_scientific_name_run': 'host_sciname',
 
 		# keeping distinct from platform as it seems to have other information
		'platform_sam': 'platform_other',
		'platform_run': 'platform_other',

		'sample_type_sam_ss_dpl131': 'sample_type',
		'sample_type_run_s_dpl517': 'sample_type',

		'scientific_name_sam': 'organism_sciname',
		'scientific_name_run': 'organism_sciname'
	}

# everything here has been checked against a massive BQ file to see if they have run/sample
# specific versions. for run and sample specific versions (including hypotheticals) see SVR.
common_col_to_ranchero_col = {
	# run_acc -- used for run-indexed tables
	'acc': 'run_index',
	"Run": "run_index",
	'run_accession': 'run_index',

	# sample_index -- used for sample-indexed table
	# for NCBI data we use the BioSample (SAMD/SAME/SAMN) ID
	# columns should ALWAYS be type str, not a list nor set
	# if there's more than one sample name, use other_samp_names
	'biosample': 'sample_index',
	"BioSample": "sample_index",

	# date_collected
	# BQ splits this into collection_date_sam and collection_date_run
	"Collection_Date": "date_collected",
	"sample_collection_date_sam_s_dpl127": "date_collected",

	# SRX_id
	# DO NOT USE experiment_sam! that is something totally different! and there's no experiment_run afaik
	# but if there were it might be just as whacky as experiment_sam so let's not even go there
	"Experiment": "SRX_id",
	'experiment': 'SRX_id',

	# locational stuff
	# standardize.py will expect these when trying to convert to one geoloc
	"geo_loc_name_country": "geoloc_country",
	'geo_loc_name_country_calc': "geoloc_country_calc",
	"geo_loc_name_country_continent": "geoloc_continent",
	"geo_loc_name": "geoloc_name",
	"lat_lon": "geoloc_latlon",
	
	# host
	# standardize.py will expect these when trying to convert to one host
	"Host": "host",
	"Host_disease": "host_disease",
	'host_disease': 'host_disease',

	# other metadata
	"Assay Type": "assay_type",
	"AvgSpotLen": "avgspotlen",
	"Bases": "bases",
	'total_bases_run': 'bases',
	'bioproject': 'BioProject',  # already capitalized in NCBI Run Sel
	"BioSampleModel": "biosamplemodel",
	'biosamplemodel_sam': 'biosamplemodel',  # should be impossible for this to have a run version
	"Bytes": "bytes",
	"Center Name": "center_name", # underscore to match BQ format
	"collected_by": "collected_by", # both BQ and NCBI Run Sel use this format
	"DATASTORE filetype": "datastore_filetype",
	"DATASTORE provider": "datastore_provider",
	"DATASTORE region": "datastore_region",
	'datastore_filetype': 'datastore_filetype',
	'datastore_provider': 'datastore_provider',
	'datastore_region': 'datastore_region',
	'genotype_sam_ss_dpl92': 'genotype',  # no obvious run equivalent
	"Instrument": "instrument",  # already decapitalized in BQ
	"isolation_source": "isolation_source",
	'insdc_center_name_sam': 'center_name_insdc',  # should be impossible for this to have a run version
	"Library Name": "library_name", # underscore to match BQ format
	"LibraryLayout": "librarylayout", # no underscore to match BQ format
	"LibrarySelection": "libraryselection",
	"LibrarySource": "librarysource",
	'mbytes': 'mybytes',
	'mtb_lineage_sam': 'lineage_mtb',  # this is uncommon, and seems to have no run equivalent
	'mycobacteriaceae_family_sam': 'mycobacteriaceae_family_sam',
	'mycobacterium_genus_sam': 'mycobacterium_genus_sam',
	"Organism": "organism",
	'organism': 'organism',
	"Platform": "platform",
	'primary_search': 'primary_search',
	"ReleaseDate": "release_date",
	'releasedate': 'release_date',
	'sample_acc': 'XRS_id',  # SRS/ERS/DRS accession
	'strain_genotype_sam_s_dpl382': 'strain_geno',  # no obvious run equivalent
	'strain_sam_ss_dpl139': 'strain',
	"version": "run_file_version",
	"Sample Name": "other_id",  # *sometimes* SAME/SAMN/SAMD but often something else
	"SRA Study": "sra_study", # SRP ID
	"strain": "strain",
	
}

common_col_to_ranchero_col_plus_sra = {k: f"{v}_sra" for k, v in common_col_to_ranchero_col.items()}

# common-sense guesses of columns that your data might have that should match to Ranchero columns
# these are *guesses* that may be incorrect, so use with caution
extended_col_to_ranchero = {
	# sample_index
	'sample': 'sample_index', 

	# date_collected
	'date': 'date_collected',
	'date_coll': 'date_collected',

	'lineage': 'lineage_mtb'
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
	"mbytes": pl.Int32(),
	"run_file_version": pl.Int16(),
	"self_count": pl.Int32(),
	"tax_id": pl.Int32(),
	"total_count": pl.Int32()
}

# currently unused
polars_cast_not_attributes = {
	"assay_type": str,
	"BioProject": str,
	"BioSample": str,
	"collection_date_sam": str,
	"datastore_filetype": list[str],
	"datastore_provider": list[str],
	'genotype_sra': str,
	'geographic_location__country_and_or_sea__sam': str,
	'geo_loc_name_country_calc': str,
	'geo_loc_name_sam': str,
	'host_sra': str,
	'isolate_info': str,
	'isolation_source_sam': str,
	'library_name': str,
	'librarylayout': str,
	'libraryselection': str,
	'librarysource': str,
	'organism': str,
	'other_id_sra': str,
	'platform': str,
	'primary_search': str,
	'run_accession': set,
	'scientific_name_sra': str,
	'sra_study': str,
	'strain_sra': str
}
