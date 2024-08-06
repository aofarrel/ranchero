import polars as pl

ncbi_run_selector_col_to_ranchero_col = {
	# double check these for accuracy!!
	"Run": "run_accession",
	"Assay Type": "assay_type",
	"AvgSpotLen": "avgspotlen",
	"Bases": "bases",
	#"BioProject": "BioProject",
	#"BioSample": "BioSample",
	"BioSampleModel": "biosamplemodel_sam",
	"Bytes": "bytes",
	"Center Name": "center_name",
	"collected_by": "collected_by",
	"Collection_Date": "collection_date_sam",
	"Consent": "consent",
	"DATASTORE filetype": "datastore_filetype",
	"DATASTORE provider": "datastore_provider",
	"DATASTORE region": "datastore_region",
	"Experiment": "experiment", # SRX ID
	"geo_loc_name_country": "geo_loc_name_country",
	"geo_loc_name_country_continent": "geo_loc_name_country_continent",
	"geo_loc_name": "geo_loc_name",
	"Host_disease": "host_disease",
	"Host": "host",
	"Instrument": "instrument",
	"isolate": "isolate",
	"isolation_source": "isolation_source",
	"lat_lon": "lat_lon",
	"Library Name": "library_name", # underscore to match bq format
	"LibraryLayout": "librarylayout", # no underscore to match bq format
	"LibrarySelection": "libraryselection",
	"LibrarySource": "librarysource",
	"Organism": "organism",
	"Platform": "platform",
	"ReleaseDate": "releasedate",
	"create_date": "run_file_create_date",
	"version": "version",
	"Sample Name": "sample_name",
	"SRA Study": "sra_study", # SRP ID
	"strain": "strain"
}

bq_col_to_ranchero_col = {
	'acc': 'run_accession', 
	'bioproject': 'BioProject', 
	'biosample': 'BioSample', 
	'genotype_sam_ss_dpl92': 'genotype_sra', 
	'host_sam': 'host_sra', 
	'host_scientific_name_sam': 'host_sciname_sra',
	'isolate_sam_ss_dpl100': 'isolate_info',
	#'mtb_lineage_sam': 'lineage_sra', # this is uncommon!
	'sample_acc': 'other_id_sra', 
	'scientific_name_sam': 'scientific_name_sra',
	'strain_sam_ss_dpl139': 'strain_sra'
}

polars_cast = {
	"bases": pl.Int16(),
	"bytes": pl.Int16()
}

recommended_sra_columns = [
	# acc --> run_accession
	'assay_type',
	'bases',
	'BioProject',
	'BioSample',
	'collection_date_sam',
	'datastore_filetype',
	'datastore_provider',
	# genotype_sam_ss_dpl92 --> 'genotype_sra'
	'genotype_sra',
	'geographic_location__country_and_or_sea__sam',
	'geo_loc_name_country_calc',
	'geo_loc_name_sam',
	'host_sra',
	'isolate_info',
	'isolation_source_sam',
	'library_name',
	'librarylayout',
	'librarysource',
	#'lineage_sra',  too rare?
	'organism',
	'other_id_sra',
	'platform',
	'primary_search',
	'run_accession',
	'scientific_name_sra',
	'strain_sra'
]