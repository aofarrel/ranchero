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

bq_col_to_ranchero_col_minimal = {
	'acc': 'run_accession', 
	'bioproject': 'BioProject', 
	'biosample': 'BioSample', 
	'sample_acc': 'other_id_sra'
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

not_strings = {
	"avgspotlen": pl.Int16(),
	"bases": pl.Int64(),
	"bytes": pl.Int64(),
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
	'librarysource': str,
	'organism': str,
	'other_id_sra': str,
	'platform': str,
	'primary_search': str,
	'run_accession': set,
	'scientific_name_sra': str,
	'strain_sra': str
}

# includes rancherorized and non-rancheroized columns
recommended_sra_columns = [
	'acc'
	'assay_type',
	'bases',
	'BioProject',
	'bioproject',
	'BioSample',
	'biosample',
	'collection_date_sam',
	'datastore_filetype',
	'datastore_provider',
	'genotype_sam_ss_dpl92',
	'genotype_sra',
	'geographic_location__country_and_or_sea__sam',
	'geo_loc_name_country_calc',
	'geo_loc_name_sam',
	'host_sra',
	'isolate_info',
	'isolate_sam_ss_dpl100',
	'isolation_source_sam',
	'library_name',
	'librarylayout',
	'librarysource',
	'lineage_sra',
	'mbytes',
	'organism',
	'other_id_sra',
	'platform',
	'primary_search',
	'run_accession',
	'scientific_name_sra',
	'strain_sra'
]