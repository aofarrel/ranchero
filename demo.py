import time
start = time.time()
try:
	import src as Ranchero
except Exception as e:
	print("Failed to import Ranchero! Please make sure you have installed its dependencies.")
	print("Ranchero is currently not pip-installable, so you will want to run this script in the same directory as Ranchero (import looks for ./src/)")
	print(f"Exception was: {e}")
	exit(1)
import polars as pl
pl.Config.set_fmt_table_cell_list_len(5)
pl.Config.set_fmt_float("mixed")
pl.Config.set_tbl_cols(-1)

print("\n\nRanchero is designed to make wrangling bioinformatics metadata, especially NCBI metadata, a little bit easier.")
print()
print("""Ranchero creates, merges, and updates polars dataframes from TSVs, BigQuery result tables, or NCBI run selector. In a Ranchero dataframe, every row represents either:
	* One biological sample, analogous to SAMN/SAME/SAMD on SRA -- we call this "sample-indexed"
	* One "run" from the sequencer, analogous to SRR/ERR/DRR on SRA -- we call this "run-indexed"
		--> Note that a single "run" may have multiple FASTQ files, especially when dealing with Illumina PE data
This distinction is moot if you only have one run per sample, but it comes in handy when you're not working with INSDC data.""")

print()
print("We'll be going through an example of some BigQuery data, in ndjson (new-delimited JSON) format, pulled from the nih-sra-datastore.sra.metadata table.")
print("Note that what you get out of BigQuery may not be compatiable with polars' built-in JSON reader. Ranchero will attempt to fix the formatting of JSONs in order to actually be able to read them.")
print("Now, let's keep in mind that run accessions in this kind of BigQuery output always have an \"attributes\" field, which contains key-value pairs of valuable metadata that nevertheless often doesn't fit NCBI's typical column standards.")
print("It looks like this:")
print('\n\n"attributes":[{"k":"bases","v":"375160864"},{"k":"bytes","v":"273363275"},{"k":"run_file_create_date","v":"2018-08-26T21:00:00.000Z"},{"k":"collected_by_sam","v":"Phthisiopneumology Institute/Yale SPH"},{"k":"collection_date_sam","v":"2013"},{"k":"culture_id_sam","v":"S23633"},{"k":"host_disease_sam","v":"Tuberculosis"},{"k":"host_sam","v":"Homo sapiens"},{"k":"isolate_sam_ss_dpl100","v":"Not applicable"},{"k":"isolation_source_sam","v":"Mycobacterial Culture"},{"k":"lat_lon_sam_s_dpl34","v":"Not applicable"},{"k":"strain_sam_ss_dpl139","v":"Not applicable"},{"k":"primary_search","v":"482716"},{"k":"primary_search","v":"9914694"},{"k":"primary_search","v":"MTB_moldova_S23633"},{"k":"primary_search","v":"PRJNA482716"},{"k":"primary_search","v":"S23633"},{"k":"primary_search","v":"S23633_1.fastq.gz"},{"k":"primary_search","v":"SAMN09914694"},{"k":"primary_search","v":"SRP156366"},{"k":"primary_search","v":"SRR7755692"},{"k":"primary_search","v":"SRS3715459"},{"k":"primary_search","v":"SRX4611618"}]')
print("\n\nInstead of keeping one nightmarishly large attributes column, Ranchero instead pulls every value in any run accessions's \"attributes\" dictionary into a new column.")
print("(primary_search, which usually has repeated keys, has special handling -- all unique values are compiled into a single primary_search column.)")
print("Ranchero can also optionally drop all samples that aren't paired Illumina data, but we won't enable that in this example.")
input("\n\nPress enter to continue...\n\n")


print("Let's parse a JSON from a BigQuery search as an example. This is a shortened version of some results a BigQuery search of the entire Mycobacterium genus.")
#print("If you don't want to sort through the entire genus like I did, here's a different query you could try (but be aware this will exclude samples which are not on the tax_analysis table):")
#print("""SELECT *
#FROM `nih-sra-datastore.sra.metadata` as m, `nih-sra-datastore.sra_tax_analysis_tool.tax_analysis` as tax
#WHERE m.acc=tax.acc and tax_id=77643 and m.librarysource!="TRANSCRIPTOMIC" and tax.total_count > 10000""")
print("\nBecause this BigQuery search was done on the Cloud-based Metadata Table, it is run-indexed, but also has BioSample IDs and taxonomic information. Be aware that the tax_analysis table adds a \"name\" field that is the scientific name of the organism, not a sample name.")

mycobact_from_BigQuery = Ranchero.from_bigquery("./inputs/demo/bq_tba6_randomish.json")
old_dataframe = mycobact_from_BigQuery
old_col_count = mycobact_from_BigQuery.shape[1]
print(f"\nWe've imported a {mycobact_from_BigQuery.shape[0]} row dataframe with {old_col_count} columns.")
print("The columns are as follows:")
print(mycobact_from_BigQuery.columns)

print(f"\nBecause {mycobact_from_BigQuery.shape[1]} columns may not fit on the screen very well, we'll just print a few columns here (still, we recommend your terminal display be at least 200 characters per line)...")
input("\n\nPress enter to continue (consider resizing your terminal window first)...\n\n")

view_cols = ['run_index', # will only exist after rancheroization; we want it at the front
	'acc', 'assay_type', 'center_name', 'instrument', 'librarylayout', 'librarysource', 'biosample', 'organism', 
	'geo_loc_name_country_calc', 'geo_loc_name_country_continent_calc', 'geo_loc_name_sam', 'collection_date_sam', 
	'genotype_sam_ss_dpl92', 'host_sam', 'isolate_sam_ss_dpl100', 'isolation_source_sam', 'lat_lon_sam_s_dpl34', 
	'geographic_location__country_and_or_sea__sam', 'host_scientific_name_sam', 'anonymised_bovine_id_sam', 'strain_sam_ss_dpl139',
	'host_tissue_sampled_sam_s_dpl239', 'passage_history_sam_s_dpl312', 
	'country', 'region', 'continent', 'latlon', 'isolation_source', 'geoloc_info'] # only after rancheroize/standardize
Ranchero.super_print(mycobact_from_BigQuery.select(Ranchero.valid_cols(mycobact_from_BigQuery, view_cols)), "input dataframe (selected columns)")

print("\nYou'll notice some issues right away:")
print("  * Our run-index column has the ambigious name \"acc\" which could also apply to a BioSample accession")
print("  * geo_loc_name_sam is a list, but only has one/zero values and would be simpler as a string")
print('  * columns that previous where just "missing" are now properly converted to null values')
print("  * Some of those columns could probably be merged into one more generic column")
print()
input("\n\nPress enter to continue...\n\n")
print("The do-it-all function for cleaning up your dataframes is rancheroize. Let's rancheroize this dataframe.")
mycobact_from_BigQuery = Ranchero.rancheroize(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(Ranchero.valid_cols(mycobact_from_BigQuery, view_cols)), "dataframe after running rancheroize() (selected columns)")
print()
print("\nChanges made:")
print("  * lat_lon_sam_s_dpl34 now has shorter name latlon")
print("  * Several columns were merged together, most notably ones related to location and how a sample was isolated")
print(f" --> This means the dataframe now stores the same amount of data in only {mycobact_from_BigQuery.shape[1]} columns (decreased from {old_col_count} columns, recall that not all columns are printed here)")

print("\nWhile we have standardized the columns, we haven't really standardized the actual data. First, let's standardize_countries(). This will convert countries into their ISO codes 3166 codes, as well as attempt to pull out information below a country level (state, province, town, etc).")
print("You might be thinking \"I bet that'll require a lot of string functions and be very slow.\" You are correct on the first point, but thanks to the speed of polars, we can get away with these sorts of things much faster than expected.")
print("As a very rough benchmark: An older verison of Ranchero that used pandas would take about 8 hours to run on samples from MTBC. This version using polars can standardize metadata in 15 minutes for the entire Mycobacterium genus on the same hardware (Intel-based 2019 MacBook Pro).")
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'geoloc_info']), "country information after rancheroize, but before standardize_countries()")
mycobact_from_BigQuery = Ranchero.standardize_countries(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'continent', 'country', 'region']), "dataframe after standardizing location (geoloc_info --> country, region, continent)")

print("\nNeat, right?")
print("This dataset is mostly MTBC, which almost always has a host. Let's standardize information about host organism.")
print('Ranchero has a built-in dictionary of common and scientific names that it checks against to assign hosts. Because some animals have referred to by only amibigious names, like "bovine" (which is probably Bos taurus but could be Bos indicus) there is also a confidence score. 1 is low confidence, 2 is moderate, 3 is high.')
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'host']), "country information after rancheroize, but before standardize_hosts()")
mycobact_from_BigQuery = Ranchero.standardize_hosts(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'host_scienname', 'host_commonname', 'host_confidence']), "dataframe after standardizing hosts (host --> host_scienname, host_commonname, 'host_confidence')")

print("\nIn addition to wrangling your cattle, Ranchero can also attempt to standardize the taxonomic information of your samples.")
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'strain_sam_ss_dpl139', 'subtype_sam', 'organism', 'pathotype_sam', 'serotype_sam', 'serovar_sam', 'genotype_sam_ss_dpl92', 'scientific_name_sam', 'subgroup_sam', 'mlva___spoligotype_sam']), "dataframe after standardizing taxonomic information (host --> host_scienname, host_commonname, 'host_confidence')")
mycobact_from_BigQuery = Ranchero.taxoncore(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'strain', 'lineage', 'organism', 'clade']), "dataframe after standardizing taxonomic information (all that stuff --> strain, lineage, organism (as in scientific name), clade (within Mycobacterium genus)")
print('You\'ll notice that for samples where we don\'t know the MTB lineage, we say "tuberculosis: unclassified" as we don\'t know if it\'s animal-adapted lineage or a human-adapted one.')
print("Additionally, SRR21747047 had \"S035\" in the \"strain_sam_ss_dpl139\" column but that was not included as a strain in the final data table. This is because S035 as a strain-name isn't in Ranchero's built-in taxonomic dictionary, as I'm not aware of literature charcterizing it, and because this field sometimes is used for sample IDs.")
print("You can modify the dictionary Ranchero uses for taxonomic information, and virtually everything else involving these sorts of text replaces, by editing files in src/statics as needed.")

print('\nFinally, let\'s standardize what tissue/environment your samples are taken from. This is very open to interpretation, so unlike country metadata, Ranchero doesn\'t try to cover *absolutely everything* in the Mycobacterium genus and will leave some values "as reported" (not shown in this example).')
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'isolation_source']), "where-sample-came-from-but-not-geographic-location information after rancheroize, but before standardize_sample_source()")
mycobact_from_BigQuery = Ranchero.standardize_sample_source(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'isolation_source']), "where-sample-came-from-but-not-geographic-location information after standardization")

print("\nLet's compare the dataframe before and after all that rancheroizing and standardizing.")
print("------------ INPUT DATAFRAME ------------")
Ranchero.report(old_dataframe)
print("------------ CURRENT DATAFRAME ------------")
Ranchero.report(mycobact_from_BigQuery)
print("\n\nThere's more Ranchero can do, but this about covers the basics. Have a great day! 🐄\n")

