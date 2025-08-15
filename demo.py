EXAMPLE_FILE_PATH = "./inputs/demo/bq_tba6_randomish.json"
# This is a demo that introduces a user to the basics of Ranchero -- why it exists and what it can do.
# It's focused on wrangling a JSON file from a BigQuery search of the NIH SRA metadata data table. You
# can use ranchero for standardizing virtually any arbitrary CSV/TSV/JSON, but it was built with NCBI
# in mind, so that's what we're showing here.
#
# Most of this demo's results (number of columns, etc) aren't hardcoded, so you can try replacing
# EXAMPLE_FILE_PATH with any arbitrary JSON from BigQuery (but if it's not a pathogen/parasite,
# the host columns probably won't exist).

import os
import time
try:
	import ranchero as Ranchero
except Exception as e:
	print("Failed to import Ranchero! Please make sure you have installed its dependencies.")
	print(f"Exception was: {e}")
	exit(1)
try:
	os.path.isfile(EXAMPLE_FILE_PATH)
except Exception as e:
	print(f"Could not find example file at {EXAMPLE_FILE_PATH} -- modify line one of demo.py to the correct path to continue")
import polars as pl
pl.Config.set_fmt_table_cell_list_len(5)
pl.Config.set_fmt_float("mixed")
pl.Config.set_tbl_cols(-1)

print("\n\nRanchero is designed to make wrangling bioinformatics metadata, especially NCBI metadata, a little bit easier.")
print()
print("""Ranchero creates, merges, and updates polars dataframes from TSVs, BigQuery result tables, or NCBI run selector. In a Ranchero dataframe, every row represents either:
	* One biological sample, analogous to SAMN/SAME/SAMD on SRA -- we call this "sample-indexed"
	* One "run" from the sequencer, analogous to SRR/ERR/DRR on SRA -- we call this "run-indexed"
The run/sample distinction is moot if you only have one run per sample, but when working with INDSC data, you can have multiple runs per sample.""")

print()
s1 = "We'll be going through an example of some BigQuery data, in ndjson (new-delimited JSON) format, pulled from the Cloud-based Metadata Table, also known as the nih-sra-datastore.sra.metadata table. "
s2 = "When running BigQuery on the Cloud-based Metadata Table, you will get one result (line/row) per run accession (SRR/ERR/DRR) matching your query. A run accession typically contains one FQ or BAM file. "
s3 = "(Exception: For Illumina PE data, a run accession typically is uploaded as 2 fastq files, one forward, one reverse.) "
s4 = "A BioSample (SAMN/SAME/SAMD or SRS/ERS/DRS) contains at least one run accession. Since it can be more than one, our ndjson may have multiple lines with the same BioSample. "
s5 = "However, we can expect every run accession to be unique, so Ranchero uses the run accession column as an index."
print(s1+s2+s3+s4+s5)
s1 = "\nSome tips for using BigQuery:"
b1 = "\n\t* There's a total of six SRA metadata tables you can search with BigQuery. You can query multiple at once to get more metadata."
b2 = "\n\t* Ranchero expects BigQuery ndjsons to be from searching nih-sra-datastore.sra.metadata and/or nih-sra-datastore.sra_tax_analysis_tool.tax_analysis and may not standardize columns from other tables."
b3 = "\n\t* Be aware that nih-sra-datastore.sra_tax_analysis_tool.tax_analysis adds a \"name\" field that is the scientific name of the organism, not a sample name."
b4 = "\n\t* NCBI has some good examples here: https://www.ncbi.nlm.nih.gov/sra/docs/sra-bigquery-examples/"
b5 = "\n\t* The ndjson you get out of BigQuery may not be compatiable with polars' built-in JSON reader. Ranchero will attempt to fix the formatting of JSONs in order to actually be able to read them."
print(s1+b1+b2+b3+b4+b5)
input("\n\nPress enter to continue...\n\n")

s1 = "When running on the nih-sra-datastore.sra.metadata data table, every result (ie, every run accession) will have some standard metadata fields, such as \"librarysource\" and \"librarylayout\". "
s2 = "Each run accession also has an \"attributes\" field, which contains key-value pairs of valuable metadata. Some of these are standardized by NCBI, some... not so much. "
s3 = "As an example:"
s4 = '\n\n"attributes":[{"k":"bases","v":"375160864"},{"k":"bytes","v":"273363275"},{"k":"run_file_create_date","v":"2018-08-26T21:00:00.000Z"},{"k":"collected_by_sam","v":"Phthisiopneumology Institute/Yale SPH"},{"k":"collection_date_sam","v":"2013"},{"k":"culture_id_sam","v":"S23633"},{"k":"host_disease_sam","v":"Tuberculosis"},{"k":"host_sam","v":"Homo sapiens"},{"k":"isolate_sam_ss_dpl100","v":"Not applicable"},{"k":"isolation_source_sam","v":"Mycobacterial Culture"},{"k":"lat_lon_sam_s_dpl34","v":"Not applicable"},{"k":"strain_sam_ss_dpl139","v":"Not applicable"},{"k":"primary_search","v":"482716"},{"k":"primary_search","v":"9914694"},{"k":"primary_search","v":"MTB_moldova_S23633"},{"k":"primary_search","v":"PRJNA482716"},{"k":"primary_search","v":"S23633"},{"k":"primary_search","v":"S23633_1.fastq.gz"},{"k":"primary_search","v":"SAMN09914694"},{"k":"primary_search","v":"SRP156366"},{"k":"primary_search","v":"SRR7755692"},{"k":"primary_search","v":"SRS3715459"},{"k":"primary_search","v":"SRX4611618"}]'
s5 = "\n\nInstead of keeping one nightmarishly large attributes column, Ranchero instead pulls every value in any run accessions's \"attributes\" dictionary into a new column. "
s6 = "(primary_search, which usually has repeated keys, has special handling -- all unique values are compiled into a single primary_search column.) "
s7 = "Ranchero can also optionally drop all samples that aren't paired Illumina data, but we won't enable that in this example. "
print(s1+s2+s3+s4+s5+s6+s7)
input("\n\nPress enter to parse a JSON from BigQuery as an example...\n\n")

view_cols = ['run_index', # will only exist after rancheroization; we want it at the front
	'acc', 'assay_type', 'center_name', 'instrument', 'librarylayout', 'librarysource', 'biosample', 'organism', 
	'geo_loc_name_country_calc', 'geo_loc_name_country_continent_calc', 'geo_loc_name_sam', 'collection_date_sam', 
	'genotype_sam_ss_dpl92', 'host_sam', 'isolation_source_sam', 'lat_lon_sam_s_dpl34', 
	'geographic_location__country_and_or_sea__sam', 'host_scientific_name_sam', 'anonymised_bovine_id_sam', 'strain_sam_ss_dpl139',
	'host_tissue_sampled_sam_s_dpl239', 'passage_history_sam_s_dpl312', 
	'country', 'region', 'continent', 'latlon', 'isolation_source', 'geoloc_info'] # only after rancheroize/standardize

start = time.time()
mycobact_from_BigQuery = Ranchero.from_bigquery(EXAMPLE_FILE_PATH)
convert_time = time.time() - start
old_dataframe = mycobact_from_BigQuery
old_col_count = mycobact_from_BigQuery.shape[1]
old_valid_view_cols = Ranchero.valid_cols(mycobact_from_BigQuery, view_cols)
s1 = f"\nWe've converted this ndjson into a {mycobact_from_BigQuery.shape[0]} row dataframe with {old_col_count} columns in {convert_time} seconds. "
s2 = "Although we used Ranchero to build it, it is still a polars dataframe, so you can run any arbitrary polars expression on it. "
s3 = "We won't be going over the basics of polars here, but we will note that when we print the dataframe here, \"null\" represents a literal pl.Null value, ie an empty value."
print(s1+s2+s3)
print("The columns are as follows:")
print(mycobact_from_BigQuery.columns)
print(f"\nBecause {mycobact_from_BigQuery.shape[1]} columns may not fit on the screen very well, we'll just print {len(old_valid_view_cols)} columns here.")
print("\nNOTE: This print will use non-ASCII characters for table borders, and will show best on a terminal window of at least 210 characters per line.")
input("\n\nPress enter to continue (!!consider resizing your terminal window first!!)...\n\n")

Ranchero.super_print(mycobact_from_BigQuery.select(Ranchero.valid_cols(mycobact_from_BigQuery, view_cols)), "input dataframe (selected columns)")
print("\n\nYou'll notice some issues right away:")
print("  * Our run-index column has the ambigious name \"acc\" which could also apply to a BioSample accession")
print("  * Some columns aren't helpful for our use case, such as \"anonymised_bovine_id_sam\"")
print("  * geo_loc_name_sam is a list, but each list only has has one/zero values and would be simpler as a string")
print("  * geo_loc_name_sam is also inconsistent -- sometimes it's just a country, sometimes it's a country and a region")
print("  * geo_loc_name_sam is empty for ERR10610304, but it does have a value in geo_loc_name_country_calc")
print("  * host_sam has a mixture of scientific names and generic names (see SRR23174078's \"Bovine\")")
print("  * Some values are filled in with values that basically mean nothing, such as \"uncalculated\" or \"Not applicable\"")
print("    --> This means we can't tell if a cell actually has meaningful data just by determining if it's null (empty) or not")
print("    --> This also leads to entire useless columns, such as host_tissue_sampled_sam_s_dpl239, which only exists due to a string literal \"missing\" value")
print("  * Many of those columns can be merged into one more generic column")
print()
print("The do-it-all function for cleaning up your dataframes, without changing much actual data, is rancheroize(). Let's rancheroize this dataframe.")
input("\n\nPress enter to continue...\n\n")

start = time.time()
mycobact_from_BigQuery = Ranchero.rancheroize(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(Ranchero.valid_cols(mycobact_from_BigQuery, view_cols)), f"dataframe after running rancheroize() (selected columns, completed in {time.time() - start} seconds.)")
print()
print("\nChanges made:")
print("  * lat_lon_sam_s_dpl34 now has shorter name latlon")
print('  * Data that previously was just "missing" or other iterations of "not available" are now properly converted to null values')
print("  * Some columns were coalesced into a single string column, such as \"center_name\" and \"insdc_center_name_sam\" becoming just \"center_name\"")
merged_location_columns = Ranchero.valid_cols(old_dataframe, ['geo_loc_name_country_calc', 'geographic_location__country_and_or_sea__sam', 'geo_loc_name_country_continent_calc', 'geo_loc_name_sam'])
print(f"  * Some columns were merged together into a single string column, such as {merged_location_columns} becoming just \"geoloc_info\"")
print(f"  * The coalesced and merged columns means the dataframe now stores basically the same amount of data in only {mycobact_from_BigQuery.shape[1]} columns (decreased from {old_col_count} columns, recall that not all columns are printed here)")
input("\n\nPress enter to continue...\n\n")

s1 = "While we have standardized the columns, we haven't really standardized the actual data within said columns. "
s2 = "It is still difficult to compare data due to many different standards or ways of referring to the same thing. "
s3 = "For example, although we have concatenated geographic location information into one column (\"geoloc_info\"), it contains continents, countries, countries-with-regions-in-the-same-string, regions by themselves, or just empty lists. "
s4 = "(You may wonder why ranchero combined the columns in the first place -- it's due to a lack of standardization. Sometimes a column that has \"country\" in the name will actually be a continent, for instance.)"
print(s1+s2+s3+s4)
s1 = "\nTo address this, let's run standardize_countries(). This will convert countries into their ISO codes 3166 codes, as well as attempt to pull out information below a country level (state, province, town, etc)."
s2 = "You might be thinking \"I bet that'll require a lot of string functions and be very slow.\" You are correct on the first point, but thanks to the speed of polars, we can get away with these sorts of things much faster than expected."
print(s1+s2)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'geoloc_info']), "country information after rancheroize, but before standardize_countries()")
input("\n\nPress enter to standardize location information...\n\n")

start = time.time()
mycobact_from_BigQuery = Ranchero.standardize_countries(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'continent', 'country', 'region']), f"dataframe after standardizing location (geoloc_info --> country, region, continent) -- completed in {time.time() - start} seconds")
print("Neat, right?")
input("\n\nPress enter to continue...\n\n")

print("This dataset is mostly MTBC, which almost always has a host. Let's standardize information about host organism.")
s1 = 'Ranchero has a built-in dictionary of common and scientific names that it checks against to assign hosts. '
s2 = 'Because some animals have referred to by only amibigious names, like "bovine" (which is probably Bos taurus but could be Bos indicus) there is also a confidence score. '
s3 = '1 is low confidence, 2 is moderate, 3 is high.'
print(s1+s2+s3)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'host']), "country information after rancheroize, but before standardize_hosts()")
input("\n\nPress enter to standardize host information...\n\n")

start = time.time()
mycobact_from_BigQuery = Ranchero.standardize_hosts(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'host_scienname', 'host_commonname', 'host_confidence']), f"dataframe after standardizing hosts (host --> host_scienname, host_commonname, 'host_confidence') -- completed in {time.time() - start} seconds")
input("\n\nPress enter to continue...\n\n")

print("In addition to wrangling your cattle, Ranchero can also attempt to standardize the taxonomic information of your samples.")
Ranchero.super_print(mycobact_from_BigQuery.select(Ranchero.valid_cols(mycobact_from_BigQuery, ['run_index', 'strain_sam_ss_dpl139', 'subtype_sam', 'organism', 'pathotype_sam', 'serotype_sam', 'serovar_sam', 'genotype_sam_ss_dpl92', 'scientific_name_sam', 'subgroup_sam', 'mlva___spoligotype_sam'])), "dataframe before standardizing taxonomic information")
input("\n\nPress enter to standardize taxonomic information...\n\n")

start = time.time()
mycobact_from_BigQuery = Ranchero.taxoncore(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'strain', 'lineage', 'organism', 'clade']), f"dataframe after standardizing taxonomic information (all that stuff --> strain, lineage, organism (as in scientific name), clade (within Mycobacterium genus) -- completed in {time.time() - start} seconds")
s1 = 'You\'ll notice that for samples where we don\'t know the MTB lineage, we say "tuberculosis: unclassified" as we don\'t know if it\'s animal-adapted lineage or a human-adapted one. '
s2 = "Additionally, SRR21747047 had \"S035\" in the \"strain_sam_ss_dpl139\" column but that was not included as a strain in the final data table. "
s3 = "This is because S035 as a strain-name isn't in Ranchero's built-in taxonomic dictionary, as I'm not aware of literature charcterizing it, and because this field sometimes is used for sample IDs. "
s4 = "You can modify the dictionary Ranchero uses for taxonomic information, and virtually everything else involving these sorts of text replaces, by editing files in src/statics as needed."
print(s1+s2+s3+s4)
input("\n\nPress enter to continue...\n\n")

print('Finally, let\'s standardize what tissue/environment your samples are taken from. This is very open to interpretation, so unlike country metadata, Ranchero doesn\'t try to cover *absolutely everything* in the Mycobacterium genus and will leave some values "as reported" (not shown in this example). Additionally, since some people use this column for geographic or host information, Ranchero will attempt to pull that information if present here and put it in the appropriate column.')
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'isolation_source']), "sample source information after rancheroize, but before standardize_sample_source()")
input("\n\nPress enter to standardize sample source...\n\n")

start = time.time()
mycobact_from_BigQuery = Ranchero.standardize_sample_source(mycobact_from_BigQuery)
Ranchero.super_print(mycobact_from_BigQuery.select(['run_index', 'isolation_source']), f"sample source information after standardization -- completed in {time.time() - start} seconds")
input("\n\nPress enter to contine...\n\n")

print("\nLet's compare the dataframe before and after all that rancheroizing and standardizing.")
print("------------ INPUT DATAFRAME ------------")
Ranchero.report(old_dataframe)
print("------------ CURRENT DATAFRAME ------------")
Ranchero.report(mycobact_from_BigQuery)
print("\n\nThere's more Ranchero can do, but this about covers the basics. Have a great day! 🐄\n")

