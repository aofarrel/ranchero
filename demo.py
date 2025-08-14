import time
start = time.time()
import ranchero as Ranchero
print(f"⏰ {(time.time() - start):.3f} seconds to import")
import polars as pl
pl.Config.set_fmt_table_cell_list_len(5)
pl.Config.set_fmt_float("mixed")
pl.Config.set_tbl_cols(-1)

print("**NOTE: Tables in this demo that use actual SRA data get big fast. For better readability in this demo, set your terminal display to at least 200 characters per line.**")
print()

print("Ranchero is designed to make wrangling bioinformatics metadata, especially NCBI metadata, a little bit easier.")
print()
print("""Ranchero creates, merges, and updates polars dataframes from TSVs, BigQuery result tables, or NCBI run selector. In a Ranchero dataframe, every row represents either:
	* One biological sample, analogous to SAMN/SAME/SAMD on SRA -- we call this "sample-indexed"
	* One "run" from the sequencer, analogous to SRR/ERR/DRR on SRA -- we call this "run-indexed"
		--> Note that a single "run" may have multiple FASTQ files, especially when dealing with Illumina PE data
This distinction is moot if you only have one run per sample, but it comes in handy when you're not working with INSDC data.""")

print()
print("Some additional features specific to INSDC BigQuery data:")
print("* Key-value pairs in the attributes/attr field are unpacked and turned into columns")
print("  * primary_search, which usually has repeated keys, has special handling -- all unique values are compiled into a single primary_search column")
print("* Optional: Drop all samples that aren't paired Illumina data")
print()
print("Let's parse a JSON from a BigQuery search as an example. This is a shortened version of some results from the following BigQuery search:")
print("""SELECT *
FROM `nih-sra-datastore.sra.metadata` as m, `nih-sra-datastore.sra_tax_analysis_tool.tax_analysis` as tax
WHERE m.acc=tax.acc and tax_id=77643 and m.librarysource!="TRANSCRIPTOMIC" and tax.total_count > 10000""")

print("Because this BigQuery search was done on the Cloud-based Metadata Table and the Cloud-based Taxonomy Analysis Table, it is run-indexed, but also has BioSample IDs and taxonomic information. Be aware that the tax_analysis table adds a \"name\" field that is the scientific name of the organism, not a sample name.")

bq_tba5_compact = Ranchero.from_bigquery("./inputs/demo/tba5_selections_compact.json")
print(bq_tba5_compact)
print("You'll notice some issues right away:")
print("  * Our run-index column has the ambigious name \"acc\" which could also apply to a BioSample accession")
print("  * geo_loc_name_sam is a list, but only has one/zero values and would be simpler as a string")
print("  * SRR9915575 had an attribute with the key lat_lon_sam_s_dpl34, but the value was the literal string \"none\" -- compare to SRR13381273 which had no lat_lon_sam_s_dpl34 key and ended up with an actual null value")
print()
print("The do-it-all function for cleaning up your dataframes is rancheroize. Let's rancheroize this dataframe.")
bq_tba5_compact = Ranchero.rancheroize(bq_tba5_compact)
print(bq_tba5_compact)
print()
print("Changes made:")
print("  * lat_lon_sam_s_dpl34 now has shorter name latlon, and SRR9915575's \"none\" value is now a pl.Null")

#TODO: go over standardizing country/region



#bq_tba6_head15 = Ranchero.from_bigquery("./inputs/demo/bq_tba6_head15.json")
#print(bq_tba6_head15)


#print("You can update a sample-indexed dataframe with another sample-indexed TSV file. Note how this TSV file doesn't have run information, but we can still merge with it since it has the same indexing method as the dataframe.")


