import time
start = time.time()
import src as Ranchero
print(f"⏰ {(time.time() - start):.3f} seconds to import")

print("Ranchero is designed to make wrangling bioinformatics metadata, especially NCBI metadata, a little bit easier.")

print("Ranchero can take in data from TSVs, bigquery, or NCBI run selector. For bigquery imports, the 'attrs' field's dictionaries are turned into columns.")
start = time.time()
coscolla = Ranchero.from_tsv("./inputs/coscolla.tsv")
tiny_test = Ranchero.from_bigquery("./inputs/truncated/smol.json")
flatten_test = Ranchero.from_bigquery("./inputs/truncated/bq-flatten-test.json")
PRJNA834606_sra = Ranchero.from_ncbi_run_selector("./inputs/PRJNA834606_sra.csv")
left = Ranchero.from_bigquery("./inputs/test/left.json")
right = Ranchero.from_bigquery("./inputs/test/right.json")
print(f"⏰ {time.time() - start} seconds to parse 6 tiny files")

print("The `primary_search` field is used as a sort of sample/run identifier in NCBI's databases. All values for `primary_search` will be perserved.")
what_about_primary_search = [{"k":"bases","v":1000},{"k":"primary_search","v":"foo"},{"k":"primary_search","v":"bar"}]
print('what_about_primary_search = [{"k":"bases","v":1000},{"k":"primary_search","v":"foo"},{"k":"primary_search","v":"bar"}]')
print("becomes")
print(Ranchero.NeighLib.concat_dicts_with_shared_keys(what_about_primary_search))


print("Ranchero can find which run accessions are paired illumina...")
print(Ranchero.get_paired_illumina(tiny_test))
print("Or not!")
print(Ranchero.get_paired_illumina(tiny_test, inverse=True))



print("Ranchero can also identify different types of mycobacteria in your data.")
import polars as pl
data = pl.DataFrame({"organism": [
	"Mycobacterium", 
	"Mycobacterium [tuberculosis] TKK-01-0051",
	"Mycobacterium avium", 
	"MycobACTERIUm avium", 
	"Mycobacterium AVIUM complex",
	"Mycobacterium avium complex sp.",
	"Mycobacterium canettii",
	"Mycobacterium canettii CIPT 140010059",
	"Mycobacterium smegmatis"
] } )
print(Ranchero.classify_bacterial_family(data))



print("The recommended way to squeeze as much strain information out as possible, while dropping as much extraneous information, is as follows:")
print("1) Drop everything that doesn't start with 'myco' in the organism column")
print("2) Use the strainify function")
print("3) Clean up the organism column using rm_tuberculosis_suffixes()")


print("Now let's look at a 193,710 line JSON from BigQuery. This will take about five minutes to import and flatten all of the k-v dictionaries...")
start = time.time()
tba5 = Ranchero.from_bigquery("./inputs/bq-results-20240710-211044-1720646162304.json")
print(f"⏰ {time.time() - start} seconds to parse")


start = time.time()
v8 = Ranchero.from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
print(f"v8 parsing time: {time.time() - start}")


print("Ranchero also supports merging metadata tables. This works best when merging upon run_index (SRR/ERR/DRR).")
merge_per_run = Ranchero.merge_polars_dataframes(tiny_test, flatten_test, 'run_index', 'small')
print(merge_per_run)
print("After merging, we can convert to a sample-indexed dataframe.")
merge_per_sample = Ranchero.run_index_to_sample_index(merge_per_run)
print(merge_per_sample)
print("This creates a lot of lists. We can flatten lists that make sense to flatten using hella_flat(). This function is pre-configured to handle common NCBI columns differently. For example, by default:")
print("* All unique values of BioProject will be kept as a list")
print("* The values of mbytes will be summed, creating a non-list integer")
print("* All values, unique or otherwise, of librarylayout will be kept as a list")
print("* The datastore_region region column will be dropped, as is not particularly useful in a sample-indexed context")
print("* A warning will be thrown if geoloc_country_calc has more than one unique value, as a single sample should only exist in one country")
print("You can change which columns have which behavior (sum, keep all, keep unique, drop, unchanged, warn) by modifying columns.py")
merge_per_sample = Ranchero.hella_flat(merge_per_sample)
print(merge_per_sample)
Ranchero.to_tsv(merge_per_run, "./zeroth_per_run.tsv")
Ranchero.to_tsv(merge_per_sample, "./zeroth_per_sample.tsv")

start = time.time()
leftright_merge = Ranchero.merge_polars_dataframes(left, right, 'run_index', 'hello', 'there', 'folks')
Ranchero.to_tsv(leftright_merge, "./Leftright.tsv")
print(f"Leftright time: {time.time() - start}")

start = time.time()
rightleft_merge = Ranchero.merge_polars_dataframes(right, left, 'run_index')
Ranchero.to_tsv(rightleft_merge, "./Rightleft.tsv")
print(f"Rightleft time: {time.time() - start}")


# Merging merged datasets to prove that the "country" column doesn't gain duplicates
start = time.time()
zippo = Ranchero.merge_polars_dataframes(leftright_merge, rightleft_merge, 'run_index')
zippo = Ranchero.run_index_to_sample_index(zippo)
zippo = Ranchero.hella_flat(zippo)
print(zippo)
print(f"Zippo time: {time.time() - start}")