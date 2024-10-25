import time
start = time.time()
import src as Ranchero
print(f"Module import time: {time.time() - start}")
start_from_scratch = True
do_run_index_merges = True

module_start = time.time()


def inital_file_parse():
	#we don't immediately rancheroize as this is faster
	start, tba6 = time.time(),Ranchero.from_bigquery("./inputs/tba6_no_tax_table_bq_2024-09-19.json")
	print(f"Parsed tba6 file from bigquery in {time.time() - start} seconds")  # should be under five minutes for tba5, less for tba6

	start, tba6 = time.time(), Ranchero.drop_non_tb_columns(tba6)
	print(f"Dropped non-TB-related columns in {time.time() - start} seconds")
	start, tba6 = time.time(), Ranchero.NeighLib.drop_known_unwanted_columns(tba6)
	print(f"Dropped even more columns in {time.time() - start} seconds")

	start, tba6 = time.time(), Ranchero.rancheroize(tba6)
	print(f"Rancheroized in {time.time() - start} seconds")

	start, tba6 = time.time(), Ranchero.standardize_sample_source(tba6)
	print(f"Standardized sample sources in {time.time() - start}s seconds")

	start, tba6 = time.time(), Ranchero.taxoncore(tba6)
	print(f"Standardized taxonomic, strain, and lineage information in {time.time() - start}s seconds")

	
	
	start, tba6 = time.time(), Ranchero.classify_bacterial_family(Ranchero.rm_all_phages(tba6))
	#tba6 = Ranchero.rm_not_MTBC(tba6) # use rm_row_if_col_null if you want non-MTBC mycobacteria -- although the function doesn't seem to work anyway
	print(f"Removed phages and classified bacterial family in {time.time() - start} seconds")

	start, tba6 = time.time(), Ranchero.drop_lowcount_columns(tba6)
	print(f"Removed yet more columns in {time.time() - start}s seconds")

	start = time.time()
	Ranchero.to_tsv(tba6, "tba6_no_nonsense.tsv")
	print(f"Wrote to disk in {time.time() - start}s seconds")
	return tba6

def run_merges(tba6):
	coscolla = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/coscolla_sans_weird.tsv"))
	coscolla = Ranchero.explode_delimited_index(coscolla)
	coscolla = coscolla.drop(['coscolla_number_of_reads'])
	print("Processed Coscolla data")

	napier = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/napier_samples_github_sans_weird.csv", sep=","))
	napier = Ranchero.explode_delimited_index(napier, delimter="_")
	print("Processed Napier data")

	nextstrain = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/nextstrain_tb_global_metadata.tsv"))
	nextstrain = Ranchero.explode_delimited_index(nextstrain)
	nextstrain = nextstrain.drop(["date_collected", 'Pyrazinamide','Capreomycin','Ethambutol','Rifampicin','Isoniazid','Ethionamide','Streptomycin','Pyrazinamide','Fluoroquinolones','Kanamycin','Amikacin']) # dates unreliable, antibiotic data mostly null
	print("Processed Nextstrain data")

	# PRJNA834606 probably doesn't have any extra data we don't already have
	#PRJNA834606 = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/PRJNA834606_sra.csv", sep=","))
	#print("Processed PRJNA834606 data")

	merged = Ranchero.merge_dataframes(tba6, coscolla, merge_upon="run_index", left_name="tba6", right_name="Coscolla", put_right_name_in_this_column="collection")
	#Ranchero.print_col_where(merged, 'sample_index', 'SAMN02360560')

	merged = Ranchero.merge_dataframes(merged, napier, merge_upon="run_index", right_name="Napier", put_right_name_in_this_column="collection")
	#Ranchero.print_col_where(merged, 'sample_index', 'SAMN02360560')

	merged = Ranchero.merge_dataframes(merged, nextstrain, merge_upon="run_index", right_name="nextstrain", put_right_name_in_this_column="collection")
	#Ranchero.print_col_where(merged, 'sample_index', 'SAMN02360560')

	merged = Ranchero.rancheroize(merged)
	#Ranchero.print_col_where(merged, 'sample_index', 'SAMN02360560')

	start = time.time()
	Ranchero.to_tsv(merged, "./merged_by_run.tsv")
	print(f"Wrote to disk in {time.time() - start}s seconds")
	return merged




########################################################################




if start_from_scratch:
	tba6 = inital_file_parse()
else:
	start, tba6 = time.time(), Ranchero.from_tsv("tba6_no_nonsense.tsv")
	print(f"Imported tba6 file without extremely irrelevant columns in {time.time() - start} seconds")

if do_run_index_merges:
	merged = run_merges(tba6)
else:
	start, merged = time.time(), Ranchero.from_tsv("merged_by_run.tsv")
	print(f"Imported run-indexed tba6 file, with some merges, in {time.time() - start} seconds")


print("Columns so far:")
print(merged.columns)

Ranchero.NeighLib.print_only_where_col_not_null(merged, 'isolation_source')

print("By type:")
Ranchero.NeighLib.print_value_counts(merged, ['mycobact_type'])



start = time.time()
merged = Ranchero.standardize_hosts(merged)
print(f"Standardized hosts in {time.time() - start}s seconds")

start = time.time()
merged = Ranchero.unmask_badgers(merged)
print(f"Unmasked badgers in {time.time() - start}s seconds")

start = time.time()
merged = Ranchero.standardize_host_disease(merged)
print(f"Standardized host disease in {time.time() - start}s seconds")

start = time.time()
merged = Ranchero.NeighLib.nullify(merged)
print(f"Nullified useless strings in {time.time() - start}s seconds")


print("Checking sample source...")
Ranchero.NeighLib.print_value_counts(tba6, ['sample_source'])
print("Checking hosts...")
Ranchero.NeighLib.print_value_counts(tba6, ['host', 'host_scienname', 'host_confidence', 'host_streetname'])
print("Checking host disease...")
Ranchero.NeighLib.print_value_counts(tba6, ['host_disease'])

#Ranchero.print_col_where(tba6, 'sample_index', 'SAMN02360560') # TODO: don't remember why this is here or why it is empty




# merged with sample-indexed data
start, merged_flat = time.time(), Ranchero.hella_flat(merged)
print(f"Flattened everything in {time.time() - start} seconds")

start, merged_by_sample = time.time(),Ranchero.run_index_to_sample_index(merged_flat)


print(f"Converted run indeces to sample indeces in {time.time() - start} seconds")
#Ranchero.to_tsv(merged_by_sample, "./merged_per_sample_not_flat.tsv")

start, merged_by_sample = time.time(), Ranchero.hella_flat(merged_by_sample)
print(f"Flattened samples in {time.time() - start} seconds")
Ranchero.to_tsv(merged_by_sample, "./merged_per_sample.tsv")



start = time.time()
menardo = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/menardo_stripped.tsv"))
denylist = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/denylist_2024-07-23_lessdupes.tsv"))
standford_1 = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/max_standford_YYYY-MM.tsv"))
standford_2 = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/max_standford_YYYY-MM-DD.tsv"))
standford_3 = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/max_standford_DD-MM-YYYY.tsv")) # this one should NOT overwrite left
standford_4 = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/max_standford_slashdates.tsv")) # ditto
tba3 = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/tba3_redo.tsv"))
july_2024_valid = Ranchero.rancheroize(Ranchero.from_tsv("./inputs/2024-06-25-valid-samples-with-diff.tsv"))
tree_metadata_v8_rc10 = Ranchero.from_tsv("./inputs/tree_metadata_v8_rc10.tsv")
tree_metadata_v8_rc10 = Ranchero.rancheroize(tree_metadata_v8_rc10)
tree_metadata_v8_rc10.drop(['BioProject', 'isolation_source', 'host']) # we are parsing these directly from SRA now
print(f"Finished reading a bunch more metadata in  {time.time() - start} seconds")

start = time.time()
merged = Ranchero.merge_dataframes(merged_by_sample, menardo, merge_upon="sample_index", right_name="menardo", put_right_name_in_this_column="collection")
print(f"Merged with menardo in {time.time() - start} seconds")

start = time.time()
merged = Ranchero.merge_dataframes(merged, denylist, merge_upon="sample_index", right_name="denylist", put_right_name_in_this_column="collection")
print(f"Merged with denylist in {time.time() - start} seconds")


start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_1, merge_upon="sample_index", right_name="standford", put_right_name_in_this_column="collection", fallback_on_left=False)
print(f"Merged with standford1 in {time.time() - start} seconds")
start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_2, merge_upon="sample_index", right_name="standford", put_right_name_in_this_column="collection", fallback_on_left=False)
print(f"Merged with standford2 in {time.time() - start} seconds")
start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_3, merge_upon="sample_index", right_name="standford", put_right_name_in_this_column="collection", fallback_on_left=True)
print(f"Merged with standford3 in {time.time() - start} seconds")
start, merged = time.time(), Ranchero.merge_dataframes(merged, standford_4, merge_upon="sample_index", right_name="standford", put_right_name_in_this_column="collection", fallback_on_left=True)
print(f"Merged with standford4 in {time.time() - start} seconds")

merged = Ranchero.NeighLib.nullify(Ranchero.cleanup_dates(merged))

print("This should be null, not a dash")
Ranchero.print_col_where(merged, 'sample_index', 'SAMN04522082', cols_of_interest=['sample_index', 'date_collected', 'collection'])

print("was 2012-01-01 now should be 2012")
Ranchero.print_col_where(merged, 'sample_index', 'SAMN06094120', cols_of_interest=['sample_index', 'date_collected', 'collection'])

print("was a datetime now should lack time")
Ranchero.print_col_where(merged, 'sample_index', 'SAMN30380812', cols_of_interest=['sample_index', 'date_collected', 'collection'])

print("was 2011-05-28 and should remain that way (don't overwrite with 5/28/2011)")
Ranchero.print_col_where(merged, 'sample_index', 'SAMN15098222', cols_of_interest=['sample_index', 'date_collected', 'collection'])

print("should remain YYYY-MM-DD or just YYYY")
Ranchero.print_col_where(merged, 'sample_index', 'SAMEA3281360', cols_of_interest=['sample_index', 'date_collected', 'collection'])

print("ideally should be YYYY-MM-DD but won't be")
Ranchero.print_col_where(merged, 'sample_index', 'SAMN04522083', cols_of_interest=['sample_index', 'date_collected', 'collection'])

print("sra says 2013, standford says 7/5/05, fallback on 2013")
Ranchero.print_col_where(merged, 'sample_index', 'SAMN02487169', cols_of_interest=['sample_index', 'date_collected', 'collection'])

print("sra says 2008, standford says 6/30/05, fallback on 2008")
Ranchero.print_col_where(merged, 'sample_index', 'SAMN20351579', cols_of_interest=['sample_index', 'date_collected', 'collection'])

Ranchero.NeighLib.big_print_polars(merged.filter(pl.col("date_collected").str.contains(r"\d{2}/\d{2}/\d{2}")), "merged has date slashes in 2 2 2 format", ['sample_index', 'date_collected'])
Ranchero.NeighLib.big_print_polars(merged.filter(pl.col("date_collected").str.contains(r"\d{2}/\d{2}/\d{4}")), "merged has date slashes in 2 2 4 format", ['sample_index', 'date_collected'])

Ranchero.NeighLib.big_print_polars(merged, "merged hosts and dates", ['sample_index', 'date_collected', 'host', 'host_scienname', 'host_commonname'])
Ranchero.NeighLib.big_print_polars(tree_metadata_v8_rc10, "v8rc10 hosts and dates", ['sample_index', 'date_collected', 'host'])







Ranchero.to_tsv(merged, "./ranchero_partial.tsv")



start = time.time()
merged = Ranchero.merge_dataframes(merged, tree_metadata_v8_rc10, merge_upon="sample_index", right_name="tree_metadata_v8_rc10", put_right_name_in_this_column="collection", fallback_on_left=False)
print(f"Merged with old tree metadata file in {time.time() - start} seconds")



merged = Ranchero.merge_dataframes(merged, tba3, merge_upon="sample_index", right_name="tba3", put_right_name_in_this_column="collection")
merged = Ranchero.merge_dataframes(merged, july_2024_valid, merge_upon="sample_index", right_name="july_2024_valid", put_right_name_in_this_column="collection")













print(merged)
Ranchero.print_col_where(merged, 'sample_index', 'SAMEA1573039') # seems to disappear, has run index ERR18131
print(merged.select(['date_collected', 'host', 'sample_index']))
#Ranchero.NeighLib.print_value_counts(merged)







merged = Ranchero.merge_dataframes(merged, tree_metadata_v8_rc10, merge_upon="sample_index", right_name="tree_metadata_v8_rc10", put_right_name_in_this_column="collection")



print(merged)



print(f"Finished entire module in {time.time() - module_start} seconds")


