import time
start = time.time()
import src as Ranchero

print(f"Module import time: {time.time() - start}")


# Parse tba6 column and drop columns
# For subsequent runs you can skip these and do this instead:
start, tba6 = time.time(), Ranchero.from_tsv("tba6_no_nonsense.tsv")
print(f"Imported tba6 file without extremely irrelevant columns in {time.time() - start} seconds")

#start, tba6 = time.time(), Ranchero.from_bigquery("./inputs/tba6_no_tax_table_bq_2024-09-19.json")
#print(f"Parsed tba6 file from bigquery in {time.time() - start} seconds")  # should be under five minutes for tba5, less for tba6
#start, tba6 = time.time(), Ranchero.drop_non_tb_columns(tba6)
#print(f"Dropped non-TB-related columns in {time.time() - start} seconds")
#start, tba6 = time.time(), Ranchero.NeighLib.drop_known_unwanted_columns(tba6)
#print(f"Dropped even more columns in {time.time() - start} seconds")
#start, tba6 = time.time(), Ranchero.classify_bacterial_family(Ranchero.rm_all_phages(tba6))
#tba6 = Ranchero.rm_not_MTBC(tba6) # use rm_row_if_col_null if you want non-MTBC mycobacteria
#print(f"Removed phages, classified bacterial family, and removed all rows that seem to not be MTBC in {time.time() - start} seconds")
#start, tba6 = time.time(), Ranchero.drop_lowcount_columns(tba6)
#print(f"Removed yet more columns in {time.time() - start}s seconds")
#Ranchero.to_tsv(tba6, "tba6_no_nonsense.tsv")



# merge with run-indexed data
coscolla = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/coscolla_sans_weird.tsv"))
coscolla = Ranchero.explode_delimited_index(coscolla)
print("Processed Coscolla data")

napier = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/napier_samples_github.csv", sep=","))
napier = Ranchero.explode_delimited_index(napier, delimter="_")
print("Processed Napier data")

#nextstrain = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/nextstrain_tb_global_metadata.tsv"))
#nextstrain = Ranchero.explode_delimited_index(nextstrain)
#print("Processed Nextstrain data")

PRJNA834606 = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/PRJNA834606_sra.csv", sep=","))
print("Processed PRJNA834606 data")

merged = Ranchero.merge_polars_dataframes(tba6, coscolla, merge_upon="run_index", right_name="Coscolla", put_right_name_in_this_column="source")
merged = Ranchero.merge_polars_dataframes(merged, napier, merge_upon="run_index", right_name="Napier", put_right_name_in_this_column="source")
#merged = Ranchero.merge_polars_dataframes(merged, nextstrain, merge_upon="run_index", right_name="nextstrain", put_right_name_in_this_column="source")
merged = Ranchero.merge_polars_dataframes(merged, PRJNA834606, merge_upon="run_index", right_name="PRJNA834606", put_right_name_in_this_column="source")
merged = Ranchero.merge_polars_dataframes(merged,)


Ranchero.to_tsv(merged, "./merged_by_run.tsv")


# merged with sample-indexed data
start, merged_by_sample = time.time(), Ranchero.run_index_to_sample_index(merged)
print(f"Converted run indeces to sample indeces in {time.time() - start} seconds")
start, merged_by_sample = time.time(), Ranchero.hella_flat(merged_by_sample)
print(f"Flattened samples in {time.time() - start} seconds")
Ranchero.to_tsv(merged_by_sample, "./merged_per_sample.tsv")

menardo = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/menardo_2018.csv", sep=","))
denylist = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/denylist_2024-07-23.tsv"))
tree_metadata_v8_rc10 = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/tree_metadata_v8_rc10.tsv"))
tba3 = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/tba3_redo.tsv"))
standford = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/max_standford_samples.tsv"))
july_2024_valid = Ranchero.NeighLib.rancheroize_polars(Ranchero.from_tsv("./inputs/2024-06-25-valid-samples-with-diff.tsv"))

exit(1)


Ranchero.NeighLib.print_value_counts(merged)


exit(1)



#Ranchero.print_unique_rows(tba5)


#tba5 = Ranchero.rm_tuberculosis_suffixes(tba5)


