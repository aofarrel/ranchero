import time
start = time.time()
import src as Ranchero
print(f"Module import time: {time.time() - start}")

# start = time.time()
# tba5 = Ranchero.from_bigquery("./inputs/bq-results-20240710-211044-1720646162304.json")
# print(f"tba5 parse time: {time.time() - start}")  # should be under five minutes
# start = time.time()
# tba5 = Ranchero.drop_non_tb_columns(tba5)
# Ranchero.to_tsv(tba5, "tba5_drop_non_tb_columns.tsv")
# print(f"tba5 drop column and write time: {time.time() - start}")
start = time.time()
tba5 = Ranchero.from_tsv("tba5_drop_non_tb_columns.tsv")
print(f"tba5 TSV import time: {time.time() - start}")
start = time.time()
tba5 = Ranchero.rm_tuberculosis_suffixes(tba5)
tba5 = Ranchero.rm_all_phages(tba5)
tba5 = Ranchero.get_known_mycobacteria(tba5)
Ranchero.print_unique_rows(tba5)
print(f"tba5 shave suffix time: {time.time() - start}")


#tba5 = Ranchero.run_index_to_sample_index(tba5, "foo")
#print(f"tba5 run to sample time: {time.time() - start}")
#start = time.time()
#tba5 = Ranchero.hella_flat(tba5)
#print(f"tba5 flat time: {time.time() - start}")
#Ranchero.to_tsv(tba5, "./tba5_per_sample.tsv")



