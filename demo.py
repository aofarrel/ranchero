import time
start = time.time()
import src as Ranchero
print(f"⏰ {(time.time() - start):.3f} seconds to import")

print("Ranchero is designed to make wrangling bioinformatics metadata, especially NCBI metadata, a little bit easier.")

print("The `primary_search` field is used as a sort of sample/run identifier in NCBI's databases. All values for `primary_search` will be perserved.")
what_about_primary_search = [{"k":"bases","v":1000},{"k":"primary_search","v":"foo"},{"k":"primary_search","v":"bar"}]
print('what_about_primary_search = [{"k":"bases","v":1000},{"k":"primary_search","v":"foo"},{"k":"primary_search","v":"bar"}]')
print("becomes")
print(Ranchero.NeighLib.concat_dicts_with_shared_keys(what_about_primary_search))

