# Example of filtering based on column values

import sys
import src as Ranchero
import polars as pl

metadata = Ranchero.from_tsv(sys.argv[1], check_index=False, auto_rancheroize=False, list_columns=['library_ID'])
metadata = Ranchero.NeighLib.check_index(metadata, manual_index_column='filename')

# Optional: Remove some extra columns we don't care about (just to make outputs cleaner)
metadata = metadata.drop(['collection', 'not_on_sra'])

# Optional: In this example, library_ID was a list, but we want just the longest/largest one
# When library_ID is a list of strings, list.max() will return the longest string
metadata = metadata.with_columns(pl.col('library_ID').list.max().alias('library_ID'))

# The actual filtering based on the "is_fail" column's value
# In this case, passing and failing's contents are mutually exclusive since this is a simple boolean
passing = Ranchero.hella_flat(metadata.filter(pl.col("is_fail") == False), force_index="filename")
failing = Ranchero.hella_flat(metadata.filter(pl.col("is_fail") == True), force_index="filename")

# Let's split hairs in the "passing" dataframe a bit more...
# In this example, the 'library_selection' and 'ntsm_score' columns are what tends to be null
passing_has_metadata = passing.filter((pl.col('library_selection').is_not_null()).and_(pl.col('ntsm_score').is_not_null()))
passing_lacks_metadata = passing.filter((pl.col('library_selection').is_null()).and_(pl.col('ntsm_score').is_null()))

# Any other nulls lurking?
for column in passing_has_metadata.columns:
	if Ranchero.NeighLib.get_null_count_in_column(passing_has_metadata, column) > 0:
		print("Found some nulls!")
		print(passing_has_metadata.select(column))

# Don't forget, you could also fill in nulls like this if you're confident in what should be there:
#passing = passing.with_columns(pl.col('library_source').fill_null('GENOMIC'))
#passing = passing.with_columns(pl.col('library_strategy').fill_null('WGS'))
#passing = passing.with_columns(pl.col('filetype').fill_null('bam'))

# Write dataframes to the disk
Ranchero.to_tsv(failing.drop("is_fail"), "not_on_sra_FAIL.tsv")
Ranchero.to_tsv(passing_has_metadata.drop("is_fail"), "SRA_upload_2025-05-18.tsv")
Ranchero.to_tsv(passing_lacks_metadata.drop("is_fail"), "not_on_sra_lacks_metadata.tsv")