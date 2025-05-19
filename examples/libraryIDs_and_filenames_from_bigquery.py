# This is an example of using Ranchero to wrangle human data from a BQ JSONL.
# We're going to parse the primary_search fields to get library IDs and
# filenames of samples on the BQ table.

import sys
import src as Ranchero
import polars as pl
Ranchero.pl.Config.set_tbl_rows(160)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(200)
pl.Config.set_fmt_str_lengths(500)
pl.Config.set_fmt_table_cell_list_len(5)

table = Ranchero.from_bigquery(sys.argv[1])
table = Ranchero.extract_filename(table, 'primary_search', 'files')
table = Ranchero.NeighLib.add_list_len_col(table, 'files', 'files_len')

# In this example, I did a BQ search of all run accessions in the HPRC and HPRC+ BioProjects,
# so that's what I'll call my dataset when printing it.
# Note: We didn't bother rancheroizing the dataframe, so columns retain their BQ names rather
# than their typical names ('acc' instead of 'run_index' for example).
Ranchero.NeighLib.super_print_pl(table, "HPRC/HPRC+ Samples on SRA", select=['acc', 'sample_name', 'files_len', 'files'])
Ranchero.to_tsv(table.select(['acc', 'sample_name', 'files_len', 'files']), "HPRC_SRA_2025-05-09.tsv")