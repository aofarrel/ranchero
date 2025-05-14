import sys
import src as Ranchero
import polars as pl
Ranchero.pl.Config.set_tbl_rows(160)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(200)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_fmt_table_cell_list_len(5)

metadata = Ranchero.from_tsv(sys.argv[1], auto_rancheroize=False, list_columns=["run_index"])
metadata = Ranchero.NeighLib.extract_primary_lineage(metadata, lineage_column='tbprof_sublin', output_column='tbprof_primary_lin')

Ranchero.NeighLib.report(metadata) # get a pretty report

# By setting and_id_columns=True we can avoid printing run and SRX accession and focus on just BioSample (sample_index).
# Print sample IDs where region is Kinshasa
#Ranchero.NeighLib.print_a_where_b_equals_these(metadata, "sample_index", "region", ["Kinshasa"], and_id_columns=False)
# Print sample IDs where country is Belarus or DRC  
#Ranchero.NeighLib.print_a_where_b_equals_these(metadata, "sample_index", "country", ["BEL", "DRC"], and_id_columns=False)
# Print sample IDs where TB Profiler lineage is L8
#Ranchero.NeighLib.print_a_where_b_equals_these(metadata, "sample_index", "tbprof_primary_lin", ["lineage8"], and_id_columns=False)

# Print sample IDs tagged as M. bovis, and also print what lineage TBProfiler thinks they are
# TBProfiler reports M. bovis as "La1" -- we can also essentially do vice-versa
organism_is_bovis = Ranchero.NeighLib.print_a_where_b_equals_these(metadata, "sample_index", "organism", ["Mycobacterium bovis"], ["tbprof_primary_lin", "tbprof_sublin", "lineage"], and_return_filtered=True)
Ranchero.NeighLib.print_value_counts(organism_is_bovis, only_these_columns=["tbprof_primary_lin", "tbprof_sublin", "lineage"])
exit(1)
tbprof_is_bovis = Ranchero.NeighLib.print_a_where_b_equals_these(metadata, "sample_index", "tbprof_primary_lin", ["La1"], ["organism", "lineage"], and_return_filtered=True)
Ranchero.NeighLib.print_value_counts(tbprof_is_bovis, only_these_columns=["organism", "lineage"])