import sys
import src as Ranchero
import polars as pl
Ranchero.pl.Config.set_tbl_rows(160)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(200)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_fmt_table_cell_list_len(5)

table = Ranchero.from_tsv(sys.argv[1], auto_rancheroize=False, list_columns=["run_index"]) # collection is also a list col but not in all versions

# If you just want to view value counts without writing to a TSV, do this instead:
Ranchero.NeighLib.print_value_counts(table, ['tbprof_drtype', 'tbprof_sublin', 'organism', 'lineage', 'continent', 'country'])

Ranchero.NeighLib.tsv_value_counts(table, 'tbprof_drtype', './counts_tbprof_drtype.tsv')
Ranchero.NeighLib.tsv_value_counts(table, 'tbprof_sublin', './counts_tbprof_sublin.tsv')
Ranchero.NeighLib.tsv_value_counts(table, 'organism', './counts_organism.tsv')
Ranchero.NeighLib.tsv_value_counts(table, 'lineage', './counts_literature_lineage.tsv')
Ranchero.NeighLib.tsv_value_counts(table, 'continent', './counts_continent.tsv') 
Ranchero.NeighLib.tsv_value_counts(table, 'country', './counts_country.tsv') 

# Let's say we want less specific TBProfiler lineages, eg, we want to group La1.2 with other La1
table = Ranchero.extract_primary_lineage(table, lineage_column='tbprof_sublin', output_column='tbprof_primary_lin')
Ranchero.NeighLib.tsv_value_counts(table, 'tbprof_primary_lin', './counts_tbprof_primary_lin.tsv') 
