import sys
import src as Ranchero
import polars as pl
Ranchero.pl.Config.set_tbl_rows(160)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(200)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_fmt_table_cell_list_len(5)

full_rc17 = Ranchero.from_tsv(sys.argv[1], auto_rancheroize=False, list_columns=["run_index", "collection"])
on_the_tree = Ranchero.from_tsv(sys.argv[2], auto_rancheroize=False)
vcf_list = Ranchero.from_tsv(sys.argv[3], auto_rancheroize=False)
translate_g_samples_to_new_names = Ranchero.from_tsv(sys.argv[4], auto_rancheroize=False)

# remove "tree" from full_rc17 because it's outdated
full_rc17 = full_rc17.with_columns(
    collection_real=pl.col("collection").list.eval(
        pl.element().filter(~pl.element().is_in(["tree"]))
    )
).drop("collection").rename({"collection_real": "collection"})



table = table.with_columns(pl.when(pl.col("collection").list.contains(pl.lit("input"))).then(True).otherwise(False).alias('input'))
table = table.with_columns(pl.when(pl.col("collection").list.contains(pl.lit("tbprofiler"))).then(True).otherwise(False).alias('tbprofiler'))
table = table.with_columns(pl.when(pl.col("collection").list.contains(pl.lit("diff"))).then(True).otherwise(False).alias('diff'))


print(table.select(["diff", "collection"]))


exit(1)

# If you just want to view value counts without writing to a TSV, do this instead:
# Ranchero.NeighLib.print_value_counts(table, ['tbprof_drtype', 'tbprof_sublin', 'organism', 'lineage', 'continent', 'country'])

Ranchero.NeighLib.tsv_value_counts(table, 'tbprof_drtype', './counts_tbprof_drtype.tsv')
Ranchero.NeighLib.tsv_value_counts(table, 'tbprof_sublin', './counts_tbprof_sublin.tsv')
Ranchero.NeighLib.tsv_value_counts(table, 'organism', './counts_organism.tsv')
Ranchero.NeighLib.tsv_value_counts(table, 'lineage', './counts_literature_lineage.tsv')
Ranchero.NeighLib.tsv_value_counts(table, 'continent', './counts_continent.tsv') 
Ranchero.NeighLib.tsv_value_counts(table, 'country', './counts_country.tsv') 

# Let's say we want less specific TBProfiler lineages, eg, we want to group La1.2 with other La1
table = Ranchero.extract_primary_lineage(table, lineage_column='tbprof_sublin', output_column='tbprof_primary_lin')
Ranchero.NeighLib.tsv_value_counts(table, 'tbprof_primary_lin', './counts_tbprof_primary_lin.tsv') 
