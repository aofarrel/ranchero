import sys
import csv
import src as Ranchero
import polars as pl
pl.Config.set_tbl_rows(10)
pl.Config.set_tbl_cols(15)
pl.Config.set_tbl_width_chars(150)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_fmt_table_cell_list_len(5)
pl.Config.set_tbl_hide_dataframe_shape(True)

def translate_column(polars_df, col_to_translate, new_col, dictionary):
    # from newer version of ranchero
    if new_col not in polars_df.columns:
        polars_df = polars_df.with_columns(pl.lit(None).alias(new_col))
    for key, value in dictionary.items():
        polars_df = polars_df.with_columns(
            pl.when(pl.col(col_to_translate) == pl.lit(key))
            .then(pl.lit(value)).otherwise(pl.col(new_col)).alias(new_col)
        )
    return polars_df

rc17 = Ranchero.from_tsv(sys.argv[1], auto_rancheroize=False, list_columns=["run_index", "collection"])

# remove "tree" from rc17 because it's outdated
rc17 = rc17.with_columns(
    collection_real=pl.col("collection").list.eval(
        pl.element().filter(~pl.element().is_in(["tree"]))
    )
).drop("collection").rename({"collection_real": "collection"})

# convert str columns where we can (not always necessary!)
rc17 = rc17.with_columns(pl.col("tbprof_pct_reads_mapped").cast(pl.Float32).alias("tbprof_pct_reads_mapped_float"))
rc17 = rc17.with_columns(pl.col("tbprof_median_coverage").cast(pl.Int32).alias("tbprof_median_coverage_int"))

with open(sys.argv[2], 'r') as file:
   on_the_tree = [line.rstrip('\n') for line in file.readlines()]
rc17 = rc17.with_columns(
    pl.when(pl.col("sample_index").is_in(on_the_tree))
    .then(True)
    .otherwise(False)
    .alias("tree")
)

'''
FOR THE FINAL FINAL FINAL FINAL STUFF
with open(sys.argv[3], 'r') as file:
   vcf_list = file.readlines()
translate_g_samples_to_new_names = Ranchero.from_tsv(sys.argv[4], auto_rancheroize=False).to_dict()

rc17 = rc17.with_columns(
    pl.when(pl.col("sample_index").is_in(vcf_list))
    .then(True)
    .otherwise(False)
    .alias("vcf")
)

rc17_translated = translate_column(rc17, "sample_index", "sample_index_new", translate_g_samples_to_new_names)

rc17 = rc17_translated
'''

rc17 = Ranchero.extract_primary_lineage(rc17, lineage_column='tbprof_sublin', output_column='tbprof_primary_lin')

# pipeline progress
rc17 = rc17.with_columns(pl.when(pl.col("collection").list.contains(pl.lit("input"))).then(True).otherwise(False).alias('input'))
rc17 = rc17.with_columns(pl.when(pl.col("collection").list.contains(pl.lit("diff"))).then(True).otherwise(False).alias('diff'))

# has metadata
rc17 = rc17.with_columns(pl.when(pl.col("tbprof_sublin").is_not_null()).then(True).otherwise(False).alias('has_tbprof_sublin'))
rc17 = rc17.with_columns(pl.when(pl.col("tbprof_primary_lin").is_not_null()).then(True).otherwise(False).alias('has_tbprof_dr'))
rc17 = rc17.with_columns(pl.when(pl.col("tbprof_primary_lin").is_not_null()).then(True).otherwise(False).alias('has_tbprof_lin'))
rc17 = rc17.with_columns(pl.when(pl.col("pheno_source").is_not_null()).then(True).otherwise(False).alias('pDST'))
rc17 = rc17.with_columns(pl.when(pl.col("date_collected").is_not_null()).then(True).otherwise(False).alias('has_date'))
rc17 = rc17.with_columns(pl.when(pl.col("country").is_not_null()).then(True).otherwise(False).alias('has_country'))
rc17 = rc17.with_columns(pl.when(pl.col("organism").is_not_null()).then(True).otherwise(False).alias('has_organism'))
rc17 = rc17.with_columns(pl.when(pl.col("continent").is_not_null()).then(True).otherwise(False).alias('has_continent'))
rc17 = rc17.with_columns(pl.when(
    (pl.col("host_commonname").is_not_null())
    .or_(pl.col("host_scienname").is_not_null()))
.then(True).otherwise(False).alias('has_host'))

# is sus
rc17 = rc17.with_columns(pl.when(pl.col('tbprof_pct_reads_mapped_float').le(pl.lit(80))).then(True).otherwise(False).alias('low_pct_mapped'))
rc17 = rc17.with_columns(pl.when(pl.col('tbprof_median_coverage_int').le(pl.lit(10))).then(True).otherwise(False).alias('low_median_cov'))

rc17_input_only = rc17.filter(pl.col('input'))
rc17_diff_only = rc17.filter(pl.col('diff'))
rc17_tree_only = rc17.filter(pl.col('tree'))

Ranchero.to_tsv(rc17.filter('pDST'), "rc17_everything_with_pDST.tsv")
Ranchero.to_tsv(rc17_tree_only.filter('pDST'), "rc17_angiesubsettree_with_pDST.tsv")

exit(1)

print("------------ OVERALL COUNTS ------------")
print(f"{str(rc17.shape[0]).zfill(6)} samples that we have *any* metadata for")
print("Of which, how many have metadata?")
print(f"--> pDST:\t\t{str(rc17.filter(pl.col('pDST')).shape[0]).zfill(6)}")
print(f"--> country:\t\t{str(rc17.filter(pl.col('has_country')).shape[0]).zfill(6)}")
print(f"--> host:\t\t{str(rc17.filter(pl.col('has_host')).shape[0]).zfill(6)}")
print(f"--> date:\t\t{str(rc17.filter(pl.col('has_date')).shape[0]).zfill(6)}")
rc17 = None

print("------------ INPUT COUNTS ------------")
print(f"{rc17_input_only.shape[0]} samples input into the pipeline")
print("Of which, how many have metadata?")
print(f"--> pDST:\t\t{str(rc17_input_only.filter(pl.col('pDST')).shape[0]).zfill(6)}")
print(f"--> country:\t\t{str(rc17_input_only.filter(pl.col('has_country')).shape[0]).zfill(6)}")
print(f"--> host:\t\t{str(rc17_input_only.filter(pl.col('has_host')).shape[0]).zfill(6)}")
print(f"--> date:\t\t{str(rc17_input_only.filter(pl.col('has_date')).shape[0]).zfill(6)}")
print(f"--> TBProf DR:\t\t{str(rc17_input_only.filter(pl.col('has_tbprof_dr')).shape[0]).zfill(6)}")
print(f"--> TBProf lin:\t\t{str(rc17_input_only.filter(pl.col('has_tbprof_lin')).shape[0]).zfill(6)}")
print("How many are dubious?")
print(f"--> Found {rc17_input_only.filter(pl.col('low_pct_mapped')).shape[0]} samples with less than 80% reads mapping")
print(f"--> Found {rc17_input_only.filter(pl.col('low_median_cov')).shape[0]} samples with less than 10x median coverage")

print("------------ DIFF COUNTS ------------")
print(f"There's a total of {rc17_diff_only.shape[0]} samples with diff files")
print("Of which, how many have metadata?")
print(f"--> pDST:\t\t{str(rc17_diff_only.filter(pl.col('pDST')).shape[0]).zfill(6)}")
print(f"--> country:\t\t{str(rc17_diff_only.filter(pl.col('has_country')).shape[0]).zfill(6)}")
print(f"--> host:\t\t{str(rc17_diff_only.filter(pl.col('has_host')).shape[0]).zfill(6)}")
print(f"--> date:\t\t{str(rc17_diff_only.filter(pl.col('has_date')).shape[0]).zfill(6)}")
print(f"--> TBProf DR:\t\t{str(rc17_diff_only.filter(pl.col('has_tbprof_dr')).shape[0]).zfill(6)}")
print(f"--> TBProf lin:\t\t{str(rc17_diff_only.filter(pl.col('has_tbprof_lin')).shape[0]).zfill(6)}")
print("How many are dubious?")
print(f"--> Found {rc17_diff_only.filter(pl.col('low_pct_mapped')).shape[0]} samples with less than 80% reads mapping")
print(f"--> Found {rc17_diff_only.filter(pl.col('low_median_cov')).shape[0]} samples with less than 10x median coverage")

print("------------ TREE COUNTS ------------")
print(f"There's a total of {rc17_tree_only.shape[0]} samples on the tree (as of a few weeks ago)")
print(f"--> pDST:\t\t{str(rc17_tree_only.filter(pl.col('pDST')).shape[0]).zfill(6)}")
print(f"--> country:\t\t{str(rc17_tree_only.filter(pl.col('has_country')).shape[0]).zfill(6)}")
print(f"--> host:\t\t{str(rc17_tree_only.filter(pl.col('has_host')).shape[0]).zfill(6)}")
print(f"--> date:\t\t{str(rc17_tree_only.filter(pl.col('has_date')).shape[0]).zfill(6)}")
print(f"--> TBProf DR:\t\t{str(rc17_tree_only.filter(pl.col('has_tbprof_dr')).shape[0]).zfill(6)}")
print(f"--> TBProf lin:\t\t{str(rc17_tree_only.filter(pl.col('has_tbprof_lin')).shape[0]).zfill(6)}")
print("How many are dubious?")
print(f"--> Found {rc17_tree_only.filter(pl.col('low_pct_mapped')).shape[0]} samples with less than 80% reads mapping")
print(f"--> Found {rc17_tree_only.filter(pl.col('low_median_cov')).shape[0]} samples with less than 10x median coverage")
print("Specific value counts (only ten rows shown here, but full counts are dumped to TSV):")
Ranchero.NeighLib.print_value_counts(rc17_tree_only.rename({"organism": "organism_per_SRA"}), ['organism_per_SRA'])
Ranchero.NeighLib.print_value_counts(rc17_tree_only, ['tbprof_drtype'])
Ranchero.NeighLib.print_value_counts(rc17_tree_only, ['tbprof_primary_lin'])
Ranchero.NeighLib.print_value_counts(rc17_tree_only, ['country'])
Ranchero.NeighLib.print_value_counts(rc17_tree_only, ['continent'])





"""
Ranchero.NeighLib.tsv_value_counts(rc17_tree_only, 'tbprof_primary_lin', './counts_tbprof_primary_lin.tsv') 
Ranchero.NeighLib.tsv_value_counts(rc17_tree_only, 'tbprof_median_coverage', './counts_tbprof_median_cov.tsv')
Ranchero.NeighLib.tsv_value_counts(rc17_tree_only, 'tbprof_drtype', './counts_tbprof_drtype.tsv')
Ranchero.NeighLib.tsv_value_counts(rc17_tree_only, 'tbprof_sublin', './counts_tbprof_sublin.tsv')
Ranchero.NeighLib.tsv_value_counts(rc17_tree_only, 'organism', './counts_organism.tsv')
Ranchero.NeighLib.tsv_value_counts(rc17_tree_only, 'lineage', './counts_literature_lineage.tsv')
Ranchero.NeighLib.tsv_value_counts(rc17_tree_only, 'continent', './counts_continent.tsv') 
Ranchero.NeighLib.tsv_value_counts(rc17_tree_only, 'country', './counts_country.tsv') 
"""





