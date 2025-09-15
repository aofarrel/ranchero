# In this example, we analyze a TSV output from ranchero with a collection column that includes information
# about samples that were:
# * input into a pipeline
# * successfully passed most of the pipeline to generate a MAPLE diff
# * successfully placed on a phylogenetic tree at the end of the pipeline
# * denylisted
# Values in other columns are used to further determine which step in the pipeline a sample may have
# failed in and whether that sample was manually pruned from the tree (or failed placement).

import src as Ranchero
import polars as pl
Ranchero.pl.Config.set_tbl_rows(160)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(200)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_fmt_table_cell_list_len(5)

v16 = Ranchero.from_tsv("./ranchero_rc16_less_columns.tsv", auto_rancheroize=False, list_columns=["collection", "run_id"])
v16 = Ranchero.NeighLib.extract_primary_lineage(v16, lineage_column='tbprof_sublin', output_column='tbprof_primary_lin')

# You may additionally want to check that everything in "diff" is also in "input",
# and that nothing in "tree" is on "denylist".

v16_input = v16.filter(v16["collection"].list.contains("input"))
v16_input_valid = v16_input.filter(~v16_input["collection"].list.contains("denylist"))
v16_input_invalid = v16_input.filter(v16_input["collection"].list.contains("denylist"))

v16_diff = v16.filter(v16["collection"].list.contains("diff"))
v16_valid_diff = v16_diff.filter(~v16_diff["collection"].list.contains("denylist"))
v16_invalid_diff = v16_diff.filter(v16_diff["collection"].list.contains("denylist"))

v16_failed_pipeline = v16_input.filter(~v16_input["collection"].list.contains("diff"))
v16_failed_pipeline = v16_failed_pipeline.filter(~v16_failed_pipeline["collection"].list.contains("denylist"))
v16_failed_pipeline_early = v16_failed_pipeline.filter(v16_failed_pipeline["tbprof_pct_reads_mapped"].is_null())
v16_failed_pipeline_late = v16_failed_pipeline.filter(v16_failed_pipeline["tbprof_pct_reads_mapped"].is_not_null())
v16_failed_pipeline_late_good_coverage = v16_failed_pipeline_late.filter(v16_failed_pipeline_late["tbprof_median_coverage"] > 9)
v16_failed_pipeline_late_good_coverage_decent_percent_mapped = v16_failed_pipeline_late_good_coverage.filter(v16_failed_pipeline_late_good_coverage["tbprof_pct_reads_mapped"] > 80.0)

v16_on_tree = v16_valid_diff.filter(v16_valid_diff["collection"].list.contains("tree"))
v16_pruned = v16_valid_diff.filter(~v16_valid_diff["collection"].list.contains("tree"))
v16_pruned_not_canettii = v16_pruned.filter(v16_pruned["organism"] != "Mycobacterium canettii") # already know good median coverage since it passed pipeline
v16_pruned_not_canettii_good_coverage_decent_percent_mapped = v16_pruned_not_canettii.filter(v16_pruned_not_canettii["tbprof_pct_reads_mapped"] > 80.0)

print("\nBecause we didn't catch some denylist reasons until after running samples,")
print("a small number of samples were input/made it to diff which have since been")
print("denylisted. No denylisted sample is currently on the tree.")

print("\n.....INPUT SAMPLES.....")
print(f"{v16_input.shape[0]} samples input")
print(f"-> {v16_input_valid.shape[0]} input samples not on denylist")
print(f"-> {v16_input_invalid.shape[0]} input samples that were on denylist")

print("\n.....PASSING SAMPLES.....")
print(f"{v16_diff.shape[0]} generated a diff file")
print(f"-> {v16_valid_diff.shape[0]} diff files not on denylist")
print(f"-> {v16_invalid_diff.shape[0]} diff files that were on denylist")

print("\n.....FAILING SAMPLES.....")
print(f"{v16_failed_pipeline.shape[0]} non-denylist samples failed the pipeline")
print(f"-> {v16_failed_pipeline_early.shape[0]} failed before TBProfiler")
#Ranchero.NeighLib.print_value_counts(v16_failed_pipeline_early, ['organism'])
print(f"-> {v16_failed_pipeline_late.shape[0]} failed after TBProfiler")
print(f"->-> {v16_failed_pipeline_late_good_coverage.shape[0]} of which >9x median coverage")
print(f"->->-> {v16_failed_pipeline_late_good_coverage_decent_percent_mapped.shape[0]} of which had more than 80% of their reads mapping to H37Rv")

print("\n.....TREE SAMPLES.....")
print(f"{v16_on_tree.shape[0]} on tree")

print("\n.....PRUNED SAMPLES.....")
print(f"{v16_pruned.shape[0]} passing samples (pass QC, not denylisted) were removed from the tree")
print(f"->{v16_pruned_not_canettii.shape[0]} of which not M. canettii")
print(f"->-> {v16_pruned_not_canettii_good_coverage_decent_percent_mapped.shape[0]} of which >9x median coverage and had more than 80% of their reads mapping to H37Rv")
