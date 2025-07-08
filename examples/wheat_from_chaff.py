# Doesn't really use ranchero functions...
# Takes in a bunch of hardcoded files piped from find (ex: `find . -name "*.bedgraph" > INDEX_storage1_ash_bedgraph`)
# Spits out a list of paths to MD5sum


import sys
import src as Ranchero
import polars as pl
Ranchero.pl.Config.set_tbl_rows(1000)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(160)
pl.Config.set_fmt_str_lengths(30)
pl.Config.set_fmt_table_cell_list_len(5)


storage1_bedgraph = Ranchero.from_tsv("/Users/aofarrel/INDEX_storage1_ash_bedgraph", check_index=False, auto_rancheroize=False)
storage1_bedgraph_height = storage1_bedgraph.shape[0]
storage1_bedgraph = Ranchero.NeighLib.check_index(storage1_bedgraph, manual_index_column='path')
assert storage1_bedgraph.shape[0] == storage1_bedgraph_height
storage1_bedgraph = Ranchero.add_column_with_this_value(storage1_bedgraph, "disk", "storage1")
storage1_bedgraph = Ranchero.add_column_with_this_value(storage1_bedgraph, "ext", "bedgraph")
storage1_bedgraph = storage1_bedgraph.with_columns(pl.col("path").str.extract(r"([^/]+)$", 1).alias("basename"))
storage1_bedgraph = storage1_bedgraph.with_columns(pl.col("basename").str.replace(r"_to_H37Rv_below_10x_coverage\.bedgraph$", "", literal=False).alias("sample"))
storage1_bedgraph = storage1_bedgraph.with_columns(pl.col("sample").str.replace(r"\.to_H37Rv_below_10x_coverage\.bedgraph$", "", literal=False).alias("sample"))
storage1_bedgraph = storage1_bedgraph.with_columns(pl.col("sample").str.replace(r"_to_Ref\.H37Rv_below_10x_coverage\.bedgraph$", "", literal=False).alias("sample"))
storage1_bedgraph = storage1_bedgraph.with_columns(pl.col('path').alias('bedgraph_path'))

storage1_diff = Ranchero.from_tsv("/Users/aofarrel/INDEX_storage1_ash_diff", check_index=False, auto_rancheroize=False)
storage1_diff_height = storage1_diff.shape[0]
storage1_diff = Ranchero.NeighLib.check_index(storage1_diff, manual_index_column='path')
assert storage1_diff.shape[0] == storage1_diff_height
storage1_diff = Ranchero.add_column_with_this_value(storage1_diff, "disk", "storage1")
storage1_diff = Ranchero.add_column_with_this_value(storage1_diff, "ext", "diff")
storage1_diff = storage1_diff.with_columns(pl.col("path").str.extract(r"([^/]+)$", 1).alias("basename"))
storage1_diff = storage1_diff.with_columns(pl.col("basename").str.replace(r"\.diff$", "", literal=False).alias("sample"))
storage1_diff = storage1_diff.with_columns(pl.col('path').alias('diff_path'))

storage1_vcf = Ranchero.from_tsv("/Users/aofarrel/INDEX_storage1_ash_vcf", check_index=False, auto_rancheroize=False)
storage1_vcf_height = storage1_vcf.shape[0]
storage1_vcf = Ranchero.NeighLib.check_index(storage1_vcf, manual_index_column='path')
assert storage1_vcf.shape[0] == storage1_vcf_height
storage1_vcf = Ranchero.add_column_with_this_value(storage1_vcf, "disk", "storage1")
storage1_vcf = Ranchero.add_column_with_this_value(storage1_vcf, "ext", "vcf")
storage1_vcf = storage1_vcf.with_columns(pl.col("path").str.extract(r"([^/]+)$", 1).alias("basename"))
storage1_vcf = storage1_vcf.with_columns(pl.col("basename").str.replace(r"\.vcf$", "", literal=False).alias("sample"))
storage1_vcf = storage1_vcf.with_columns(pl.col('path').alias('vcf_path'))

# concat
storage1 = pl.concat([storage1_bedgraph, storage1_diff, storage1_vcf], how="align", rechunk=True)
assert storage1.shape[0] == storage1_bedgraph_height + storage1_diff_height + storage1_vcf_height
print(f"Got {storage1.shape[0]} files in /storage1/ash")

# remove B.S.
storage1_height_start = storage1.shape[0]
storage1 = storage1.with_columns(
	pl.when(pl.col("path").str.starts_with("./mycosra_psuedoancestral/"))
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("psuedoancestral_investigation"))
storage1 = storage1.remove(pl.col("psuedoancestral_investigation")).drop("psuedoancestral_investigation")
print(f"Removed {storage1_height_start - storage1.shape[0]} reruns of psuedoancestral files (NOT DROPPING THE PSUEDOANCESTRALS, JUST THE RERUNS)")

storage1_height_start = storage1.shape[0]
storage1 = storage1.with_columns(
	pl.when(pl.col("path").str.contains("call-"))
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("miniwdl_crap"))
storage1 = storage1.remove(pl.col("miniwdl_crap")).drop("miniwdl_crap")
print(f"Removed {storage1_height_start - storage1.shape[0]} files from miniwdl run folders (some might be okay but for now we're skipping them)")


# KNOWN BAD
storage1_height_start = storage1.shape[0]
storage1 = storage1.with_columns(
	pl.when(pl.col("path").str.starts_with("./everything_else/partial_outs_backup/open_data/tba3_rand_runs/old_versions/"))
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("tba3_old_versions"))
storage1 = storage1.remove(pl.col("tba3_old_versions")).drop("tba3_old_versions")
print(f"Removed {storage1_height_start - storage1.shape[0]} files from old tba3 runs")

storage1_height_start = storage1.shape[0]
storage1 = storage1.with_columns(
	pl.when((pl.col("path").str.starts_with("./everything_else/dupes/")).or_(pl.col("path").str.starts_with("./dupes/")))
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("known_dupe"))
storage1 = storage1.remove(pl.col("known_dupe")).drop("known_dupe")
print(f"Removed {storage1_height_start - storage1.shape[0]} files from the dupes folder (there could still be other dupes though)")

storage1_height_start = storage1.shape[0]
storage1 = storage1.with_columns(
	pl.when(pl.col("path").str.starts_with("./everything_else/attic_dont_delete/NCHHSTP-DTBE-Varpipe-WGS/"))
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("varpipe"))
storage1 = storage1.remove(pl.col("varpipe")).drop("varpipe")
print(f"Removed {storage1_height_start - storage1.shape[0]} files from the NCHHSTP varpipe workflow folder")

storage1_height_start = storage1.shape[0]
storage1 = storage1.with_columns(
	pl.when(
		(pl.col("path").str.starts_with("./decontam_analysis"))
		.or_(pl.col("path").str.contains("2024-tba3-comparison")))
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("decontam_analysis"))
storage1 = storage1.remove(pl.col("decontam_analysis")).drop("decontam_analysis")
print(f"Removed {storage1_height_start - storage1.shape[0]} files of manual analysis")

storage1_height_start = storage1.shape[0]
storage1 = storage1.with_columns(
	pl.when(pl.col("path").str.contains("lineage"))
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("lineage_run"))
storage1 = storage1.remove(pl.col("lineage_run")).drop("lineage_run")
print(f"Removed {storage1_height_start - storage1.shape[0]} files of lineage runs (which are likely all super old)")

storage1_height_start = storage1.shape[0]
storage1 = storage1.with_columns(
	pl.when(pl.col("sample") == pl.lit(''))
	.then(pl.lit(True))
	.otherwise(pl.lit(False))
	.alias("null"))
storage1 = storage1.remove(pl.col("null")).drop("null")
print(f"Removed {storage1_height_start - storage1.shape[0]} files without proper sample names (usually these are concatenated indeces of other files)")

storage1_height_start = storage1.shape[0]
storage1 = storage1.remove(pl.col("sample").str.ends_with('.bedgraph')).remove(pl.col("sample") == 'party_time🎉').remove(pl.col("sample").str.starts_with('r')).remove(pl.col("sample").str.starts_with('input'))
print(f"Removed {storage1_height_start - storage1.shape[0]} more nonsense files")


print(storage1.sort("sample"))


