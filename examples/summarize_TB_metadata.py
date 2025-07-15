rc = "rc172025-07-08TREEONLY"

import polars as pl
import src as Ranchero
_b_ = "\033[1m"
_bb_ = "\033[0m"
_c_ = "\033[0;36m"

print(f"{_b_}Processing inputs, outputs, denylist, and what's on the tree{_bb_}")

sra_only = Ranchero.from_tsv("merged_per_sample_rc0.tsv", list_columns=['run_index'], auto_rancheroize=False)
sra_only_sample_count = sra_only.shape[0]

final_tree = Ranchero.from_tsv("./ranchero_output_archive/2025-07-08-FINAL_ranchero_rc17.subset.annotated.tsv", auto_rancheroize=False, list_columns=['run_index', 'pheno_source', 'SRX_id']).drop('collection', strict=False)
for column in final_tree.columns:
	if column in sra_only and column not in ['sample_index']:
		sra_only = sra_only.drop(column)

sra_only_merged  = Ranchero.merge_dataframes(sra_only, final_tree, merge_upon="sample_index", right_name="tree", indicator="collection", drop_exclusive_right=True)
all_merged       = Ranchero.merge_dataframes(sra_only, final_tree, merge_upon="sample_index", right_name="tree", indicator="collection", drop_exclusive_right=False)
print(f"2025-07-08 final tree file ({final_tree.shape[0]}):")
print(f"->{sra_only_merged.filter(pl.col('collection') == pl.lit('tree')).shape[0]} SRA samples had metadata added")
print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples had metadata added")
samples_on_sra = pl.Series(sra_only.select('sample_index')).to_list()
#print(all_merged.filter(~pl.col('sample_index').is_in(samples_on_sra)).select(['sample_index', 'collection']))

old_tree = Ranchero.from_tsv("./inputs/pipeline/samples on tree 2024-12-12.txt", auto_rancheroize=False)
tree_only_merged = Ranchero.merge_dataframes(final_tree, old_tree, merge_upon="sample_index", right_name="old_tree", indicator="collection", drop_exclusive_right=True)
sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, old_tree, merge_upon="sample_index", right_name="old_tree", indicator="collection", drop_exclusive_right=True)
all_merged      = Ranchero.merge_dataframes(all_merged, old_tree, merge_upon="sample_index", right_name="old_tree", indicator="collection", drop_exclusive_right=False)
assert tree_only_merged.shape[0] == final_tree.shape[0]
print(f"2024-12-12 not-final tree file ({old_tree.shape[0]}):")
print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('old_tree'))).shape[0]} SRA samples had metadata added")
print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples had metadata added")

diffs = Ranchero.from_tsv("./inputs/pipeline/probable_diffs.txt", auto_rancheroize=False)
tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, diffs, merge_upon="sample_index", right_name="diffs", indicator="collection", drop_exclusive_right=True)
sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, diffs, merge_upon="sample_index", right_name="diff", indicator="collection", drop_exclusive_right=True)
all_merged      = Ranchero.merge_dataframes(all_merged, diffs, merge_upon="sample_index", right_name="diff", indicator="collection", drop_exclusive_right=False)
assert tree_only_merged.shape[0] == final_tree.shape[0]
print(f"Probable diffs file ({diffs.shape[0]}):")
print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('diff'))).shape[0]} SRA samples had metadata added")
print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples had metadata added")

tbprofiler = Ranchero.from_tsv("./inputs/tbprofiler/tbprofiler_basically_everything_rancheroized_lesscolumns_FIXMISSINGDATA.tsv", auto_rancheroize=False)
tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=True)
sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=True)
all_merged      = Ranchero.merge_dataframes(all_merged, tbprofiler, merge_upon="sample_index", right_name="tbprofiler", indicator="collection", drop_exclusive_right=False)
assert tree_only_merged.shape[0] == final_tree.shape[0]
print(f"Probable tbprofiler file ({tbprofiler.shape[0]}): KNOWN TO EXCLUDE SOME SAMPLES)")
print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('tbprofiler'))).shape[0]} SRA samples had metadata added")
print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples had metadata added")
print(all_merged.filter(~pl.col('sample_index').is_in(samples_on_sra)).select(['sample_index', 'collection', 'tbprof_drtype']))

inputs = Ranchero.from_tsv("./inputs/pipeline/probable_inputs.txt", auto_rancheroize=False)
tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, inputs, merge_upon="sample_index", right_name="inputs", indicator="collection", drop_exclusive_right=True)
sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, inputs, merge_upon="sample_index", right_name="input", indicator="collection", drop_exclusive_right=True)
all_merged      = Ranchero.merge_dataframes(all_merged, inputs, merge_upon="sample_index", right_name="input", indicator="collection", drop_exclusive_right=False)
assert tree_only_merged.shape[0] == final_tree.shape[0]
print(f"Probable inputs file ({inputs.shape[0]}):")
print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('input'))).shape[0]} SRA samples had metadata added")
print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples had metadata added")

denylist = Ranchero.from_tsv("./inputs/pipeline/denylist_2024-07-23_lessdupes.tsv", auto_rancheroize=False)
tree_only_merged = Ranchero.merge_dataframes(tree_only_merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=True)
sra_only_merged = Ranchero.merge_dataframes(sra_only_merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=True)
all_merged      = Ranchero.merge_dataframes(all_merged, denylist, merge_upon="sample_index", right_name="denylist", indicator="collection", drop_exclusive_right=False)
assert tree_only_merged.shape[0] == final_tree.shape[0]
print(f"Denylist file ({denylist.shape[0]}):")
print(f"->{sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('denylist'))).shape[0]} SRA samples had metadata added")
print(f"->{all_merged.shape[0] - sra_only_sample_count} non-SRA samples had metadata added")

print(f"{_b_}Host information{_bb_}")
has_host = tree_only_merged.select(pl.col('host_commonname').filter(pl.col('host_commonname').is_not_null()))
human = has_host.filter(pl.col('host_commonname') == pl.lit('human'))
cowish = has_host.filter([
	pl.col('host_commonname').str.contains_any(['domestic cattle', 'bovine', 'guar', 'gaur', 'buffalo', 'bison'])
])
badger = has_host.filter([
	pl.col('host_commonname').str.contains(pl.lit('badger'))
])
deer = has_host.filter([
	pl.col('host_commonname').str.contains_any(['deer', 'elk', 'moose'])
])
pig = has_host.filter([
	pl.col('host_commonname').str.contains_any(['pig', 'boar'])
])
with pl.Config(tbl_cols=-1, tbl_rows=100):
	print(has_host.select(pl.col('host_commonname').value_counts(sort=True)))
	print(cowish.select(pl.col('host_commonname').value_counts(sort=True)))
	print(deer.select(pl.col('host_commonname').value_counts(sort=True)))
	print(pig.select(pl.col('host_commonname').value_counts(sort=True)))
print(f"{has_host.shape[0]} ({has_host.shape[0] / tree_only_merged.shape[0] * 100 :.2f}%) of samples on the tree have any host annotation at all")
print(f"->{human.shape[0]} ({human.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly human")
print(f"->{cowish.shape[0]} ({cowish.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly cattle")
print(f"->{badger.shape[0]} ({badger.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly badger")
print(f"->{deer.shape[0]} ({deer.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly deer")
print(f"->{pig.shape[0]} ({pig.shape[0] / has_host.shape[0] * 100 :.2f}%) are explictly pig")

print(f"{_b_}date collected{_bb_}")
has_date_collected = tree_only_merged.select(pl.col('date_collected').filter(pl.col('date_collected').is_not_null()))
print(f"There's {has_date_collected.shape[0]} ({has_date_collected.shape[0] / tree_only_merged.shape[0] * 100 :.2f}%) samples on the final tree with a date_collected value.")

print(f"{_b_}TBProfiler{_bb_}")
print("Out of the samples on the tree, what is median and mean of the median coverage?")
print(tree_only_merged.filter(pl.col('tbprof_median_coverage').is_not_null()).select(pl.median('tbprof_median_coverage')))
print(tree_only_merged.filter(pl.col('tbprof_median_coverage').is_not_null()).select(pl.mean('tbprof_median_coverage')))

print("What's on the tree WITHOUT TBProfiler lineage information? (sometimes tbprofiler can't assign a lineage or may assign two)")
with pl.Config(tbl_cols=-1, tbl_rows=100):
	lacks_tbprof_main =  tree_only_merged.filter(~pl.col('tbprof_main_lin').is_not_null()).select(['sample_index', 'tbprof_main_lin', 'tbprofiler_lineage_usher', 'lineage'])
	print(lacks_tbprof_main)
print("What's on the tree WITH TBProfiler lineage information? (sometimes tbprofiler can't assign a lineage or may assign two)")
has_tbprof_main_lin = tree_only_merged.filter(pl.col('tbprof_main_lin').is_not_null()).select(['tbprof_main_lin', 'tbprofiler_lineage_usher', 'lineage'])
has_one_tbprof_main_lin = has_tbprof_main_lin.filter(~pl.col('tbprof_main_lin').str.contains(';'))
print(f"A total of {has_tbprof_main_lin.shape[0]} ({has_tbprof_main_lin.shape[0] / tree_only_merged.shape[0] * 100 :.2f}%) samples have TBProf lineage info")
print(f"->Of which {has_one_tbprof_main_lin.shape[0]} ({has_one_tbprof_main_lin.shape[0] / has_tbprof_main_lin.shape[0] * 100 :.2f}% of have main lineage is True) samples have just one TBProf lineage main lineage")

four = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage4')])
two = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage2')])
one = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage1')])
three = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage3')])
la1 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('La1')])
six = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage6')])
five = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage5')])
la3 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('La3')])
la2 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('La2')])
lineage7 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage7')])
lineage9 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage9')])
lineage8 = has_one_tbprof_main_lin.filter([pl.col('tbprof_main_lin') == pl.lit('lineage8')])

print(f"->{four.shape[0]} ({four.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage4")
print(f"->{two.shape[0]} ({two.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage2")
print(f"->{one.shape[0]} ({one.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage1")
print(f"->{three.shape[0]} ({three.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage3")
print(f"->{la1.shape[0]} ({la1.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are La1")
print(f"->{six.shape[0]} ({six.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage6")
print(f"->{five.shape[0]} ({five.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage5")
print(f"->{la3.shape[0]} ({la3.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are la3")
print(f"->{la2.shape[0]} ({la2.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are la2")
print(f"->{lineage7.shape[0]} ({lineage7.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage7")
print(f"->{lineage9.shape[0]} ({lineage9.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage9")
print(f"->{lineage8.shape[0]} ({lineage8.shape[0] / has_one_tbprof_main_lin.shape[0] * 100 :.2f}%) are lineage8")

has_tbprof_dr = tree_only_merged.filter(pl.col('tbprof_drtype').is_not_null())
print(f"{has_tbprof_dr.shape[0]}  ({has_tbprof_dr.shape[0] / tree_only_merged.shape[0] * 100 :.2f}%) samples have a tbprof dr type")

sense = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('Sensitive')])
mdr = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('MDR-TB')])
other = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('Other')])
prexdrtb = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('Pre-XDR-TB')])
hr = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('HR-TB')])
rr = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('RR-TB')])
xdr = has_tbprof_dr.filter([pl.col('tbprof_drtype') == pl.lit('XDR-TB')])

print(f"->{sense.shape[0]} ({sense.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are Sensitive")
print(f"->{other.shape[0]} ({other.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are explictly other")
print(f"->{mdr.shape[0]} ({mdr.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are MDR-TB")
print(f"->{prexdrtb.shape[0]} ({prexdrtb.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are Pre-XDR-TB")
print(f"->{hr.shape[0]} ({hr.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are MDR-TB")
print(f"->{rr.shape[0]} ({rr.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are RR")
print(f"->{xdr.shape[0]} ({xdr.shape[0] / has_tbprof_dr.shape[0] * 100 :.2f}%) are XDR-TB")


print(f"{_b_}TBProfiler{_bb_}")


#print("Have TBProfiler lineage but not normal lineage (excluding Fran and deny and tree)")
#sra_only_merged_tbprofilerlineage = sra_only_merged.filter(pl.col('collection').list.contains(pl.lit('tbprofiler')))
#sra_only_merged_sralineage = sra_only_merged.filter(pl.col('lineage').is_not_null())

