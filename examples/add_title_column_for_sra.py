import sys
import src as Ranchero
import polars as pl

HIC_Y3_Y4_part2 = pl.read_csv('../HPRC_metadata/submissions/HIC_Y3_Y4_part2/HIC_Y3_Y4_part2_paired.tsv', separator='\t')
HIC_Y3_Y4_part2 = Ranchero.NeighLib.replace_substring_with_col_value(HIC_Y3_Y4_part2, "sample_id", "title",
	"Illumina Sequencing of Omni-C Libraries of SAMPLENAME")
Ranchero.dfprint(HIC_Y3_Y4_part2.select(["sample_id", "title"]), str_len=1000)
Ranchero.to_tsv(HIC_Y3_Y4_part2, "../HPRC_metadata/submissions/HIC_Y3_Y4_part2/HIC_Y3_Y4_part2_sra.tsv")