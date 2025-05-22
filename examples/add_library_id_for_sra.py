# Using regex and ranchero to extract library name from a fastq filename

import os
import sys
import src as Ranchero
import polars as pl

HIC_Y3_Y4_part2 = pl.read_csv('../HPRC_metadata/submissions/HIC_Y3_Y4_part2/HIC_Y3_Y4_part2_sra.tsv', separator='\t')
HIC_Y3_Y4_part2 = Ranchero.NeighLib.basename_col(HIC_Y3_Y4_part2, "filename", "library_ID")
Ranchero.dfprint(HIC_Y3_Y4_part2.select(["library_ID", "filename"]), str_len=100)
Ranchero.to_tsv(HIC_Y3_Y4_part2, "../HPRC_metadata/submissions/HIC_Y3_Y4_part2/bar.tsv")