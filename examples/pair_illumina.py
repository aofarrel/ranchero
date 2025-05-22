import sys
import src as Ranchero
import polars as pl

HIC_Y3_Y4_part2 = pl.read_csv('../HPRC_metadata/submissions/HIC_Y3_Y4_part2/HIC_Y3_Y4_part2_data_table.csv')
HIC_Y3_Y4_part2 = Ranchero.NeighLib.pair_illumina_reads(HIC_Y3_Y4_part2, "path")

# Dataframe now has R1 and R2 column (and maybe a prexisting filename column, which
# we will drop here -- don't do this if you already have important stuff there!)
# To follow SRA standard, we have to rename/drop some columns.
HIC_Y3_Y4_part2 = HIC_Y3_Y4_part2.drop(Ranchero.NeighLib.valid_cols(HIC_Y3_Y4_part2, ["filename"]))
HIC_Y3_Y4_part2 = HIC_Y3_Y4_part2.rename({"R1": "filename", "R2": "filename2"})
Ranchero.to_tsv(HIC_Y3_Y4_part2, "../HPRC_metadata/submissions/HIC_Y3_Y4_part2/HIC_Y3_Y4_part2_paired.tsv")