# Merge two metadata TSV files upon a shared column
# Usage: merge_metadata.py <tsv_1> <tsv_2> <shared_column>

import os
import sys
import src as Ranchero

shared_column = sys.argv[3]

tsv1 = Ranchero.from_tsv(sys.argv[1], check_index=False, auto_rancheroize=False)
tsv1 = Ranchero.hella_flat(Ranchero.fix_index(tsv1, manual_index_column=shared_column), force_index=shared_column)

tsv2 = Ranchero.from_tsv(sys.argv[2], check_index=False, auto_rancheroize=False)
tsv2 = Ranchero.hella_flat(Ranchero.fix_index(tsv2, manual_index_column=shared_column), force_index=shared_column)

merged = Ranchero.hella_flat(Ranchero.merge_dataframes(
	left=tsv1, right=tsv2, 
	left_name=os.path.basename(sys.argv[1]), right_name=os.path.basename(sys.argv[2]),
	merge_upon=shared_column, force_index=shared_column), force_index=shared_column)

Ranchero.dfprint(merged, cols=10, rows=-1, width=190)
Ranchero.to_tsv(merged, "merged_metadata.tsv")