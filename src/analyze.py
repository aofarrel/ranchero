import polars as pl
import pandas as pd
from src.neigh import NeighLib

def get_paired_illumina(polars_df, flip=False):
	if NeighLib.check_columns_exist(polars_df, ['platform', 'librarylayout']):
		if not flip:
			return polars_df.filter(
				(pl.col('platform') == 'ILLUMINA') & 
				(pl.col('librarylayout') == 'PAIRED')
			)
		else:
			return polars_df.filter(
				(pl.col('platform') != 'ILLUMINA') & 
				(pl.col('librarylayout') != 'PAIRED')
			)
	else:
		print("Cannot check if paired Illumina due to missing columns.")

def get_organism_tuberculosis(polars_df, flip=False):
	if NeighLib.check_columns_exist(polars_df, ['platform', 'librarylayout']):
		if not flip:
			return polars_df.filter(
				(pl.col('organism') == 'Mycobacterium tuberculosis')
			)
		else:
			return polars_df.filter(
				(pl.col('organism') != 'Mycobacterium tuberculosis')
			)
	else:
		print("Cannot check if organism field is Mycobacterium tuberculosis due to missing columns.")





# pandas versions -- not comprehensive
def get_paired_illumina_pandas(pandas_df):
	return pandas_df.loc[pandas_df['platform'] == 'ILLUMINA'].loc[pandas_df['librarylayout'] == 'PAIRED']
