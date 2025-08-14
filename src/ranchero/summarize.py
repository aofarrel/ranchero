possible_host_columns = ['host_commonname', 'host_scienname',  # ranchero columns
	'host', 'host_sciname', 'host_taxid',                      # feasible columns
	'host_taxid_sam',  'host_run', 'host_sam']                 # NCBI columns


types_of_pig = ['pig', 'boar', 'Sus scrofa']

def host_pig(polars_df):
	host_columns = NeighLib.get_valid_cols(polars_df, possible_host_columns)
	pigs = 0
	for host_col in host_columns:
		# TODO: there should be an option to not match on substrings for dogfish, catfish, etc
		pigs += polars_df.filter([pl.col(host_col).str.contains_any(types_of_pig)]).shape[0]
	return pigs

def col_median(polars_df, col):
	return polars_df.filter(pl.col(col).is_not_null()).select(pl.median(col)).item()

def col_mean(polars_df, col):
	return polars_df.filter(pl.col(col).is_not_null()).select(pl.mean(col)).item()