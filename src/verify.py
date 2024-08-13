import polars as pl

def verify_is_run_indexed():
	pass

def verify_is_sample_indexed():
	pass

def verify_sample_indexed_dates_consistent(flatten=False):
	pass

def verify_sample_indexed_countries_consistent(flatten=False):
	pass

def verify_sample_indexed_organism_consistent(flatten=False):
	pass

def verify_acc_and_acc1(polars_df):
	if "acc" in polars_df.columns and "acc_1" in polars_df.columns:
		comparison = polars_df.select([
			((pl.col("acc") == pl.col("acc_1")) | (pl.col("acc").is_null() & pl.col("acc_1").is_null())).all().alias("are_equal")
		])
		return comparison[0, "are_equal"]
	else:
		return True