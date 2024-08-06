import pandas as pd
import polars as pl
from contextlib import suppress
import os
from tqdm import tqdm
from src.neigh import NeighLib
from src.dictionaries import columns
from src.dictionaries import null_values

tqdm.pandas()
verbose = True  # TODO: convert to logging
write_intermediate_files = True  # TODO: make args-y

#def polars_to_pandas(polars_df):
#

def polars_to_tsv(polars_df, path):
	polars_df.write_csv(path, separator='\t', include_header=True, null_value='')

def pandas_to_tsv(pandas_df, path):
	pandas_df.to_csv(path, sep='\t', index=False)

def polars_from_tsv(tsv, sep='\t', try_parse_dates=True, null_values=null_values.null_values, ignore_errors=True):
	return pl.read_csv(tsv, separator=sep, try_parse_dates=try_parse_dates, null_values=null_values, ignore_errors=ignore_errors)

def pandas_from_tsv(tsv):
	return pd.read_csv(tsv, sep='\t')

def pandas_fix_attributes_dictionaries(pandas_df, rancheroize=False):
	"""
	Uses NeighLib.concat_dicts to turn the weird format of the attributes column into flat dictionaries
	"""
	if verbose:
		print("Concatenating dictionaries...")
		pandas_df['attributes'] = pandas_df['attributes'].progress_apply(NeighLib.concat_dicts)
	else:
		pandas_df['attributes'] = pandas_df['attributes'].apply(NeighLib.concat_dicts)
	pandas_flatdic, pandas_df = pandas_df.copy(), None  # supposedly increases effeciency
	if rancheroize: pandas_flatdic.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)	
	if write_intermediate_files: pandas_to_tsv(pandas_flatdic, f'./intermediate/flatdicts.tsv')
	return pandas_flatdic

def pandas_json_normalize(pandas_df, use_polars=True, rancheroize=False):
	"""
	JSON-normalize the "attributes" column into new columns. use_polars is faster but might break things.
	Regardless of use_polars, input and output are pandas dataframes. Assumes pandas_fix_attributes_dictionaries() was run but
	that shouldn't be necessary.
	"""
	outfile = './intermediate/normalized.tsv'
	print("Normalizing...")

	if use_polars:
		# polars-and-pandas version
		# Even if you set this up to not read the intermedite file to set pandas_df, you'll get less columns in the end?
		# However, this does seem to correctly pull out "bytes" and other JSON data, so we'll take it.
		just_attributes = pl.json_normalize(pandas_df['attributes'], strict=False)  # just_attributes is a polars dataframe
		# collection_date_sam is likely present in both dataframes, so rename one of them
		just_attributes = just_attributes.rename({'collection_date_sam': 'collection_date_from_attributes'})
		dupe_columns = NeighLib.get_dupe_columns_of_two_polars(just_attributes, pl.from_pandas(pandas_df))
		if len(dupe_columns) > 0:
			# TODO: just rename the columns like we did with collection_date_sam!!
			raise AssertionError("Found columns from the attributes section that conflict with an existing column")
		bq_jnorm = pd.concat([pandas_df.drop(columns=['attributes']), just_attributes.to_pandas()], axis=1)
		if verbose: print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
		if rancheroize: bq_jnorm.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)
		if write_intermediate_files:
			pandas_to_tsv(bq_jnorm, outfile)
			print(f"Wrote JSON-normalized dataframe to {outfile}")
	else:
		# pure pandas version
		# This is the slowest, but it acceptable enough. 
		just_attributes = pd.json_normalize(pandas_df['attributes'])  # just_attributes is a Python dictionary
		# collection_date_sam is likely present in both dataframes, so rename one of them
		just_attributes['collection_date_from_attributes'] = mydict.pop('collection_date_sam')
		bq_jnorm = pd.concat([pandas_df.drop(columns=['attributes']), just_attributes], axis=1)
		if verbose: print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
		if rancheroize: bq_jnorm.rename(columns=columns.bq_col_to_ranchero_col, inplace=True)
		if write_intermediate_files: 
			pandas_to_tsv(bq_jnorm, outfile)
			print(f"Wrote JSON-normalized dataframe to {outfile}")
	
	return bq_jnorm

	# pure polars version -- doesn't seem to work properly
	#just_attributes = pl.json_normalize(pandas_df['attributes'], strict=False)  # just_attributes is a polars dataframe
	#pandas_df = polars_from_tsv(f"./intermediate/{path.basename(bq_file)}_flatdicts.tsv", try_parse_dates=False)
	#bq_jnorm = pl.concat([pandas_df.drop('attributes'), just_attributes], how="diagonal")
	#if verbose: print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
	#with suppress(pl.exceptions.SchemaFieldNotFoundError): bq_jnorm.rename(columns.bq_col_to_ranchero_col)
	#if write_intermediate_files: polars_to_tsv(bq_jnorm, f'./intermediate/{path.basename(bq_file)}_normalized_polars.tsv')

def pandas_from_ncbi_run_selector(csv):
	run_raw = pd.read_csv(csv, sep=sep)
	run_renamed = run_raw.rename(columns=columns.run_selector_columns)  # for compatibility with other formats
	return run_renamed

def pandas_from_bigquery(bq_file, write_intermediate_files=True, fix_attributes=True, normalize_attributes=True, merge_into_biosamples=True, polars_normalize=True):
	"""
	1) Read bigquery JSON
	2) if fix_attributes or normalize_attributes: Turns "attributes" column's lists of one-element k/v pairs into dictionaries
	3) if normalize_attributes: JSON-normalize the "attributes" column into their own columns
	4) if merge_into_biosamples: Merge run accessions with the same BioSample, under the assumption that they've the same metadata

	Notes:
	* normalize_attributes and fix_attributes work on the "attributes" column, not the "j_attr" column
	* if polars_normalize, use the under development polars version of json normalize -- this is faster but could be unstable
	"""
	bq_raw = pd.read_json(bq_file)
	if verbose: print(f"{bq_file} has {len(bq_raw.columns)} columns and {len(bq_raw.index)} rows")

	if fix_attributes or normalize_attributes:
		bq_fixed = pandas_fix_attributes_dictionaries(bq_raw)
		if verbose: print(f"{bq_file} has {len(bq_fixed.columns)} columns and {len(bq_fixed.index)} rows")
	if normalize_attributes:  # requires pandas_fix_attributes_dictionaries() to happen first
		bq_norm = pandas_json_normalize(bq_fixed, use_polars=polars_normalize)
		NeighLib.assert_unique_columns(bq_norm)
		if verbose: print(f"{bq_file} has {len(bq_norm.columns)} columns and {len(bq_norm.index)} rows")
		print(verify_acc_and_acc1(pl.from_pandas(bq_norm)))
	
	if merge_into_biosamples:
		bq_to_merge = bq_norm if bq_norm is not None else (bq_fixed if bq_fixed is not None else bq_raw)
		bq_to_merge.to_csv(f'./intermediate/{os.path.basename(bq_file)}_temp_read_polars.tsv', sep='\t', index=False)
		bq_jnorm = pandas_from_tsv(polars_flatten(f"./intermediate/{os.path.basename(bq_file)}_temp_read_polars.tsv", upon='BioSample', input_sep='\t', try_parse_dates=True, keep_all_columns=False))

	
	print("Pandas to polars:")
	ptp = pl.from_pandas(bq_to_merge)
	print("Pandas to Polars to Pandas to Polars:")
	ptptptp = pl.from_pandas(ptp.to_pandas())
	print("Pandas to TSV to polars:")
	ptttp = polars_from_tsv(f'./intermediate/{os.path.basename(bq_file)}_temp_read_polars.tsv')
	print("Pandas to TSV to Pandas to Polars:")
	ptttptp = pl.from_pandas(pandas_from_tsv(f'./intermediate/{os.path.basename(bq_file)}_temp_read_polars.tsv'))

	pl.testing.assert_frame_equal(ptp, ptptptp)
	pl.testing.assert_frame_equal(ptp, ptttp)
	pl.testing.assert_frame_equal(ptp, ptttptp)

	return bq_jnorm if bq_jnorm is not None else bq_flatdicts  # bq_flatdircts if not normalize_attributes

def polars_flatten(input_file, upon='BioSample', input_sep='\t', try_parse_dates=True, keep_all_columns=False, rancheroize=True):
	"""
	Flattens an input file using polars group_by().agg(). This is designed to essentially turn run accession indexed dataframes
	into BioSample-indexed dataframes. Because Ranchero uses a mixture of Pandas and Polars, this function writes the output
	to the disk and returns the path to that file, rather than trying to retrun the dataframe itself.

	If rancheroize, attempt to rename columns to ranchero format.
	"""
	print(f"Flattening {upon}...")
	not_flat = polars_from_tsv(input_file)

	print(verify_acc_and_acc1(not_flat)) # TODO: make this actually do something, like drop acc_1

	if rancheroize:
		#if verbose: print(list(not_flat.columns))
		not_flat = not_flat.rename(columns.bq_col_to_ranchero_col)
		#if verbose: print(list(not_flat.columns))
	
	if keep_all_columns:
		# not tested!
		columns_to_keep = not_flat.col.copy().remove(upon)
		flat = not_flat.group_by(upon).agg(columns_to_keep)
		for nested_column in not_flat.col:
			flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})
			flat = flat_neo  # silly workaround for flat = flat.with_columns(...).rename(...) throwing an error about duped columns
	else:
		columns_to_keep = columns.recommended_sra_columns
		columns_to_keep.remove(upon)
		flat = not_flat.group_by(upon).agg(pl.col(columns_to_keep))
		for nested_column in columns.recommended_sra_columns:
			flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})
			flat = flat_neo  # silly workaround for flat = flat.with_columns(...).rename(...) throwing an error about duped columns

	flat_neo = flat_neo.unique() # doesn't seem to drop anything but may as well leave it
	path = f"./intermediate/{os.path.basename(input_file)}_flattened.tsv"
	polars_to_tsv(flat_neo, path)
	return path

def verify_acc_and_acc1(polars_df):
	if "acc" in polars_df.columns and "acc_1" in polars_df.columns:
		comparison = polars_df.select([
			((pl.col("acc") == pl.col("acc_1")) | (pl.col("acc").is_null() & pl.col("acc_1").is_null())).all().alias("are_equal")
		])
		return comparison[0, "are_equal"]
	else:
		return True

def store_known_multi_accession_metadata(pandas_df_indexed_by_runs):
	"""
	Stores some metadata from run accessions that share a BioSample so you can later verify things didn't get lost when
	they are BioSample-indexed.
	"""
	pass

def verify_pandas_polars_switch(tsv):
	pass