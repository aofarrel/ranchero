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
write_intermediate_files = False  # TODO: make args-y

def unnest_polars_list_columns(polars_df):
	""" Unnests list data (but not the way explode() does it) so it can be writen to CSV format
	Credit: deanm0000 on GitHub, via https://github.com/pola-rs/polars/issues/17966#issuecomment-2262903178
	"""
	return polars_df.with_columns(
		(
			pl.lit("[")
			+ pl.col(x).list.eval(pl.lit('"') + pl.element() + pl.lit('"')).list.join(",")
			+ pl.lit("]")
		).alias(x)
		for x, y in polars_df.schema.items()
		if y == pl.List(pl.String)
	)

def polars_to_tsv(polars_df, path):
	columns_with_type_list = [col for col, dtype in zip(polars_df.columns, polars_df.dtypes) if dtype == pl.List]
	if len(columns_with_type_list) > 0:
		print("Lists will be converted to strings before writing to file")
		if verbose: print(f"Columns to convert: {columns_with_type_list}")
	prepared_polars_df = unnest_polars_list_columns(polars_df)
	columns_with_type_list = [col for col, dtype in zip(prepared_polars_df.columns, prepared_polars_df.dtypes) if dtype == pl.List]
	prepared_polars_df.write_csv(path, separator='\t', include_header=True, null_value='')

def pandas_to_tsv(pandas_df, path):
	pandas_df.to_csv(path, sep='\t', index=False)

def polars_from_tsv(tsv, sep='\t', try_parse_dates=True, null_values=null_values.null_values, ignore_errors=True):
	return pl.read_csv(tsv, separator=sep, try_parse_dates=try_parse_dates, null_values=null_values, ignore_errors=ignore_errors)

def pandas_from_tsv(tsv):
	return pd.read_csv(tsv, sep='\t')

def polars_fix_attributes_and_json_normalize(polars_df, rancheroize=False, cast_types=True, keep_primary_search=True):
	"""
	Uses NeighLib.concat_dicts to turn the weird format of the attributes column into flat dictionaries,
	then do some JSON normalization to output a polars dataframe.

	1. Create a tempoary pandas dataframe
	2. .apply(NeighLib.concat_dicts) to the attributes column in the pandas df
	3. Run polars_json_normalize to add new columns to the polars dataframe
	4. If rancheroize: rename columns
	5. Polars will default to str type for new columns; if cast_types, cast the most common not-string folders to
	   more correct types (example: tax_id --> pl.Int32). Note that this will also run on existing columns that
	   were not added by polars_json_normalize()!
	6. if write_intermediate_files: Write to ./intermediate/flatdicts.tsv
	7. Return polars dataframe.

	Performance is very good on my largest datasets, but I'm interested in avoiding the panadas conversion if possible.
	See comments for various attempts.
	"""
	# Made various attempts to avoid using pandas:
	#polars_df.select(pl.map_batches(["attributes"], NeighLib.concat_dicts)) # could not determine output type
	#
	#for row in polars_df.select("attributes"):
	#	row.to_frame().map_rows(NeighLib.concat_dicts_tuple) # could not determine output type
	#
	# doesn't error, but not what we're looking for:
	#exploded = polars_df.explode("attributes")
	#attributes = polars_flatten(exploded, upon="biosample", keep_all_columns=True, rancheroize=False)
	#
	# Since it runs below ten seconds even on the full size dataframe, I consider the pandas usage acceptable,
	# even though it annoys me on priniciple.

	temp_pandas_df = polars_df.to_pandas()  # TODO: probably faster to just convert the attributes column
	if keep_primary_search:  # TODO: benchmark these two options
		if verbose:
			print("Concatenating dictionaries with Pandas...")
			temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(NeighLib.concat_dicts_primary_search)
		else:
			temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(NeighLib.concat_dicts_primary_search)
	else:
		if verbose:
			print("Concatenating dictionaries with Pandas...")
			temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(NeighLib.concat_dicts)
		else:
			temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(NeighLib.concat_dicts)
	normalized = polars_json_normalize(polars_df, temp_pandas_df['attributes'])
	if rancheroize: normalized.rename(columns.bq_col_to_ranchero_col)
	if cast_types: normalized = NeighLib.cast_politely(normalized)
	if write_intermediate_files: polars_to_tsv(normalized, f'./intermediate/flatdicts.tsv')
	return normalized

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

def polars_json_normalize(polars_df, pandas_attributes_series, rancheroize=False, collection_date_sam_workaround=True):
	"""
	polars_df: polars df to concat to at the end
	pandas_attributes_series: pandas series of dictionaries that will json normalized

	We do this seperately so we can avoid converting the entire dataframe in and out of pandas.
	"""
	attributes_rows = pandas_attributes_series.shape[0]
	assert polars_df.shape[0] == attributes_rows, f"Polars dataframe has {polars_df.shape[0]} rows, but the pandas_attributes has {attributes_rows} rows" 
	
	print(f"Normalizing {attributes_rows} rows (this might take a while)...")
	just_attributes = pl.json_normalize(pandas_attributes_series, strict=False, max_level=1, infer_schema_length=100000)  # just_attributes is a polars dataframe
	assert polars_df.shape[0] == just_attributes.shape[0], f"Polars dataframe has {polars_df.shape[0]} rows, but normalized attributes we want to horizontally combine it with has {just_attributes.shape[0]} rows" 

	if verbose: print("Concatenating to the original dataframe...")
	if collection_date_sam_workaround:
		# polars_df already has a collection_date_sam which it converted to YYYY-MM-DD format. to avoid a merge conflict and to
		# fall back on the attributes version (which perserves dates that failed to YYYY-MM-DD convert), drop collection_date_sam
		# from polars_df before merging.
		bq_jnorm = pl.concat([polars_df.drop(['attributes', 'collection_date_sam']), just_attributes], how="horizontal")
	else:
		bq_jnorm = pl.concat([polars_df.drop('attributes'), just_attributes], how="horizontal")
	print(f"An additional {len(just_attributes.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
	if verbose: print(f"Columns added: {just_attributes.columns}")
	with suppress(pl.exceptions.SchemaFieldNotFoundError): bq_jnorm.rename(columns.bq_col_to_ranchero_col)
	if write_intermediate_files: polars_to_tsv(bq_jnorm, f'./intermediate/normalized_pure_polars.tsv')
	return bq_jnorm

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

def polars_from_ncbi_run_selector(csv):
	run_raw = pl.read_csv(csv)
	run_renamed = run_raw.rename(columns.ncbi_run_selector_col_to_ranchero_col)  # for compatibility with other formats
	return run_renamed

def pandas_from_ncbi_run_selector(csv):
	run_raw = pd.read_csv(csv)
	run_renamed = run_raw.rename(columns=columns.ncbi_run_selector_col_to_ranchero_col)  # for compatibility with other formats
	return run_renamed

def polars_from_bigquery(bq_file, write_intermediate_files=True, nullify=True, normalize_attributes=True, merge_into_biosamples=False):
	""" 
	1. Reads a bigquery JSON into a polars dataframe
	2. (optional) Splits the attributes columns into new columns (combines fixing the attributes column and JSON normalizing)
	3. Rancheroize columns
	4. (optional) Merge by BioSample
	Returns a polars dataframe.
	"""
	bq_raw = pl.read_json(bq_file)
	if verbose: print(f"{bq_file} has {bq_raw.width} columns and {len(bq_raw)} rows")
	if normalize_attributes:
		bq_norm = polars_fix_attributes_and_json_normalize(bq_raw)
		rancheroized =  NeighLib.rancheroize_polars(bq_norm)
	else:
		rancheroized =  NeighLib.rancheroize_polars(bq_raw)	
	# create nulls here, where we have as many not-list columns as possible (eg, after normalize attributes but before biosample merge)
	# TODO: do other replacememnts here too!
	if nullify: rancheroized = rancheroized.with_columns(pl.col(pl.Utf8).replace(null_values.null_values_dictionary))
	if merge_into_biosamples:
		rancheroized = polars_flatten(rancheroized, upon='BioSample', keep_all_columns=False)
	return rancheroized


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
	if merge_into_biosamples:
		bq_to_merge = bq_norm if bq_norm is not None else (bq_fixed if bq_fixed is not None else bq_raw)
		bq_jnorm = (polars_flatten(pl.from_pandas(bq_to_merge), upon='BioSample', keep_all_columns=False, rancheroize=True)).to_pandas()
	return bq_jnorm if bq_jnorm is not None else bq_flatdicts  # bq_flatdircts if not normalize_attributes


def polars_flatten(polars_df, upon='BioSample', keep_all_columns=False):
	"""
	Flattens an input file using polars group_by().agg(). This is designed to essentially turn run accession indexed dataframes
	into BioSample-indexed dataframes. Assumes columns are already rancheroized.

	If rancheroize, attempt to rename columns to ranchero format.
	"""
	print(f"Flattening {upon}s...")

	not_flat = polars_df
	print(not_flat)
	if verbose:
		non_uniques = not_flat.group_by(upon).count().filter(pl.col("count") > 1)[upon]
		number_non_unique = len(non_uniques)
		print(f"Found {number_non_unique} non-unique values for {upon}: {non_uniques}")
	
	if keep_all_columns:
		# not tested!
		columns_to_keep = list(not_flat.columns)
		with suppress(ValueError): columns_to_keep.remove(upon)  # if it's not in there, who cares?
		
		flat = not_flat.group_by(upon).agg(columns_to_keep)
		#for nested_column in not_flat.columns:
		#	flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})
		#	flat = flat_neo  # silly workaround for flat = flat.with_columns(...).rename(...) throwing an error about duped columns
		flat_neo = flat
	else:
		columns_to_keep = columns.recommended_sra_columns
		with suppress(ValueError): columns_to_keep.remove(upon)  # if it's not in there, who cares?
		columns_to_keep_really = []
		columns_in_df = list(not_flat.columns)
		for column in columns_to_keep:
			if column in columns_in_df:
				columns_to_keep_really.append(column)
		flat = not_flat.group_by(upon).agg(pl.col(columns_to_keep_really))
		if verbose: NeighLib.super_print_pl(flat)
		
		#for nested_column in columns.recommended_sra_columns:
		#	if nested_column not in columns_in_df:
		#		continue
		#	else:
		#		if verbose: print(f"Nested column {nested_column} present")
		#		print(flat.with_columns(pl.col(nested_column).list.to_struct()))
		#		flat_neo = flat.with_columns(pl.col(nested_column).list.to_struct()).unnest(nested_column).rename({"field_0": nested_column})

	#flat_neo = flat_neo.unique() # doesn't seem to drop anything but may as well leave it
	#if write_intermediate_files: polars_to_tsv(flat_neo, f"./intermediate/polars_flattened.tsv")
	return flat

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