import polars as pl
import pandas as pd
from src.neigh import NeighLib
from src.dictionaries import tuberculosis_organisms

def get_paired_illumina(polars_df, inverse=False):
	NeighLib.check_columns_exist(polars_df, ['platform', 'librarylayout'], err=True, verbose=True)
	if not inverse:
		return polars_df.filter(
			(pl.col('platform') == 'ILLUMINA') & 
			(pl.col('librarylayout') == 'PAIRED')
		)
	else:
		return polars_df.filter(
			(pl.col('platform') != 'ILLUMINA') & 
			(pl.col('librarylayout') != 'PAIRED')
		)

def drop_lowcount_columns(polars_df, cutoff=3, verbose=True):
	dropped = []
	starting_columns = len(polars_df.columns)
	for column in polars_df.columns:
		if column == 'plaftorm' or column == 'librarylayout' or column == 'taxid':
			continue
		counts = polars_df.select([pl.col(column).value_counts(sort=True)])
		if len(counts) < cutoff:
			dropped.append(column)
			polars_df = polars_df.drop(column)
	ending_columns = len(polars_df.columns)
	if verbose: print(f"Removed {starting_columns - ending_columns} columns with less than {cutoff} unique values")
	if verbose: print(dropped)
	return polars_df


def rm_all_phages(polars_df, inverse=False, column='organism'):
	NeighLib.check_columns_exist(polars_df, [column], err=True, verbose=True)
	if not inverse:
		return polars_df.filter(~pl.col(column).str.contains_any(["phage"]))
	else:
		return polars_df.filter(pl.col(column).str.contains_any(["phage"]))

def rm_all_not_beginning_with_myco(polars_df, inverse=False, column='organism'):
	NeighLib.check_columns_exist(polars_df, [column], err=True, verbose=True)
	if not inverse:
		return polars_df.filter(pl.col(column).str.starts_with("Myco", case=False))
	else:
		return polars_df.filter(~pl.col(column).str.starts_with("Myco", case=False))

def rm_tuberculosis_suffixes(polars_df, rm_variants=False, clean_variants=True, column='organism'):
	"""
	polars regex doesn't support look-ahead/look-behind, so this is very cringe
	"""
	NeighLib.check_columns_exist(polars_df, [column], err=True, verbose=True)

	# manually handle the avium complex's weirdest member
	polars_df = polars_df.with_columns(
		pl.col(column).str.replace(r"Mycobacterium \[tuberculosis\] TKK-01-0051", "Mycobacterium avium complex")
	)

	# This is the best I can do without look-around regex features. I don't like it, but it works.
	if rm_variants:
		# "Mycobacterium tuberculosis variant bovis" --> "Mycobacterium tuberculosis"
		polars_df = polars_df.with_columns(
			pl.col(column).str.replace(r"Mycobacterium tuberculosis variant .*", "Mycobacterium tuberculosis")
		)
	else:
		# "Mycobacterium tuberculosis variant bovis" --> "HOVERCRAFT_OF_EELS_VARIANT bovis"
		polars_df = polars_df.with_columns(
			pl.col(column).str.replace(r"Mycobacterium tuberculosis variant", "HOVERCRAFT_OF_EELS_VARIANT")
		)
	
	# TODO: this doesn't work properly, regardless of escaping the period in the regex string 
	#if rm_sp:
	#	# "Mycobacterium tuberculosis sp. DSM 3803" --> "Mycobacterium tuberculosis" is expected but not happening
	#	polars_df = polars_df.with_columns(
	#		pl.col(column).str.replace(r"Mycobacterium tuberculosis sp.*", "Mycobacterium tuberculosis")
	#	)
	#else:
	#	# "Mycobacterium tuberculosis sp. DSM 3803" --> "HOVERCRAFT_OF_EELS_SPECIES DSM 3803" is expected but not happening
	#	polars_df = polars_df.with_columns(
	#		pl.col(column).str.replace(r"Mycobacterium tuberculosis sp.*", "HOVERCRAFT_OF_EELS_SPECIES")
	#	)

	polars_df = polars_df.with_columns(
			pl.col(column).str.replace(r"Mycobacterium tuberculosis .*", "Mycobacterium tuberculosis")
	)
	polars_df = polars_df.with_columns(
			pl.col(column).str.replace(r"Mycobacterium sp.*", "Mycobacterium sp.") # this will break if someone names a new species "M. sp-something"
	)

	if clean_variants:
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"HOVERCRAFT_OF_EELS_VARIANT africanum.*", "Mycobacterium tuberculosis variant africanum"))
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"HOVERCRAFT_OF_EELS_VARIANT bovis.*", "Mycobacterium tuberculosis variant bovis"))
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"HOVERCRAFT_OF_EELS_VARIANT caprae.*", "Mycobacterium tuberculosis variant caprae"))
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"HOVERCRAFT_OF_EELS_VARIANT microti.*", "Mycobacterium tuberculosis variant microti"))
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"HOVERCRAFT_OF_EELS_VARIANT pinnipedii.*", "Mycobacterium tuberculosis variant pinnipedii"))
		
		# NTM, not TB variants, but whatever
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"Mycobacterium abscessus .*", "Mycobacterium abscessus"))
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"Mycobacterium canettii .*", "Mycobacterium canettii"))
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"Mycolicibacterium smegmatis .*", "Mycolicibacterium smegmatis"))  # old name Mycobacterium smegmatis is on NCBI but seems to have no suffixes
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"Mycobacteroides abscessus .*", " abscessus"))  # old name Mycobacterium abscessus is on NCBI but seems to have no suffixes
		
	else:
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"HOVERCRAFT_OF_EELS_VARIANT", "Mycobacterium tuberculosis variant"))
	return polars_df

def get_known_organisms(polars_df, 
		regex=tuberculosis_organisms.recommended_mycobacteria_regex, # match to this regex string
		inverse=False, # returns what does NOT match the regex
		column='organism', # column to look for regex match
		rm=False, # remove rows that match regex (or don't match if inverse)
		flag_column=None,                # if not None, create new column with this name, and in that column...
		match_value="Mycobacteria",      # ...flag matches with this string
	):
	NeighLib.check_columns_exist(polars_df, [column], err=True, verbose=False)

	if flag_column is not None:
		if not NeighLib.check_columns_exist(polars_df, [flag_column], err=False, verbose=False):
			# flag column doesn't already exist, we can just throw anything in it without fear of overwriting
			polars_df = polars_df.with_columns(
				pl.when(
					pl.col(column).str.count_matches(f"{regex}") == 1
				)
				.then(pl.lit(match_value))
				.alias(flag_column)
			)
		else:
			# flag column already exists, don't overwrite existing columns
			polars_df = polars_df.with_columns(
			pl.when(
				(pl.col(column).str.count_matches(f"{regex}") == 1) & (pl.col(flag_column).is_null())
			)
			.then(pl.lit(match_value))
			.otherwise(pl.col(flag_column))  # Keep the original value if it exists
			.alias(flag_column)
			)
	if rm:
		if inverse:
			polars_df = polars_df.filter(~pl.col(column).str.contains(regex))
		else:
			polars_df = polars_df.filter(pl.col(column).str.contains(regex))
	return polars_df

def rm_not_MTBC(polars_df):
	polars_df = get_known_organisms(polars_df, inverse=False, regex=tuberculosis_organisms.recommended_MTBC_regex, rm=True)
	return polars_df

def rm_row_if_col_null(polars_df, column='mycobact_type'):
	return polars_df.filter(~pl.col(column).is_null())

def classify_bacterial_family(polars_df, in_column='organism', out_column='mycobact_type', avium_and_abscess_separate_from_NTM=True):
	NeighLib.check_columns_exist(polars_df, [in_column], err=True, verbose=True)
	if NeighLib.check_columns_exist(polars_df, [out_column], err=False, verbose=False):
		print(f"Wanted to create new column {out_column} but it already exists!")
		exit(1)
	# start with most specific
	if avium_and_abscess_separate_from_NTM:
		polars_df = get_known_organisms(polars_df, regex=tuberculosis_organisms.avium_regex, match_value="M. avium complex", flag_column=out_column, column=in_column)
		polars_df = get_known_organisms(polars_df, regex=tuberculosis_organisms.abscessus_regex, match_value="M. abscessus complex", flag_column=out_column, column=in_column)
	polars_df = get_known_organisms(polars_df, regex=tuberculosis_organisms.mycolicibacterium_regex, match_value="mycolicibacteria", flag_column=out_column, column=in_column)
	polars_df = get_known_organisms(polars_df, regex=tuberculosis_organisms.leprosy_regex, match_value="Leprosy", flag_column=out_column, column=in_column)
	polars_df = get_known_organisms(polars_df, regex=tuberculosis_organisms.NTM_regex, match_value="NTM", flag_column=out_column, column=in_column)
	polars_df = get_known_organisms(polars_df, regex=tuberculosis_organisms.recommended_MTBC_regex, match_value="MTBC", flag_column=out_column, column=in_column)
	polars_df = get_known_organisms(polars_df, regex=tuberculosis_organisms.recommended_mycobacteria_regex, column=in_column, match_value="Unclassified mycobacteria", flag_column=out_column)
	print(polars_df)
	return polars_df

def print_unique_rows(polars_df, column='organisms', sort=True):
	if sort:
		NeighLib.super_print_pl(polars_df.select("organism").unique().sort("organism"), f"unique {column}")
	else:
		NeighLib.super_print_pl(polars_df.select("organism").unique(), f"unique {column}")

# pandas versions -- not comprehensive
def get_paired_illumina_pandas(pandas_df):
	return pandas_df.loc[pandas_df['platform'] == 'ILLUMINA'].loc[pandas_df['librarylayout'] == 'PAIRED']
