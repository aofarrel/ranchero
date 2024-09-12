import polars as pl
import pandas as pd
from src.neigh import NeighLib
from src.dictionaries import tuberculosis_organisms

def get_paired_illumina(polars_df, inverse=False):
	if not NeighLib.check_columns_exist(polars_df, ['platform', 'librarylayout']):
		print("Cannot check if paired Illumina due to missing columns: platform, librarylayout")
		exit(1)
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

def rm_all_phages(polars_df, inverse=False, column='organism'):
	if not NeighLib.check_columns_exist(polars_df, [column]):
		print(f"Wanted to check column {column} but it doesn't exist in the dataframe")
		exit(1)
	if not inverse:
		return polars_df.filter(~pl.col(column).str.contains_any(["phage"]))
	else:
		return polars_df.filter(pl.col(column).str.contains_any(["phage"]))

def rm_all_not_beginning_with_myco(polars_df, inverse=False, column='organism'):
	if not NeighLib.check_columns_exist(polars_df, [column]):
		print(f"Wanted to check column {column} but it doesn't exist in the dataframe")
		exit(1)

	if not inverse:
		return polars_df.filter(pl.col(column).str.starts_with("Myco", case=False))
	else:
		return polars_df.filter(~pl.col(column).str.starts_with("Myco", case=False))

def get_organism_tuberculosis_strict(polars_df, inverse=False, column='organism'):
	if not NeighLib.check_columns_exist(polars_df, [column]):
		print(f"Wanted to check column {column} but it doesn't exist in the dataframe")
		exit(1)

	if strict:
		if not inverse:
			return polars_df.filter(pl.col(column).str.contains_any(['Mycobacterium tuberculosis']))
		else:
			return polars_df.filter(~pl.col(column).str.contains_any(['Mycobacterium tuberculosis']))
		
def rm_tuberculosis_suffixes(polars_df, rm_variants=False, clean_variants=True, column='organism'):
	"""
	polars regex doesn't support look-ahead/look-behind, so this is very cringe
	"""
	if not NeighLib.check_columns_exist(polars_df, [column]):
		print(f"Wanted to check column {column} but it doesn't exist in the dataframe")
		exit(1)

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
			r"Mycolicibacterium smegmatis .*", "Mycolicibacterium smegmatis"))  # old name Mycobacterium smegmatis on NCBI but seems to have no suffixes
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"Mycobacteroides abscessus .*", " abscessus"))  # old name Mycobacterium abscessus on NCBI but seems to have no suffixes
		
	else:
		polars_df = polars_df.with_columns(pl.col(column).str.replace(
			r"HOVERCRAFT_OF_EELS_VARIANT", "Mycobacterium tuberculosis variant"))
	return polars_df

# This should be run after rm_tuberculosis_suffixes(), as it doesn't have regex matches
def get_known_mycobacteria(polars_df, inverse=False, column='organism', include_unknowns=False):
	if not NeighLib.check_columns_exist(polars_df, [column]):
		print(f"Wanted to check column {column} but it doesn't exist in the dataframe")
		exit(1)

	if include_unknowns:
		matches = tuberculosis_organisms.everything_mycobacterium_flavored_and_unknowns
	else:
		matches = tuberculosis_organisms.everything_mycobacterium_flavored

	return polars_df.filter(pl.col(column).str.contains_any(matches))

def get_known_NTM(polars_df, inverse=False, column='organism'):
	pass
def get_known_leprosy(polars_df, inverse=False, column='organism'):
	pass
def get_known_MTBC(polars_df, inverse=False, column='organism'):
	pass
def get_known_avium_complex(polars_df, inverse=False, column='organism'):
	pass
def get_known_abscessus_complex(polars_df, inverse=False, column='organism'):
	pass
def get_known_tuberculosis(polars_df, inverse=False, column='organism'):
	pass

def print_unique_rows(polars_df, column='organisms', sort=True):
	if sort:
		NeighLib.super_print_pl(polars_df.select("organism").unique().sort("organism"), f"unique {column}")
	else:
		NeighLib.super_print_pl(polars_df.select("organism").unique(), f"unique {column}")

# pandas versions -- not comprehensive
def get_paired_illumina_pandas(pandas_df):
	return pandas_df.loc[pandas_df['platform'] == 'ILLUMINA'].loc[pandas_df['librarylayout'] == 'PAIRED']
