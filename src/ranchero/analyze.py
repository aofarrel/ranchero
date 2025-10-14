import polars as pl


def rm_all_not_beginning_with_myco(polars_df, inverse=False, column='organism'):
	self.NeighLib.check_columns_exist(polars_df, [column], err=True, verbose=True)
	if not inverse:
		return polars_df.filter(pl.col(column).str.starts_with("Myco", case=False))
	else:
		return polars_df.filter(~pl.col(column).str.starts_with("Myco", case=False))

def rm_tuberculosis_suffixes(polars_df, rm_variants=False, clean_variants=True, column='organism'):
	"""
	polars regex doesn't support look-ahead/look-behind, so this is very cringe
	"""
	self.NeighLib.check_columns_exist(polars_df, [column], err=True, verbose=True)

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



def print_unique_rows(polars_df, column='organisms', sort=True):
	if sort:
		self.NeighLib.super_print_pl(polars_df.select("organism").unique().sort("organism"), f"unique {column}")
	else:
		self.NeighLib.super_print_pl(polars_df.select("organism").unique(), f"unique {column}")

# pandas versions -- not comprehensive
def get_paired_illumina_pandas(pandas_df):
	import pandas as pd
	return pandas_df.loc[pandas_df['platform'] == 'ILLUMINA'].loc[pandas_df['librarylayout'] == 'PAIRED']
