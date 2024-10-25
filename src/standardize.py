from src.statics import host_species, sample_sources, kolumns
from .config import RancheroConfig
import polars as pl
from . import _NeighLib as NeighLib

class ProfessionalsHaveStandards():
	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to NeighLib class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger\

	def standardize_sample_source(self, polars_df):
		if polars_df.schema['isolation_source'] == pl.List:
			return self.standardize_sample_source_as_list(polars_df)
		else:
			return self.standardize_sample_source_as_string(polars_df)


	def simple_dictionary_match(self, polars_df, match_column, key, value, exact=False):
		"""
		Replace a pl.Utf8 column's values with the values in a dictionary per its key-value pairs.
		Case-insensitive, even with "exact." 'exact' will not match substrings.
		"""
		assert polars_df.schema[match_column] == pl.Utf8
		if exact:
			polars_df = polars_df.with_columns([
				pl.when(pl.col(match_column) == f"(?i){key}")
				.then(pl.lit(value))
				.otherwise(
					pl.when(pl.col(match_column).is_not_null())
					.then(pl.col(match_column))
					.otherwise(None))
				.alias(match_column)])
			
		else:
			polars_df = polars_df.with_columns([
				pl.when(pl.col(match_column).str.contains(f"(?i){key}"))
				.then(pl.lit(value))
				.otherwise(
					pl.when(pl.col(match_column).is_not_null())
					.then(pl.col(match_column))
					.otherwise(None))
				.alias(match_column)])
		return polars_df

	def dictionary_match_on_list(self, polars_df, match_column, key, value, exact=False):
		assert polars_df.schema[match_column] == pl.List(pl.Utf8)
		if exact:
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col(match_column).list.contains(f"(?i){key}").any()
				)
				.then(pl.lit([value]))
				.otherwise(
					pl.when(pl.col(match_column).is_not_null())
					.then(pl.col(match_column))
					.otherwise(None)
				).alias(match_column)
			])

		else:
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col(match_column).list.eval(pl.element().str.contains(f"(?i){key}")).list.any()
				)
				.then(pl.lit([value]))
				.otherwise(
					pl.when(pl.col(match_column).is_not_null())
					.then(pl.col(match_column))
					.otherwise(None)
				).alias(match_column)
			])

		return polars_df

	def standardize_host_disease(self, polars_df):
		assert 'host_disease' in polars_df.columns
		for host_disease, simplified_host_disease in sample_sources.host_disease_exact_match.items():
			polars_df = self.simple_dictionary_match(polars_df, 'host_disease', host_disease, simplified_host_disease, exact=True)
		for host_disease, simplified_host_disease in sample_sources.host_disease.items():
			polars_df = self.simple_dictionary_match(polars_df, 'host_disease', host_disease, simplified_host_disease, exact=False)
		return polars_df

	def standardize_sample_source_as_list(self, polars_df, write_hosts=True, write_lineages=True):
		assert 'isolation_source' in polars_df.columns
		assert polars_df.schema['isolation_source'] == pl.List
		if write_lineages:
			assert polars_df.schema['lineage_sam'] == pl.Utf8
			assert polars_df.schema['strain_sam_ss_dpl139'] == pl.Utf8
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)lineage4.6.2.2')).list.any()
				)
				.then(pl.lit('lineage4.6.2.2'))
				.otherwise(pl.col('lineage_sam'))
				.alias('lineage_sam')
			])
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)H37Rv')).list.any()
				)
				.then(pl.lit('H37Rv'))
				.otherwise(pl.col('strain_sam_ss_dpl139'))
				.alias('strain_sam_ss_dpl139')
			])

		if write_hosts:
			human = pl.lit(['Homo sapiens']) if polars_df.schema['host'] == pl.List else pl.lit('Homo sapiens') 
			mouse = pl.lit(['Mus musculus']) if polars_df.schema['host'] == pl.List else pl.lit('Mus musculus') 
			cow = pl.lit(['Bos tarus']) if polars_df.schema['host'] == pl.List else pl.lit('Bos tarus') 
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)human')).list.any()
				)
				.then(human)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)mouse')).list.any()
				)
				.then(mouse)
				.otherwise(pl.col('host'))
				.alias('host')
			])
			polars_df = polars_df.with_columns([
				pl.when(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)cow')).list.any()
				)
				.then(cow)
				.otherwise(pl.col('host'))
				.alias('host')
			])


		# and now, the stuff in the actual sample source column 
		polars_df = polars_df.with_columns(
			pl.col('isolation_source').list.eval(pl.element().filter(pl.element() != 'DNA')).alias('isolation_source')
		)
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)laboratory strain')).list.any()
				.or_(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)lab strain')).list.any()
				)
			)
			.then(pl.lit(['laboratory strain']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)culture')).list.any()
				.and_(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)sputum')).list.any()
				)
			)
			.then(pl.lit(['culture from sputum']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)culture')).list.any()
				.and_(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)feces')).list.any()
				)
			)
			.then(pl.lit(['culture from feces']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])
		polars_df = polars_df.with_columns([
			pl.when(
				pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)scrapate')).list.any()
				.and_(
					pl.col('isolation_source').list.eval(pl.element().str.contains('(?i)granuloma')).list.any()
				)
			)
			.then(pl.lit(['scrapate of gramuloma']))
			.otherwise(pl.col('isolation_source'))
			.alias('isolation_source')
		])

		for sample_source, simplified_sample_source in sample_sources.sample_source.items():
			polars_df = self.dictionary_match_on_list(polars_df, 'isolation_source', sample_source, simplified_sample_source, exact=False)		
		for sample_source, simplified_sample_source in sample_sources.sample_source_exact_match.items():
			polars_df = self.dictionary_match_on_list(polars_df, 'isolation_source', sample_source, simplified_sample_source, exact=True)
		
		self.logging.info(f"The isolation_source column has type list. We will be .join()ing them into strings.") # done AFTER most standardization
		polars_df = polars_df.with_columns(
			pl.col("host").list.join(", ").alias("host")
		)

		return polars_df

	def standardize_sample_source_as_string(self, polars_df):
		assert 'isolation_source' in polars_df.columns
		assert polars_df.schema['isolation_source'] == pl.Utf8
		for sample_source, simplified_sample_source in sample_sources.sample_source_exact_match.items():
			polars_df = self.simple_dictionary_match(polars_df, 'isolation_source', sample_source, simplified_sample_source, exact=True)
		for sample_source, simplified_sample_source in sample_sources.sample_source.items():
			polars_df = self.simple_dictionary_match(polars_df, 'isolation_source', sample_source, simplified_sample_source, exact=False)
		return polars_df
	
	def standarize_hosts(self, polars_df, eager=True):
		if polars_df.schema['host'] == pl.List:
			self.logging.info(f"The host column has type list. We will take the first value as the source of truth.") # done BEFORE most standardization
			polars_df.with_columns(pl.col('host').list.first().alias('host'))
		assert polars_df.schema['host'] == pl.Utf8
		if eager:
			polars_df = self.standardize_hosts_eager(polars_df)
		else:
			polars_df = self.standardize_hosts_lazy(polars_df)
		return polars_df

	def standardize_hosts_eager(self, polars_df):
		"""
		Checks for string matches in hosts column. This is "eager" in the sense that matches are checked even
		though non-nulls are not filled in, so you could use this to overwrite. Except that isn't implemented yet.

		Assumes polars_df has column 'host' but not 'host_scienname', 'host_confidence', nor 'host_commonname'

		We have to use "host_scienname" instead of "host_sciname" as there is already an sra column with that name.
		"""
		polars_df = polars_df.with_columns(host_scienname=None, host_confidence=None, host_commonname=None)
		assert polars_df.schema['host'] == pl.Utf8
		
		for host, (sciname, confidence, streetname) in host_species.species.items():
			polars_df = polars_df.with_columns([
				pl.when(pl.col('host').str.contains(host))
				.then(pl.lit(sciname))
				.otherwise(
					pl.when(pl.col('host_scienname').is_not_null())
					.then(pl.col('host_scienname'))
					.otherwise(None))
				.alias("host_scienname"),
				
				pl.when(pl.col('host').str.contains(host))
				.then(pl.lit(confidence))
				.otherwise(
					pl.when(pl.col('host_confidence').is_not_null())
					.then(pl.col('host_confidence'))
					.otherwise(None))
				.alias("host_confidence"),
				
				pl.when(pl.col('host').str.contains(host))
				.then(pl.lit(streetname))
				.otherwise(
					pl.when(pl.col('host_commonname').is_not_null())
					.then(pl.col('host_commonname'))
					.otherwise(None))
				.alias("host_commonname"),
			])

		for host, (sciname, confidence, streetname) in host_species.exact_match_only.items():
			polars_df = polars_df.with_columns([
				pl.when(pl.col('host') == host)
				.then(pl.lit(sciname))
				.otherwise(
					pl.when(pl.col('host_scienname').is_not_null())
					.then(pl.col('host_scienname'))
					.otherwise(None))
				.alias("host_scienname"),
				
				pl.when(pl.col('host') == host)
				.then(pl.lit(confidence))
				.otherwise(
					pl.when(pl.col('host_confidence').is_not_null())
					.then(pl.col('host_confidence'))
					.otherwise(None))
				.alias("host_confidence"),
				
				pl.when(pl.col('host') == host)
				.then(pl.lit(streetname))
				.otherwise(
					pl.when(pl.col('host_commonname').is_not_null())
					.then(pl.col('host_commonname'))
					.otherwise(None))
				.alias("host_commonname"),
			])
		return polars_df

	def standardize_hosts_lazy(self, polars_df):
		"""
		Does not perform string-matching in hosts column if the columns we're writing to are not null.
		Assumes polars_df has column 'host' but not 'host_sciname', 'host_confidence', nor 'host_commonname'

		TODO: This doesn't work properly, and is low-priority because the overwrite version, which should be hella
		inefficient, runs blazingly fast on a 258,411 row dataframe.
		"""
		polars_df = polars_df.with_columns(host_sciname=None, host_confidence=None, host_commonname=None)
		
		for host, (sciname, confidence, streetname) in host_species.species.items():
			polars_df = polars_df.with_columns([
				pl.when(pl.col('host').str.contains(host).and_(pl.col('host_sciname').is_null()))
				.then(pl.lit(sciname))
				.alias("host_sciname"),
				
				pl.when(pl.col('host').str.contains(host))
				.then(pl.lit(confidence))
				.otherwise(
					pl.when(pl.col('host_confidence').is_not_null())
					.then(pl.col('host_confidence'))
					.otherwise(None))
				.alias("host_confidence"),
				
				pl.when(pl.col('host').str.contains(host))
				.then(pl.lit(streetname))
				.otherwise(
					pl.when(pl.col('host_commonname').is_not_null())
					.then(pl.col('host_commonname'))
					.otherwise(None))
				.alias("host_commonname"),
			])
		return NeighLib.nullify(polars_df, 'host_commonname')

	def cleanup_dates(self, polars_df):
		"""
		Notes:
		* len_bytes() is way faster than len_chars()
		* yeah you can have mutliple expressions in one with_columns() but that'd require tons of alias columns so I'm not doing that
		"""

		polars_df = polars_df.with_columns([
			pl.when((pl.col('date_collected').str.len_bytes() == 9).and_(pl.col('date_collected').str.contains("/"))) # YYYY/YYYY
			.then(None)
			.otherwise(
					pl.when(pl.col('date_collected').is_not_null())
					.then(pl.col('date_collected'))
					.otherwise(None))
				.alias("date_collected"),
		])
		polars_df = polars_df.with_columns([
			pl.when((pl.col('date_collected').str.len_bytes() == 21).and_(pl.col('date_collected').str.contains("/"))) # 2016-07-01/2018-06-30
			.then(None)
			.otherwise(
					pl.when(pl.col('date_collected').is_not_null())
					.then(pl.col('date_collected'))
					.otherwise(None))
				.alias("date_collected"),
		])

		return NeighLib.nullify(polars_df, 'date_collected')

	def unmask_badgers(self, polars_df):
		"""
		TODO: this doesn't add a confidence score
		"""
		if 'anonymised_badger_id_sam' in polars_df.columns:
			polars_df = polars_df.with_columns([
				pl.when((pl.col('anonymised_badger_id_sam').is_not_null()) & (pl.col('host_commonname').is_null()))
				.then(pl.lit('badger'))
				.otherwise(pl.col('host_commonname'))
				.alias('host_commonname')
			])
		return polars_df

	# because polars runs with_columns() matches in parallel, this is probably the most effecient way to do this. but having four functions for it is ugly.
	def taxoncore_O(self, polars_df, match_string, i_organism, exact=False):
		if exact:
			polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element() == match_string).list.any())
				.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism')
			])
		else:
			polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
				.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism')
			])
		return polars_df
	
	def taxoncore_OS(self, polars_df, match_string, i_organism, i_strain):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_strain)).otherwise(pl.col('i_strain')).alias('i_strain')
		])
		return polars_df
	
	def taxoncore_OL(self, polars_df, match_string, i_organism, i_lineage):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_lineage)).otherwise(pl.col('i_lineage')).alias('i_lineage')
		])
		return polars_df
	
	def taxoncore_OLS(self, polars_df, match_string, i_organism, i_lineage, i_strain):
		polars_df = polars_df.with_columns([pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_organism)).otherwise(pl.col('i_organism')).alias('i_organism'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_lineage)).otherwise(pl.col('i_lineage')).alias('i_lineage'),pl.when(pl.col('taxoncore_list').list.eval(pl.element().str.contains(match_string)).list.any())
			.then(pl.lit(i_strain)).otherwise(pl.col('i_strain')).alias('i_strain')
		])
		return polars_df


	def only_stuff_probably_on_tree(self, polars_df, identifers=['tba4', 'tba3']):
		pass

	def sort_out_taxoncore_columns(self, polars_df):
		"""
		Some columns in polars_df will be in list all_taxoncore_columns. We want to use these taxoncore columns to create three new columns:
		* i_organism should be of form "Mycobacterium" plus one more word, with no trailing "subsp." or "variant", if a specific organism can be imputed from a taxoncore column, else null
		* i_lineage should be of form "L" followed by a float if a specific lineage can be imputed from a taxoncore column, else null
		* i_strain are strings if a specific strain can be imputed from a taxoncore column, else null

		Rules:
		* Any column with value "Mycobacterium tuberculosis H37Rv" sets i_organism to "Mycobacterium tuberculosis", i_lineage to "L4.8", and i_strain to "H37Rv"
		* Any column with value "Mycobacterium variant bovis" sets i_organism to "Mycobacterium bovis"
		* Any column with "lineage" followed by numbers sets i_lineage to "L" plus the numbers, minus any whitespace (there may be periods between the numbers, keep them)

		"""
		assert 'i_organism' not in polars_df.columns
		assert 'i_lineage' not in polars_df.columns
		assert 'i_strain' not in polars_df.columns
		assert 'taxoncore_list' not in polars_df.columns
		merge_these_columns = [col for col in polars_df.columns if col in sum(kolumns.merge__special_taxonomic_handling.values(), [])]

		for col in merge_these_columns:
			if polars_df.schema[col] == pl.List:
				polars_df = polars_df.with_columns(pl.col(col).list.join(", ").alias(col))
			assert polars_df.schema[col] == pl.Utf8
		# taxoncore_list used for most matches,
		# but to extract lineages with regex we also need a column without lists
		polars_df = polars_df.with_columns(pl.concat_list([pl.col(col) for col in merge_these_columns]).alias("taxoncore_list"))
		polars_df = polars_df.with_columns(pl.col("taxoncore_list").list.join("; ").alias("taxoncore_str"))
		for col in merge_these_columns:
			polars_df = polars_df.drop(col)
		polars_df = polars_df.with_columns(i_lineage=None, i_organism=None, i_strain=None)
		# we start off with ones that are less precise first, to allow overwriting later

		# attempt to copy over specific lineages using string column
		# TODO: adjust for africanuum

		polars_df = polars_df.with_columns([pl.when(
			pl.col('taxoncore_str').str.contains(r'\b[Ll][0-9]{1}(\.[0-9]{1})*')
			.and_(~pl.col('taxoncore_str').str.contains(r'\b[Ll][0-9]{2,}'))
			) 
			.then(pl.lit('Mycobacterium tuberculosis')).otherwise(pl.col('i_organism')).alias('i_organism'),
			pl.when(pl.col('taxoncore_str').str.contains(r'\bL[0-9]{1}(\.[0-9]{1})*')
				.and_(~pl.col('taxoncore_str').str.contains(r'\b[Ll][0-9]{2,}'))
			)
			#.then(pl.col('taxoncore_str')).otherwise(pl.col('i_lineage')).alias('i_lineage')])	
			.then(pl.col('taxoncore_str').str.extract(r'\bL[0-9](\.[0-9]{1})*', 0)).otherwise(pl.col('i_lineage')).alias('i_lineage')])

		NeighLib.print_value_counts(polars_df, ['i_lineage'])

		for 

		polars_df = self.taxoncore_O(polars_df, '(?i)Mycobacterium tuberuclosis',
			i_organism='Mycobacterium tubercuclosis!!!',
			exact=True)
		polars_df = self.taxoncore_O(polars_df, '(?i)Mycobacterium tubercuclosis',
			i_organism='Mycobacterium tubercuclosis!!',
			exact=True)
		polars_df = self.taxoncore_O(polars_df, '(?i)Mycobacterium tuberculosis',
			i_organism='Mycobacterium tubercuclosis!',
			exact=True)

		polars_df = self.taxoncore_OL(polars_df, '(?i)Harlem', # typo of Haarlem
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L4.1.2')

		polars_df = self.taxoncore_OS(polars_df, 'MtZ',
			i_organism='Mycobacterium tuberculosis',
			i_strain='Zaragoza')

		# Euro-American (L4)
		polars_df = self.taxoncore_OL(polars_df, '(?i)EuroAmerican',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L4')
		polars_df = self.taxoncore_OL(polars_df, '(?i)Euro-American',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L4')

		polars_df = self.taxoncore_O(polars_df, '(?i)canetti', # common typo that also matches the correct form
			i_organism='Mycobacterium canettii')
		polars_df = self.taxoncore_O(polars_df, '(?i)pinnipedi', # pinnipedi/pinnipedii
			i_organism='Mycobacterium pinnipedii')
		polars_df = self.taxoncore_O(polars_df, '(?i)mungi',
			i_organism='Mycobacterium mungi')
		polars_df = self.taxoncore_O(polars_df, '(?i)microti',
			i_organism='Mycobacterium microti')
		polars_df = self.taxoncore_OL(polars_df, '(?i)bovis',
			i_organism='Mycobacterium bovis',
			i_lineage='La1')
		polars_df = self.taxoncore_OL(polars_df, '(?i)caprae',
			i_organism='Mycobacterium caprae',
			i_lineage='La2')
		polars_df = self.taxoncore_OL(polars_df, '(?i)orygis',
			i_organism='Mycobacterium orygis',
			i_lineage='La3')
		polars_df = self.taxoncore_O(polars_df, '(?i)subsp. tuberculosis',
			i_organism='Mycobacterium tuberuclosis')

		# LAM (4.3)
		polars_df = self.taxoncore_OL(polars_df, 'LAM', # case-sensitive
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L4.3')

		# Beijing (L2.2.1)
		polars_df = self.taxoncore_OL(polars_df, r'\b2\.2\.1(\s|$)',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L2.2.1')
		polars_df = self.taxoncore_OL(polars_df, '(?i)Beijing',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L2.2.1')

		# mentions Bejing but isn't (processed *after* Beijing)
		polars_df = self.taxoncore_OL(polars_df, '(?i)Proto-Beijing',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L2')
		polars_df = self.taxoncore_OL(polars_df, '(?i)non-Beijing',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L2')

		# Manila
		polars_df = self.taxoncore_OLS(polars_df, '(?i)Manila',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L1',
			i_strain='Manila')
		polars_df = self.taxoncore_OLS(polars_df, '(?i)ST-19', # TODO: this might be lead to dodgy false matches for ST-199, etc
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L1',
			i_strain='Manila')
		polars_df = self.taxoncore_OLS(polars_df, '(?i)ST279',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L1',
			i_strain='Manila')

		polars_df = self.taxoncore_OLS(polars_df, '(?i)Ravenel',
			i_organism='Mycobacterium bovis',
			i_lineage='La1',
			i_strain='Ravenel')

		polars_df = self.taxoncore_OS(polars_df, 'Zaragoza',
			i_organism='Mycobacterium tuberculosis',
			i_strain='Zaragoza')


		polars_df = self.taxoncore_OS(polars_df, '(?i)CDC1551', # https://biocyc.org/organism-summary?object=MTBCDC1551
			i_organism='Mycobacterium tuberculosis',
			i_strain="Oshkosh")
		polars_df = self.taxoncore_OS(polars_df, '(?i)Oshkosh', # https://biocyc.org/organism-summary?object=MTBCDC1551
			i_organism='Mycobacterium tuberculosis',
			i_strain="Oshkosh")

		polars_df = self.taxoncore_OS(polars_df, '(?i)STB-K',
			i_organism='Mycobacterium canettii',
			i_strain='STB-K')
		polars_df = self.taxoncore_OS(polars_df, '(?i)Percy302',
			i_organism='Mycobacterium canettii',
			i_strain='STB-K')

		polars_df = self.taxoncore_OL(polars_df, '(?i)Haarlem',
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L4.1.2')

		#polars_df = self.taxoncore_OLS(polars_df, r'(?i)\bH37Rv\b(?!-like)', # H37Rv, but not "H37Rv-like"
		#	i_organism='Mycobacterium tuberculosis',
		#	i_lineage='L4',
		#	i_strain='H37Rv')

		polars_df = self.taxoncore_OL(polars_df, '(?i)africanum',
			i_organism='Mycobacterium africanum',
			i_lineage='L5/L6')

		polars_df = self.taxoncore_OLS(polars_df, 'BCG',
			i_organism='Mycobacterium bovis',
			i_lineage='La1.2',
			i_strain='BCG')

		polars_df = self.taxoncore_OL(polars_df, '(?i)Aethiop', # Nebenzahl-Guimaraes et al. proposed name for L7 is "Aethiop vetus" (PMC5320646)
			i_organism='Mycobacterium tuberculosis',
			i_lineage='L7')

		return polars_df

	def merge_organism_columns():
		pass

	def merge_strain_columns():
		pass

	def merge_lineage_columns_as_if_mtbc():
		pass

	def merge_lineage_columns_broadly():
		# excludes the organism column
		pass

	def standardize_countries():
		pass

	def standardize_TB_lineages(self,
		drop_non_standarized=True,
		guess_from_old_names=True,
		guess_from_ST=False,
		strains_of_note=True):
		"""
		drop_non_standarized: Stuff that cannot be standardized is turned into np.nan/null (if false, leave it untouched)
		guess_from_old_names: Guess lineage from older names, eg, assume "Beijing" means "L2.2.1"
		guess_from_ST: Assume "ST XXX" is SIT and use SITVIT2's dictionary to convert to a lineage
		strains_of_note: Maintain the names of notable strains such as Oshkosh, BCG, etc
		"""
		pass

	def standardize_sources(flag_hosts_and_locations=False): # default to true once things are working
		"""
		flag_hosts_and_locations: Additionally return a list of BioSamples and suspected hosts and locations. This
		can be helpful for situations where no host is listed, but the source says something like "human lung."
		"""
		pass