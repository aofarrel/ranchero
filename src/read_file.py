import pandas as pd
import polars as pl
from contextlib import suppress
import os
import csv
from collections import OrderedDict # dictionaries are ordered in Python 3.7+, but OrderedDict has a better popitem() function we need
from src.statics import kolumns, null_values
from .config import RancheroConfig
from . import _NeighLib as NeighLib
from . import _Standardizer as Standardizer

# my crummy implementation of https://peps.python.org/pep-0661/
globals().update({f"_cfg_{name}": object() for name in [
	"auto_cast_types", "auto_parse_dates", "auto_rancheroize", "auto_standardize",
	"check_index", "ignore_polars_read_errors", "indicator_column",
	"intermediate_files", "rm_dupes", "rm_not_pared_illumina"
]})
_SENTINEL_TO_CONFIG = {
	_cfg_rm_dupes: "rm_dupes",
	_cfg_auto_cast_types: "auto_cast_types",
	_cfg_auto_parse_dates: "auto_parse_dates",
	_cfg_auto_rancheroize: "auto_rancheroize",
	_cfg_auto_standardize: "auto_standardize",
	_cfg_check_index: "check_index",
	_cfg_ignore_polars_read_errors: "ignore_polars_read_errors",
	_cfg_intermediate_files: "intermediate_files",
	_cfg_indicator_column: "indicator_column",
	_cfg_rm_not_pared_illumina: "rm_not_pared_illumina",
}

class FileReader():

	def __init__(self, configuration: RancheroConfig = None):
		if configuration is None:
			raise ValueError("No configuration was passed to FileReader class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger
			if self.logging.getEffectiveLevel() == 10:
				try:
					from tqdm import tqdm
					tqdm.pandas()
				except ImportError:
					self.logging.warning("Failed to import tqdm -- pandas operations will not show a progress bar")

	def _sentinal_handler(self, arg):
		"""Handles "allow overriding config" variables in function calls"""
		if arg in _SENTINEL_TO_CONFIG:
			config_attr = _SENTINEL_TO_CONFIG[arg]
			check_me = getattr(self.cfg, config_attr)
			assert check_me != arg, f"Configuration for '{config_attr}' is invalid or uninitialized"
			return check_me
		else:
			return arg

	def read_metadata_injection(self, injection_file, delimiter='\t', drop_columns=[]):
		"""
		Creates a list of dictionaries for metadata injection. Metadata injection is designed to mutate an existing pl.Dataframe's data
		rather than just adding more rows onto the end. This function just reads the file; actual metadata injection is done in
		standardize.py()

		The first value acts as the "key" that will be matched upon. It's recommend to use BioProject, sample_id, or run_id for this.
		Metadata injection works best when you are running it on a dataframe that has already been cleaned up and standardized with
		Ranchero. You can use - (hyphen) to mark null values in your metadata injection TSV/CSV.
		"""
		dict_list = []
		with open(injection_file, mode='r') as file:
			reader = csv.DictReader(file, delimiter=delimiter)
			for row in reader:
				clean_row = OrderedDict((key, value) for key, value in row.items() if value != '-' and key not in drop_columns)
				dict_list.append(clean_row)
		return dict_list

	def polars_from_ncbi_run_selector(self, csv, drop_columns=list(), check_index=_cfg_check_index, auto_rancheroize=_cfg_auto_rancheroize):
		"""
		1. Read CSV
		2. Drop columns in drop_columns, if any
		3. Check index (optional)
		4. Rancheroize (optional)
		"""
		check_index = self._sentinal_handler(check_index)
		auto_rancheroize = self._sentinal_handler(auto_rancheroize)
		auto_standardize = self._sentinal_handler(auto_standardize)
		
		polars_df = pl.read_csv(csv)
		polars_df = polars_df.drop(drop_columns)
		if check_index: NeighLib.check_index(polars_df)
		if auto_rancheroize: 
			polars_df = NeighLib.rancheroize_polars(polars_df)
			if auto_standardize:
				polars_df = Standardizer.standardize_everything(polars_df)	
		return polars_df

	def polars_from_tsv(self, tsv, delimiter='\t', drop_columns=list(), explode_upon=None,
		index=None,
		glob=True,
		list_columns=None,
		auto_parse_dates=_cfg_auto_parse_dates,
		auto_rancheroize=_cfg_auto_rancheroize,
		auto_standardize=_cfg_auto_standardize,
		check_index=_cfg_check_index, 
		ignore_polars_read_errors=_cfg_ignore_polars_read_errors, 
		null_values=null_values.nulls_CSV):
		"""
		1. Read a TSV (or similar) and convert to Polars dataframe
		2. Drop columns in drop_columns, if any
		3. Explode the index (optional)
		4. Check index (optional)
		5. Rancheroize (optional)
		"""
		auto_rancheroize = self._sentinal_handler(auto_rancheroize)
		auto_parse_dates = self._sentinal_handler(auto_parse_dates)
		check_index = self._sentinal_handler(check_index)
		auto_standardize = self._sentinal_handler(auto_standardize)
		ignore_polars_read_errors = self._sentinal_handler(ignore_polars_read_errors)

		polars_df = pl.read_csv(tsv, separator=delimiter, try_parse_dates=auto_parse_dates, null_values=null_values, 
			ignore_errors=ignore_polars_read_errors, glob=glob)
		if len(drop_columns) != 0:
			polars_df = polars_df.drop(drop_columns)
			self.logging.info(f"Dropped {drop_columns}")
		
		if list_columns is not None:
			for column in list_columns:
				polars_df = polars_df.with_columns(
					polars_df[column]
					.str.strip_chars("[]").str.replace_all("'", "", literal=True)
					.str.split(",")
					.alias(column)
				)

		if explode_upon != None:
			polars_df = self.polars_explode_delimited_rows(polars_df, column=NeighLib.get_index_column(polars_df, quiet=True), 
				delimiter=explode_upon, drop_new_non_unique=check_index)
		if check_index: NeighLib.check_index(polars_df, manual_index_column=index)
		if auto_rancheroize: 
			self.logging.info("Rancheroizing dataframe from TSV...")
			print(index)
			polars_df = NeighLib.rancheroize_polars(polars_df, index=index)
			if auto_standardize:
				self.logging.info("Standardizing dataframe from TSV...")
				polars_df = Standardizer.standardize_everything(polars_df)
		return polars_df

	def fix_efetch_file(self, efetch_xml):
		'''
		Handles what appears to be the two most typical ways for efetch to crap itself:

		-------- CASE A --------
		<?xml version="1.0" encoding="UTF-8"  ?>
		<EXPERIMENT_PACKAGE_SET>
		<EXPERIMENT_PACKAGE><EXPERIMENT accession="SRX28844847">[...]</EXPERIMENT_PACKAGE></EXPERIMENT_PACKAGE_SET>
		<?xml version="1.0" encoding="UTF-8"  ?>
		<EXPERIMENT_PACKAGE_SET>
		<EXPERIMENT_PACKAGE><EXPERIMENT accession="SRX28704751">[...]</EXPERIMENT_PACKAGE></EXPERIMENT_PACKAGE_SET>

		Into this:
		<?xml version="1.0" encoding="UTF-8"  ?>
		<EXPERIMENT_PACKAGE_SET>
		<EXPERIMENT_PACKAGE><EXPERIMENT accession="SRX28844847">[...]</EXPERIMENT_PACKAGE>
		<EXPERIMENT_PACKAGE><EXPERIMENT accession="SRX28704751">[...]</EXPERIMENT_PACKAGE>
		</EXPERIMENT_PACKAGE_SET>

		-------- CASE B --------
		<?xml version="1.0" encoding="UTF-8" ?>
		<!DOCTYPE EXPERIMENT_PACKAGE_SET>
		<EXPERIMENT_PACKAGE_SET>
		  <EXPERIMENT_PACKAGE>
			[...]
		  </EXPERIMENT_PACKAGE>
		  <EXPERIMENT_PACKAGE_SET>
		    <EXPERIMENT_PACKAGE>
		
		Because:
		* The XML header shouldn't be repeated
		* Having multiple packages of multiple experiments really isn't helpful for our purposes
		* More newlines = easier to navigate in certain text editors
		'''
		out_file_path = f"{os.path.splitext(efetch_xml)[0]}_modified.xml"
		remove_lines = ['<!DOCTYPE EXPERIMENT_PACKAGE_SET>\n',
						'<?xml version="1.0" encoding="UTF-8"  ?>\n', # two spaces
						'<?xml version="1.0" encoding="UTF-8" ?>\n',  # one space
						'<EXPERIMENT_PACKAGE_SET>\n',
						'</EXPERIMENT_PACKAGE_SET>\n',
						'  </EXPERIMENT_PACKAGE_SET>\n',
						'  <EXPERIMENT_PACKAGE_SET>\n'
		]

		with open(efetch_xml, 'r') as in_file:
			lines = in_file.readlines()
		xml_headers = [line for line in lines if line.startswith("<?xml")]
		nice_lines = [line for line in lines if not line in remove_lines]
		
		if len(xml_headers) > 1:
			likely_one_line_per_experiment_package_set = True
		else:
			likely_one_line_per_experiment_package_set = False


		with open(out_file_path, 'w') as out_file:
			out_file.write('<?xml version="1.0" encoding="UTF-8"  ?>\n')
			if not likely_one_line_per_experiment_package_set:
				out_file.write('<!DOCTYPE EXPERIMENT_PACKAGE_SET>\n')
			out_file.write('<EXPERIMENT_PACKAGE_SET>\n')
			for line in nice_lines:
				if likely_one_line_per_experiment_package_set:
					line = line.removesuffix('\n') # to make handling next removesuffix() easier
					line = line.removesuffix('</EXPERIMENT_PACKAGE_SET>')
					experiment_packages = line.split('<EXPERIMENT_PACKAGE>')
					for i, experiment in enumerate(experiment_packages):
						if experiment != '':
							out_file.write('<EXPERIMENT_PACKAGE>'+experiment+'\n')
				else:
					out_file.write(line)
			out_file.write('</EXPERIMENT_PACKAGE_SET>\n')
		self.logging.warning(f"Reformatted XML saved to {out_file_path}")
		return out_file_path

	def from_efetch(self, efetch_xml, index_by_file=False, group_by_file=True, check_index=_cfg_check_index):
		"""
		1. Convert the output of efetch into an XML format that is actually correct (harder than you'd expect)
		2. Convert the resulting dictionary into a Polars dataframe that is actually useful (also hard)

		For #1 we have to account for at least three different ways edirect can spit out something:
		  a) Actually valid XML file
		  b) Several <EXPERIMENT_PACKAGE_SET>s, one of which is unmatched
		  c) Like b but there's also additional <?xml version="1.0" encoding="UTF-8"  ?> headers thrown in for fun
		 """
		import xml
		import xmltodict
		with open(efetch_xml, "r") as file:
			xml_content = file.read()
		
		try:
			cursed_dictionary = xmltodict.parse(xml_content)
		except xml.parsers.expat.ExpatError:
			better_xml = self.fix_efetch_file(efetch_xml)
			with open(better_xml, "r") as file:
				xml_content = file.read()
			cursed_dictionary = xmltodict.parse(xml_content)
		
		# Regardless of whether or not we had to fix the XML file, cursed_dictionary kind of looks like this:
		#
		# {"EXPERIMENT_PACKAGE_SET":
		# 	{"EXPERIMENT_PACKAGE":
		# 		[ # "list_of_experiments"
		#			{ # this an "actual experiment" dict, index[0] of list_of_experiments
		#	 			{'EXPERIMENT': dict with SRX ID, library layout and stategy, instrument, etc}
		#	 			{'SUBMISSION': dict including center_name, SUB ID, etc}
		#	 			{'Organization': dict with stuff about submitter}
		#	 			{'STUDY': dict with stuff about the BioProject}
		#	 			{'SAMPLE': dict with very basic stuff about BioSample}
		#	 			{'Pool': ignore this one, it's a multiplexing thing I think, sometimes it's missing}
		#	 			{'RUN_SET': 
		#	 				{
		#	 					'@runs': '1',
		#	 					'@bases': '10705886485',
		#	 					'@spots': '500788',
		#	 					'@bytes': '4550349867', 
		#	 					'RUN': dict with keys:  @accession, @alias, @total_spots, @total_bases, @size,
		#												@load_done, @published, @is_public, @cluster_name,
		#  												@has_taxanalysis, @static_data_available, IDENTIFIERS,
		# 												EXPERIMENT_REF, RUN_ATTRIBUTES, pool, SRAFiles, CloudFiles,
		# 												Statistics, Databases, Bases
		#	 				}
		#				}
		# 			},
		# 			{ # the next "actual experiment" dict, index[1] of list_of_experiments
		# 				{'EXPERIMENT': as above }
		# 				{'SUBMISSION': as above }
		# 				{'Organization': as above }
		# 				{'STUDY': as above }
		# 				{'SAMPLE': as above }
		# 				{'Pool': as above }
		# 				{'RUN_SET': as above }
		# 			}
		#		]
		# 	}
		# }
		# This is, as the name implies, extremely cursed, so we'll try to make this make a bit more sense.
		# It appears that every "actual_experiment" contains one run accession and one BioSample. This means
		# we are basically indexed by run accession (SRR), and that BioSamples can be repeated.
		blessed_dataframes = list()
		assert len(cursed_dictionary) == 1
		for EXPERIMENT_PACKAGE_SET, EXPERIMENT_PACKAGE in cursed_dictionary.items():
			assert EXPERIMENT_PACKAGE_SET == "EXPERIMENT_PACKAGE_SET"
			assert type(EXPERIMENT_PACKAGE) == dict
			assert list(EXPERIMENT_PACKAGE.keys()) == ["EXPERIMENT_PACKAGE"]
			for list_of_experiments in EXPERIMENT_PACKAGE.values():
				assert type(list_of_experiments) == list
				for actual_experiment in list_of_experiments:
					assert type(actual_experiment) == dict
					if len(actual_experiment) == 7 or len(actual_experiment) == 6: # whether or not "Pool" is present
						
						# TODO: DDBJ/ENA data probably have additional external IDs and will need special handling

						# checks for data structure
						assert len(actual_experiment['RUN_SET']) == 5
						assert type(actual_experiment['RUN_SET']['RUN']['RUN_ATTRIBUTES']) == dict
						assert len(actual_experiment['RUN_SET']['RUN']['RUN_ATTRIBUTES']) == 1
						assert type(actual_experiment['RUN_SET']['RUN']['RUN_ATTRIBUTES']['RUN_ATTRIBUTE']) == list # of k-v dictionaries

						# get information that's always there
						BioSample = (actual_experiment['SAMPLE']['IDENTIFIERS']['EXTERNAL_ID']['#text'])
						SRR_id = actual_experiment['RUN_SET']['RUN']['@accession']
						alias = actual_experiment['RUN_SET']['RUN']['@alias']
						
						# these ones aren't always present
						try:
							total_spots = actual_experiment['RUN_SET']['RUN']['@total_spots']
						except KeyError:
							total_spots = None
						try:
							total_bases = actual_experiment['RUN_SET']['RUN']['@total_bases']
						except KeyError:
							total_bases = None
						try:
							# NOT ORIGINAL SUBMITTED FILE SIZE BYTES! CAN BE AN ORDER OF MAG SMALLER!
							archive_data_bytes = actual_experiment['RUN_SET']['RUN']['@size']
						except KeyError:
							archive_data_bytes = None
						
						# these are analogous to (but may not be equivalent to) the j_attr stuff you get from BigQuery
						attributes = actual_experiment['RUN_SET']['RUN']['RUN_ATTRIBUTES']['RUN_ATTRIBUTE']
						normalized_attr = pl.json_normalize(attributes, max_level=1)
						pivoted = normalized_attr.transpose(header_name="VALUE", column_names="TAG")
						
						submitted_files, submitted_file_sizes_bytes, submitted_file_sizes_gibi = list(), list(), list()
						for file_dict in actual_experiment['RUN_SET']['RUN']['SRAFiles']['SRAFile']:
							filename = file_dict['@filename']
							file_bytes = int(file_dict['@size'])
							file_gibi = file_bytes / (1024 ** 3)
							if filename != SRR_id: # exclude the .srr file (which has no extension here for some reason)
								submitted_files.append(filename)
								submitted_file_sizes_bytes.append(file_bytes)
								submitted_file_sizes_gibi.append(file_gibi)
						
						blessed_dictionary = {
							'SRR_id': SRR_id,
							'BioSample': BioSample,
							'submitted_files': submitted_files,
							'submitted_files_bytes': submitted_file_sizes_bytes,
							'submitted_files_gibytes': submitted_file_sizes_gibi,
							'alias': alias, 
							'total_bases': total_bases,
							'archive_data_bytes': archive_data_bytes,
						}
						blessed_dataframe = pl.concat([pl.DataFrame(blessed_dictionary), pivoted], how='horizontal')
						blessed_dataframes.append(blessed_dataframe)
					else:
						self.logging.error("Expected 6 or 7 keys per experiment dictionary, found... not that")
						for thing in actual_experiment:
							self.logging.error(thing)
						exit(1)
		blessed_dataframe = pl.concat(blessed_dataframes, how='diagonal').rename({'SRR_id': 'run_index', 'BioSample': 'sample_index'})
		if index_by_file:
			if group_by_file:
				blessed_dataframe = NeighLib.flatten_all_list_cols_as_much_as_possible(blessed_dataframe.group_by("submitted_files").agg(
						[c for c in blessed_dataframe.columns if c != 'submitted_files']
				), force_index='submitted_files')
			if check_index: # must come AFTER the group_by option
				blessed_dataframe = NeighLib.check_index(blessed_dataframe, manual_index_column='submitted_files')
			return blessed_dataframe
		else:
			# TODO: sum() submitted_file_sizes
			return NeighLib.flatten_all_list_cols_as_much_as_possible(blessed_dataframe.group_by('run_index').agg(
				[pl.col(col).unique().alias(col) for col in blessed_dataframe.columns if col != 'run_index']
			))

	def fix_bigquery_file(self, bq_file):
		out_file_path = f"{os.path.basename(bq_file)}_modified.json"
		with open(bq_file, 'r') as in_file:
			lines = in_file.readlines()
		with open(out_file_path, 'w') as out_file:
			out_file.write("[\n")
			for i, line in enumerate(lines):
				if i < len(lines) - 1:
					out_file.write(line.strip() + ",\n")
				else:
					out_file.write(line.strip() + "\n")
			out_file.write("]\n")
		self.logging.warning(f"Reformatted JSON saved to {out_file_path}")
		return out_file_path


	def polars_from_bigquery(self, bq_file, drop_columns=list(), normalize_attributes=True):
		""" 
		1. Reads a bigquery JSON into a polars dataframe
		2. (optional) Splits the attributes columns into new columns (combines fixing the attributes column and JSON normalizing)
		3. Rancheroize columns
		"""
		try:
			polars_df = pl.read_json(bq_file)
			if self.logging.getEffectiveLevel() == 10: self.logging.debug(f"{bq_file} has {polars_df.width} columns and {len(polars_df)} rows")
		except pl.exceptions.ComputeError:
			self.logging.warning("Caught exception reading JSON file. Attempting to reformat it...")
			try:
				polars_df = pl.read_json(self.fix_bigquery_file(bq_file))
				if self.logging.getEffectiveLevel() == 10: self.logging.debug(f"Fixed input file has {polars_df.width} columns and {len(polars_df)} rows")
			except pl.exceptions.ComputeError:
				self.logging.error("Caught exception reading JSON file after attempting to fix it. Giving up!")
				exit(1)
		polars_df = polars_df.drop(drop_columns)

		if normalize_attributes and "attributes" in polars_df.columns:  # if column doesn't exist, return false
			polars_df = self.polars_fix_attributes_and_json_normalize(polars_df)
		return polars_df


	def polars_json_normalize(self, polars_df, pandas_attributes_series, rancheroize=False, collection_date_sam_workaround=True):
		"""
		polars_df: polars df to concat to at the end
		pandas_attributes_series: !!!pandas!!! series of dictionaries that will json normalized

		We do this seperately so we can avoid converting the entire dataframe in and out of pandas.
		"""
		attributes_rows = pandas_attributes_series.shape[0]
		assert polars_df.shape[0] == attributes_rows, f"Polars dataframe has {polars_df.shape[0]} rows, but the pandas_attributes has {attributes_rows} rows" 
		
		self.logging.info(f"Normalizing {attributes_rows} rows (this might take a while)...")
		just_attributes_df = pl.json_normalize(pandas_attributes_series, strict=False, max_level=1, infer_schema_length=100000)
		assert polars_df.shape[0] == just_attributes_df.shape[0], f"Polars dataframe has {polars_df.shape[0]} rows, but normalized attributes we want to horizontally combine it with has {just_attributes_df.shape[0]} rows" 

		if self.logging.getEffectiveLevel() == 10: self.logging.info("Concatenating to the original dataframe...")
		if collection_date_sam_workaround and 'collection_date_sam' in polars_df.columns:
			# when working in BQ data, polars_df already has a collection_date_sam which it converted to YYYY-MM-DD format. to avoid a merge conflict and to
			# fall back on the attributes version (which perserves dates that failed to YYYY-MM-DD convert), drop collection_date_sam
			# from polars_df before merging.
			bq_jnorm = pl.concat([polars_df.drop(['attributes', 'collection_date_sam']), just_attributes_df], how="horizontal")
		else:
			bq_jnorm = pl.concat([polars_df.drop('attributes'), just_attributes_df], how="horizontal")
		self.logging.info(f"An additional {len(just_attributes_df.columns)} columns were added from split 'attributes' column, for a total of {len(bq_jnorm.columns)}")
		if self.logging.getEffectiveLevel() == 10: self.logging.debug(f"Columns added: {just_attributes_df.columns}")
		if rancheroize: bq_jnorm = NeighLib.rancheroize_polars(bq_jnorm)
		if self.cfg.intermediate_files: NeighLib.polars_to_tsv(bq_jnorm, f'./intermediate/normalized_pure_polars.tsv')
		return bq_jnorm

	def polars_run_to_sample(self, polars_df, sample_index='sample_index', run_index='run_index'):
		"""Public wrapper for run_to_sample_index()"""
		return self.run_to_sample_index(polars_df, sample_index=sample_index, run_index='run_index')

	def get_not_unique_in_col(self, polars_df, column):
		return polars_df.filter(pl.col(column).is_duplicated())
		# polars_df.filter(pl.col(column).is_duplicated()).select(column).unique()

	def merge_row_duplicates(self, polars_df, column):
		'''SRR1196512, 4.8, null + SRR1196512, 4.8, South Africa --> SRR1196512, 4.8, South Africa'''
		polars_df = polars_df.sort(column)
		polars_df = polars_df.group_by(column).agg(
			[pl.col(col).forward_fill().last().alias(col) for col in polars_df.columns if col != column]
		)
		return polars_df

	def polars_explode_delimited_rows(self, polars_df, column="run_index", delimiter=";", drop_new_non_unique=True):
		"""
		column 			some_other_column		
		"SRR123;SRR124"	12
		"SRR125" 		555

		becomes

		column 			some_other_column		
		"SRR123"		12
		"SRR124"		12
		"SRR125" 		555
		"""
		exploded = (polars_df.with_columns(pl.col(column).str.split(delimiter)).explode(column)).unique()
		if len(polars_df) == len(polars_df.select(column).unique()) and len(exploded) != len(exploded.select(column).unique()) and drop_new_non_unique:
			self.logging.info(f"Exploding created non-unique values for the previously unique-only column {column}, so we'll be merging...")
			exploded = self.merge_row_duplicates(exploded, column)
			if len(exploded) != len(exploded.select(column).unique()): # probably should never happen
				self.logging.warning("Attempted to merge duplicates caused by exploding, but it didn't work.")
				self.logging.warning(f"Debug information: Exploded df has len {len(exploded)}, unique in {column} len {len(exploded.select(column).unique())}")
				raise ValueError
		else:
			# there aren't unique values to begin with so who cares lol (or exploding didn't make a difference)
			pass
		return exploded

	def run_to_sample_grouping_simple(self, polars_df, run_index, sample_index):
		grouped_df = (
			polars_df
			.group_by(sample_index)
			.agg([
				pl.concat_list(run_index).alias(run_index),
				*[pl.concat_list(col).alias(col) for col in non_index_columns]
			])
		)
		return grouped_df

	def run_to_sample_grouping_clever_method(self, polars_df, run_index, sample_index):
		"""
		At the cost of a slower initial process, this ultimately saves 10-20 seconds upon being flattened.
		"""
		self.logging.debug("Using some tricks...")
		non_index_columns = [col for col in polars_df.columns if col not in [run_index, sample_index]]
		listbusters, listmakers, listexisters = [], [], [col for col, dtype in polars_df.schema.items() if (isinstance(dtype, pl.List) and dtype.inner == pl.Utf8)]
		
		df_without_lists_of_string_columns = polars_df.select([
			pl.col(col) for col, dtype in polars_df.schema.items() 
			if not (isinstance(dtype, pl.List) and dtype.inner == pl.Utf8) # for some reason n_unique works on lists of integers
		])
		
		# get a dataframe that tells us the number of unique values with doing a group_by()
		df_agg_nunique = df_without_lists_of_string_columns.group_by(sample_index).n_unique()
		for other_column in non_index_columns:
			# if non-index column isn't already a list (of any type), but is in df_without_lists_of_string_columns:
			if polars_df.schema[other_column] is not pl.List and other_column in df_agg_nunique.columns:
				if ((df_agg_nunique.select(pl.col(other_column) == 1).to_series()).all()):
					listbusters.append(other_column)
				else:
					listmakers.append(other_column)
		self.logging.debug(f"Does not need to become a list: {listbusters}")
		self.logging.debug(f"Will become a list (but might be flattened later): {listmakers}")
		self.logging.debug(f"Already a list: {listexisters}")

		grouped_df_ = (
			polars_df
			.group_by(sample_index)
			.agg([
				pl.concat_list(run_index).alias(run_index),
				*[
					(pl.first(col).alias(col) if col in listbusters else pl.concat_list(col).alias(col))
					for col in non_index_columns
				]
			])
		)

		if self.logging.getEffectiveLevel() == 10:
			NeighLib.print_only_where_col_not_null(grouped_df_, 'collection')
			NeighLib.print_only_where_col_not_null(grouped_df_, 'primary_search')

		return grouped_df_


	def run_to_sample_index(self, polars_df, run_index='run_index', sample_index='sample_index', skip_rancheroize=False, drop_bad_news=True):
		"""
		Flattens an input file using polar. This is designed to essentially turn run accession indexed dataframes
		into BioSample-indexed dataframes. This will typically create columns of type list.
		REQUIRES run index to be called "run_index" and sample index to be called "sample_index" exactly.

		run_index | sample_index | foo
		-------------------------------
		SRR123    | SAMN1        | bar
		SRR124    | SAMN1        | buzz
		SRR125    | SAMN2        | bizz
		SRR126    | SAMN3        | bar
					 ⬇️
		run_index       | sample_index | foo
		---------------------------------------
		[SRR123,SRR124] | SAMN1        | [bar, buzz]
		[SRR125]        | SAMN2        | [bizz]
		[SRR126]        | SAMN3        | [bar]
		"""
		self.logging.info("Converting from run-index to sample-index...")
		NeighLib.check_index(polars_df, manual_index_column=run_index)
		assert sample_index in polars_df.columns
		assert run_index in polars_df.columns
		self.logging.debug(f"Sample index {sample_index} is in columns, and so is run index {run_index}")

		self.logging.debug("Before changing the dataframe, null counts in each column are as follows:")
		self.logging.debug(polars_df.null_count())
		duplicated_samples = polars_df.filter(pl.col(run_index).is_duplicated())
		if duplicated_samples.shape[0] > 0:
			if drop_bad_news:
				self.logging.warning(f"Found {duplicated_samples.shape[0]} duplicated run indeces in {run_index}. Dropping...")
				polars_df = polars_df.filter(~pl.col(run_index).is_duplicated())
			else:
				self.logging.error(f"""Found {duplicated_samples.shape[0]} duplicated run indeces in {run_index}.
					To drop these in-place instead of erroring, set drop_bad_news to True. Here's some of those dupes:""")
				NeighLib.super_print_pl(duplicated_samples)
				exit(1)
		else:
			self.logging.debug("Did not find any duplicates in the run_index column")
		

		self.logging.debug("After duplicated sample check, null counts in each column are as follows:")
		self.logging.debug(polars_df.null_count())

		polars_df = NeighLib.check_index(polars_df, manual_index_column=run_index)
		self.logging.debug("Ran check_index as a double-check and saved return to polars_df")

		self.logging.debug("After check_index(), null counts in each column are as follows:")
		self.logging.debug(polars_df.null_count())
		nulls_in_sample_index = polars_df.with_columns(pl.when(
			pl.col(sample_index).is_null())
			.then(pl.col(run_index))
			.otherwise(None)
			.alias(f"{run_index} with null {sample_index}")).drop_nulls(subset=f"{run_index} with null {sample_index}")
		if nulls_in_sample_index.shape[0] > 0:
			if drop_bad_news:
				self.logging.warning(f"Found {nulls_in_sample_index.shape[0]} rows with null values for {sample_index}. Dropping...")
				polars_df = polars_df.drop_nulls(subset=sample_index)
			else:
				self.logging.error(f"""Found {nulls_in_sample_index.shape[0]} rows with null values for {sample_index}.
					To drop these in-place instead of erroring, set drop_bad_news to True. Here's {run_index} where {sample_index} is null:""")
				NeighLib.super_print_pl(nulls_in_sample_index)
				exit(1)
		self.logging.debug("After handling of nulls in sample index, null counts in each column are as follows:")
		self.logging.debug(polars_df.null_count())

		if not skip_rancheroize:
			self.logging.debug("Rancheroizing run-indexed dataframe")
			polars_df = NeighLib.rancheroize_polars(polars_df) # runs check_index()
		else:
			NeighLib.check_index(polars_df) # it's your last chance to find non-SRR/ERR/DRR run indeces

		# try to reduce the number of lists being concatenated -- this does mean running group_by() twice
		polars_df = NeighLib.null_lists_of_len_zero(
			NeighLib.flatten_all_list_cols_as_much_as_possible(
				self.run_to_sample_grouping_clever_method(polars_df, run_index, sample_index)
			)
		)
		duplicated_samples = polars_df.filter(pl.col(sample_index).is_duplicated())
		if duplicated_samples.shape[0] > 0:
			if drop_bad_news:
				self.logging.warning(f"Found {duplicated_samples.shape[0]} duplicated sample indeces in {sample_index}. Dropping...")
				polars_df = polars_df.filter(~pl.col(sample_index).is_duplicated())
			else:
				self.logging.error(f"""Found {duplicated_samples.shape[0]} duplicated sample indeces in {sample_index}.
					To drop these in-place instead of erroring, set drop_bad_news to True. Here's {run_index} where {sample_index} is null:""")
				NeighLib.super_print_pl(duplicated_samples)
				exit(1)
		return polars_df

	def polars_fix_attributes_and_json_normalize(self, polars_df, rancheroize=False, keep_all_primary_search_and_host_info=True):
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
		6. if intermediate_files: Write to ./intermediate/flatdicts.tsv
		7. Return polars dataframe.

		Performance is very good on my largest datasets, but I'm interested in avoiding the panadas conversion if possible.

		Configurations used:
		* cast_types (set)
		* intermediate_files (set)
		* verbose (set)
		"""
		temp_pandas_df = polars_df.to_pandas()  # TODO: probably faster to just convert the attributes column
		if keep_all_primary_search_and_host_info:  # TODO: benchmark these two options
			if self.logging.getEffectiveLevel() == 10:
				self.logging.info("Concatenating dictionaries with Pandas...")
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(NeighLib.concat_dicts_with_shared_keys)
			else:
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(NeighLib.concat_dicts_with_shared_keys)
		else:
			if self.logging.getEffectiveLevel() == 10:
				self.logging.info("Concatenating dictionaries with Pandas...")
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(NeighLib.concat_dicts)
			else:
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(NeighLib.concat_dicts)
		normalized = self.polars_json_normalize(polars_df, temp_pandas_df['attributes'])
		if rancheroize: normalized = NeighLib.rancheroize_polars(normalized)
		if self.cfg.auto_cast_types: normalized = NeighLib.cast_politely(normalized)
		if self.cfg.intermediate_files: NeighLib.polars_to_tsv(normalized, f'./intermediate/flatdicts.tsv')
		return normalized
