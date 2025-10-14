import polars as pl
from contextlib import suppress
import os
import csv
from tqdm import tqdm
from collections import OrderedDict # dictionaries are ordered in Python 3.7+, but OrderedDict has a better popitem() function we need
from .statics import kolumns, null_values
from .config import RancheroConfig  # should import the *class*

barformat = '{desc:<25.24}{percentage:3.0f}%|{bar:15}{r_bar}'

# https://peps.python.org/pep-0661/
_DEFAULT_TO_CONFIGURATION = object()

class FileReader():

	def __init__(self, configuration, naylib, professionals):
		if configuration is None:
			raise ValueError("No configuration was passed to FileReader class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger
			if self.logging.getEffectiveLevel() == 20:
				try:
					from tqdm import tqdm
					tqdm.pandas()
				except ImportError:
					self.logging.warning("Failed to import tqdm -- pandas operations will not show a progress bar")
			self.NeighLib = naylib
			self.Standardizer = professionals

	def _default_fallback(self, cfg_var, value):
		if value == _DEFAULT_TO_CONFIGURATION:
			return self.cfg.get_config(cfg_var)
		return value

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

	def polars_from_ncbi_run_selector(self,
		csv,
		drop_columns=list(),
		check_index=_DEFAULT_TO_CONFIGURATION,
		auto_rancheroize=_DEFAULT_TO_CONFIGURATION,
		auto_standardize=_DEFAULT_TO_CONFIGURATION):
		"""
		1. Read CSV
		2. Drop columns in drop_columns, if any
		3. Check index (optional)
		4. Rancheroize (optional)
		"""
		check_index = self._default_fallback("check_index", check_index)
		auto_rancheroize = self._default_fallback("auto_rancheroize", auto_rancheroize)
		auto_standardize = self._default_fallback("auto_standardize", auto_standardize)
		
		polars_df = pl.read_csv(csv)
		polars_df = polars_df.drop(drop_columns)
		if check_index: polars_df = self.NeighLib.check_index(polars_df, df_name=os.path.basename(csv))
		if auto_rancheroize: 
			polars_df = self.NeighLib.rancheroize_polars(polars_df)
			if auto_standardize:
				polars_df = self.Standardizer.standardize_everything(polars_df)	
		return polars_df

	def polars_from_tsv(self, tsv, delimiter='\t', drop_columns=list(), explode_upon=None,
		index=None,
		glob=True,
		list_columns=None,
		list_columns_are_internally_dquoted=False,
		auto_parse_dates=_DEFAULT_TO_CONFIGURATION, 
		auto_rancheroize=_DEFAULT_TO_CONFIGURATION, 
		auto_standardize=_DEFAULT_TO_CONFIGURATION, 
		check_index=_DEFAULT_TO_CONFIGURATION,      
		ignore_polars_read_errors=_DEFAULT_TO_CONFIGURATION,
		null_values=null_values.nulls_CSV):
		"""
		1. Read a TSV (or similar) and convert to Polars dataframe
		2. Drop columns in drop_columns, if any
		3. Explode the index (optional)
		4. Check index (optional)
		5. Rancheroize (optional)
		"""
		check_index = self._default_fallback("check_index", check_index)
		auto_rancheroize = self._default_fallback("auto_rancheroize", auto_rancheroize)
		auto_standardize = self._default_fallback("auto_standardize", auto_standardize)
		auto_parse_dates = self._default_fallback("auto_parse_dates", auto_parse_dates)
		ignore_polars_read_errors = self._default_fallback("ignore_polars_read_errors", ignore_polars_read_errors)

		df_name = os.path.basename(tsv)

		polars_df = pl.read_csv(tsv, separator=delimiter, try_parse_dates=auto_parse_dates, null_values=null_values, 
			ignore_errors=ignore_polars_read_errors, glob=glob)
		if len(drop_columns) != 0:
			polars_df = polars_df.drop(drop_columns)
			self.logging.info(f"Dropped {drop_columns} from {df_name}")

		self.logging.debug(f"{df_name} currently has these columns: {polars_df.columns}")

		if index is not None:
			polars_df = self.NeighLib.mark_index(polars_df, index)
			new_index = self.NeighLib.get_index(polars_df, index)
			self.logging.debug(f"Marked manually-input index {index} as {new_index}")
			index = new_index
		
		if list_columns is not None:

			if list_columns_are_internally_dquoted:
				for column in list_columns:
					polars_df = polars_df.with_columns(
						polars_df[column]
						.str.strip_chars("[]").str.replace_all('"', '', literal=True)
						.str.split(",")
						.alias(column)
					)

			else:
				# assumes list columns are internally single quoted
				for column in list_columns:
					polars_df = polars_df.with_columns(
						polars_df[column]
						.str.strip_chars("[]").str.replace_all("'", "", literal=True)
						.str.split(",")
						.alias(column)
					)

		if explode_upon != None:
			# TODO: this function call had column=self.NeighLib.get_index_column(polars_df, quiet=True) but I'm not sure why we
			# would want the quiet version, since it wouldn't return a str during error cases...
			polars_df = self.polars_explode_delimited_rows(polars_df, column=self.NeighLib.get_index(polars_df, guess=True), 
				delimiter=explode_upon, drop_new_non_unique=check_index)
		if auto_rancheroize:
			self.logging.info(f"Rancheroizing dataframe from {df_name}...")
			polars_df = self.NeighLib.rancheroize_polars(polars_df, input_index=index)
			if auto_standardize:
				self.logging.info(f"Standardizing dataframe from {df_name}...")
				polars_df = self.Standardizer.standardize_everything(polars_df)
		
		# run check index AFTER rancheroize so index name can be changed
		if check_index: polars_df = self.NeighLib.check_index(polars_df, df_name=os.path.basename(tsv))

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
		* More newlines = easier to navigate in text editors

		This also attempts to handle the additional weirdness of non-efetch XMLs from web-view SRA, but this isn't
		deeply tested. Use at your own risk!
		'''
		self.logging.info("Reformatting XML file...")
		out_file_path = f"{os.path.splitext(efetch_xml)[0]}_modified.xml"
		remove_lines = ['<!DOCTYPE EXPERIMENT_PACKAGE_SET>\n',
						'<?xml version="1.0" encoding="UTF-8"  ?>\n', # two spaces
						'<?xml version="1.0" encoding="UTF-8" ?>\n',  # one space
						'<EXPERIMENT_PACKAGE_SET>\n',
						'</EXPERIMENT_PACKAGE_SET>\n',
						'  </EXPERIMENT_PACKAGE_SET>\n',
						'  <EXPERIMENT_PACKAGE_SET>\n',
						'<?xml version="1.0" ?>\n' # seems to only exist in XMLs from web-view, not efetch
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
			if likely_one_line_per_experiment_package_set:
				for line in nice_lines:
					line = line.removesuffix('\n') # to make handling next removesuffix() easier
					line = line.removesuffix('</EXPERIMENT_PACKAGE_SET>')
					experiment_packages = line.split('<EXPERIMENT_PACKAGE>')
					for i, experiment in enumerate(experiment_packages):
						if experiment != '':
							out_file.write('<EXPERIMENT_PACKAGE>'+experiment+'\n')
			else:
				# there are multiple "experiment package sets", and any one of them may have
				# any number of "experiments"
				for line in nice_lines:
					experiment_packages = line.split('<EXPERIMENT_PACKAGE>')
					for i, experiment in enumerate(experiment_packages):
						if experiment != '': # the 0th value is usually an empty string
							out_file.write('<EXPERIMENT_PACKAGE>'+experiment+'\n')
			out_file.write('</EXPERIMENT_PACKAGE_SET>\n')
		self.logging.warning(f"Reformatted XML saved to {out_file_path}")
		return out_file_path

	def handle_run_accession_dictionary(self, run_accession_dictionary, BioSample):
		# This should probably only be called by from_efetch()
		SRR_id = run_accession_dictionary['@accession']
		alias = run_accession_dictionary['@alias']

		# these ones aren't always present, but if they are, might as well get their data
		try:
			total_spots = run_accession_dictionary['@total_spots']
		except KeyError:
			total_spots = None
		try:
			total_bases = run_accession_dictionary['@total_bases']
		except KeyError:
			total_bases = None
		try:
			# NOT ORIGINAL SUBMITTED FILE SIZE BYTES! CAN BE AN ORDER OF MAG SMALLER!
			archive_data_bytes = run_accession_dictionary['@size']
		except KeyError:
			archive_data_bytes = None

		submitted_files, submitted_file_sizes_bytes, submitted_file_sizes_gibi = list(), list(), list()
		for file_dict in run_accession_dictionary['SRAFiles']['SRAFile']:
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
		
		# if there is RUN_ATTRIBUTES, check its structure, then use it
		# NOTE: For some reason, all runs in efetch XMLs seem to have RUN_ATTRIBUTES, while only some runs
		# in an NCBI-web-view-derived XML will have them. I haven't tested this extensively though, maybe
		# eukaryotes will be Built Different as it were...
		if 'RUN_ATTRIBUTES' in run_accession_dictionary.keys():
			self.logging.debug(f"Found RUN_ATTRIBUTES for {SRR_id}")
			assert type(run_accession_dictionary['RUN_ATTRIBUTES']) == dict
			assert len(run_accession_dictionary['RUN_ATTRIBUTES']) == 1

			# data that comes from efetch will have structure ['RUN_SET']['RUN']['RUN_ATTRIBUTES']['RUN_ATTRIBUTE']
			# where that last one is a python list of key-value dictionaries, which seem analogous to the j_attr stuff
			# you get from BigQuery
			if type(run_accession_dictionary['RUN_ATTRIBUTES']['RUN_ATTRIBUTE']) == list: # of k-v dictionaries
				attributes = run_accession_dictionary['RUN_ATTRIBUTES']['RUN_ATTRIBUTE']
				normalized_attr = pl.json_normalize(attributes, max_level=1)
				pivoted = normalized_attr.transpose(header_name="VALUE", column_names="TAG")
				blessed_dataframe = pl.concat([pl.DataFrame(blessed_dictionary), pivoted], how='horizontal')

			# data that comes from web-SRA will eliminate the middleman somewhat and instead give you a
			# dictionary directly, rather than faffing about with a list of k-v dictionaries
			else:
				attributes = pl.DataFrame(run_accession_dictionary['RUN_ATTRIBUTES']['RUN_ATTRIBUTE'])
				blessed_dataframe = pl.concat([pl.DataFrame(blessed_dictionary), attributes], how='horizontal')
		else:
			self.logging.debug(f"No RUN_ATTRIBUTES found for {SRR_id} (this is fine but we may be missing some metadata)")
			blessed_dataframe = pl.DataFrame(blessed_dictionary)
		self.logging.debug(f"Processed {SRR_id} from {BioSample}")
		return blessed_dataframe

	def _cleanup_efetch_dictionary(self, xmltodict_dict, index_by_file, group_by_file, check_index, xml_name):
		"""
		Takes in the dictionary parsed by from_efetch() and makes it significantly less cursed.
		Regardless of whether or not we had to fix the XML file, cursed_dictionary kind of looks like this:
		
		 {"EXPERIMENT_PACKAGE_SET":
			{"EXPERIMENT_PACKAGE":
				[ # "list_of_experiments"
					{ # this an "actual experiment" dict, index[0] of list_of_experiments
						{'EXPERIMENT': dict with SRX ID, library layout and stategy, instrument, etc}
						{'SUBMISSION': dict including center_name, SUB ID, etc}
						{'Organization': dict with stuff about submitter}
						{'STUDY': dict with stuff about the BioProject}
						{'SAMPLE': dict with very basic stuff about BioSample}
						{'Pool': ignore this one, it's a multiplexing thing I think, sometimes it's missing}
						{'RUN_SET': 
							{
								'@runs': '1',
								'@bases': '10705886485',
								'@spots': '500788',
								'@bytes': '4550349867', 
								'RUN': dict(*) with keys:  @accession, @alias, @total_spots, @total_bases, @size,
														@load_done, @published, @is_public, @cluster_name,
		 												@has_taxanalysis, @static_data_available, IDENTIFIERS,
														EXPERIMENT_REF, RUN_ATTRIBUTES(**), pool, SRAFiles, CloudFiles,
														Statistics, Databases, Bases
							}
						}
					},
					{ # the next "actual experiment" dict, index[1] of list_of_experiments
						{'EXPERIMENT': as above }
						{'SUBMISSION': as above }
						{'Organization': as above }
						{'STUDY': as above }
						{'SAMPLE': as above }
						{'Pool': as above }
						{'RUN_SET': as above }
					}
				]
			}
		 }
		 CAVEATS:
		     (*)  RUN is sometimes a list of dictionaries if dealing with a multi-run BioSample
		     (**) RUN_ATTRIBUTES sometimes is missing if from web-view
		
		 This is, as the name implies, extremely cursed, so we'll try to make this make a bit more sense.
		 It appears that every "actual_experiment" contains one run accession and one BioSample. This means
		 we are basically indexed by run accession (SRR), and that BioSamples can be repeated.



		"""
		blessed_dataframes = list()
		run_attributes_present = None # TODO: make this a bool flag for preventing constant debug prints?
		cursed_dictionary = xmltodict_dict
		assert len(cursed_dictionary) == 1
		for EXPERIMENT_PACKAGE_SET, EXPERIMENT_PACKAGE in cursed_dictionary.items():
			assert EXPERIMENT_PACKAGE_SET == "EXPERIMENT_PACKAGE_SET"
			assert type(EXPERIMENT_PACKAGE) == dict
			assert list(EXPERIMENT_PACKAGE.keys()) == ["EXPERIMENT_PACKAGE"]
			for list_of_experiments in EXPERIMENT_PACKAGE.values():
				assert type(list_of_experiments) == list
				for actual_experiment in tqdm(list_of_experiments, desc=f"Processing {xml_name}'s 'experiments''", ascii='➖🌱🐄', bar_format=barformat):
					assert type(actual_experiment) == dict
					if len(actual_experiment) == 7 or len(actual_experiment) == 6: # whether or not "Pool" is present
						
						# TODO: DDBJ/ENA data probably have additional external IDs and will need special handling

						assert len(actual_experiment['RUN_SET']) == 5

						# External IDs sometimes fall into the common issue of "making a list of two-element dictionaries, each with
						# the same keys, when you could have just made one dictionary using the keys as ACTUAL KEYS." For some reason,
						# efetch XMLs seem to only put BioSamples here (so this is always just one dictionary) but web-view XMLS may
						# decide to put GEO accessions here, which turns actual_experiment['SAMPLE']['IDENTIFIERS']['EXTERNAL_ID'] into
						# a list of dictionaries.

						# {'@namespace': 'BioSample', '#text': 'SAMN18577972'}
						if type(actual_experiment['SAMPLE']['IDENTIFIERS']['EXTERNAL_ID']) == dict:
							BioSample = (actual_experiment['SAMPLE']['IDENTIFIERS']['EXTERNAL_ID']['#text'])
						
						# [{'@namespace': 'BioSample', '#text': 'SAMN18577972'}, {'@namespace': 'GEO', '#text': 'GSM5221531'}]
						elif type(actual_experiment['SAMPLE']['IDENTIFIERS']['EXTERNAL_ID']) == list:
							BioSample = "COULDNT_PARSE_BIOSAMPLE"
							for two_element_dictionary in actual_experiment['SAMPLE']['IDENTIFIERS']['EXTERNAL_ID']:
								if two_element_dictionary['@namespace'] == 'BioSample':
									BioSample = two_element_dictionary['#text']

						# should never happen
						else:
							self.logging.error("Couldn't parse external identifiers!")
							self.logging.error(f"Identifiers dictionary: {actual_experiment['SAMPLE']['IDENTIFIERS']}")
							self.logging.error(f"External identifers: {actual_experiment['SAMPLE']['IDENTIFIERS']['EXTERNAL_ID']}")
							raise TypeError

						# This seems to *always* be the case when dealing with XMLs from efetch, and when dealing with
						# one-run-accession-per-BioSample cases within XMLs from web-view
						if type(actual_experiment['RUN_SET']['RUN']) == dict:
							run_accession_dictionary = actual_experiment['RUN_SET']['RUN']
							blessed_dataframe = self.handle_run_accession_dictionary(run_accession_dictionary, BioSample)
							blessed_dataframes.append(blessed_dataframe)
						
						# This is a BioSample with multiple run accessions, each one getting its own dictionary. Why
						# this doesn't happen when dealing with multiple-run-accession-BioSamples with efetch XMLs, idk...
						# I assume they're "indexed" by run accession, and web-view is "indexed" by BioSample?
						elif type(actual_experiment['RUN_SET']['RUN']) == list:
							for run_accession_dictionary in actual_experiment['RUN_SET']['RUN']:
								blessed_dataframe = self.handle_run_accession_dictionary(run_accession_dictionary, BioSample)
								blessed_dataframes.append(blessed_dataframe)
						
						# This should never happen!
						else:
							self.logging.error("If this is public, open-access data from NCBI, please open a GitHub issue with this output:")
							self.logging.error(f"actual_experiment['RUN_SET']['RUN'] (type: {type(actual_experiment['RUN_SET']['RUN'])}")
							self.logging.error(actual_experiment['RUN_SET']['RUN'])
							raise TypeError
						
					else:
						self.logging.error("Expected 6 or 7 keys per experiment dictionary, found... not that")
						for thing in actual_experiment:
							self.logging.error(thing)
						exit(1)
		return pl.concat(blessed_dataframes, how='diagonal')


	def from_efetch(self, efetch_xml, index_by_file=False, group_by_file=True, check_index=_DEFAULT_TO_CONFIGURATION, rancheroize=_DEFAULT_TO_CONFIGURATION):
		"""
		1. Convert the output of efetch into an XML format that is actually correct (harder than you'd expect)
		2. Convert the resulting dictionary into a Polars dataframe that is actually useful (also hard)

		For #1 we have to account for at least three different ways edirect can spit out something:
		  a) Actually valid XML file
		  b) Several <EXPERIMENT_PACKAGE_SET>s, one of which is unmatched
		  c) Like b but there's also additional <?xml version="1.0" encoding="UTF-8"  ?> headers thrown in for fun
		"""
		rancheroize = self._default_fallback("rancheroize", rancheroize)
		check_index = self._default_fallback("check_index", check_index)

		import xml
		try:
			import xmltodict
		except ImportError:
			self.logging.error("This function requires xmltodict, but it doesn't appear to be installed")
		with open(efetch_xml, "r") as file:
			xml_content = file.read()
		
		try:
			cursed_dictionary = xmltodict.parse(xml_content)
			better_xml = None
		except xml.parsers.expat.ExpatError:
			better_xml = self.fix_efetch_file(efetch_xml)
			with open(better_xml, "r") as file:
				xml_content = file.read()
			cursed_dictionary = xmltodict.parse(xml_content)
		xml_name = os.path.basename(efetch_xml) if better_xml is None else os.path.basename(better_xml) # for logging
		blessed_dataframe = self._cleanup_efetch_dictionary(cursed_dictionary, index_by_file, group_by_file, check_index, xml_name)
		if self.logging.getEffectiveLevel() == 10:
			self.NeighLib.super_print_pl(blessed_dataframe.select(self.NeighLib.valid_cols(blessed_dataframe, ['SRR_id', 'BioSample', 'TAG', 'VALUE'])), "XML as converted")
		
		# TODO: add check for BioSample containing 'COULDNT_PARSE_BIOSAMPLE'
		
		blessed_dataframe = blessed_dataframe.rename({'BioSample': 'sample_id', 'SRR_id': 'run_id'})

		if index_by_file:
			blessed_dataframe = self.NeighLib.mark_index(blessed_dataframe.rename({'submitted_files': 'file'}), 'file')
			file_index = self.NeighLib.get_index(blessed_dataframe, guess=False)
			if group_by_file:
				blessed_dataframe = self.NeighLib.flatten_all_list_cols_as_much_as_possible(blessed_dataframe.group_by(file_index).agg(
						[c for c in blessed_dataframe.columns if c != file_index]
				), force_index=file_index)
		else:
			# TODO: sum() submitted_file_sizes
			blessed_dataframe = self.NeighLib.mark_index(blessed_dataframe.rename({'run_id': 'run'}), 'run')
			run_id = self.NeighLib.get_index(blessed_dataframe, guess=False)
			if blessed_dataframe.select(pl.col(run_id).n_unique() != pl.col(run_id).len()):
				self.logging.warning(f"Found non-unique values for {run_id} (SRR_id)")
			blessed_dataframe = self.NeighLib.flatten_all_list_cols_as_much_as_possible(blessed_dataframe.group_by(run_id).agg(
				[pl.col(col).unique().alias(col) for col in blessed_dataframe.columns if col != run_id]
			), force_index=run_id)
		if check_index: blessed_dataframe = self.NeighLib.check_index(blessed_dataframe, df_name=xml_name)
		if rancheroize: blessed_dataframe = self.NeighLib.rancheroize(rancheroize)
		return blessed_dataframe

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


	def polars_from_bigquery(self, bq_file, drop_columns=list(), normalize_attributes=True,
		auto_rancheroize=_DEFAULT_TO_CONFIGURATION, 
		auto_standardize=_DEFAULT_TO_CONFIGURATION): 
		""" 
		1. Reads a bigquery JSON into a polars dataframe
		2. (optional) Splits the attributes columns into new columns (combines fixing the attributes column and JSON normalizing)
		3. Rancheroize columns
		"""
		auto_rancheroize = self._default_fallback("auto_rancheroize", auto_rancheroize)
		auto_standardize = self._default_fallback("auto_standardize", auto_standardize)

		try:
			polars_df = pl.read_json(bq_file)
			self.logging.debug(f"{bq_file} has {polars_df.width} columns and {len(polars_df)} rows")
		except pl.exceptions.ComputeError:
			self.logging.warning("Caught exception reading JSON file. Attempting to reformat it...")
			try:
				polars_df = pl.read_json(self.fix_bigquery_file(bq_file))
				self.logging.debug(f"Fixed input file has {polars_df.width} columns and {len(polars_df)} rows")
			except pl.exceptions.ComputeError:
				self.logging.error("Caught exception reading JSON file after attempting to fix it. Giving up!")
				exit(1)
		polars_df = polars_df.drop(drop_columns)

		if normalize_attributes and "attributes" in polars_df.columns:  # if column doesn't exist, return false
			polars_df = self.polars_fix_attributes_and_json_normalize(polars_df, rancheroize=auto_rancheroize)
		if auto_rancheroize:
			if self.NeighLib.get_index(polars_df) == self.NeighLib.get_hypothetical_index_fullname('run_id'):
				# in case json_normalize also ran rancheroize
				# TODO: should we really allow rancheroize to run twice like that?
				polars_df = self.NeighLib.rancheroize_polars(polars_df)
			else:
				polars_df = self.NeighLib.rancheroize_polars(polars_df, input_index='acc')
		if auto_standardize:
			polars_df = self.Standardizer.standardize_everything(polars_df)
		return polars_df


	def polars_json_normalize(self, polars_df, pandas_attributes_series, rancheroize=_DEFAULT_TO_CONFIGURATION, collection_date_sam_workaround=True):
		"""
		polars_df: polars df to concat to at the end
		pandas_attributes_series: !!!pandas!!! series of dictionaries that will json normalized

		We do this seperately so we can avoid converting the entire dataframe in and out of pandas.
		"""
		rancheroize = self._default_fallback("auto_rancheroize", rancheroize)
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
		if rancheroize: bq_jnorm = self.NeighLib.rancheroize_polars(bq_jnorm)
		if self.cfg.intermediate_files: self.NeighLib.polars_to_tsv(bq_jnorm, f'./intermediate/normalized_pure_polars.tsv')
		return bq_jnorm

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

	def polars_explode_delimited_rows(self, polars_df, column="run_id", delimiter=";", drop_new_non_unique=True):
		"""
		column			some_other_column		
		"SRR123;SRR124"	12
		"SRR125"		555

		becomes

		column			some_other_column		
		"SRR123"		12
		"SRR124"		12
		"SRR125"		555
		"""
		self.logging.debug(f"Exploding on {column} with delimter {delimiter} (drop_new_non_unique={drop_new_non_unique})...")
		assert column in polars_df.columns
		exploded = (polars_df.with_columns(pl.col(column).str.split(delimiter)).explode(column)).unique()
		if len(polars_df) == len(polars_df.select(column).unique()) and len(exploded) != len(exploded.select(column).unique()) and drop_new_non_unique:
			self.logging.info(f"Exploding created non-unique values for the previously unique-only column {column}, so we'll be merging...")
			exploded = self.merge_row_duplicates(exploded, column)
			if len(exploded) != len(exploded.select(column).unique()): # probably should never happen
				self.logging.error("Attempted to merge duplicates caused by exploding, but it didn't work.")
				self.logging.error(f"Debug information: Exploded df has len {len(exploded)}, unique in {column} len {len(exploded.select(column).unique())}")
				raise ValueError
		else:
			# there aren't unique values to begin with so who cares lol (or exploding didn't make a difference)
			pass
		return exploded

	def run_to_sample_grouping_simple(self, polars_df, run_id, sample_id):
		grouped_df = (
			polars_df
			.group_by(sample_id)
			.agg([
				pl.concat_list(run_id).alias(run_id),
				*[pl.concat_list(col).alias(col) for col in non_index_columns]
			])
		)
		return grouped_df

	def run_to_sample_grouping_clever_method(self, polars_df, run_id, sample_id):
		"""
		At the cost of a slower initial process, this ultimately saves 10-20 seconds upon being flattened.
		"""
		self.logging.debug("Using some tricks...")
		non_index_columns = [col for col in polars_df.columns if col not in [run_id, sample_id]]
		listbusters, listmakers, listexisters = [], [], [col for col, dtype in polars_df.schema.items() if (isinstance(dtype, pl.List) and dtype.inner == pl.Utf8)]
		
		df_without_lists_of_string_columns = polars_df.select([
			pl.col(col) for col, dtype in polars_df.schema.items() 
			if not (isinstance(dtype, pl.List) and dtype.inner == pl.Utf8) # for some reason n_unique works on lists of integers
		])
		
		# get a dataframe that tells us the number of unique values with doing a group_by()
		df_agg_nunique = df_without_lists_of_string_columns.group_by(sample_id).n_unique()
		for other_column in non_index_columns:
			# if non-index column isn't already a list (of any type), but is in df_without_lists_of_string_columns:
			if polars_df.schema[other_column] is not pl.List and other_column in df_agg_nunique.columns:
				if ((df_agg_nunique.select(pl.col(other_column) == 1).to_series()).all()):
					listbusters.append(other_column)
				else:
					listmakers.append(other_column)
		self.logging.debug(f"Does not need to become a list: {listbusters}")
		self.logging.debug(f"Will become a list (but might be flattened later):")
		for col in listmakers:
			# this is helpful for detecting columns that end up getting wiped out
			self.logging.debug(f"--> {col}, which currently has mode and count of {self.NeighLib.get_most_common_non_null_and_its_counts(polars_df, col)}")
		self.logging.debug(f"Already a list: {listexisters}")

		grouped_df_ = (
			polars_df
			.group_by(sample_id)
			.agg([
				pl.concat_list(run_id).alias(run_id),
				*[
					(pl.first(col).alias(col) if col in listbusters else pl.concat_list(col).alias(col))
					for col in non_index_columns
				]
			])
		)

		return grouped_df_


	def run_to_sample_index(self, polars_df, current_run_id='__index__run_id', current_sample_id='sample_id',
		output_run_id="run_id", output_sample_id="__index__sample_id", skip_rancheroize=False, drop_bad_news=True):
		"""
		Flattens an input file using polar. This is designed to essentially turn run accession indexed dataframes
		into BioSample-indexed dataframes. This will typically create columns of type list.
		REQUIRES run index to be called "run_id" and sample index to be called "sample_id" exactly.

		run_id | sample_id | foo
		-------------------------------
		SRR123    | SAMN1        | bar
		SRR124    | SAMN1        | buzz
		SRR125    | SAMN2        | bizz
		SRR126    | SAMN3        | bar
					 ⬇️
		run_id       | sample_id | foo
		---------------------------------------
		[SRR123,SRR124] | SAMN1        | [bar, buzz]
		[SRR125]        | SAMN2        | [bizz]
		[SRR126]        | SAMN3        | [bar]
		"""
		self.logging.info("Converting from run-index to sample-index...")
		assert polars_df.filter(pl.col(current_run_id).is_duplicated()).shape[0] == 0 # handled by check_index
		assert polars_df.schema[current_sample_id] == pl.Utf8
		assert current_run_id in polars_df.columns
		assert current_sample_id in polars_df.columns
		run_id_will_temporarily_be = self.NeighLib.get_hypothetical_index_basename(current_run_id)
		samp_index_will_temporarily_be = self.NeighLib.get_hypothetical_index_fullname(current_sample_id)
		assert run_id_will_temporarily_be not in polars_df.columns
		assert samp_index_will_temporarily_be not in polars_df.columns
		assert output_run_id not in polars_df.columns
		assert output_sample_id not in polars_df.columns
		if current_sample_id != 'sample_index':
			assert 'sample_index' not in polars_df

		# check the run index AND the sample index, since both are currently strings
		# the check_index of current_sample_id does NOT overwrite the current df on purpose!
		# edit: there really isn't a benefit of checking the hypothetical sample index at this point since the main
		# things we gotta check for (dupes) will exist at this point
		self.logging.info("Checking index as it currently exists...")
		polars_df = self.NeighLib.check_index(polars_df, manual_index_column=current_run_id, allow_bad_name=True)
		#self.NeighLib.check_index(polars_df, manual_index_column=current_sample_id, allow_bad_name=True)

		if not skip_rancheroize:
			self.logging.info("Rancheroizing run-indexed dataframe first (skip this by setting skip_rancheroize)...")
			polars_df = self.NeighLib.rancheroize_polars(polars_df) # runs check_index() too, and converts __index__acc

		# try to reduce the number of lists being concatenated -- this does mean running group_by() twice
		version_with_nested_lists = self.run_to_sample_grouping_clever_method(polars_df, current_run_id, current_sample_id)
		version_with_nested_lists = self.NeighLib.mark_index(version_with_nested_lists, current_sample_id, rm_existing_index=True)

		# yes, we null lists of len zero TWICE
		polars_df = self.NeighLib.null_lists_of_len_zero(
			self.NeighLib.flatten_all_list_cols_as_much_as_possible(
				self.NeighLib.null_lists_of_len_zero(version_with_nested_lists)
			)
		)
		polars_df = self.NeighLib.check_index(polars_df)
		polars_df = polars_df.rename({run_id_will_temporarily_be: output_run_id, samp_index_will_temporarily_be:output_sample_id})
		polars_df = self.NeighLib.check_index(polars_df)
		return polars_df

	def polars_fix_attributes_and_json_normalize(self, polars_df,
		rancheroize=None,     # has default fallback
		auto_cast_types=None, # has default fallback
		keep_all_primary_search_and_host_info=True):
		"""
		Uses self.NeighLib.concat_dicts to turn the weird format of the attributes column into flat dictionaries,
		then do some JSON normalization to output a polars dataframe.

		1. Create a tempoary pandas dataframe
		2. .apply(self.NeighLib.concat_dicts) to the attributes column in the pandas df
		3. Run polars_json_normalize to add new columns to the polars dataframe
		4. If rancheroize: rename columns
		5. Polars will default to str type for new columns; if cast_types, cast the most common not-string folders to
		   more correct types (example: tax_id --> pl.Int32). Note that this will also run on existing columns that
		   were not added by polars_json_normalize()!
		6. if intermediate_files: Write to ./intermediate/flatdicts.tsv
		7. Return polars dataframe.

		Performance is very good on my largest datasets, but I'm interested in avoiding the panadas conversion if possible.

		Configurations used:
		* rancheroize (fallback)
		* cast_types (fallback)
		* intermediate_files (set)
		* verbose (set)
		"""
		self.logging.warning("Temporarily converting polars dataframe to pandas (this requires importing pandas which may add >10 seconds)")
		temp_pandas_df = polars_df.to_pandas()  # TODO: probably faster to just convert the attributes column
		cast_types = self._default_fallback('auto_cast_types', auto_cast_types)
		rancheroize = self._default_fallback('auto_rancheroize', rancheroize)
		if keep_all_primary_search_and_host_info:  # TODO: benchmark these two options
			if self.logging.getEffectiveLevel() == 10:
				self.logging.info("Concatenating dictionaries with Pandas...")
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(self.NeighLib.concat_dicts_with_shared_keys)
			else:
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(self.NeighLib.concat_dicts_with_shared_keys)
		else:
			if self.logging.getEffectiveLevel() == 10:
				self.logging.info("Concatenating dictionaries with Pandas...")
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].progress_apply(self.NeighLib.concat_dicts)
			else:
				temp_pandas_df['attributes'] = temp_pandas_df['attributes'].apply(self.NeighLib.concat_dicts)
		normalized = self.polars_json_normalize(polars_df, temp_pandas_df['attributes'], rancheroize=rancheroize)
		if rancheroize: normalized = self.NeighLib.rancheroize_polars(normalized)
		if cast_types: normalized = self.NeighLib.cast_politely(normalized)
		if self.cfg.intermediate_files: self.NeighLib.polars_to_tsv(normalized, f'./intermediate/flatdicts.tsv')
		return normalized
