import os
import csv
import json
import logging
import polars as pl
import tqdm

class RancheroConfig:

	def print_config(self):
		print("Configuration:")
		for keys, values in self.__dict__.items():
			if keys == "unwanted":
				# print the "unwanted" dictionary in a pretty way
				for keys, values in self.unwanted.items():
					print(f"Unwanted {keys}: {values}")
			else:
				print(f"{keys}: {values}")

	def print_config_dataframe(self):
		# does not save the dataframe, as we don't want to have to update this df constantly
		# TODO: this doesn't handle self.unwanted properly
		stuff = self.__dict__.copy()
		for keys in stuff:
			if keys == 'taxoncore_ruleset' and stuff['taxoncore_ruleset'] is not None:
				print(f"⋆ {keys}: Initialized with {len(stuff['taxoncore_ruleset'])} values")
			else:
				print(f"⋆ {keys}: {stuff[keys]}")

	def read_config(self, config_file: str):
		raise ValueErrror("Reading configuration files currently isn't implemented!")

	def prepare_taxoncore_dictionary(self, tsv='./src/statics/taxoncore_v3.tsv'):
		if os.path.isfile(tsv):
			with open(tsv, 'r') as tsvfile:
				taxoncore_rules = []
				for row in csv.DictReader(tsvfile, delimiter='\t'):
					rule = {
						"when": row["when"],
						#"strain": row["strain"],
						#"lineage": row["lineage"],
						"strain": pl.Null if row["strain"] == "None" else row["strain"],
						"lineage": pl.Null if row["lineage"] == "None" else row["lineage"],
						"organism": row["organism"],
						"group": row["bacterial group"],
						"comment": row["comment"]
					}
					taxoncore_rules.append(rule)
			#with open("./src/statics/generated_taxoncore_dictionary.py", "w") as outfile:
			#	outfile.write(f"# This file is automatically generated by config.py from {tsv}\n")
			#	json.dump(taxoncore_rules, outfile)
			return taxoncore_rules
		else:
			if os.path.isfile("./src/statics/generated_taxoncore_dictionary.py"):
				self.logger.warning("""Found a generated taxoncore dictionary, but not its source TSV.
					We can still use all standardize functions, but be aware the dictionary may be 'stale'.""")
				return taxoncore_rules
			else:
				self.logger.warning(f"""Found neither taxoncore TSV nor generated dictionary at {tsv} 
					(workdir: {os.getcwd()}). Certain functions will not work.""")
				return None
		

	def _make_default_config(self):
		""" Creates a default configuration, called by __init__"""

		# Automatically cast types when reading a file
		self.auto_cast_types = True

		# Automatically parse dates when reading a file
		self.auto_parse_dates = True

		# Automatically rancheroize dataframes upon file read
		self.auto_rancheroize = True

		# Automatically standardize dataframes upon file read (dataframe must be rancheroized)
		self.auto_standardize = True

		# When doing things that might modify the index, check it for integrity/lack of duplicates
		self.check_index = True

		# Values in run_index column must start with SRR, ERR, or DRR
		self.force_SRR_ERR_DRR_run_index = True

		# Values in sample_index column must start with SAMN, SAME, or SAMD
		self.force_SAMN_SAME_SAMD_sample_index = False

		# How to handle columns relating to host information found in "attrs" in BQ JSONs, which are much less useful
		# if they had been combined into a single column like we would locational data n stuff, but can add a ton of
		# columns with barely any filled-in values
		#   dictionary: Create a single 'host_info' column with a list(dict()) of key-value pairs
		#   drop: Drop them
		#   columns: Treat like anything else in attrs -- each key becomes its own column
		# What we consider to be "host information columns" is defined in kolumns.host_info
		self.host_info_behavior = 'drop'

		# Ignore polars read errors when parsing a file -- recommended to keep this as true
		self.ignore_polars_read_errors = True

		# Indicator column when merging dataframes
		self.indicator_column = 'collection'

		# Write intermediate files to the disk
		self.intermediate_files = False

		# Log level -- logging.DEBUG = 10, logging.INFO = 20, etc
		self.loglevel = logging.INFO

		# If 'platform' and 'layout' columns exist and have type pl.Utf8 (string), remove all samples that aren't
		# "PAIRED" for 'layout' and "ILLUMINA" for 'platform'
		self.paired_illumina_only = False

		# Try to (mostly) use polars when normalizing the dataframe
		self.polars_normalize = True

		# When checking the index, automatically remove rows that have duplicate values in that index
		# Note that if the dataframe is run-indexed, this will NOT remove duplicate sample_index values by design
		self.rm_dupes = True

		# Try to remove phages when standardizing taxonomic information
		self.rm_phages = True

		# Ruleset for standardizing taxonomic information -- updated by self.prepare_taxoncore_dictionary(),
		# so leave this as None here
		self.taxoncore_ruleset = None

		# When column equals key, filter out rows that have anything in that key's value list
		self.unwanted = {
			"assay_type": ['Tn-Seq', 'ChIP-Seq'],
			"platform": None
		}

	def _setup_logger(self):
		"""Sets up a logger instance"""
		if not logging.getLogger().hasHandlers(): # necessary to avoid different modules logging all over each other
			logger = logging.getLogger(__name__)
			logging.basicConfig(format='%(levelname)s:%(funcName)s:%(message)s', level=self.loglevel)
		return logger


	def __init__(self):
		""" Creates a fallback configuration if read_config() isn't run"""
		self._make_default_config()
		self.logger = self._setup_logger()
		self.taxoncore_ruleset = self.prepare_taxoncore_dictionary()
		self.print_config_dataframe()
