import os
import csv
import json
import logging

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
		#import polars as pl
		#del stuff['unwanted']
		#print(pl.from_dict(stuff, strict=False))

	def read_config(self, config_file: str):
		raise ValueErrror("Reading configuration files currently isn't implemented!")

	def prepare_taxoncore_dictionary(self, tsv='./src/statics/taxoncore_v3.tsv'):
		if os.path.isfile(tsv):
			with open(tsv, 'r') as tsvfile:
				taxoncore_rules = []
				for row in csv.DictReader(tsvfile, delimiter='\t'):
					rule = {
						"when": row["when"],
						"strain": row["strain"],
						"lineage": row["lineage"],
						#"strain": None if row["strain"] == "None" else row["strain"],
						#"lineage": None if row["lineage"] == "None" else row["lineage"],
						"organism": row["organism"],
						"group": row["bacterial group"],
						"comment": row["comment"]
					}
					taxoncore_rules.append(rule)
			with open("./src/statics/generated_taxoncore_dictionary.py", "w") as outfile:
				outfile.write(f"# This file is automatically generated by config.py from {tsv}\n")
				json.dump(taxoncore_rules, outfile)
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
		self.auto_cast_types = True
		self.auto_parse_dates = True
		self.auto_rancheroize = True
		self.check_index = True
		self.force_SRR_ERR_DRR_run_index = True
		self.force_SAMN_SAME_SAMD_sample_index = True
		self.ignore_polars_read_errors = True
		self.indicator_column = "collection"
		self.intermediate_files = False
		self.loglevel = logging.DEBUG # DEBUG = 10, INFO = 20
		self.paired_illumina_only = False
		self.polars_normalize = True
		self.rm_dupes = True
		self.rm_phages = True
		self.taxoncore_ruleset = None  # updated by self.prepare_taxoncore_dictionary()
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
