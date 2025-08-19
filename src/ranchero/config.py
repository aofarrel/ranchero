import os
import csv
import sys
import logging
import polars as pl
import tqdm
import yaml
from typing import Literal, TypedDict
import importlib.resources as resources
loggerhead = {10: "DEBUG", 20: "INFO", 30: "WARN", 40: "ERROR"}

# This takes a lot of inspiration from how polars handles configuration,
# but I'm still not happy with it (nor does it actually enforce types).

if sys.version_info >= (3, 10):
	from typing import TypeAlias
else:
	from typing_extensions import TypeAlias

# valid options for dupe_index_handling
DupeIndexOptions: TypeAlias = Literal[
	"error",
	"verbose_error", 
	"warn",
	"verbose_warn",
	"silent",
	"allow",
	"dropall",
	"keep_most_data" # be aware this sorts the dataframe
]

# valid options for host_info_handling
HostInfoOptions: TypeAlias = Literal[
	"dictionary",
	"drop",
	"options"
]

class ConfigParameters(TypedDict):
	#read_file: dict
	auto_cast_types: bool
	auto_parse_dates: bool
	auto_rancheroize: bool
	auto_standardize: bool
	ignore_polars_read_errors: bool
	check_index: bool
	dupe_index_handling: DupeIndexOptions
	force_INSDC_runs: bool
	force_INSDC_samples: bool
	host_info_handling: HostInfoOptions
	indicator_column: str
	intermediate_files: bool
	loglevel: int
	mycobacterial_mode: bool
	paired_illumina_only: bool
	polars_normalize: bool
	rm_phages: bool
	unwanted: bool

	# not sure if this is how I want to handle this...
	taxoncore_ruleset: None

ConfigParametersList = list(ConfigParameters.__annotations__)

class ReadFileParameters(TypedDict):
	auto_cast_types: bool
	auto_parse_dates: bool
	auto_rancheroize: bool
	auto_standardize: bool
	ignore_polars_read_errors: bool

class RancheroConfig:

	# this isn't a replacement for proper type checking
	def is_in_ReadFileParameters(self, key) -> bool:
		return key in ReadFileParameters.__annotations__
	def is_in_ConfigParameters(self, key) -> bool:
		return key in ConfigParameters.__annotations__

	def print_config_raw(self) -> None:
		print(self.__dict__)

	def print_config(self) -> None:
		this_config = self.__dict__.copy()
		print("Configuration:")
		for keys, values in this_config.items():
			if keys == "unwanted":
				for keys, values in self.unwanted.items():
					print(f"* Unwanted {keys}: {values}")
			elif keys == 'read_file':
				print("File read options:")
				for k, v in self.read_file.items():
					print(f"--> {k}: {v}")
			elif keys == 'taxoncore_ruleset' and this_config['taxoncore_ruleset'] is not None:
				print(f"* {keys}: Initialized with {len(this_config['taxoncore_ruleset'])} values")
			elif keys == 'loglevel':
				print(f"* {keys}: {values} ({loggerhead[values]})")
			elif keys == 'logger': # redundant
				pass
			else:
				print(f"* {keys}: {values}")

	def read_config(self, path=None) -> ConfigParameters:
		# Just reads the file, doesn't actually set anything in and of itself
		if path is None:
			with resources.files(__package__).joinpath("config.yaml").open('r') as file:
				config = yaml.safe_load(file)
		else:
			with open(path, 'r') as file:
				config = yaml.safe_load(file)
		typed_config: ConfigParameters = config # doesn't enforce typing in and of itself
		for keys in typed_config:
			assert self.is_in_ConfigParameters(keys)
		return typed_config

	def get_config(self, option) -> str:
		if not hasattr(self, option):
			raise ValueError(f"Option {option!r} doesn't exist")
		else:
			return getattr(self, option)

	def initialize_config(self, overrides) -> None:
		for option, value in overrides.items():
			global ConfigParametersIterable
			if option not in ConfigParametersList:
				raise ValueError(f"Config initialized with option {option!r} but that doesn't seem valid? Valid parameters: {ConfigParametersList}")
			setattr(self, option, value)

	def set_config(self, overrides) -> None:
		for option, value in overrides.items():
			if not hasattr(self, option):
				raise ValueError(f"Option {option!r} doesn't exist")
			setattr(self, option, value)
			if option == 'loglevel':
				# destroy the old logger, make a new one
				logging.getLogger().handlers.clear()
				self.logger = self._setup_logger()

	def prepare_taxoncore_dictionary(self, tsv=None):
		if tsv is None:
			tsv_path = resources.files(__package__).joinpath(
				"statics/taxoncore_v4.tsv"
			)
		else:
			tsv_path = tsv

		with open(tsv_path, 'r') as tsvfile:
			taxoncore_rules = []
			for row in csv.DictReader(tsvfile, delimiter='\t'):
				rule = {
					"when": row["when"],
					"strain": pl.Null if row["strain"] == "None" else row["strain"],
					"lineage": pl.Null if row["lineage"] == "None" else row["lineage"],
					"organism": row["organism"],
					"group": row["bacterial group"],
					"comment": row["comment"]
				}
				taxoncore_rules.append(rule)
		return taxoncore_rules

	def _setup_logger(self) -> logging.Logger:
		"""Sets up a logger instance"""
		if not logging.getLogger().hasHandlers(): # necessary to avoid different modules logging all over each other
			logger = logging.getLogger(__name__)
			logging.basicConfig(format='%(levelname)s:%(funcName)s:%(message)s', level=self.loglevel)
		return logger

	#def _setup_tqdm(self):
	#	""" Sets up a TQDM instance"""
	#	tqdm.pandas(ascii='➖🌱🐄', bar_format='{desc:<10.9}{percentage:3.0f}%|{bar:12}{r_bar}') # we gotta make it cute!


	def __init__(self):
		""" Creates a fallback configuration if read_config() isn't run"""
		defaults = self.read_config()
		self.initialize_config(defaults)
		self.logger = self._setup_logger()
		self.taxoncore_ruleset = self.prepare_taxoncore_dictionary()
		#self.print_config_raw()
