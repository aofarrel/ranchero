import os
import csv
import sys
import logging
import polars as pl
import tqdm
import yaml
from typing import TypedDict, TypeAlias, Literal, get_args, get_origin, Union
import types as _types
import importlib.resources as resources
loggerhead = {10: "DEBUG", 20: "INFO", 30: "WARN", 40: "ERROR"}

# currently unused
class ReadFileParameters(TypedDict):
	auto_cast_types: bool
	auto_parse_dates: bool
	auto_rancheroize: bool
	auto_standardize: bool
	ignore_polars_read_errors: bool

# valid options for gs_metadata (any combination of these)
# as of Google Cloud SDK 535.0.0
GSMetadataOption: TypeAlias = Literal[
	"bucket",
	"content_type",
	"crc32c_hash",
	"creation_time",
	"etag",
	"generation",
	"md5_hash",
	"metageneration",
	"name",
	"size",
	"storage_class",
	"storage_class_update_time",
	"storage_url",
	"update_time"
]
GSMetadataOptions: TypeAlias = list[GSMetadataOption]

# valid options for quote_style, basically polars' CsvQuoteStyle (only one allowed)
CsvQuoteStyleOptions: TypeAlias = Literal [
	"necessary",
	"always",
	"non_numeric",
	"never"
]

# valid options for dupe_index_handling (only one allowed)
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

# valid options for host_info_handling (only one allowed)
HostInfoOptions: TypeAlias = Literal[
	"dictionary",
	"drop",
	"options"
]

# valid options for list_bracket_style (only one allowed)
ListBracketStyleOptions: TypeAlias = Literal[
	"always",
	"len_gt_one"
]

class ConfigParameters(TypedDict):
	auto_cast_types: bool
	auto_parse_dates: bool
	auto_rancheroize: bool
	auto_standardize: bool
	ignore_polars_read_errors: bool
	check_index: bool
	dupe_index_handling: DupeIndexOptions
	force_INSDC_runs: bool
	force_INSDC_samples: bool
	gs_metadata: GSMetadataOptions
	host_info_handling: HostInfoOptions
	indicator_column: str
	intermediate_files: bool
	list_bracket_style: ListBracketStyleOptions
	loglevel: int
	mycobacterial_mode: bool
	paired_illumina_only: bool
	polars_normalize: bool
	quote_style: CsvQuoteStyleOptions
	rm_phages: bool
	taxoncore_ruleset: None | str # not sure I like this...
	unwanted: dict

def _validate_against_annotation(option: str, value, expected_type) -> None:
	"""
	Raises TypeError/ValueError if value doesn't conform to expected_type.
	TODO: This seems really, really slow?
	"""
	origin = get_origin(expected_type)
	args = get_args(expected_type)

	# Literal["a","b",...] -- only one option in list of valids is allowed (dupe_index_handling, host_info_handling)
	if origin is Literal:
		allowed = args
		if value not in allowed:
			raise ValueError(
				f"Invalid value {value!r} for {option}. Must be one of {sorted(allowed)}"
			)
		return

	# list[Literal[...]] -- multiple options in list of valids is allowed (gs_metadata)
	if origin is list:
		if not isinstance(value, list):
			raise TypeError(f"{option} must be a list, got {type(value).__name__}")
		if args:
			item_ann = args[0]
			item_origin = get_origin(item_ann)
			if item_origin is Literal:
				allowed = set(get_args(item_ann))
				invalid = [v for v in value if v not in allowed]
				if invalid:
					raise ValueError(
						f"Invalid values {invalid} for {option}. "
						f"Valid options: {sorted(allowed)}"
					)
			else:
				# list of plain types: list[int], list[str], etc.
				item_real = item_origin or item_ann
				if not isinstance(item_real, type):
					raise TypeError(
						f"Unsupported item annotation for {option}: {item_ann!r}"
					)
				for v in value:
					if not isinstance(v, item_real):
						raise TypeError(
							f"{option} items must be {item_real}, got {type(v)}"
						)
		return

	# Union[...] or X | Y -- multiple allowed types (taxoncore_ruleset)
	if origin in (Union, getattr(_types, "UnionType", Union)):
		last_exc = None
		for arm in args:
			try:
				_validate_against_annotation(option, value, arm)
				return
			except (TypeError, ValueError) as e:
				last_exc = e
		raise TypeError(
			f"Value for {option!r} did not match any allowed types {args}: {last_exc}"
		)

	# various iterations of "nope"
	if expected_type in (type(None), None, pl.Null, getattr(_types, "NoneType", type(None))):
		if value is not None:
			raise TypeError(f"{option} must be None, got {type(value).__name__}")
		return

	# normal python classes
	if isinstance(expected_type, type):
		if expected_type is int and isinstance(value, bool):
			# bool is a subclass of int so this prevents weirdness
			raise TypeError(f"{option} must be int, got bool")
		if not isinstance(value, expected_type):
			raise TypeError(
				f"Invalid type for {option}: expected {expected_type}, got {type(value)}"
			)
		return

	# probably not worth doing this but maybe someday
	#if origin and origin.__qualname__.endswith("Annotated"):
	#	inner = args[0]
	#	_validate_against_annotation(option, value, inner)
	#	return

	raise TypeError(
		f"Unsupported type annotation for {option}: {expected_type!r}"
	)

class RancheroConfig:

	def __init__(self):
		defaults = self.read_config()
		self._check_and_set_parameters(defaults)
		self.logger = self._setup_logger()
		self.taxoncore_ruleset = self.prepare_taxoncore_dictionary()

	def set_config(self, parameter_dictionary) -> None:
		"""
		For updating at least one option in an EXISTING instance. Special handling for logger.
		"""
		self._check_and_set_parameters(parameter_dictionary)
		if "loglevel" in parameter_dictionary.keys():
			# destroy the old logger, make a new one
			logging.getLogger().handlers.clear()
			self.logger = self._setup_logger()

	def _check_and_set_parameters(self, parameters) -> None:
		"""
		Checks and sets parameters. Does not set the logger nor taxoncore dict, so should not be called
		externally.
		"""
		for option, value in parameters.items():
			self.check_is_in_ConfigParameters(option)
			expected_type = ConfigParameters.__annotations__[option]
			#print(f"{option}: {value} ({expected_type})")
			_validate_against_annotation(option, value, expected_type)
			setattr(self, option, value)

	def check_is_in_ConfigParameters(self, key) -> None:
		if key in ConfigParameters.__annotations__:
			return
		raise ValueError(
			f"Config passed parameter {key!r} but that doesn't seem valid? Valid parameters: {ConfigParameters.__annotations__}"
		)

	def read_config(self, path=None) -> ConfigParameters:
		# Just reads the file, doesn't actually set anything in and of itself
		if path is None:
			with resources.files(__package__).joinpath("config.yaml").open('r') as file:
				config = yaml.safe_load(file)
		else:
			with open(path, 'r') as file:
				config = yaml.safe_load(file)
		typed_config: ConfigParameters = config # doesn't enforce typing in and of itself
		return typed_config

	def get_config(self, option) -> str:
		if not hasattr(self, option):
			raise ValueError(f"Option {option!r} doesn't exist")
		else:
			return getattr(self, option)
			
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

	def print_config_raw(self) -> None:
		print(self.__dict__)

	def print_config(self) -> None:
		this_config = self.__dict__.copy()
		print("Configuration:")
		for keys, values in this_config.items():
			if keys == "unwanted":
				for keys, values in self.unwanted.items():
					print(f"* Unwanted {keys}: {values}")
			elif keys == 'read_file': # TODO: bring this back?
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
				print(f"* {keys}: {values} {type(values)}")
