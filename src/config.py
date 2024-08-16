import polars as pl

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
		del stuff['unwanted']
		del stuff['keep_all_values_of_these_shared_keys']
		print(pl.from_dict(stuff, strict=False))

	def read_config(self, config_file: str):
		raise ValueErrror("Reading configuration files currently isn't implemented!")

	def _make_default_config(self):
		""" Creates a default configuration, called by __init__"""
		self.cast_types = True
		self.ignore_polars_read_errors = True
		self.immediate_biosample_merge = False
		self.immediate_rancheroize = True
		self.immediate_try_parse_dates = True
		self.intermediate_files = True
		self.keep_all_values_of_these_shared_keys = set(["primary_search"])
		self.polars_normalize = True
		self.verbose = True
		self.unwanted = {
			"assay_type": ['Tn-Seq', 'ChIP-Seq'],
			"platform": None
		}

	def __init__(self):
		""" Creates a fallback configuration if read_config() isn't run"""
		print("Starting config")
		self._make_default_config()
		self.print_config_dataframe()