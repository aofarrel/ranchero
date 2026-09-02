import importlib.util
from typing import Any, Dict, List, Optional
from .statics import countries, drop_zone, host_disease, kolumns, null_values

class RancheroRuleset:
	def __init__(self, ruleset_folder: Optional[str] = None):
		self.countries: Dict[str, Any] = dict(getattr(countries, "COUNTRIES", {}))
		self.drop_zone:
		self.file_extensions:
		self.host_disease:
		self.host_species:
		self.HPRC_sample_ids:
		self.kolumns: Dict[str, Any] = dict(getattr(kolumns, "KOLUMNS", {}))
		self.null_values: List[str] = list(null_values.nulls_CSV)
		self.regions:
		self.sample_sources:
		self.taxoncore_ruleset = self.prepare_taxoncore_dictionary()


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
        
        