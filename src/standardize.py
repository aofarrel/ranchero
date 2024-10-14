import src.statics.countries
import src.statics.host_species
import src.statics.sample_sources
import src.statics.tuberculosis_lineages

def merge_addlID_columns():
	pass

def merge_host_columns():
	pass

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

def standardize_dates():
	pass

def standardize_hosts():
	pass

def standardize_TB_lineages(
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