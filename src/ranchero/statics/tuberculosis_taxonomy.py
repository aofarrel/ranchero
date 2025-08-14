# All information here correctly represents the general consensus of tuberculosis classification
# as far as I'm aware, however, I cannot make any guarantees of perfect accuracy. If your research
# requires you correctly identify all known members of the Mycobacterium avium complex (for example),
# double-check the lists written here.
#
# Please submit any corrections or updates in the form of a PR, preferably with supporting literature.
#
# Classification system:
#
# |------------------------------------------------- Mycobacterium flavored --------------------------------------------------|
#
#   |--leprosy--|  |----------NTM-----------|  |--mycolicibacterium--|   |---------------MTBC--------------|      |--other--|
#   | * leprosy |  |  * abscessus_complex   |                            | |--MTBC--|   |--tuberculosis--|
#    -----------   |  * avium_complex       |                            |                 * sensu_stricto
#                  |  * other_NTM           |                            |                 * in_general
#
#

def convert_to_regex(list_to_convert, case_insensitive=True):
	"""
	["Mycobacterium avium", "Mycobacterium canettii"] -->  "Mycobacterium avium|Mycobacterium canettii"
	"""
	if case_insensitive:
		return("(?i)" + "|".join(list_to_convert))
	else:
		return("|".join(list_to_convert))
		

def convert_to_regex_no_substrings(list_to_convert):
	"""
	["Mycobacterium avium", "Mycobacterium canettii"] --> "^Mycobacterium avium$|^Mycobacterium canettii$" 
	"""
	return("^"+"$|^".join(list_to_convert)+"$")


tuberculosis_sensu_stricto = ["Mycobacterium tuberculosis sensu stricto", "M. tuberculosis sensu stricto", "Mycobacterium tuberculosis subsp. tuberculosis"]

tuberculosis_in_general = tuberculosis_sensu_stricto + [
	"M. tuberculosis",
	"Mycobacterium tuberculosis",

	"M. africanum",
	"Mycobacterium africanum",
	"Mycobacterium tuberculosis variant africanum",

	"M. bovis",
	"Mycobacterium bovis",
	"Mycobacterium tuberculosis variant bovis",

	"M. caprae",
	"Mycobacterium caprae",
	"Mycobacterium tuberculosis variant caprae",

	# NCBI does *NOT* consider this TB, but TBProfiler classifies its lineage, so I'm keeping it here
	# https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=1305738&lvl=3&lin=f&keep=1&srchmode=1&unlock
	"M. orygis",
	"Mycobacterium orygis",
	"Mycobacterium tuberculosis variant orygis",

	# NCBI considers this TB, not just part of the complex
	# https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=1806&lvl=3&lin=f&keep=1&srchmode=1&unlock
	"M. microti",
	"Mycobacterium microti",
	"Mycobacterium tuberculosis variant microti",

	# variant name of M. microti
	# do NOT match on "M. muris" as that's more likely https://en.wikipedia.org/wiki/Mastophorus_muris
	"Mycobacterium tuberculosis variant muris",
	"Mycobacterium tuberculosis var. muris",
	"Mycobacterium muris",

	# NCBI considers this TB, not just part of the complex
	# https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=194542&lvl=3&lin=f&keep=1&srchmode=1&unlock
	"M. pinnipedii",
	"Mycobacterium pinnipedii",
	"Mycobacterium tuberculosis variant pinnipedii"
]

tuberculosis_complex_specific = tuberculosis_in_general + [
	"M. canetti",  # surprisingly common typo
	"M. canettii",
	"Mycobacterium canettii",

	"Mycobacterium tuberculosis complex",
	"Mycobacterium tuberculosis complex sp.",
	"Mycobacterium tuberculosis complex bacterium",
	"unclassified Mycobacterium tuberculosis complex",

	"M. mungi",
	"Mycobacterium mungi",

	"M. suricattae",
	"Mycobacterium suricattae"
]
recommended_tuberculosis_regex = convert_to_regex(tuberculosis_complex_specific)

other_MTBC_terms = [
	# WARNING: If regexing use convert_to_regex_no_substrings() or else "TB" will match "seatbelt", etc
	"chimpanzee bacillus", # MTBC member proposed by Coscolla et al. (doi: 10.3201/eid1906.121012)
	"dassie bacillus",     # MTBC member found only in Procavia capensis
	"Koch's bacillus",     # another name for Mycobacterium tuberculosis 
	"Kochs bacillus",      # typo of above
	"vole bacillus",
	"M. tb complex",
	"M. tb",
	"M.tb"
	"MTB complex",
	"Mtb",
	"MTBC",
	"Mycobacterium canetti",  # surprisingly common typo
	"Mycobacteria tuberculosis",
	"Mycobacteria tuberculosis complex",
	"Mycobacterium tb complex",
	"Mycobacterium tuberculosis complex",
	"TB",
	"Tuberculosis", # English, Spanish
	"Tuberkulose",  # German
	"Tuberculose",  # Dutch, French, Portuguese
	"Tubercolosi",  # Italian
	"Tuberkulozo",  # Esperanto
	"Tuberkulosis"  # Bahasa Indonesia, Bahasa Melayu, Tagalog
]
# See also tuberculosis_other_names.py

tuberculosis_complex_loose = tuberculosis_complex_specific + other_MTBC_terms
recommended_MTBC_regex = convert_to_regex_no_substrings(other_MTBC_terms) + convert_to_regex(tuberculosis_complex_specific)

############
# Non-MTBC # 
############

abscessus_complex = [
	"Mycobacterium abscessus",
	"Mycobacteroides abscessus",
	"Mycobacteroides abscessus ATCC 19977",
	"Mycobacterium bolletii",
	"Mycobacteroides chelonae",
	"Mycobacterium massiliense",
]
abscessus_regex = convert_to_regex(abscessus_complex)

# Mycobacterium avium avium, Mycobacterium avium hominissuis, Mycobacterium avium subsp. hominissuis logically inferred from:
# https://www.merckvetmanual.com/generalized-conditions/overview-of-tuberculosis-in-animals/overview-of-tuberculosis-in-animals
avium_complex = [
	"Mycobacterium [tuberculosis] TKK-01-0051", # yes, this is avium: https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=1324261
	"Mycobacterium avium",
	"Mycobacterium avium avium",
	"Mycobacterium avium hominissuis",
	"Mycobacterium avium subsp. hominissuis",
	"Mycobacterium avium complex sp.",
	"Mycobacterium avium subsp. paratuberculosis",
	"Mycobacterium avium XTB13-223",
	"Mycobacterium bouchedurhonense",
	"Mycobacterium chimaera",
	"Mycobacterium colombiense",
	"Mycobacterium intracellulare",
	"Mycobacterium intracellulare subsp. chimaera",
	"Mycobacterium intracellulare subsp. yongonense",
	"Mycobacterium marseillense",
	"Mycobacterium sp. TKK-01-0059", # Mycobacterium yongonense, doi:10.1016/j.ijid.2018.04.796
	"Mycobacterium timonense",
	"Mycobacterium yongonense" # doi:10.1016/j.ijid.2018.04.796
]
avium_regex = convert_to_regex(avium_complex)

# there seems to be some disagreement as to whether mycolicibacterium
# are technically NTM or not, so they're excluded here
NTM = avium_complex + abscessus_complex + [
	"Mycobacterium basiliense",
	"Mycobacterium kansasii",
	"Mycobacterium kansasi",  # if canettii is any indication, people will typo this
	"Mycobacterium kiyosense",
	"Mycobacterium lentiflavum",
	"Mycobacterium malmoense",
	"Mycobacterium mantenii",
	"Mycobacterium manteni",  # if canettii is any indication, people will typo this
	"Mycobacterium marinum",
	"Mycobacterium paragordonae",
	"Mycobacterium peregrinum", # doi:10.1128/JCM.43.12.5925-5935.2005
	"Mycobacterium riyadhense",
	"Mycobacterium scrofulaceum",
	"Mycobacterium senegalense",
	"Mycobacterium triplex",
	"Mycobacterium ulcerans",
	"Mycobacterium sp. DSM 104308",  # Mycobacterium basiliense
	"NTM",
	"Nontuberculosis mycobacteria",
	"Nontuberculosis mycobacterium",
	"Non-tuberculosis mycobacteria",
	"Non-tuberculosis mycobacterium",
	"Non-TB mycobacterium",
]
NTM_regex = convert_to_regex_no_substrings(NTM)


# aka "Mycobacterium fortuitum complex"
mycolicibacterium = [
	"Mycolicibacterium",
	"Mycolicibacterium sp.",
	"Mycolicibacteria",

	"Mycolicibacterium agri",
	"Mycobacterium agri",
	
	"Mycolicibacterium aichiense",
	"Mycobacterium aichiense",
	
	"Mycolicibacterium alvei",
	"Mycobacterium alvei",
	
	"Mycolicibacterium aubagnense",
	"Mycobacterium aubagnense",
	
	"Mycolicibacterium aurum",
	"Mycobacterium aurum",
	
	"Mycolicibacterium austroafricanum",
	"Mycobacterium austroafricanum",
	
	"Mycolicibacterium fortuitum",
	"Mycolicibacterium fortuitum complex",
	"Mycobacterium fortuitum",
	"Mycobacterium fortuitum complex",
	
	"Mycolicibacterium malmesburyense",
	"Mycobacterium malmesburyense",
	
	"Mycolicibacterium iranicum",
	"Mycobacterium iranicum",
	
	"Mycobacterium smegmatis",
	"Mycolicibacterium smegmatis",
	"Mycolicibacterium smegmatis MC2 155"
]
mycolicibacterium_regex = convert_to_regex(mycolicibacterium)

leprosy = [
	"Leprosy",
	"Hansen's disease",
	"Mycobacterium leprae",
	"Mycobacterium lepromatosis"
]
leprosy_regex = convert_to_regex(leprosy)

sulfur_cave = [
	"Candidatus Mycobacterium methanotrophicum",
	"Mycobacterium methanotrophicum Sulfur Cave",
	"Mycobacterium sp. MAG1",
	"Mycobacterium methanotrophicum"
]

other_mycobacteria = [
	# WARNING: Remove phages first or use convert_to_regex_no_substrings() if you intend on regexing 
	"Mycobacterium",
	"Mycobacterium alsense",
	"Mycobacterium asiaticum",
	"Mycobacterium basiliense",
	"Mycobacterium gordonae",
	"Mycobacterium interjectum",
	"Mycobacterium sp.",
	"Mycobacterium szulgai"
]
other_mycobacteria_regex = convert_to_regex_no_substrings(other_mycobacteria)

unidentified_but_not_metagenomic = [
	# WARNING: Use convert_to_regex_no_substrings() if you intend on regexing 
	"bacteria",
	"bacterium",
	"uncultured prokaryote",
	"unidentified"
]

non_mtbc_mycobacteria = NTM + mycolicibacterium + mycolicibacterium + leprosy + other_mycobacteria + sulfur_cave
recommended_mycobacteria_regex = recommended_MTBC_regex + convert_to_regex(non_mtbc_mycobacteria)
everything_mycobacterium_flavored = tuberculosis_complex_loose + non_mtbc_mycobacteria
everything_mycobacterium_flavored_and_unknowns = everything_mycobacterium_flavored + unidentified_but_not_metagenomic
