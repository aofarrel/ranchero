# loose matching using regex
# --> use shave_tuberculosis_suffixes() instead
#import re
#regex_m_tuberculosis = re.compile("Mycobacterium tuberculosis.*")
#regex_mycobacterium = re.compile("Mycobacterium (?!\w*phage)(?:\w*[^\W_])?")
#regex_mycobacteri = re.compile("Mycobacteri.*")  # This includes Mycobacteriales, but unfortunately will also include phages!


# non-standard terms
non_standard_MTBC = [
	"chimpanzee bacillus",
	"dassie bacillus",
	"Koch's bacillus",
	"M. tb complex",
	"M. tb",
	"M. tuberculosis",
	"M.tb"
	"MTB complex",
	"mtb complex",
	"Mtb complex",
	"Mtb",
	"MTB",
	"MTBC"
	"MYCOBACTERIUM TUBERCULOSIS"
	"TB",
	"tb",
	"Tb",
	"Tuberculosis"
	"tuberculosis",
	"TUBERCULOSIS",
]

# NCBI-style terms
unidentified_but_not_metagenomic = [
	"bacterium",
	"uncultured prokaryote",
	"unidentified"
]

tuberculosis_sensu_stricto = ["Mycobacterium tuberculosis", "Mycobacterium tuberculosis sensu stricto"]

# Note that this excludes all stuff like "Mycobacterium tuberculosis TB_RSA75",
# so use shave_tuberculosis_suffixes() first
tuberculosis = tuberculosis_sensu_stricto + [
	"Mycobacterium tuberculosis variant africanum",
	"Mycobacterium tuberculosis variant bovis",
	"Mycobacterium tuberculosis variant caprae",
	"Mycobacterium tuberculosis variant microti",
	"Mycobacterium tuberculosis variant pinnipedii"
]

tuberculosis_complex_strict = tuberculosis + [
	"Mycobacterium canetti", # common typo
	"Mycobacterium canettii",
	"Mycobacterium canettii CIPT 140010059",
	"Mycobacterium canettii CIPT 140060008",
	"Mycobacterium canettii CIPT 140070002",
	"Mycobacterium canettii CIPT 140070005",
	"Mycobacterium canettii CIPT 140070007",
	"Mycobacterium canettii CIPT 140070008",
	"Mycobacterium canettii CIPT 140070010",
	"Mycobacterium tuberculosis complex",
	"Mycobacterium tuberculosis complex sp.",
	"Mycobacterium mungi",
	"Mycobacterium orygis",
	"Mycobacterium suricattae"
]

tuberculosis_complex_loose = tuberculosis_complex_strict + non_standard_MTBC

abscessus_complex = [
	"Mycobacterium abscessus",
	"Mycobacteroides abscessus",
	"Mycobacteroides abscessus ATCC 19977",
	"Mycobacterium bolletii",
	"Mycobacteroides chelonae",
	"Mycobacterium massiliense",
]

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
	"Mycobacterium colombiense",
	"Mycobacterium intracellulare",
	"Mycobacterium intracellulare subsp. chimaera",
	"Mycobacterium intracellulare subsp. yongonense",
	"Mycobacterium marseillense",
	"Mycobacterium sp. TKK-01-0059" # Mycobacterium yongonense, doi:10.1016/j.ijid.2018.04.796
	"Mycobacterium timonense",
	"Mycobacterium yongonense" # doi:10.1016/j.ijid.2018.04.796
]

# aka "Mycobacterium fortuitum complex"
mycolicibacterium = [
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
	"Mycobacterium fortuitum",
	"Mycobacterium fortuitum complex",
	
	"Mycolicibacterium malmesburyense",
	"Mycobacterium malmesburyense",
	
	"Mycolicibacterium iranicum",
	"Mycobacterium iranicum",
	
	"Mycobacterium smegmatis",
	"Mycolicibacterium smegmatis",
	"Mycolicibacterium smegmatis MC2 155",
]

# there seems to be some disagreement as to whether mycolicibacterium
# are technically NTM or not, so they're excluded here
NTM = avium_complex + abscessus_complex + [
	"Mycobacterium kansasii",
	"Mycobacterium lentiflavum",
	"Mycobacterium malmoense",
	"Mycobacterium mantenii",
	"Mycobacterium marinum",
	"Mycobacterium paragordonae",
	"Mycobacterium peregrinum", # doi:10.1128/JCM.43.12.5925-5935.2005
	"Mycobacterium riyadhense",
	"Mycobacterium scrofulaceum",
	"Mycobacterium senegalense",
	"Mycobacterium triplex",
	"Mycobacterium ulcerans"
]

leprosy = [
	"Mycobacterium leprae",
	"Mycobacterium lepromatosis"
]

other_mycobacteria = [
	"Mycobacterium",
	"Mycobacterium alsense",
	"Mycobacterium asiaticum",
	"Mycobacterium basiliense",
	"Mycobacterium gordonae",
	"Mycobacterium interjectum",
	"Mycobacterium sp.",
	"Mycobacterium szulgai"
]

everything_mycobacterium_flavored = tuberculosis_complex_loose + NTM + mycolicibacterium + leprosy + other_mycobacteria
everything_mycobacterium_flavored_and_unknowns = everything_mycobacterium_flavored + unidentified_but_not_metagenomic
