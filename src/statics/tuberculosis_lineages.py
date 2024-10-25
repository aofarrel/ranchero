# this should ONLY be used for TUBERCULOSIS!!!

from numpy import nan as nan

specific_strains = True

organisms = {
	'Mycobacterium tuberculosis variant africanum': 'M. africanum (L5/L6)',
	'Mycobacterium tuberculosis variant bovis': 'La1 (M. bovis)',
	'Mycobacterium tuberculosis variant bovis BCG': 'La1.2 (M. bovis BCG)' if specific_strains else nan,
	'Mycobacterium tuberculosis H37Rv': 'L4.9 str. H37Rv',
	'Mycobacterium tuberculosis F11': 'str. F11',
	'Mycobacterium canetti': 'M. canettii',
	'Mycobacterium canettii CIPT 140070008': 'M. canettii',
	'Mycobacterium mungi': 'M. mungi',
	'Mycobacterium tuberculosis str. Erdman WHO': 'str. Erdman',
	'Mycobacterium tuberculosis str. Haarlem': 'L4.1.2.1',
	'Mycobacterium tuberculosis variant pinnipedii': 'M. pinnipedii',
	'Mycobacterium tuberculosis variant caprae': 'La2 (M. caprae)',
	'Mycobacterium tuberculosis variant microti': 'M. microti',
	'Mycobacterium orygis': 'La3 (M. orygis)',
	'M. tuberculosis': nan,
	'Mycobacterium': nan,
	'Mycobacterium tuberculosis': nan,
	'Mycobacterium tuberculosis sp.': nan,
	'Mycobacterium tuberculosis complex sp.': nan,
	'Mycobacterium tuberculosis complex': nan,
	'Mycobacterium tuberculosis subsp. tuberculosis': nan,
}

# convert strains to lineages
strains = {
	# useless
	'NULL_STRAIN': nan,
	'NULL_GENO': nan,
	'strains': nan,
	'MTBC': nan,
	'MTB': nan,
	'Mtb': nan,
	'M. tuberculosis': nan,
	'Mycobacterium tuberculosis': nan,
	'tuberculosis': nan,
	'Mycobacteria': nan,
	'Mycobacterium': nan,
	'Tuberculosis': nan,
	'Clinical': nan,
	'pre-extensively drug resistant': nan,
	'pan drug susceptible': nan,
	'Extensively drug resistant': nan,
	'Mycobacterium tuberculosis sp.': nan,
	'XDR-TB': nan,
	'microbial': nan,
	'Microbial': nan,
	'Lineage': nan,
	'-': nan,
	'ON-A_WT': nan,
	'subsp. tuberculosis': nan,
	'12nt insertion at rpoB': nan,
	'Thailand': nan,
	'wild type genotype': nan,
	'Sensitive': nan,
	'MDR': nan,
	'pre-XDR': nan,
	'U 777777760000000': nan,
	'Mycobacterium tuberculosis complex': nan,
	'wild type genotype': nan,
	'mono-DR': nan,
	'-': nan,


	# idk what these are but I don't like them
	'7': nan, # not lineage 7, I checked
	'5765': nan,
	'7171': nan,
	'S1': nan,
	'117': nan,
	'19': nan,
	'82': nan,
	'67': nan,
	'129': nan,
	'281': nan,
	'Proto-Beijing': 'L2',
	'H37Rv-like': nan,
	'CD1801': nan,
	'ON-A_NM': nan,
	'OFXR-14': nan,
	'OFXR-16': nan,
	'ST-25': nan,
	'ST-22': nan,
	'TWCDC': nan,
	'SB0971': nan,
	'HR': nan,
	'ST268': nan,
	'Tn11.H37Rv': nan,
	'SB0339': nan,
	'SB0971': nan,
	'SB0145': nan,
	'SB0120': nan,
	'SB0121': nan,
	'SB1216': nan,
	'SB0134': nan,
	'TBV5365': nan,
	'SB1040': nan,
	'SB0327': nan,
	'117': nan,
	'TCDC3': nan,

	# not actually strains but rather sample IDs
	'Myanmar': nan,
	'M67': nan,
	'AZE-02-047_523': nan,
	'3155': nan,

	# these largely seem to match well to a lineage in SITVIT?
	'ST-19': 'L1 str. Manila', # EAI2-Manila
	'SIT 523': nan, # Manu_ancestor
	'ST215': nan,   # T
	'ST276': nan,   # T1
	'ST298': 'L1',     # EAI3-IND
	'ST277': nan,   # T1
	'ST284': nan,   # T1
	'ST302': nan,   # X1
	'ST889': 'L1',     # EAI6
	'ST281': 'L6',     # AFRI_2
	'ST320': 'L6',     # AFRI_2
	'ST279': 'L1 str. Manila',

	# L2.2.1 Beijing
	'2.2.1':  'L2.2.1',
	'2.2.1 - East-Asian (Beijing)': 'L2.2.1',
	'2.2.1 Beijing': 'L2.2.1',
	'L2.2.1': 'L2.2.1',
	'Beijing': 'L2.2.1',
	'Beijing (L2)': 'L2.2.1',
	'Beijing - CAO': 'L2.2.1',
	'Beijing B0': 'L2.2.1',
	'East-Asian (Beijing)': 'L2.2.1',
	'East-Asian Beijing': 'L2.2.1',
	'EastAsian (Beijing)': 'L2.2.1',
	'Mycobacterium Tuberculosis Beijing genotype': 'L2.2.1',
	'2.2.1.1 - East-Asian (Beijing)': 'L2.2.1.1',
	'Beijing 000000000003771': 'L2.2.1',
	'lineage 2/beijing sublineage':'L2.2.1',
	'global L2.2.1.1.1': 'L2.2.1.1.1',
	'modern Beijing': 'L2',
	'L2.2.1 of Beijing genotype': 'L2.2.1',

	# L3 Delhi/CAS
	'Delhi/CAS': 'L3',

	# L4.1.2.1 Haarlem
	'4.1.2.1': 'L4.1.2.1',
	'L4.1.2.1': 'L4.1.2.1',
	'Haarlem': 'L4.1.2.1',
	'Euro-American (Haarlem)': 'L4.1.2.1',
	'HARLEM(4.1.2.1)': 'L4.1.2.1',
	'Haarlem_1': 'L4.1.2.1',

	# L4.1.1 X-type
	'4.1.1': 'L4.1.1',
	'Euro-American (X-type)': 'L4.1.1',

	# Ural
	'Euro-American (Ural)': 'L4.2.1',

	# L4.3 LAM
	'4.3 (LAM)': 'L4.3',
	'4.3': 'L4.3',
	'Euro-American (LAM)': 'L4.3',
	'LAM': 'L4.3',
	'LAM9(4.3.3)': 'L4.3.3',
	'LAM(4.3.4.2)': 'L4.3.4.2',
	'Euro-AmericanLAM': 'L4.3',

	# L4.4.1.1 S-type
	'4.4.1.1': 'L4.4.1.1',
	'S-type': 'L4.4.1.1',

	# L4.6.2 Cameroon
	'Euro-American (Cameroon)': 'L4.6.2',

	# L4.8 mainly T ---> seems to be L4.7 or 4.8?
	'4.8': 'L4.8',
	'Euro-American (mainly T)': 'L4',
	'mainly T': 'L4',
	'4.7 - Euro-American (mainly T)': 'L4.7',
	'4.8 - Euro-American (mainly T)': 'L4.8',

	# catalogue numbers
	'ATCC 35828': 'L4.8 str H37Rv-PZA-R',
	'Erdman (ATCC35801)': 'str. Erdman',
	'L2.2.6_RD150': 'L2.2.6',

	# other numbers
	'1.1.1': 'L1.1.1',
	'1.1.1.1': 'L1.1.1.1',
	'1.2.1': 'L1.2.1',
	'1.2.1 - Indo Oceanic': 'L1.2.1',
	'1.1.3': 'L1.1.3',

	'2': 'L2',
	'2.1': 'L2.1',
	#2.2.1 --> Beijing
	'2.2.2': 'L2.2.2',
	'global L2.2.2': 'L2.2.2',
	'global L2.2.1.1.1': 'L2.2.1.1.1',
	'2.2.1.1': 'L2.2.1.1',

	# is this actually beijing?
	'2.2.2 - East Asian (Beijing)': 'L2.2.2',
	
	'L 4': 'L4',
	'4': 'L4',
	'lineage 4': 'L4',
	'Mycobacterium tuberculosis L4': 'L4',

	'4.1': 'L4.1',
	#4.1.1 --> X-type
	'4.1.2': 'L4.1.2',
	'4.1.1.1': 'L4.1.1.1',
	'4.1.1.1 - Euro America': 'L4.1.1.1',
	'4.1.1.1 - Euro American': 'L4.1.1.1',
	#4.1.2.1 --> Haarlem
	'4.1.1.3': 'L4.1.1.3',
	'M (lineage 4.1.2.1) ': 'L4.1.1.3',

	'4.2.1': 'L4.2.1',
	'4.2.2': 'L4.2.2',

	#4.3 --> LAM
	'LAM(4.3.4.1)': 'L4.3.4.1',
	'LAM(4.3.4.2)': 'L4.3.4.2',
	'4.3.1 - Euro-American (LAM)': 'L4.3.1',
	'4.3.2 - Euro-American (LAM)': 'L4.3.2',
	'4.3.2': 'L4.3.2',
	'4.3.2.1': 'L4.3.2.1',
	'4.3.4.2': 'L4.3.4.2',
	'L4.3.4.2 of LAM genotype': 'L4.3.4.2',
	'4.3.3': 'L4.3.3',
	'4.3.3 - Euro-American (LAM)': 'L4.3.3',
	'Euro-American(4.3.3)': 'L4.3.3',
	'4.3.4.1': 'L4.3.4.1',
	'Euro-American(4.3.4.2)': 'L4.3.4.2',
	'4.3.4.2.1': 'L4.3.4.2.1',

	'4.4.1.2 - Euro-American': 'L4.4.1.2',
	'4.4.1.2 - Euro-America': 'L4.4.1.2',

	'4.5': 'L4.5',

	'UGII/L4.6.84': 'L4.6',
	'UGII/L4.6.85': 'L4.6',

	'4.7': 'L4.7',

	'4.9 - Euro American (H37Rv-like)': 'L4.9',
	
	'L 5': 'L5',
	'Lineage5': 'L5',

	'L 6': 'L6',
	'Lineage6': 'L6',

	# Nebenzahl-Guimaraes et al. proposed name for L7
	'Aethiop_vetus_229': 'L7',
	'Aethiop_vetus_232': 'L7',
	'Aethiop_vetus_233': 'L7',
	'Aethiop_vetus_234': 'L7',

	# bovis and friends
	'str. 26': 'La1.2 str. 26',
	'SB0140': 'La1 (M. bovis)',
	'SB0673': 'La1 (M. bovis)',
	'SB0121': 'La1 (M. bovis)',
	'BCG': 'La1.2 (M. bovis BCG)',
	'BCG P3': 'La1.2 (M. bovis BCG)',
	'BCG Danish 1331': 'La1.2 (M. bovis BCG)',
	'BOVIS': 'La1 (M. bovis)',
	'bovis': 'La1 (M. bovis)',
	'canettii': 'M. canettii',
	'Buffalo strain': 'La1 (M. bovis)',
	'Cattle strain': 'La1 (M. bovis)',
	'Cervid strain': 'La1 (M. bovis)', # can't find a sublineage in literature for this, but it's definitely bovis
	'lineageBOV_AFRI':'La1 (M. bovis)',
	'Danish-SSI 1331': 'La1 (M. bovis)',
	'M_bovis': 'La1 (M. bovis)',
	'M.bovis': 'La1 (M. bovis)',
	'M Bovis': 'La1 (M. bovis)',
	'M. bovis': 'La1 (M. bovis)',
	'M. bovis BCG': 'La1.2 (M. bovis BCG)',
	'BCG SL 222 Sofia': 'La1.2 (M. bovis BCG)',
	'BCG Moreau RDJ': 'La1.2 (M. bovis BCG)',
	'BCG Sofia SL222': 'La1.2 (M. bovis BCG)',
	'BCG-S48': 'La1.2 (M. bovis BCG)',
	'BCG Pasteur ATCC 35734': 'La1.2 (M. bovis BCG)',
	'BCG delta BCG1419c mutant': 'La1.2 (M. bovis BCG)',
	'M. caprae': 'La2 (M. caprae)',
	'M.orygis': 'La3 (M. orygis)',
	'M. orygis': 'La3 (M. orygis)',
	'Orygis': 'La3 (M. orygis)',
	'orygis': 'La3 (M. orygis)',
	'MUHC/MB/EPTB/Orygis/51145': 'La3 (M. orygis)',
	'BCG Pasteur 1173P2': 'La1.2 (M. bovis BCG) str. Pasteur 1173P2',
	'Pasteur 1173P2': 'La1.2 (M. bovis BCG) str. Pasteur 1173P2',
	'Pasteur 1721': 'La1.2 (M. bovis BCG) str. Pasteur 1721',
	'Ravenel': 'La1 (M. bovis) str. Ravenel',
	'STB-D / CIPT 140060008': 'M. canettii str. STB-D',
	'STB-K / CIPT 140070010': 'M. canettii str. STB-K',
	'Tice': 'La1.2 (M. bovis BCG) str. Tice',

	# noteable strains
	'26': 'La1.2 str. 26',
	'cdc1551': 'str. CDC1551',
	'CDC1551': 'str. CDC1551',
	'CDC1551 delta-mas::hyg': 'str. CDC1551',
	'Connaught': 'str. Connaught',
	'Erdman': 'str. Erdman',
	'Erdmann': 'str. Erdman',
	'F11': 'str. F11',
	'LAM3/F11': 'str. F11',
	'H37Rv': 'L4.9 str. H37Rv',
	'H37Rv delta-tgs1::hyg': 'L4.9 str. H37Rv',
	'Erdman_WHO': 'str. Erdman',
	'Euro-American (H37Rv-like)': 'L4.9',
	'H37Ra': 'L4.9 str. H37Ra',
	'H37Ra; ATCC 25177': 'L4.9 str. H37Ra',
	'H41Rv': 'str. H41Rv',
	'KZN MDR': 'str. KZN',
	'MtZ': 'str. MtZ',
	'M-strain, a nationwide strain in Japan': 'L4.1.2.1 str. M',
	'M (L4.1.2.1)': 'L4.1.2.1 str. M',
	'Mycobacterium tuberculosis H37Rv': 'H37Rv',
	'Oshkosh': 'str. CDC1551',
	'Otara': 'L4 str. Otara',
	'Ra': 'L4.3',
	'Rangipo': 'L4 str. Rangipo',
	'Rangipo.': 'L4 str. Rangipo',
	'SCAID 252': 'str. SCAID 252',

	
	# less specific old-school names
	'Delhi-CAS': 'L3',
	'EAI': 'L1',
	'East-Asian': 'L2',
	'East Asian': 'L2',
	'East-Asian (non-Beijing)': 'L2',
	'Euro-American(4)': 'L4',
	'Euro-American': 'L4',
	'Euro American': 'L4',
	'Indo-OceanicEAI': 'L1',
	'IndoOceanic(EAI)': 'L1',
	'Indo-Oceanic EAI': 'L1',
	'East-African-Indian': 'L1',
	'Indo-Oceanic': 'L1',
	'IndoOceanic (EAI)': 'L1',
	'Manila': 'L1 str. Manila', # it'd be nice to get more granular...
	'Manila 677777477413771': 'L1 str. Manila',
	'M. africanum 774077007777071': 'M. africanum (L5/L6)'
	
}

# only do this AFTER the the above
sublineages_to_lineages  = {
	'La1 (M. bovis)': 'La1',
	'La1 (M. bovis) str. Ravenel': 'La1',
	'La1.2 str. 26 (M. bovis)': 'La1',
	'La1.2 (M. bovis BCG)': 'La1',
	'La1.2 (M. bovis BCG) str. Pasteur 1173P2': 'La1',
	'La1.2 (M. bovis BCG) str. Pasteur 1721': 'La1',
	'La2 (M. caprae)': 'La2',
	'La3 (M. orygis)': 'La3',
	'M. canettii str. STB-D': 'M. canettii',
	'M. canettii str. STB-K': 'M. canettii',
	'str. 26': 'La1',
	'str. CDC1551': nan,
	'str. Erdman': nan,
	'str. H41Rv': nan,
	'str. MtZ': nan,
	'str. SCAID 252': nan,
	'str. Rangipo': nan,
}



