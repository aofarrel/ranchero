# This is for standardizing information about what a sample comes from


# TODO: is case insensitivity not working? see fungal isolate etc


# Nonspecific sample sources that are useless if you already know
# what organism you are looking at -- consider skipping this batch
# if you are comparing multiple genra at once, I suppose?
sample_sources_I_should_hope_so = [

	# Candida
	'auris',
	'Fungal isolate',
	'fungal strain',
	'fungal cell',

	# tuberculosis/MTBC/Mycobacterium genus
	'DNA from M. tuberculosis',
	'M. tuberculosis',
	'MTB isolates',
	'Mtb',
	'MTBC',
	'Mycobacterium tuberculosis complex',
	'Mycobacterium tuberculosis',
	'Mycobacteryum tuberculosis', # common typo
	'H37Rv', # standardize_sample_source_as_list() will put this in taxoncore first, standardize_sample_source_as_string() will not
]

# Sample sources that almost nobody would want
sample_sources_nonsense = [
	'1',
	'?',
	'DNA',
	'Genomic DNA',
	'to wear a mask', # ?????????
	'whole organism',
	'Yes',
]

# Handled earlier in standardizer
# TODO: Standardization should be adjustable by user here
sample_sources_otherwise_unhelpful  = [
	'Affedcted Herd',
	'bacteria',
	'bacterial cell',
	'Bacterial isolate',
	'Biological Sample',
	'Biological sample',
	'Bureau of Tuberculosis',
	'Homo sapiens',
	'human',
	'isolate frome children',
	'Lima', # location
	'na',
	'nan',
	'New Zealand', # location
	'no date',
	'no source',
	'other',
	'Pakistan', # location
	'PTB',
	'Pulmonary tuberculosis',
	'strain',
	'Specimen',
	'Systemic',
	'TBM',
	'tuberculosis',
	'veracruz', # location
	'Viet Nam', # location
]

sample_sources_nonspecific = sample_sources_I_should_hope_so + sample_sources_nonsense + sample_sources_otherwise_unhelpful

sample_source_exact_match = {
	'BAL': 'bronchoalveolar lavage',
	'Bed': 'environmental (bed)',
	'bronchial': 'bronchial (unspecified)',
	'Clinical': 'clinical (unspecified)',
	'CSF': 'cerebrospinal fluid',
	'CVC': 'central venous catheter',
	'Culture': 'culture',
	'Hospitol': 'hospital', # common typo
	'laboratory': 'laboratory-obtained strain',
	'tissue': 'tissue (unspecified)',
	'pus': 'pus',
}

if_this_and_that_then = [
	# specific culture type + tissue
	['(?i)single colony', '(?i)fecal', 'culture (single-colony) from feces'],
	['(?i)single colony', '(?i)lab', 'culture (single-colony) from lab stock'],

	# generic culture + tissue
	['(?i)culture', '(?i)sputum', 'culture from sputum'],
	['(?i)culture', '(?i)blood', 'culture from blood'],
	['(?i)culture', '(?i)\bbronch.*lavage', 'culture from bronchoalveolar lavage'],
	['(?i)culture', '(?i)cerebrospinal', 'culture from cerebrospinal fluid'],
	['(?i)culture', '(?i)lung', 'culture from lung tissue'],
	['(?i)culture', '(?i)pleural fluid', 'culture from pleural fluid'],
	['(?i)culture', '(?i)feces|fecal', 'culture from feces'],
	['(?i)culture', '(?i)liver', 'culture from liver'],
	['(?i)culture', '(?i)eye', 'culture from eye'],

	# swabs
	['(?i)swab', '(?i)axilla/groin', 'swab - axilla and/or groin'],
	['(?i)swab', '(?i)axilla and groin', 'swab - axilla and/or groin'],
	['(?i)swab', '(?i)skin', 'swab - skin'],
	
	# everything else
	['(?i)scrapate', '(?i)granuloma', 'scrapate of granuloma'],
	['(?i)biopsy', '(?i)skin', 'biopsy (skin)'],
	['(?i)biopsy', '(?i)intestine', 'biopsy (intestine)'],
	['(?i)biopsy', '(?i)thoracic', 'biopsy (thoracic)'],
	['(?i)biopsy', '(?i)pleura', 'biopsy (pleura/pleural effusion)'],
	['(?i)necropsy', '(?i)lung', 'necropsy (lung tissue)'],
	['(?i)necropsy', '(?i)spleen', 'necropsy (spleen)'],
	['(?i)necropsy', '(?i)kidney', 'necropsy (kidney)'],
	['(?i)cow', '(?i)feces', 'feces (bovine)'],
	['(?i)FFPE', '(?i)skin', 'FFPE block (skin)'],

	['(?i)ascit', '(?i)fluid', 'peritoneal fluid (ascitic)'],
]

# These are considered mutually exclusive, and whichever ones are listed first will take precident
# This means, generally speaking, we want more specific first and less specific last... with the
# exception of things that are likely in silico or experimental evolution, as those ones are often
# not appropriate to include in later analysis
sample_source = {
	
	# simulated/in silico matches should ALWAYS be done first, as there are many reasons you may not want
	# them in your analysis (no shade to the submitters of course it's just not appropriate for some things)
	'simulated': 'in silico',
	'silico': 'in silico',
	'simulated/in silico': 'in silico', # bring over matches from earlier into the correct column

	# edited/experimental evolution
	'transformant': 'experimental transformant',
	'edited': 'experimental transformant',
	'in vitro evolution': 'experimental evolution (in vitro)',
	'Laboratory experiment': 'experimental (unspecified)',
	'laboratory evolution': 'experimental evolution',

	# tubes
	'nephrostomy': 'nephrostomy',
	'tracheostomy': 'tracheostomy',
	'cholecystostomy': 'cholecystostomy',
	'exit site (dialysis)': 'dialysis exit site',
	'dialysis catheter': 'catheter (dialysis)',
	'Catheter': 'catheter',
	'Catheter Tip': 'catheter',
	'Driveline': 'driveline',

	# miscellanous highly specific stuff
	'Archaeological': 'archaeological',
	'biofilm': 'biofilm',


	### The Fluid Zone ###
	# ascitic/peritoneal fluid (ascitic is already covered in if-and-then)
	'Intra-abdominal fluid': 'peritoneal fluid',
	'Peritoneal fluid': 'peritoneal fluid',
	# BAL and friends -- BAL is too generic on its own
	'BRL': 'bronchoalveolar lavage',
	'BALF': 'bronchoalveolar lavage',
	'BAL RUL': 'bronchoalveolar lavage',
	'BAL_RUL': 'bronchoalveolar lavage',
	'\bbronch.*lavage': 'bronchoalveolar lavage',
	'bronchialLavage': 'bronchoalveolar lavage',
	'broncho-alveolar lavage': 'bronchoalveolar lavage',
	'Bronchio Alveolar Lavage': 'bronchoalveolar lavage',
	'Bronch_Lav': 'bronchoalveolar lavage',
	'bronchoalveolar lavage': 'bronchoalveolar lavage', # prevent "as reported" showing up
	'bronchial aspirate': 'bronchoalveolar aspirate',
	'Bronch_Asp': 'bronchoalveolar aspirate',
	'tracheal aspirate': 'tracheal aspirate', # TODO: why is "Trachael Aspirate" not matching?
	'Trach_Asp': 'tracheal aspirate',
	'BRONCH_WSH': 'bronchial wash',
	'bronchial wash': 'bronchial wash',
	# CSF
	'Cerebospinal fluid': 'cerebrospinal fluid',
	'cerebrospinal fluid': 'cerebrospinal fluid',
	'cerebrospinalFluid': 'cerebrospinal fluid',
	'cerebral spinal fluid': 'cerebrospinal fluid',
	# gastric
	'Gastric lavage': 'gastric lavage',
	'aspirate gastric': 'gastric aspirate',
	'Gastric Aspirate': 'gastric aspirate',
	'stomach contents': 'gastric (stomach contents)',
	'gastric juice': 'gastric fluid',
	'gastric fluid': 'gastric fluid',
	# snot
	'mucus': 'mucus',
	'nasal swab': 'mucus (nasal swab)',
	# sputum
	'AFB sputum smear': 'sputum (AFB smear)',
	'sputum throat swab': 'sputum (throat swab)',
	'sputum': 'sputum',
	'Sputa': 'sputum',
	# pleural
	'pleural fluid': 'pleural fluid',
	'pleuralFluid': 'pleural fluid',
	'thoracentesis': 'pleural fluid',
	'pleural effusion': 'pleural fluid (effusion)',
	'Pleural': 'pleural fluid',
	# synovial (joint juice)
	'synov fl': 'synovial fluid',
	'synovial': 'synovial fluid',
	# blood
	'Blood C&S': 'blood (C&S)',
	'blood': 'blood',
	# piss
	'urine': 'urine',
	'Urine, Clean Catch': 'urine',
	'Uriine': 'urine',
	# other fun fluids
	'ear discharge': 'ear discharge',
	'phlegm': 'phlegm',
	'Peritoneal dialysate': 'dialysate (peritoneal)',
	'dialysate': 'dialysate',

	# candida-specific body parts, in a specific body part
	'Nares/Axilla': 'nares and/or axilla',
	'Nares/Axilla/Groin': 'nares/axilla/groin',
	'axilla and groin': 'axilla and groin', # very common for Candida
	'axilla/groin': 'axilla and/or groin', # very common for Candida
	'Axilliae': 'axilla',
	'axilla': 'axilla',
	'underarm': 'axilla',
	'groin': 'groin',
	'Scrotal': 'groin (scrotum)',
	'nares': 'nares',

	# organs / body parts
	'brain': 'brain',
	'Intra-abdominal tissue': 'intra-abdominal tissue',
	'bone': 'bone',
	'Coccyx': 'coccyx',
	'knee': 'knee',
	'spine': 'spine',
	'homogenized mouse spleen': 'homogenized mouse spleens', # standardize singular/plural
	'lung': 'lung',
	'skin': 'skin',
	'epidermis': 'skin',
	'abdomen': 'abdomen',
	'flank': 'flank',
	'Thigh': 'thigh',
	'Vaginal': 'vaginal',
	'Conjunctiva': 'eye (conjunctiva)',
	'breast': 'breast',
	'rectal': 'rectal',
	'Toenail': 'foot (toenail)', # toe handled in last section
	
	# lab stuff
	'laboratory reference strain': 'laboratory strain ("reference")',
	'Laboratory obtained strain': 'laboratory strain (unspecified)',
	'Lab strain': 'laboratory strain (unspecified)',
	'laboratory strain': 'laboratory strain (unspecified)',

	# dead
	'Morgue': 'necropsy (morgue)',
	'Abbattoir': 'necropsy (abbattoir)',
	'slaughterhouse': 'necropsy (abbattoir)',
	'necropsy': 'necropsy',

	# owies
	'abscess': 'abscess',
	'caseum': 'caseous mass',
	'CaseousMasses': 'caseous mass',
	'lesion': 'lesion',
	'wound': 'wound',
	'Ulcer': 'ulcer',
	'scar': 'scar tissue',

	# lymph nodes (specific)
	'Cervical lymphnode biopsy': 'lymph node (cervical)',
	'Cervical lymph node': 'lymph node (cervical)',
	'Lung lymph node': 'lymph node (lung)',
	'Head lymph node': 'lymph node (head)',
	'Pectoral lymph nodes': 'lymph node (pectoral)',

	# lymph nodes (plural)
	'lymph nodes': 'lymph nodes',

	# lymph node (singular, do after all other lymphy bits)
	'lymph node': 'lymph node',
	'Lymph Node Biopsy': 'lymph node',

	# poop
	'fecal': 'feces',
	'stool': 'feces',
	'feces': 'feces',
	'animal waste': 'feces (unspecified animal)',
	'chicken dung': 'feces (chicken)',
	'dung': 'feces (unspecified animal)',

	# muscle
	'Psoas': 'muscle (psoas)',
	'muscle': 'muscle',

	# i dont even want to know
	'Drainage': 'drainage',
	'excreted bodily substance': 'excreted bodily substance (unspecified)',
	'body fluid': 'bodily fluid (unspecified)',
	'fluid': 'fluid (unspecified)',

	# lungscore?
	'PULMONARY': 'pulmonary',

	# environmental -- for MTBC, basically all "farm" stuff is tissue samples, but that may not be the case for other stuff
	'soil': 'environmental (soil)',
	'river sediment': 'environmental (river sediment)',
	'air from': 'environmental (air)',
	'farm': 'environmental (farm)',
	'wastewater': 'environmental (wastewater)', # distinct from normal water, should be done before it
	'HCU': 'environmental (HCU)',               # distinct from normal water, should be done before it
	'water': 'environmental (water)',           # normal water
	
	'Negative Control': 'negative control',
	'PCR product': 'PCR product',

	### Everything here is last for good reason ###

	# short words that could match something else by mistake
	'bile': 'bile',
	'ear': 'ear',
	'eye': 'eye',
	'foot': 'foot',
	'heel': 'foot (heel)',
	'leg': 'leg',
	'Lip': 'lip',
	'Ostomy': 'ostomy',
	'toe': 'foot (toe)',
	'back': 'back',

	# super generic
	'clinical strain': 'clinical strain',
	'clinical isolate': 'clinical',
	'clinical sample': 'clinical',
	'clinical': 'clinical',
	'hospital': 'clinical',
	'diagnostic sample': 'clincal (diagnostic sample)',
	'culture': 'culture',
	'Environmental': 'environmental',
	'Biopsy': 'biopsy',
	'Secretion': 'secretion',
	'swab': 'swab',

	# culture stuff -- should be done last, as many pathogens are "culture from X body part"
	'lawn on agar plate': 'culture (lawn/sweep)',
	'sweep': 'culture (lawn/sweep)',
	'single colony': 'culture (single colony)',
	'single cell': 'single cell',
	'in vitro': 'culture (unspecified)',
	'in-vitro': 'culture (unspecified)',
	'bacterial suspension': 'culture (unspecified)',
	
}


