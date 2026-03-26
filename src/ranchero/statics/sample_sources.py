# This is for standardizing information about what a sample comes from

exact_replacements = {
	'abdomen': 'abdomen',
	'back': 'back',
	'BAL': 'bronchoalveolar lavage',
	'Bed': 'bed',
	'Blood C&S': 'blood (C&S)',
	'blood': 'blood',
	'bone': 'bone',
	'brain': 'brain',
	'breast': 'breast',
	'bronchial': 'bronchial',
	'Bronchial Wash (Lavage)': 'bronchial wash', # this one is ambigious but I'm gonna assume wash
	'clinic': 'hospital',
	'Clinical': 'clinical',
	'Coastline': 'coastline',
	'Coccyx': 'coccyx',
	'Conjunctiva': 'eye (conjunctiva)',
	'CSF': 'cerebrospinal fluid',
	'Culture': 'culture (unspecified)',
	'CVC': 'central venous catheter',
	'ear': 'ear', # don't want to match heart!
	'epidermis': 'skin',
	'eye': 'eye',
	'farm': 'farm',
	'flank': 'flank',
	'fluid': 'fluid (unspecified)',
	'foot': 'foot',
	'HCU': 'heater-cooler unit', # in MTBC samples it's definitely not homocystinuria
	'heart': 'heart',
	'heel': 'foot',
	'Hospitol': 'hospital', # common typo
	'human': 'clinical',
	'knee': 'knee',
	'laboratory': 'laboratory-obtained strain',
	'leg': 'leg',
	'Lip': 'lip',
	'lung': 'lung', # also in more generic match since specifics of lobes hella common
	'Medical Device': 'medical device (unspecified)',
	'Nares/Axilla': 'nares and/or axilla', # very common for Candida, prevent fuzzy match for Nares/Axilla/Groin
	'ocean': 'ocean',
	'Ostomy': 'ostomy',
	'Pleural Fluid': 'pleural fluid',
	'pus': 'pus',
	'rectal': 'rectum',
	'rectum': 'rectum',
	'river sediment': 'river sediment',
	'scar': 'scar tissue',
	'skin': 'skin',
	'soil': 'soil',
	'spine': 'spine',
	'testes': 'testes', # "testicle" gets a more generic match elsewhere
	'Thigh': 'thigh',
	'throat': 'throat',
	'tissue': 'tissue (unspecified)',
	'toe': 'foot',
	'Toenail': 'foot',
	'Urine': 'urine',
	'Urine, Catheter': 'urine (catheter)',
	'wastewater': 'wastewater',
	'water': 'water',
	'whole organism': 'whole organism',
	'wrist': 'hand'
}


if_this_and_that_then = [
	# specific culture type + tissue
	['(?i)single colony', '(?i)fecal', 'culture (single-colony) from feces'],
	['(?i)single colony', '(?i)lab', 'culture (single-colony) from lab stock'],

	# generic culture + tissue
	['(?i)culture', '(?i)cell', 'culture (cell)'],
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

	# biopsies -- by cutting off the "y" we match plural and French
	['(?i)biops', '(?i)skin', 'biopsy (skin)'],
	['(?i)biops', '(?i)intestine', 'biopsy (intestine)'],
	['(?i)biops', '(?i)thoracic', 'biopsy (thoracic)'],
	['(?i)biops', '(?i)lung', 'biopsy (lung)'],
	['(?i)biops', '(?i)pleura', 'biopsy (pleura/pleural effusion)'],
	['(?i)necropsy', '(?i)lung', 'necropsy (lung tissue)'],
	['(?i)necropsy', '(?i)spleen', 'necropsy (spleen)'],
	['(?i)necropsy', '(?i)kidney', 'necropsy (kidney)'],
	['(?i)cow', '(?i)feces', 'feces (bovine)'],
	['(?i)FFPE', '(?i)skin', 'FFPE block (skin)'],

	# everything else
	['(?i)scrapate', '(?i)granuloma', 'scrapate of granuloma'],
	['(?i)aspirate', '(?i)needle', 'fine needle aspirate'],
	['(?i)core', '(?i)needle', 'core needle biopsy'],
	['(?i)ileal', '(?i)intestin', 'intestinal tissue'],
	['(?i)ascit', '(?i)fluid', 'peritoneal fluid (ascitic)'],
	['(?i)pluera', '(?i)fluid', 'pleural fluid'],

]


################################################################################################################
# These are considered mutually exclusive, and whichever ones are listed first will take precident
# This means, generally speaking, we want more specific first and less specific last... with the
# exception of things that are likely in silico or experimental evolution, as those ones are often
# not appropriate to include in later analysis

comprehensive_fuzzy = {
	
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

	# miscellanous highly specific stuff
	'Aquarium': 'aquarium',
	'Archaeological': 'archaeological',
	'biofilm': 'biofilm',
	'Bed rails': 'bed rails',


	### The Fluid Zone ###
	# ascitic/peritoneal fluid (ascitic is already covered in if-and-then)
	'Intra-abdominal fluid': 'peritoneal fluid',
	'Peritoneal fluid': 'peritoneal fluid',
	# BAL and friends -- BAL is too generic on its own
	'BRL': 'bronchoalveolar lavage',
	'BALF': 'bronchoalveolar lavage',
	'BAL RUL': 'bronchoalveolar lavage',
	'BAL_RUL': 'bronchoalveolar lavage',
	'BAL fluid': 'bronchoalveolar lavage',
	'Lavage - Bronchial': 'bronchoalveolar lavage',
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
	'Trac Asp': 'tracheal aspirate',
	'aspirate trachy': 'tracheal aspirate',
	'BRONCH_WSH': 'bronchial wash',
	'Washing, Bronchial': 'bronchial wash',
	'bronchial wash': 'bronchial wash',
	# CSF
	'Cerebospinal fluid': 'cerebrospinal fluid',
	'cerebrospinal fluid': 'cerebrospinal fluid',
	'cerebrospinalFluid': 'cerebrospinal fluid',
	'cerebral spinal fluid': 'cerebrospinal fluid',
	'Deep fluid spine': 'cerebrospinal fluid (deep puncture)',
	# gastric
	'Gastric lavage': 'gastric lavage',
	'aspirate gastric': 'gastric aspirate',
	'Gastric Aspirate': 'gastric aspirate',
	'stomach contents': 'gastric (stomach contents)',
	'gastric juice': 'gastric fluid',
	'gastric fluid': 'gastric fluid',
	# snot
	'mucus': 'mucus',
	'nasal swab': 'mucus (nasal swab)', # i mean this could be external I guess?
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
	# piss ('Urine, Catheter' already considered in exact match)
	'urine': 'urine',
	'Urine, Clean Catch': 'urine',
	'Uriine': 'urine',
	# other fun fluids
	'bile': 'bile',
	'ear discharge': 'ear discharge',
	'phlegm': 'phlegm',
	'Peritoneal dialysate': 'dialysate (peritoneal)',
	'dialysate': 'dialysate',
	'Respiratory secretions': 'secretion (respiratory)',
	'Liver Perfusate': 'perfusate (liver)',

	# candida-specific body parts, in a specific body part
	'Nares/Axilla/Groin': 'nares/axilla/groin',
	'axilla, groin and nares': 'nares/axilla/groin',
	'axilla/groin': 'axilla and/or groin', # very common for Candida
	'axilla and groin': 'axilla and/or groin', # very common for Candida
	'Axilliae': 'axilla',
	'axilla': 'axilla',
	'underarm': 'axilla',
	'groin': 'groin',
	'Scrotal': 'groin (scrotum)',
	'scrotum': 'groin (scrotum)',
	'nares': 'nares',
	'SWAB_SKIN': 'skin swab',

	# lab stuff
	'laboratory reference strain': 'laboratory strain (reference)',
	'Laboratory obtained strain': 'laboratory strain (unspecified)',
	'Lab strain': 'laboratory strain (unspecified)',
	'laboratory strain': 'laboratory strain (unspecified)',

	# dead
	'Morgue': 'morgue',
	'Abbattoir': 'abbattoir',
	'slaughterhouse': 'abbattoir',
	'necropsy': 'necropsy',

	# owies
	'abscess': 'abscess',
	'caseum': 'caseous mass',
	'CaseousMasses': 'caseous mass',
	'Decubitus': 'decubitus', # needs to be before ulcer
	'lesion': 'lesion',
	'wound': 'wound',
	'Ulcer': 'ulcer',
	'cyst': 'cyst',

	# lymph nodes (specific)
	'Cervical lymph': 'lymph node (cervical)',
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

	# incisions
	'icd pocket': 'ICD pocket',
	'incision': 'incision',

	# i dont even want to know
	'Drainage': 'drainage',
	'excreted bodily substance': 'excreted bodily substance (unspecified)',
	'body fluid': 'bodily fluid (unspecified)',
	'Biofluids': 'bodily fluid (unspecified)',
	'BODY_FLUID': 'bodily fluid (unspecified)',
	'Fluid, Body, NOS': 'bodily fluid (unspecified)',
	'Fluid body': 'bodily fluid (unspecified)',

	# locations
	'farmland': 'farm',
	'ocean': 'ocean',
	'hospitalized': 'clinical', # avoid 'hospital' fuzzy matching 'hospitalized'
	'hospital': 'hospital',
	'air from': 'air',

	# tubes
	'nephrostomy': 'nephrostomy',
	'tracheostomy': 'tracheostomy',
	'cholecystostomy': 'cholecystostomy',
	'exit site (dialysis)': 'dialysis exit site',
	'Exit site (dialysis)': 'dialysis exit site',
	'dialysis catheter': 'catheter (dialysis)',
	'Catheter': 'catheter', # ('Urine, Catheter' already considered in exact match)
	'Catheter Tip': 'catheter',
	'Driveline': 'driveline',
	'stent': 'stent', # beware in-word matches

	### Everything here is last for good reason ###
	# organs / body parts
	'bladder': 'bladder',
	'Testicle': 'testes',
	'Intra-abdominal tissue': 'intra-abdominal tissue',
	'homogenized mouse spleen': 'homogenized mouse spleens', # standardize singular/plural
	'Vaginal': 'vaginal',
	'spleen': 'spleen',
	'lung': 'lung',
	# head
	'Scalp': 'scalp',
	'sphenoid': 'head (sphenoid)', # reclassify to bone?
	# leg 
	'foot': 'foot',
	'toe': 'foot',
	'heel': 'foot',
	'ankle': 'foot',
	'Metatarsal': 'foot (metatarsal)', # reclassify to bone?
	'knee': 'leg (knee)',
	'leg': 'leg',
	'buttock': 'buttock',
	'hip': 'hip',
	'thigh': 'thigh',
	# arm -- MUST come after "underarm" and "farm"
	'elbow': 'arm (elbow)',
	'arm': 'arm',
	'wrist': 'hand',
	'Thumb': 'hand',
	'Shoulder': 'shoulder',

	# fungus/culture
	'mycleia': 'mycelium',
	'spherule': 'spherule',

	# doesn't really name the source
	'Negative Control': 'negative control',
	'PCR product': 'PCR product',

	# lungscore?
	'PULMONARY': 'pulmonary',
	'Respiratory': 'pulmonary',
	'Resp': 'pulmonary',

	# super generic
	'clinical strain': 'clinical strain',
	'clinical isolate': 'clinical',
	'clinical sample': 'clinical',
	'clinical': 'clinical',
	'patient': 'clinical',
	#'hospital': 'clinical', # could be patient, could be environmental
	'diagnostic sample': 'clincal (diagnostic sample)',
	'culture': 'culture (unspecified)',
	'Environmental': 'environmental',
	'Biopsy': 'biopsy',
	'Biopsie': 'biopsy',
	'Secretion': 'secretion',
	'swab': 'swab',
	'bone': 'bone',
	'tissue': 'tissue (unspecified)',

	# culture stuff -- should be done last, as many pathogens are "culture from X body part"
	'lawn on agar plate': 'culture (lawn/sweep)',
	'sweep': 'culture (lawn/sweep)',
	'single colony': 'culture (single colony)',
	'single cell': 'single cell',
	'in vitro': 'culture (unspecified)',
	'in-vitro': 'culture (unspecified)',
	#'bacterial suspension': 'culture', # let's just leave that one as is
	
}

standardized_values = set(list(exact_replacements.values()) + list(comprehensive_fuzzy.values()) + [lst[2] for lst in if_this_and_that_then])

# Anything isolation_source that wholestring exact matches a value in these lists
# will have that match nulled.
exact_null_nonsensical = [
	'1',
	'?',
	'DNA',
	'Genomic DNA',
	'isolate',
	'to wear a mask', # yeah that one is actually there
	'Yes',
	'WGS',
	'Bureau of Tuberculosis',
	'na',
	'nan',
	'no date',
	'no source',
	'other',
	'other source',
	'strain',
	'Specimen',
	'Systemic',
]

# If you are comparing multiple genra at once, and don't know what genus a particular
# sample comes from, but somehow have isolation_source, this might be worth not nulling.
# But by default, these are nulled.
exact_null_generic = [

	# generic bacteria
	'bacteria',
	'bacterial cell',
	'Bacterial isolate',
	'Biological Sample',
	'Biological sample',

	# generic fungus
	'Fungal isolate',
	'fungal strain',
	'fungal cell',
	'fungal isolate, other',

	# Candida / Candidozyma 
	'Candida auris',
	'auris',
	
	# tuberculosis/MTBC/Mycobacterium genus
	'DNA from M. tuberculosis',
	'M. tuberculosis',
	'MTB isolates',
	'Mtb',
	'MTBC',
	'Mycobacterium tuberculosis complex',
	'Mycobacterium tuberculosis',
	'Mycobacteryum tuberculosis', # common typo
	'tuberculosis',
]
