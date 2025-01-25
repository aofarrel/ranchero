# NOT COMPREHENSIVE BY DESIGN
# Only merges some extremely obvious ones

host_disease_exact_match = {
	'TB': 'unspecified TB',
	'Tuberculosis': 'unspecified TB',
	'tuberculosis': 'unspecified TB',
	'DOID:552': 'pneumonia',
	'DOID:399': 'unspecified TB',
	'DOID:2957': 'pulmonary TB',
	'DOID:9861': 'miliary TB',
	'DOID:4962': 'pericardial TB',
	'DOID:106': 'pleural TB',
	'DOID:1639': 'skeletal TB',
	'leprosy': 'Leprosy',
	'extra/intra - pulmonary patient': 'extra/intra-pulmonary TB',
	'TBM': 'TB meningitis',

	# extrapulmonary is sometimes written as two words, so these need to be exact matches
	'Pulmonary': 'Pulmonary TB',
	'PTB': 'Pulomonary TB'
}

host_disease = {
	'bovine': 'bovine TB',
	'Chronic pulmonary tuberculosis': 'pulmonary TB (chronic)',
	'Diffuse lepromatous leprosy': 'leprosy (Lucio)',
	'Disseminated': 'disseminated TB',
	'Extra Pulmonary': 'extrapulmonary TB',
	'Extrapulmonary': 'extrapulmonary TB',
	'infiltrative': 'infiltrative TB',
	'Lepromatous leprosy': 'leprosy (Lepromatous)',
	'miliary': 'miliary TB',
	'Mycobacterium tuberculosis infection': 'unspecified TB',
	'Pericardial': 'pericardial TB',
	'Pleural': 'pleural TB',
	'refractory': 'refractory TB',
	'skeletal': 'skeletal TB',
	'Spinal': 'spinal TB',
	'TB infection': 'unspecified TB',
	'TB meningitis': 'TB meningitis',
	'tuberculosis DOID:552': 'TB-associated pneumonia',
	'Tuberculous meningitis': 'TB meningitis',

	# do last to avoid matches to "extra pulmonary" and "lung infection"
	'pulmonary': 'Pulmonary TB',
	'Health': None,
	'host_disease_sam': None,
	'human': None,
	'homo sapiens': None,
	'infection': None,
	'Infections Sample039': None,
}

sample_sources_nonspecific  = [
	'1',
	'?',
	'Affedcted Herd',
	'bacteria',
	'bacterial cell',
	'Bacterial isolate',
	'Biological Sample',
	'Biological sample',
	'Bureau of Tuberculosis',
	'DNA from M. tuberculosis',
	'DNA',
	'Genomic DNA',
	'H37Rv', # standardize_sample_source_as_list() will put this in taxoncore first, standardize_sample_source_as_string() will not
	'Homo sapiens',
	'human',
	'isolate frome children',
	'Lima', # location
	'M. tuberculosis',
	'MTB isolates',
	'Mtb',
	'MTBC',
	'Mycobacterium tuberculosis complex', # I should hope so!
	'Mycobacterium tuberculosis',
	'Mycobacteryum tuberculosis', # common typo
	'na',
	'nan',
	'New Zealand', # location
	'no date',
	'no source',
	'other',
	'Pakistan', # location
	'PTB',
	'Pulmonary tuberculosis',
	'Specimen',
	'TBM',
	'to wear a mask',
	'tuberculosis',
	'veracruz', # location
	'Viet Nam',
	'whole organism',
	'Yes',
]

sample_source_exact_match = {
	'BAL': 'bronchoalveolar lavage',
	'bronchial': 'bronchial (unspecified)',
	'Clinical': 'clinical (unspecified)',
	'Culture': 'culture',
	'Hospitol': 'hospital', # common typo
	'laboratory': 'laboratory-obtained strain',
	'tissue': 'tissue (unspecified)',
}

if_this_and_that_then = [
	# specific culture type + tissue
	['(?i)single colony', '(?i)fecal', 'culture (single-colony) from feces'],
	['(?i)single colony', '(?i)lab', 'culture (single-colony) from lab stock'],

	# generic culture + tissue
	['(?i)culture', '(?i)sputum', 'culture from sputum'],
	['(?i)culture', '(?i)\bbronch.*lavage', 'culture from bronchoalveolar lavage'],
	['(?i)culture', '(?i)cerebrospinal', 'culture from cerebrospinal fluid'],
	['(?i)culture', '(?i)lung', 'culture from lung tissue'],
	['(?i)culture', '(?i)pleural fluid', 'culture from pleural fluid'],
	['(?i)culture', '(?i)feces|fecal', 'culture from feces'],
	['(?i)culture', '(?i)liver', 'culture from liver'],
	['(?i)culture', '(?i)eye', 'culture from eye'],
	
	# everything else
	['(?i)scrapate', '(?i)granuloma', 'scrapate of granuloma'],
	['(?i)biopsy', '(?i)skin', 'biopsy from skin'],
	['(?i)biopsy', '(?i)intestine', 'biopsy from intestine'],
	['(?i)biopsy', '(?i)thoracic', 'biopsy from thorax'],
	['(?i)biopsy', '(?i)pleura', 'biopsy from pleura/pleural effusion'],
	['(?i)necropsy', '(?i)lung', 'necropsy from lung tissue'],
	['(?i)necropsy', '(?i)spleen', 'necropsy from spleen'],
	['(?i)cow', '(?i)feces', 'feces (bovine)'],
	['(?i)FFPE', '(?i)skin', 'FFPE block (skin)'],

	['(?i)ascit', '(?i)fluid', 'ascitic fluid'],
]

# These are considered mutually exclusive
sample_source = {
	# do this one FIRST
	'simulated/in silico': 'simulated/in silico', # bring over matches from earlier into the correct column
	'lawn on agar plate': 'culture (lawn/sweep)',
	'sweep': 'culture (lawn/sweep)',
	'single colony': 'culture (single colony)',
	'single cell': 'single cell',
	
	'Archaeological': 'archaeological',

	### The Fluid Zone ###

	# BAL and friends -- BAL is too generic on its own
	'BRL': 'bronchoalveolar lavage',
	'BALF': 'bronchoalveolar lavage',
	'\bbronch.*lavage': 'bronchoalveolar lavage',
	'bronchialLavage': 'bronchoalveolar lavage',
	'bronchial aspirate': 'bronchoalveolar aspirate',
	'bronchial wash': 'bronchial wash',
	# CSF
	'cerebrospinal fluid': 'cerebrospinal fluid',
	'cerebrospinalFluid': 'cerebrospinal fluid',
	'cerebral spinal fluid': 'cerebrospinal fluid',
	'CSF': 'cerebrospinal fluid',
	# gastric
	'Gastric lavage': 'gastric lavage',
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
	# other
	'blood': 'blood',
	'synovial': 'synovial fluid', # joint fluid

	# organs
	'bone': 'bone',
	'homogenized mouse spleen': 'homogenized mouse spleens', # standardize singular/plural
	'lung': 'lung',
	'skin': 'skin',
	'epidermis': 'skin',

	'biofilm': 'biofilm',

	# environemntal -- excludes "farm" as those are all tissue samples
	'soil': 'environmental (soil)',
	'river sediment': 'environmental (river sediment)',
	'air from': 'environmental (air)',
	'HCU': 'environmental (HCU)', # do BEFORE water
	'water': 'environmental (water)',
	
	# lab stuff
	'Laboratory experiment': 'laboratory, experimental evolution',
	'laboratory evolution': 'laboratory, experimental evolution',
	'Laboratory obtained strain': 'laboratory-obtained strain',
	'laboratory reference strain': 'laboratory reference strain',
	'Lab strain': 'laboratory-obtained strain',
	'lab strain': 'laboratory-obtained strain',

	# dead
	'Morgue': 'necropsy (morgue)',
	'Abbattoir': 'necropsy (abbattoir)',
	'slaughterhouse': 'necropsy (abbattoir)',

	# owies
	'abscess': 'abscess',
	'caseum': 'caseous mass',
	'CaseousMasses': 'caseous mass',
	'lesion': 'lesion',
	'wound': 'wound',
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
	'animal waste': 'feces',

	# lungscore?
	'PULMONARY': 'pulmonary',

	# do these last to avoid overwrites
	'culture': 'culture (unspecified)',
	'in vitro': 'culture (unspecified)',
	'in-vitro': 'culture (unspecified)',
	'clinical strain': 'clinical strain',
	'clinical isolate': 'clinical (unspecified)',
	'clinical sample': 'clinical (unspecified)',
	'Environmental': 'environmental (unspecified)',
	'clinical': 'clinical (unspecified)',
	'lawn on agar plate': 'culture (lawn/sweep)',
	'hospital': 'clinical (unspecified)',
	'Negative Control': 'negative control',
	'bacterial suspension': 'culture (unspecified)',
}

# unlike the above, 
harsh_matching = {
	"animal": "a"
}



