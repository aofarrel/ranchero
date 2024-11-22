# NOT COMPREHENSIVE BY DESIGN
# Only merges some extremely obvious ones

host_disease_exact_match = {
	'TB': 'Unspecified TB',
	'Tuberculosis': 'Unspecified TB',
	'tuberculosis': 'Unspecified TB',
	'DOID:552': 'Pneumonia',
	'DOID:399': 'Unspecified TB',
	'DOID:2957': 'Pulmonary TB',
	'DOID:9861': 'Miliary TB',
	'DOID:4962': 'Pericardial TB',
	'DOID:106': 'Pleural TB',
	'DOID:1639': 'Skeletal TB',
	'leprosy': 'Leprosy',

	# extrapulmonary is sometimes written as two words, so these need to be exact matches
	'Pulmonary': 'Pulmonary TB',
	'PTB': 'Pulomonary TB'
}

host_disease = {
	'tuberculosis DOID:552': 'TB-associated pneumonia',
	'Mycobacterium tuberculosis': 'Unspecified TB',
	'Mycobacterium tuberculosis infection': 'Unspecified TB',
	'Tuberculosis TB': 'Unspecified TB',
	'TB infection': 'Unspecified TB',
	'TUBERCULOSIS': 'Unspecified TB',
	'Tuberculosis (TB)': 'Unspecified TB',

	'bovine tuberculosis': 'Bovine TB',
	'Bovine Tuberculosis': 'Bovine TB',
	'Bovine tuberculosis': 'Bovine TB',
	'bovine': 'Bovine TB',

	'skeletal': 'Skeletal TB',
	'Pericardial': 'Pericardial TB',
	'miliary': 'Miliary TB',
	'Spinal': 'Spinal TB',

	'Chronic pulmonary tuberculosis': 'Pulmonary TB (chronic)',

	'Tuberculosis lung infection': 'Pulmonary TB',
	'Tuberculosis, Pulmonary': 'Pulmonary TB',
	'Tuberculosis (pulmonary)': 'Pulmonary TB',
	'pulmonary infection': 'Pulmonary TB',

	'infiltrative': 'Infiltrative TB',

	'refractory': 'Refractory TB',

	'Extrapulmonary': 'Extrapulmonary TB',
	'Extra Pulmonary': 'Extrapulmonary TB',

	'Tuberculous meningitis': 'Meningeal TB',
	'TB meningitis': 'Meningeal TB',

	'Disseminated': 'Disseminated TB',

	'Lepromatous leprosy': 'Leprosy (Lepromatous)',
	'Diffuse lepromatous leprosy': 'Leprosy (Lucio)',

	'Health': None,
	'host_disease_sam': None,
	'human': None,
	'homo sapiens': None,
	'infection': None,
	'Infections Sample039': None,
}

sample_sources_nonspecific  = [
	'DNA',
	'Mycobacterium tuberculosis',
	'tuberculosis',
	'H37Rv', # standardize_sample_source_as_list() will put this in taxoncore first, standardize_sample_source_as_string() will not
	'Homo sapiens',
	'human',
	'Mtb',
	'MTBC',
	'1',
	'bovine',
	'nan',
	'Genomic DNA',
	'dairy cow',
	'Viet Nam',
	'to wear a mask',
	'Yes',
	'other',
	'Specimen',
	'Biological Sample',
	'DNA from M. tuberculosis'
]

sample_source_exact_match = {
	'bronchial': 'bronchial (unspecified)',
	'Clinical': 'clinical (unspecified)',
	'laboratory': 'laboratory-obtained strain',
	'tissue': 'tissue (unspecified)',
	'Culture': 'culture',
	'Sputum': 'sputum',
	'Hospitol': 'hospital',
}

sample_source = {
	'BAL': 'bronchoalveolar lavage',
	'Bronchial Lavage': 'bronchoalveolar lavage',
	'bronchialLavage': 'bronchoalveolar lavage',
	'brochoalveolar lavage': 'bronchoalveolar lavage',
	'bronchial aspirate': 'bronchoalveolar lavage',
	'bronchialLavage': 'bronchoalveolar lavage',
	'Bronchial Alveolar Lavage': 'bronchoalveolar lavage',
	'broncho alveolar lavage': 'bronchoalveolar lavage',
	'broncho-alveolar lavage': 'bronchoalveolar lavage',
	'broncho-alveolar lavage right middle lobe': 'bronchoalveolar lavage',
	'bronchoalveolar lavage': 'bronchoalveolar lavage',
	'bronchoalveolar lavage fluid': 'bronchoalveolar lavage',
	'broncioal lavage': 'bronchoalveolar lavage',
	'Bronchio Alveolar Lavage': 'bronchoalveolar lavage',
	'Broncho alveolar Lavage Fluid (BAL)': 'bronchoalveolar lavage',
	'Broncho alveolar lavage': 'bronchoalveolar lavage',
	'Bronchoalveolar lavage BS-2862': 'bronchoalveolar lavage',
	'Lavage': 'bronchoalveolar lavage',
	'Respiratory, Lower, Bronchoalveolar Lavage': 'bronchoalveolar lavage',

	'bronchial wash': 'bronchial wash',
	'Bronchial Wash': 'bronchial wash',

	'cerebrospinal fluid': 'cerebrospinal fluid',
	'cerebrospinalFluid': 'cerebrospinal fluid',
	'cerebral spinal fluid': 'cerebrospinal fluid',
	'CSF': 'cerebrospinal fluid',
	'CSF sample': 'cerebrospinal fluid',
	'Cerebospinal fluid': 'cerebrospinal fluid',

	'clinical isolate': 'clinical (unspecified)',
	'clinical sample': 'clinical (unspecified)',
	'Human clinical isolate': 'clinical (unspecified)',
	'Clinical isolates of M. tuberculosis from human patients': 'clinical (unspecified)',

	'clinical strain': 'clinical strain',

	'Mycobacterial Culture': 'culture',
	'Bacterial culture': 'culture',
	'bacterial culture': 'culture',
	'standard culture': 'culture',
	'Mycobacterial Culture': 'culture',
	'MTBC Culture Isolate': 'culture',
	'single culture': 'culture',

	'Sputum culture': 'culture from sputum',
	'sputum culture': 'culture from sputum',
	'culture from sputum': 'culture from sputum',

	'homogenized mouse spleen': 'homogenized mouse spleens', # standardize singular/plural

	'Laboratory experiment': 'laboratory, experimental evolution',
	'laboratory evolution': 'laboratory, experimental evolution',

	'Laboratory obtained strain': 'laboratory-obtained strain',
	'laboratory reference strain': 'laboratory reference strain',
	'Lab strain': 'laboratory-obtained strain',
	'lab strain': 'laboratory-obtained strain',

	'right lung': 'lung (right)',
	'left lung': 'lung (left)',
	'tissue lung': 'lung',
	'lungs': 'lung',
	'Lungs': 'lung',
	'lung sample': 'lung',
	'Lung samples': 'lung',
	'Lung tissues': 'lung',
	'Lung tissue': 'lung',
	'lung': 'lung',

	# lymph node
	'lymph node': 'Lymph node',
	'Lymph Node Biopsy': 'Lymph node',

	# lymph nodes
	'Tissue: Lymph nodes': 'Lymph nodes',
	'Tissue: lymph nodes': 'Lymph nodes',
	'lymph nodes': 'Lymph nodes',

	'Cervical lymphnode biopsy': 'Lymph node (cervical)',
	'Cervical lymph node': 'Lymph node (cervical)',
	'Lung lymph node': 'Lymph node (lung)',
	'Head lymph node': 'Lymph node (head)',
	'Pectoral lymph nodes': 'Lymph node (pectoral)',
	
	'clinical: sputum': 'sputum',
	'Human patient sputum': 'sputum',
	'human sputum': 'sputum',
	'Human sputum': 'sputum',
	'induced sputum': 'sputum',
	'Induced Sputum': 'sputum',
	'Induced sputum': 'sputum',
	'patient sputum': 'sputum',
	'Respiratory sample (sputum)': 'sputum',
	'Respiratory, Lower, Sputum-Induced': 'sputum',
	'Sputa': 'sputum',
	'sputum_': 'sputum', # sputum_221, etc
	'Sputum clinical sample': 'sputum',
	'Sputum collection': 'sputum',
	'Sputum coughed': 'sputum',
	'Sputum induced': 'sputum',
	'sputum patient': 'sputum',
	'Sputum Sample': 'sputum',
	'Sputum sample': 'sputum',
	'sputum sample': 'sputum',
	'sputum samples': 'sputum',
	'sputum specimen': 'sputum',
	'Sputum specimens': 'sputum',
	'sputum throat swab': 'sputum',
	'sputum, respiratory sample': 'sputum',
	'sputum, respiratory sample': 'sputum',
	'Sputum1': 'sputum',
	'Sputum10': 'sputum',
	'Sputum11': 'sputum',
	'Sputum12': 'sputum',
	'Sputum13': 'sputum',
	'Sputum14': 'sputum',
	'Sputum15': 'sputum',
	'Sputum16': 'sputum',
	'Sputum17': 'sputum',
	'Sputum18': 'sputum',
	'Sputum19': 'sputum',
	'Sputum2': 'sputum',
	'Sputum20': 'sputum',
	'Sputum3': 'sputum',
	'Sputum4': 'sputum',
	'Sputum5': 'sputum',
	'Sputum6': 'sputum',
	'Sputum7': 'sputum',
	'Sputum8': 'sputum',
	'Sputum9': 'sputum',
	'AFB sputum smear': 'sputum (AFB smear)',


	'pleural fluid': 'pleural fluid',
	'pleuralFluid': 'pleural fluid',

	'Pulmonary Sample': 'pulmonary',
	'PULMONARY': 'pulmonary',
}