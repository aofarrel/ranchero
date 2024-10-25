# NOT COMPREHENSIVE BY DESIGN
# Only merges some extremely obvious ones

host_disease_exact_match = {
	'TB': 'Unspecified TB',
	'Tuberculosis': 'Unspecified TB',
	'tuberculosis': 'Unspecified TB',

	# extrapulmonary is sometimes written as two words, so these need to be exact matches
	'Pulmonary TB': 'Pulmonary TB',
	'pulmonary tuberculosis': 'Pulmonary TB',
	'Pulmonary tuberculosis': 'Pulmonary TB',
	'Pulmonary Tuberculosis': 'Pulmonary TB',
}

host_disease = {
	'Mycobacterium tuberculosis': 'Unspecified TB',
	'Mycobacterium tuberculosis infection': 'Unspecified TB',
	'Tuberculosis TB': 'Unspecified TB',
	'TB infection': 'Unspecified TB',
	'TUBERCULOSIS': 'Unspecified TB',
	'Tuberculosis (TB)': 'Unspecified TB',
	'tuberculosis DOID:552': 'Unspecified TB',

	'bovine tuberculosis': 'Bovine TB',
	'Bovine Tuberculosis': 'Bovine TB',
	'Bovine tuberculosis': 'Bovine TB',
	'bovine': 'Bovine TB',

	'Chronic pulmonary tuberculosis': 'Chronic pulmonary TB',

	'Tuberculosis lung infection': 'Pulmonary TB',
	'Tuberculosis, Pulmonary': 'Pulmonary TB',
	'Tuberculosis (pulmonary)': 'Pulmonary TB',
	'Lung tuberculosis': 'Pulmonary TB',
	'pulmonary infection': 'Pulmonary TB',

	'infiltrative tuberculosis': 'Infiltrative TB',

	'refractory tuberculosis': 'Refractory TB',

	'Extrapulmonary tuberculosis': 'Extrapulmonary TB',
	'Extra Pulmonary Tuberculosis': 'Extrapulmonary TB',
	'extrapulmonary tuberculosis': 'Extrapulmonary TB',

	'Spinal tuberculosis': 'Spinal TB',

	'Tuberculous meningitis': 'Meningeal TB',
	'TB Meningitis': 'Meningeal TB',
	'TB meningitis': 'Meningeal TB',

	'Disseminated tuberculosis': 'Disseminated TB',
	'Disseminated Pulmonary tuberculosis': 'Disseminated TB',

	'leprosy': 'Leprosy',
	'Leprosy': 'Leprosy',
	'Lepromatous leprosy': 'Leprosy (Lepromatous)',
	'Diffuse lepromatous leprosy': 'Leprosy (Lucio)',

	'Health': "nan",
	'host_disease_sam': "nan",
	'human': "nan",
	'homo sapiens': "nan",
	'infection': "nan",
	'Infections Sample039': "nan",
}

sample_source_exact_match = {
	'bronchial': 'bronchial (unspecified)',
	'Clinical': 'clinical (unspecified)',
	'laboratory': 'laboratory-obtained strain',
	'tissue': 'tissue (unspecified)',
	'Mycobacterium tuberculosi': "nan",
	'Culture': 'culture',
	'Mtb': "nan",
	'1': "nan",
	'to wear a mask': "nan",
	'Yes': "nan",
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

	'Mycobacterial Culture': 'culture',
	'Bacterial culture': 'culture',
	'bacterial culture': 'culture',
	'standard culture': 'culture',
	'Mycobacterial Culture': 'culture',
	'single culture': 'culture',

	'Sputum culture': 'culture from sputum',
	'sputum culture': 'culture from sputum',
	'culture from sputum': 'culture from sputum',

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
	'Sputum or cerebrospinal fluid': 'sputum',
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


	'pleural fluid': 'pleural fluid',
	'pleuralFluid': 'pleural fluid',

	'Pulmonary Sample': 'pulmonary',
	'PULMONARY': 'pulmonary',

	'other': "nan",
	'Specimen': "nan",
	'Biological Sample': "nan",
	'Biological sample': "nan",
	'DNA from M. tuberculosis': "nan",
	
}