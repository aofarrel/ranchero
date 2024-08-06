# NOT COMPREHENSIVE BY DESIGN
# Only merges some extremely obvious ones

from numpy import nan as nan

sample_source = {
	'Bronchial Lavage': 'Broncial lavage',
	'bronchialLavage': 'Broncial lavage',

	'bronchial wash': 'Bronchial wash',
	'Bronchial Wash': 'Bronchial wash',

	'cerebrospinal fluid': 'Cerebrospinal fluid',
	'cerebrospinalFluid': 'Cerebrospinal fluid',
	'cerebral spinal fluid': 'Cerebrospinal fluid',
	'CSF': 'Cerebrospinal fluid',
	'CSF sample': 'Cerebrospinal fluid',
	'Cerebospinal fluid': 'Cerebrospinal fluid',

	'clinical isolate': 'Clinical',
	'clinical': 'Clinical',
	'clinical sample': 'Clinical',
	'Human clinical isolate': 'Clinical',

	'culture': 'Culture',
	'Mycobacterial Culture': 'Culture',
	'Bacterial culture': 'Culture',
	'bacterial culture': 'Culture',
	'standard culture': 'Culture',

	'Sputum culture': 'Culture from sputum',
	'sputum culture': 'Culture from sputum',
	'culture from sputum': 'Culture from sputum',

	'laboratory': 'Laboratory',
	'Laboratory obtained strain': 'Laboratory-obtained strain',
	'laboratory reference strain': 'Laboratory reference strain',
	'Lab strain': 'Laboratory-obtained strain',
	'lab strain': 'Laboratory-obtained strain',

	'right lung': 'Lung (right)',
	'left lung': 'Lung (left)',
	'tissue lung': 'Lung',
	'lungs': 'Lung',
	'Lungs': 'Lung',
	'lung sample': 'Lung',
	'Lung samples': 'Lung',
	'Lung tissues': 'Lung',
	'Lung tissue': 'Lung',
	'lung': 'Lung',

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
	
	'sputum': 'Sputum',
	'sputum sample': 'Sputum',
	'Sputum sample': 'Sputum',
	'Sputum Sample': 'Sputum',
	'Human sputum': 'Sputum',
	'sputum specimen': 'Sputum',
	'Sputum specimens': 'Sputum',
	'sputum patient': 'Sputum',
	'patient sputum': 'Sputum',
	'Sputum collection': 'Sputum',

	'pleural fluid': 'Pleural fluid',
	'pleuralFluid': 'Pleural fluid',

	'Pulmonary Sample': 'Pulmonary',

	'tissue': 'Tissue',

	'other': nan,
	'Specimen': nan,
	
}