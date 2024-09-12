# NOT COMPREHENSIVE BY DESIGN
# Only merges some extremely obvious ones

from numpy import nan as nan

host_disease = {
	'TB': 'Unspecified TB',
	'Tuberculosis': 'Unspecified TB',
	'tuberculosis': 'Unspecified TB',
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
	'Pulmonary TB': 'Pulmonary TB',
	'pulmonary tuberculosis': 'Pulmonary TB',
	'Pulmonary tuberculosis': 'Pulmonary TB',
	'Pulmonary Tuberculosis': 'Pulmonary TB',
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

	'Health': nan,
	'host_disease_sam': nan,
	'human': nan,
	'homo sapiens': nan,
	'infection': nan,
	'Infections Sample039': nan,
}

sample_source = {
	'BAL': 'Broncial lavage',
	'Bronchial Lavage': 'Broncial lavage',
	'bronchialLavage': 'Broncial lavage',
	'brochoalveolar lavage': 'Broncial lavage',
	'bronchial': 'Broncial lavage',
	'bronchial aspirate': 'Broncial lavage',
	'bronchialLavage': 'Broncial lavage',
	'Bronchial Alveolar Lavage': 'Broncial lavage',
	'broncho alveolar lavage': 'Broncial lavage',
	'broncho-alveolar lavage': 'Broncial lavage',
	'broncho-alveolar lavage right middle lobe': 'Broncial lavage',
	'bronchoalveolar lavage': 'Broncial lavage',
	'bronchoalveolar lavage fluid': 'Broncial lavage',
	'broncioal lavage': 'Broncial lavage',
	'Bronchio Alveolar Lavage': 'Broncial lavage',
	'Broncho alveolar Lavage Fluid (BAL)': 'Broncial lavage',
	'Broncho alveolar lavage': 'Broncial lavage',
	'Bronchoalveolar lavage BS-2862': 'Broncial lavage',
	'Lavage': 'Broncial lavage',
	'Respiratory, Lower, Bronchoalveolar Lavage': 'Broncial lavage',

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
	'Clinical isolates of M. tuberculosis from human patients': 'Clinical',

	'culture': 'Culture',
	'Mycobacterial Culture': 'Culture',
	'Bacterial culture': 'Culture',
	'bacterial culture': 'Culture',
	'standard culture': 'Culture',
	'Mycobacterial Culture': 'Culture',

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
	
	'clinical: sputum': 'Sputum',
	'Human patient sputum': 'Sputum',
	'human sputum': 'Sputum',
	'Human sputum': 'Sputum',
	'induced sputum': 'Sputum',
	'Induced Sputum': 'Sputum',
	'Induced sputum': 'Sputum',
	'patient sputum': 'Sputum',
	'Respiratory sample (sputum)': 'Sputum',
	'Respiratory, Lower, Sputum-Induced': 'Sputum',
	'Sputa': 'Sputum',
	'Sputum clinical sample': 'Sputum',
	'Sputum collection': 'Sputum',
	'Sputum coughed': 'Sputum',
	'Sputum induced': 'Sputum',
	'Sputum or cerebrospinal fluid': 'Sputum',
	'sputum patient': 'Sputum',
	'Sputum Sample': 'Sputum',
	'Sputum sample': 'Sputum',
	'sputum sample': 'Sputum',
	'sputum samples': 'Sputum',
	'sputum specimen': 'Sputum',
	'Sputum specimens': 'Sputum',
	'sputum throat swab': 'Sputum',
	'sputum': 'Sputum',
	'sputum, respiratory sample': 'Sputum',
	'sputum, respiratory sample': 'Sputum',
	'Sputum1': 'Sputum',
	'Sputum10': 'Sputum',
	'Sputum11': 'Sputum',
	'Sputum12': 'Sputum',
	'Sputum13': 'Sputum',
	'Sputum14': 'Sputum',
	'Sputum15': 'Sputum',
	'Sputum16': 'Sputum',
	'Sputum17': 'Sputum',
	'Sputum18': 'Sputum',
	'Sputum19': 'Sputum',
	'Sputum2': 'Sputum',
	'Sputum20': 'Sputum',
	'Sputum3': 'Sputum',
	'Sputum4': 'Sputum',
	'Sputum5': 'Sputum',
	'Sputum6': 'Sputum',
	'Sputum7': 'Sputum',
	'Sputum8': 'Sputum',
	'Sputum9': 'Sputum',


	'pleural fluid': 'Pleural fluid',
	'pleuralFluid': 'Pleural fluid',

	'Pulmonary Sample': 'Pulmonary',

	'tissue': 'Tissue',

	'other': nan,
	'Specimen': nan,
	'Biological Sample': nan,
	'Biological sample': nan,
	'DNA from M. tuberculosis': nan,
	
}