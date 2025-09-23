# Called by standardize.py's dictionary_match() function -- not case sensitive

# Only called if mycobacterial_mode == True
host_disease_exact_match_mycobacterial = {
	'TBM': 'TB meningitis',
	'Pulmonary': 'pulmonary TB', # distinguish from "extra pulmonary"
	'PTB': 'pulomonary TB',
	'Lepromatous leprosy': 'leprosy (Lepromatous)', # distinguish from diffuse lepromatous leprosy
}

host_disease_exact_match = {
	'TB': 'unspecified TB',
	'Tuberculosis': 'unspecified TB',
	'tuberculosis': 'unspecified TB',
	'Tuberculose': 'unspecified TB',
	'Tuberculosis TB': 'unspecified TB',
	'Tuberculosis (TB)': 'unspecified TB',
	'DOID:552': 'pneumonia',
	'DOID:399': 'unspecified TB',
	'DOID:2957': 'pulmonary TB',
	'DOID:9861': 'miliary TB',
	'DOID:4962': 'pericardial TB',
	'DOID:106': 'pleural TB',
	'DOID:1639': 'skeletal TB',
	'leprosy': 'leprosy',
	'extra/intra - pulmonary patient': 'extra/intra-pulmonary TB',
}

host_disease_substring_match = {
	'Chronic pulmonary tuberculosis': 'pulmonary TB (chronic)',
	'Diffuse lepromatous leprosy': 'leprosy (Lucio)',
	'Mycobacterium tuberculosis infection': 'unspecified TB',
	'Tuberculous meningitis': 'TB meningitis',
	'TB infection': 'unspecified TB',
	'TB meningitis': 'TB meningitis',
	'tuberculosis DOID:552': 'TB-associated pneumonia',
	'johnes disease': "paratuberculosis (Johne's disease)"
}

host_disease_substring_match_mycobacterial = {
	'bovine': 'bovine TB',
	'Disseminated': 'disseminated TB',
	'Extra Pulmonary': 'extrapulmonary TB',
	'Extrapulmonary': 'extrapulmonary TB',
	'infiltrative': 'infiltrative TB',
	'miliary': 'miliary TB',
	'Pericardial': 'pericardial TB',
	'Pleural': 'pleural TB',
	'refractory': 'refractory TB',
	'skeletal': 'skeletal TB',
	'Spinal': 'spinal TB',
	'Buruli ulcer': 'Buruli ulcer',

	# do last to avoid matches to "extra pulmonary" and "lung infection"
	'pulmonary': 'Pulmonary TB',
	'Health': None,
	'host_disease_sam': None,
	'human': None,
	'homo sapiens': None,
	'infection': None,
	'Infections Sample039': None,
}