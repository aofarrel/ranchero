# This is for sample sources that often show up in the wrong column, at least
# when working with MTBC data. The reason why we run sample_source through
# this instead of the full country/species dictionaries is just to save time.

exact_one_column_writes = {
	# Ex: "country": {'New Zealand':'NZL', 'Pakistan':'PAK', 'Viet Nam':'VNM'}
	#
	# Search "isolation source" for "New Zealand", "Pakistan", and "Viet Nam". If 
	# hit, remove that match from isolation_source, and put "NZL", "PAK", or "VNM"
	# respectively in the "country" column.

	"country": {'New Zealand':'NZL', 'Pakistan':'PAK', 'Viet Nam':'VNM'},
	"region": {'Lima':'Lima', 'veracruz':'Veracruz'},
	"host_disease":
		{'TB': 'unspecified TB',
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
		'extra/intra - pulmonary patient': 'extra/intra-pulmonary TB'}
}

# Skipped if not mycobacterial_mode
exact_one_column_writes_mycobacterial = {
	"host_disease": {'PTB':'pulmonary TB', 'Pulmonary tuberculosis':'pulmonary TB', 'TBM':'TB meningitis'},
	
	"strain": {'H37Rv': 'H37Rv'},
	"strain_sam_ss_dpl139": {'H37Rv': 'H37Rv'},
	
	"lineage": {'lineage4.6.2.2':'lineage4.6.2.2'},
	"lineage_sam": {'lineage4.6.2.2':'lineage4.6.2.2'},
}

# inner dict value[0] overwrites to isolation_source
# inner dict value[1] writes to outer dict key column if said column is null
exact_two_column_writes = {
	"host":
		{'Affedcted Herd': ['vetrinary', 'bovine'],  # deliberate typo for a bunch of M. bovis samps
		'Homo sapiens': ['patient', 'Homo sapiens'],
		'human': ['patient', 'Homo sapiens'],
		'patient': ['patient', 'patient'], # mid-confidence
		'isolate frome children': ['patient (pediatric)', 'Homo sapiens'],
		'Otaria flavescens': ['vetrinary', 'Otaria flavescens'],
		'Locustana pardilana (brown locust)': ['vetrinary', 'Locustana pardilana']}
}

substring_two_column_writes = {
	"host":
		{'(?i)human|sapiens|children': ['patient', 'Homo sapiens'],
		'(?i)mouse': ['vetrinary', 'mouse'], # mid-confidence
		'(?i)musculus': ['vetrinary', 'Mus musculus'],
		'(?i)cow|taurus': ['vetrinary', 'Bos taurus'],
		'(?i)dairy|beef': ['vetrinary', 'cattle'], # mid-confidence
		'Rhipicephalus': ['vetrinary', 'Rhipicephalus microplus'],
		'(?i)vetrinary|veterinary|animal': ['vetrinary', 'vetrinary']}
}