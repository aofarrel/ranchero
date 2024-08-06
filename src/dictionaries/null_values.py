null_values = [
	'missing',
	'Missing',
	
	'not applicable',
	'Not Applicable',
	'Not applicable',
	
	'nan',
	'Nan',
	'NaN',
	'NAN',
	
	'no data',
	'No data',
	'No Data',
	
	'not collected',
	'Not collected',
	'Not Collected',

	'null',
	'Null',
	
	'unknown',
	'Unknown'
]

# this is its own thing to avoid issues with "NA" being "North America"
null_values_plus_NA = null_values.append("NA")