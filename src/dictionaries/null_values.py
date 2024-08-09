import polars as pl

null_values = [
	'missing',
	'Missing',
	'MISSING',

	'n/a',
	'N/A',
	
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

	'unspecified',
	'Unspecified',
	
	'unknown',
	'Unknown'
]

# this is its own thing to avoid issues with "NA" being "North America"
null_values_plus_NA = null_values.append("NA")

null_values_dictionary = {key: None for key in null_values}

null_values_regex = "(?i)\b(missing|not applicable|nan|no data|not collected|unspecified|unknown)\b"