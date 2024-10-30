# These values are used for turning stuff like "no data" and "not applicable" into null values.


# This is used for matching on columns of type pl.List(pl.Utf8), which are lists of strings.
# You CAN, and likely should, keep the empty string in this list. Also, you can't use regex here.
null_values = [
	'', # intentional empty string
	'missing',
	'Missing',
	'MISSING',
	'n/a',
	'N/A',
	'nan',
	'Nan',
	'NaN',
	'NAN',
	'no data',
	'No data',
	'No Data',
	'not abblicable',
	'not applicable',
	'Not Applicable',
	'Not applicable',
	'Not available',
	'Not Available',
	'not available',
	'not available: not collected',
	'not collected',
	'Not collected',
	'Not Collected',
	'NOT COLLECTED',
	'not known',
	'Not Provided',
	'Not provided',
	'Not specified',
	'not specified',
	'null',
	'Null',
	'uncalculated',
	'Unknown'
	'unknown',
	'unspecified',
	'Unspecified',
]

# this is its own thing to avoid issues with "NA" being "North America"
null_values_plus_NA = null_values.append("NA")

null_values_dictionary = {key: None for key in null_values}

# This is used for matching on columns of type pl.Utf8, which are strings.
# YOU CANNOT HAVE AN EMPTY STRING HERE or else it will turn all values into null.
null_values_regex = [
	r'(?i)missing',
	r'(?i)n/a',
	r'(?i)nan',
	r'(?i)no data',
	r'(?i)not applicable',
	r'(?i)Not available',
	r'(?i)not available: not collected',
	r'(?i)not collected',
	r'(?i)not known',
	r'(?i)Not Provided',
	r'(?i)Not specified',
	r'(?i)null',
	r'(?i)uncalculated',
	r'(?i)unknown',
	r'(?i)unspecified',
]
null_values_regex_plus_NA = null_values.append("\bNA\b")
