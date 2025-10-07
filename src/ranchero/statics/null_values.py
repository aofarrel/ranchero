# These values are used for turning stuff like "no data" and "not applicable" into null values.
# There are three main ways this is done:
# 1. pl.read_csv(null_values=null_value_list) when importing a CSV/TSV file
# 2. pl.when(pl.col(col).str.contains_any(null_value_list)) and its list equivalent, which doesn't support regex
#    but can be case insensitive via ascii_case_insensitive=True
# 3. pl.when(pl.col(col).str.contains(null_value_str))) and its list equivalent, which supports regex (kind of)
# (In theory .is_in() and .replace() could be used too, but I've had some issues getting those to work as I'd expect.)
# There is no equivalent for #1 in pl.read_json(), so our main focus is going to be the other two methods.


# 1) Whole-string matching, case sensitive, no regex
nulls_CSV = [
	'missing','Missing','MISSING',
	'n/a','N/A',
	'nan','Nan','NaN','NAN',
	'no data','No data','No Data',
	'None','none',
	'not abblicable','not applicable','Not Applicable','Not applicable',
	'Not available','Not Available','not available',
	'not available: not collected',
	'not collected','Not collected','Not Collected','NOT COLLECTED',
	'not known','Not Known', 'Not known',
	'not present',
	'Not Provided','Not provided',
	'Not recorded',
	'Not specified','not specified',
	'null','Null',
    'nan',
    'not determined',
	'-',
	'uncalculated',
	'Unknown','unknown',
	'unspecified','Unspecified',
]

# 2) Anywhere-in-string matching, case insensitive, no regex
# WARNING: This may cause unexpected behavior!
nulls_pl_contains_any = [
	'missing',
	'n/a',
	'no data',
    'none',
	'not abblicable',
	'not applicable',
	'Non applicable',
	'Not available',
	'not collected',
	'not determined',
	'not known',
	'Not Provided',
	'Not recorded',
	'Not specified',
	'null',
	'uncalculated',
	'unknown',
	#'unspecified', --> too generic and causes issues with host_disease "unspecified TB"
]

# 3) Regex permitting, anywhere-in-string matching and case sensitive
nulls_pl_contains = [
	r'^$', # used to drop empty values from lists post-merging
	r'^-$',
]

nulls_pl_contains_plus_NA = nulls_pl_contains + [r'^NA$']
