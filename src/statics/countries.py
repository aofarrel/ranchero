# Used to standardize the names of countries into their three-letter ISO 3166 codes
# Notes:
# * Some older country names are used for compatiability with older data (eg: East Timor vs Timor-Leste)
# * This should only be used for complete string matching, not substring matching, thanks to situations
#   such as "Korea" or "Congo" representing more than one country
# * Exact match doesn't need regex flags at it uses pl.when(pl.col(match_column) == f"(?i){key}")
# * Fuzzy match uses pl.when(pl.col(match_column).str.contains(f"(?i){key}")) 

exact_match_problematic_substrings = {
	'hospital': None,                  # unhelpful
	'Mali': 'MLI',                     # prevent match with SoMALIa
	'Niger': 'NER',                    # prevent match with Nigeria
	'patient': None,                   # unhelpful
	'Republic of the Congo': 'COD',    # prevent match with COG
	'The Congo': 'COG',                # prevent match with COD
	'United States': 'USA',            # prevent match with VIR/UMI
	'South Africa': 'ZAF'              # prevent match with the general region
}

exact_match_shorthands = {
	'DPRK': 'PRK',
	'GB': 'GBR',
	'IRE': 'IRL',
	'PRC': 'CHN',
	'UK': 'GBR',
	'US': 'USA'
}

exact_match_common_typos = {
	'Argentia': 'ARG',
	'Ethopia': 'ETH',
	'Marocco': 'MAR'
}

exact_match_ofarrells_wrath = {
	# Look, CIV, I have an apostrophe in my name too. I get it.
	# Fuzzy match *should* be okay here, but let's not risk it.
	"Cote D Ivoire": 'CIV',
	"Cote d''Ivoire": 'CIV',
	"Cote d'Ivoire": 'CIV',
	"Côte d'Ivoire": 'CIV',
	"Cote d\'Ivoire": 'CIV',
	"Republic of Côte d'Ivoire": 'CIV',
	"Cote d_Ivoire": 'CIV',
	"Ivory Coast": 'CIV',
	"IVORY_COAST": 'CIV'
}

exact_match = {**exact_match_problematic_substrings, **exact_match_shorthands, **exact_match_common_typos, **exact_match_ofarrells_wrath}

substring_match = {
	'Afghanistan': 'AFG',
	'Albania': 'ALB',
	'Algeria': 'DZA',
	'Angola': 'AGO',
	'Argentina': 'ARG',
	'Aruba': 'ABW',
	'Australia': 'AUS',
	'Austria': 'AUT',
	'Azerbaijan': 'AZE',
	'Bangladesh': 'BGD',
	'Belarus': 'BLR',
	'Belgium': 'BEL',
	'Benin': 'BEN',
	'Bhutan': 'BTN',
	'Bosnia and Herzegovina': 'BIH',
	'Botswana': 'BWA',
	'Brazil': 'BRA',
	'Britain': 'GBR',
	'Britain': 'GBR', # substring matches Great Britain, Kingdom of Great Britain, etc
	'British Virgin Islands': 'VGB',
	'Bulgaria': 'BGR',
	'Burkina Faso': 'BFA',
	'Burundi': 'BDI',
	'Cambodia': 'KHM',
	'Cameroon': 'CMR',
	'Canada': 'CAN',
	'Cape Verde': 'CPV',
	'Central African Republic': 'CAF',
	'Chile': 'CHL',
	'China': 'CHN',
	'Colombia': 'COL',
	'Comoros': 'COM',
	'Croatia': 'HRV',
	'Czech Republic': 'CZE',
	'Czechia': 'CZE',
	'Democratic Republic of the Congo': 'COD',
	'Denmark': 'DNK',
	'Djibouti': 'DJI',
	'Dominican Republic': 'DOM',
	'East Timor': 'TLS',
	'Ecuador': 'ECU',
	'Egypt': 'EGY',
	'El Salvador': 'SLV',
	'Eritrea': 'ERI',
	'Estonia': 'EST',
	'Eswatini': 'SWZ',
	'Ethiopia': 'ETH',
	'Finland': 'FIN',
	'France': 'FRA',
	'Gabon': 'GAB',
	'Gambia': 'GMB',
	'Georgia': 'GEO',
	'Germany': 'DEU',
	'Ghana': 'GHA',
	'Gibraltar': 'GIB',
	'Greece': 'GRC',
	'Greenland': 'GRL',
	'Guadeloupe': 'GLP',
	'Guam': 'GUM',
	'Guatemala': 'GTM',
	'Guinea': 'PNG',
	'Haiti': 'HTI',
	'Honduras': 'HND',
	'Hong Kong': 'HKG',
	'Hungary': 'HUN',
	'India': 'IND',
	'Indonesia': 'IDN',
	'Iran': 'IRN',
	'Iraq': 'IRQ',
	'Ireland': 'IRL',
	'Israel': 'ISR',
	'Italy': 'ITA',
	'Jamaica': 'JAM',
	'Japan': 'JPN',
	'Kazakhstan': 'KAZ',
	'Kenya': 'KEN',
	'Korea': 'KOR',
	'Kuwait': 'KWT',
	'Kyrgyzstan': 'KGZ',
	'Laos': 'LAO',
	'Latvia': 'LVA',
	'Lebanon': 'LBN',
	'Liberia': 'LBR',
	'Libya': 'LBY',
	'Lithuania': 'LTY',
	'Macedonia': 'MKD', # substring matches North Macedonia, former Yugoslav Republic of Macedonia, etc
	'Madagascar': 'MDG',
	'Malawi': 'MWI',
	'Malaysia': 'MYS',
	'Malta': 'MLT',
	'Martinique': 'MTQ',
	'Mayotte': 'MYT',
	'Mexico': 'MEX',
	'Moldova': 'MDA',
	'Mongolia': 'MNG',
	'Montenegro': 'MNE',
	'Morocco': 'MAR',
	'Mozambique': 'MOZ',
	'Myanmar': 'MMR',
	'Namibia': 'NAM',
	'Nepal': 'NPL',
	'Netherlands': 'NLD', # substring matches The Netherlands
	'New Caledonia': 'NCL',
	'New Zealand': 'NZL',
	'Nigeria': 'NGA',
	'North Korea': 'PRK',
	'North Macedonia': 'MKD',
	'Northern Mariana Islands': 'MNP',
	'Norway': 'NOR',
	'Oman': 'OMN',
	'Pakistan': 'PAK',
	'Palau': 'PLW',
	'Palestine': 'PAL',
	'Panama': 'PAN',
	'Papua New Guinea': 'PNG',
	'Paraguay': 'PRY',
	'Peru': 'PER',
	'Philippines': 'PHL', # substring matches The Philipines
	'Poland': 'POL',
	'Portugal': 'PRT',
	'Romania': 'ROU',
	'Russia': 'RUS', # substring matches Russian Federation
	'Rwanda': 'RWA',
	'Saudi Arabia': 'SAU',
	'Senegal': 'SEN',
	'Serbia': 'SRB',
	'Sierra Leone': 'SLE',
	'Singapore': 'SGP',
	'Slovakia': 'SLK',
	'Slovenia': 'SVN',
	'Somalia': 'SOM',
	'South Korea': 'KOR',
	'Spain': 'ESP',
	'Sri Lanka': 'LKA',
	'Suden': 'SDN',
	'Suriname': 'SUR',
	'Swaziland': 'SWZ',
	'Sweden': 'SWE',
	'Switzerland': 'CHE',
	'Syria': 'SYR', # substring matches Syrian Arab Republic
	'Taiwan': 'TWN',
	'Tajikistan': 'TJK',
	'Tanzania': 'TZA',
	'Thailand': 'THA',
	'Timor Leste': 'TLS',
	'Timor-Leste': 'TLS',
	'Togo': 'TGO',
	'Tunisia': 'TUN',
	'Turkey': 'TUR',
	'Turkiye': 'TUR',
	'Turkmenistan': 'TKM',
	'Türkiye': 'TUR',
	'Uganda': 'UGA',
	'Ukraine': 'UKR',
	'United Kingdom': 'GBR',
	'United States Minor Outlying Islands': 'UMI',
	'United States Virgin Islands': 'VIR',
	'United States of America': 'USA',
	'Uruguay': 'URY',
	'Uzbekistan': 'UZB',
	'Venezuela': 'VEN',
	'Viet Nam': 'VNM',
	'Vietnam': 'VNM',
	'Yemen': 'YEM',
	'Zambia': 'ZMB',
	'Zimbabwe': 'ZWE',
	'uncalculated': None,
}