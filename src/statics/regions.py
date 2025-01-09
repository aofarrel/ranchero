# For translating regions back in to countries.

continents = {
	'Africa': 'Africa',
	'Asia': 'Asia',
	'Europe': 'Europe',
	'North America': 'North America',
	'Oceania': 'Oceania',
	'South America': 'South America'
}

regions_to_countries = {
	'Chwezi Clinic': 'ZAF',
	'Durban Chest Clinic': 'ZAF',
	'Ethembeni Clinic': 'ZAF',
	"Richard's Bay Clinic": 'ZAF',
	'Westville Prison': 'ZAF',
	'King Dinuzulu Hospital': 'ZAF',
	'Dundee Hospital': 'GBR',
	"St Mary's Kwa-Magwaza Hospital": 'ZAF',
	"St Margaret's TB Hospital": 'ZAF',

	'North America; USA': 'USA',
	'USA, North America': 'USA',
	'USA; North America': 'USA',
	'North America; USA': 'USA',
	
	'Africa; Niger': 'NER',
	'Niger; Africa': 'NER',
	
	'Africa; South Africa': 'ZAF',
	'South Africa; Africa': 'ZAF',

	'South Africa': 'ZAF',
	'Uganda': 'UGA',
	'Sweden': 'SWE',
	'Iran': 'IRN',
	'Romania': 'ROU',
	'Timor Leste': 'TLS',
	'Timor-Leste': 'TLS',
	'Taiwan': 'TWN',
	'North Macedonia': 'MKD',
	'Montenegro': 'MNE',
	'China': 'CHN',
}

# substring match
regions_to_smaller_regions = {
	"Cote d'Ivoire: Zoukougbeu": "Zoukougbeu",
	"Cote d'Ivoire: Kongouanou": "Kongouanou",
	"Capetown_": "Capetown",
	"Beijing-": "Beijing",
	"HARWARDEN": "Harwarden",
	"AMBERLEYHILLS": "Amberley Hills",
	"MOLESWORTH": "Molesworth",
	"NORTHOTAGO": "Northotago",
	"SAN JUAN CAPISTRANO": "San Juan Capistrano",
	"SanJuan Capistrano": "San Juan Capistrano",
	"Chiang rai": "Chiang Rai",
	"JOHNSON CITY": "Johnson City",
	"LAKETEKAPO": "Laketekapo",
	"RANGATAIKI": "Rangataiki",
	"PITTSBURGH": "Pittsburgh",
	"Durban Site_": "Durban",
	"Port Elizabeth_": "Port Elizabeth",
	"The former Yugoslav Republic of Macedonia: Kocani": "Kocani",
	"The former Yugoslav Republic of Macedonia: Vesala, Tetovo": "Vesala, Tetovo",
	"Veracurz": "Veracruz", # 99% sure this is a typo

}

# Unusued due to ambiguity:
# Catherine Booth: CAN/ZAF
# Goodwins Clinic: any number of places
# Siloah Clinic: DEU/ZAF