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
	'Osindisweni Hospital - Occ Health, staff clinic': 'ZAF',


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
	"AGANA": "Hagåtña", # capital of Guam which has since been renamed
	"AKAROA": "Akaroa",
	"AMBERLEYHILLS": "Amberley Hills",
	"Beijing-": "Beijing",
	"Blood - human Isolation date: September 26, 1989": None,
	"BRONX": "Bronx", # not necessarily new york
	"Capetown_": "Capetown",
	"CHEVIOTHILLS": "Cheviot Hills",
	"Chiang rai": "Chiang Rai",
	"Cote d'Ivoire: Divo": "Divo",
	"Cote d'Ivoire: Kongouanou": "Kongouanou",
	"Cote d'Ivoire: Sakassou": "Sakassou",
	"Cote d'Ivoire: Toumoudi": "Toumoudi",
	"Cote d'Ivoire: Yamoussoukro": "Yamoussoukro",
	"Cote d'Ivoire: Zoukougbeu": "Zoukougbeu",
	"CURRAGHMORE": "Curraghmore",
	"Durban Site_": "Durban",
	"FLUSHING": "Flushing",
	"GISBORNE": "Gisborne",
	"Hagatna": "Hagåtña",
	"Hagatña": "Hagåtña",
	"HAMILTON": "Hamilton",
	"HARIHARI": "Hari Hari",
	"HARWARDEN": "Harwarden",
	"HATEPE": "Hatepe",
	"HIMATANGI": "Himatangi",
	"INANGAHUA": "Inangahua",
	"INCHBONNIE": "Inchbonnie",
	"JOHNSON CITY": "Johnson City",
	"KOKATAHI": "Kokatahi",
	"KONA": "Kona",
	"LAKETEKAPO": "Laketekapo",
	"LINDISPASS": "Lindis Pass",
	"LITTLEWANGANUI": "Little Wanganui",
	"LUMSDEN": "Lumsden",
	"MIKONUI": "Mikonui",
	"MOLESWORTH": "Molesworth",
	"NEW YORK": "New York",
	"NORTHOTAGO": "Northotago",
	"OTAGO": "Otago",
	"OTUREHUA": "Oturehua",
	"OWAKA": "Owaka",
	"PALMERSTONNORTH": "Palmerston North",
	"PITTSBURGH": "Pittsburgh",
	"Port Elizabeth_": "Port Elizabeth",
	"POUGHKEEPSIE": "Poughkeepsie", # okay this is probably new york
	"RANGATAIKI": "Rangitaiki", # seems to have two spellings, using the "official" one here
	"RANGITAIKI": "Rangitaiki",
	"SAN JUAN CAPISTRANO": "San Juan Capistrano",
	"SanJuan Capistrano": "San Juan Capistrano",
	"SPRINGSJUNCTION": "Springs Junction",
	"TAMUNING": "Tamuning",
	"TANGIMOANA": "Tangimoana",
	"TARAMAKAU": "Taramakau",
	"The former Yugoslav Republic of Macedonia: Gurgurnica, Tetovo": "Gurgurnica, Tetovo",
	"The former Yugoslav Republic of Macedonia: Kocani": "Kocani",
	"The former Yugoslav Republic of Macedonia: Vesala, Tetovo": "Vesala, Tetovo",
	"TROY": "Troy",
	"TURAKINA": "Turakina",
	"TURANGI": "Tūrangi",
	"Turangi": "Tūrangi",
	"UTICA": "Utica", # not necessarily new york
	"Veracurz": "Veracruz", # 99% sure this is a typo
	"WAIRAUVALLEY": "Wairau Valley",
	"WANGAPEKAVALLEY": "Wangapeka Valley",
	"WESTLAND": "Westland",
	"Zoo-": "Zoo",

	# DO THESE ONES LAST
	"Cote d'Ivoire": None,
	"Côte d'Ivoire": None,
	"Cote d''Ivoire": None

}

# Unusued due to ambiguity:
# Catherine Booth: CAN/ZAF
# Goodwins Clinic: any number of places
# Siloah Clinic: DEU/ZAF