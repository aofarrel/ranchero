import ranchero as Ranchero

#xml = Ranchero.from_efetch("/Users/aofarrel/Downloads/c_auris.xml")
#print(xml)

Ranchero.Configuration.set_config({"mycobacterial_mode": False})

json = Ranchero.from_bigquery("/Users/aofarrel/Downloads/bq-results-20250801-215940-1754085716223.json")
print(json.columns)
Ranchero.super_print(json.select(
	['__index__run', 'BioProject', 'date_collected', 'host_scienname', 'isolation_source', 'isolation_source_raw', 'continent', 'country', 'region']),
"cool stuff")