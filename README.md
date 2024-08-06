# Ranchero
 When your *Mycobacterium bovis* isn't cooperating, you need Ranchero.

 > [!WARNING]  
 > Ranchero is currently being rebased. Things will change and break.

 Ranchero is a work-in-progress Python solution to the dozens of different metadata formats used in genomic datasets. While it is specifically focused on NCBI's collection of *Mycobacterium tuberculosis complex* metadata, it still has utility for other organisms. For information on what Ranchero considers "a sample" and the like, see (./docs/data_structure.md)[./docs/data_structure.md].

 ## Dependencies
 * [pandas](https://pandas.pydata.org/) >= 2.0.0
 * [pyarrow](https://pypi.org/project/pyarrow/), even if not working with Apache Arrow datasets
 * [polars](https://github.com/pola-rs/polars) for Python

 ## Supported inputs
 * JSON files directly from BigQuery
 * CSV files directly from NCBI Run Selector
 * Any arbitrary TSV file, provided it has a "BioSample" or "run_accession" column

 ## Unsupported inputs
 * Excel (but Excel supports output to TSV)
 * XML from NCBI "full summary" file download
 * JSON files not directly from BigQuery
 * CSV files not directly from NCBI Run Selector

 ## Features
 * Flatten all of those "missing" and "Not Applicable" strings into NaNs/empty strings
 * Convert countries into three-letter country codes per (ISO 3166)[https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes]
 * Convert dates to YYYY-MM-DD format per (ISO 8601)[https://en.wikipedia.org/wiki/ISO_8601], which support for missing months/days
 * Convert common host animal names to a standarized `Genus species "common name"` format
 * (tuberculosis only) Convert old-school strain names to the modern lineage system
