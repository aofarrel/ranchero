# Ranchero
Is your mycobacterial metadata a mess? Grab the *M. bovis* by the horns with Ranchero.

Ranchero is a Python solution to the dozens of different metadata formats used in genomic datasets. While it is specifically focused on NCBI's collection of *Mycobacterium tuberculosis complex* metadata, it still has utility for other organisms. For information on what Ranchero considers "a sample" and the like, see [./docs/data_structure.md](./docs/data_structure.md). For information on how to configure Ranchero, see [.docs/configuration.md](.docs/configuration.md).

In addition to housing Ranchero itself, this repo also contains the scripts used to generate metadata TSVs for various pathogens UCSC is keeping an eye on, such as the metadata used to annotated [the Taxonium SRA tree for *Mycobacterium tuberculosis complex*](https://taxonium.org/tuberculosis/SRA?xType=x_dist). You can find those scripts in [./compilations](./compilations). 

 ## Features
  * Powered by polars
    * Standardize entire genera in minutes thanks to polars' impressive speeds
    * Use [polars expressions](https://docs.pola.rs/api/python/stable/reference/expressions/index.html) to do things I didn't think of
 * Pre-configured to standardize dozens of common NCBI metadata fields
    * Automatically merge columns of similar data types into a single column, filling in nulls/empty values as you go
    * (MTBC only) Automatically handle lineage, strain, and scientific name
    * (MTBC only) Convert old-school strain names (Beijing, LAM, etc) to the modern lineage system (L2.2.1, L4.3, etc)
 * Convert all of those pesky "missing" and "Not Applicable" strings into proper null values
 * Input a TSV of metadata to "inject" into an existing dataframe, optionally overriding metadata already present
 * Convert countries into three-letter country codes per [ISO 3166](https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes)
 * Convert dates to YYYY-MM-DD format into an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)-like format
 * Convert common host animal names to the standardized *Genus species* format when possible, as well a common name and confidence score

 ## Installation
 Because ranchero currently relies on a very specific version of polars, it is recommended to install it a [venv](https://docs.python.org/3/library/venv.html) like this:
 ```
 python3 -m venv ./buildvenv
 source buildvenv/bin/activate
 pip install ranchero
 ```


 ## Supported inputs

  | Platform                | Expected format                     | Ranchero function   |
  |-------------------------|-------------------------------------|---------------------|
  | BigQuery                | newline-delimited JSONL<sup>†</sup> | from_bigquery()     |
  | Enterz Direct (efetch)  | XML<sup>‡</sup>                     | from_efetch()       |
  | NCBI SRA web search     | XML<sup>‡</sup>                     | from_efetch()       |
  | Excel/LibreOffice       | TSV (XLSX not supported)            | from_tsv()          |
  | Google Sheets           | TSV                                 | from_tsv()          |
  | NCBI Run Selector       | CSV                                 | from_run_selector() |
  | basically anything else | TSV                                 | from_tsv()          |

   <sup>†</sup> BQ typically outputs JSONs in a format polars does not like; from_bigquery() will fix it on the fly.  
   <sup>‡</sup> efetch typically outputs an invalid XML; from_efetch() will fix it on the fly. However, note that only `-db sra -format native -mode xml` and output from NCBI SRA web search is supported.


 ## Dependencies
 If you are pip-installing as recommended above, these will be included automatically.
 * Python >= 3.10
 * [pandas](https://pandas.pydata.org/) >= 2.0.0
 * [pyarrow](https://pypi.org/project/pyarrow/)
 * [polars](https://github.com/pola-rs/polars) for Python == 1.27.0
 * [tqdm](https://github.com/tqdm/tqdm)
 * xmltodict for working with Enterz Direct files
