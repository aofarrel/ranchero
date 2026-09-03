# Ranchero
Is your metadata a mess? Grab the *M. bovis* by the horns with Ranchero.

Ranchero is a Python solution to the dozens of different metadata formats used in genomic datasets. While it is specifically focused on NCBI's collection of *Mycobacterium* metadata, it also has utility for other organisms.

> [!NOTE]  
> Although functional as-is, Ranchero is undergoing a cleanup/refactor as time allows. More extensive documentation and examples will be provided once this cleanup is complete.

In addition to housing Ranchero itself, this repo also contains the scripts used to generate metadata TSVs for various pathogens UCSC is keeping an eye on, such as the metadata used to annotated [the Taxonium SRA tree for *Mycobacterium tuberculosis complex*](https://taxonium.org/tuberculosis/SRA?xType=x_dist). You can find those scripts in [./compilations](./compilations). 

For information on what Ranchero considers "a sample" and the like, see [./docs/data_structure.md](./docs/data_structure.md). 

 ## Features
  * Powered by polars
    * Standardize entire genera in minutes thanks to polars' impressive speeds
    * Use [polars expressions](https://docs.pola.rs/api/python/stable/reference/expressions/index.html) to do things I didn't think of
 * Pre-configured to standardize dozens of common NCBI metadata fields
    * Automatically merge columns of similar data types into a single column, filling in nulls/empty values as you go
    * (Mycobacteria only) Automatically handle lineage, strain, and scientific name
    * (Mycobacteria only) Convert old-school strain names (Beijing, LAM, etc) to the modern lineage system (L2.2.1, L4.3, etc)
 * Input a TSV of metadata to "inject" into an existing dataframe, optionally overriding metadata already present
 * Convert all of those "missing," "not collected," and "Not Applicable" strings into proper null values
 * Convert countries into three-letter country codes per [ISO 3166](https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes), following [a consistent set of standards](./docs/location_handling.md), with special handling for tricky cases (such as all four countries that contain the word "Guinea")
 * Convert dates to YYYY-MM-DD format into an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)-like format
 * Convert common host animal names to the standardized *Genus species* format when possible, as well a common name and confidence score

 ## Installation
 Because ranchero currently relies on a very specific version of polars for consistent handling of null values, it is recommended to install it a [venv](https://docs.python.org/3/library/venv.html) like this:
 ```
 python3 -m venv ./rancherovenv
 source rancherovenv/bin/activate
 pip install ranchero
 ```
 For development branches not currently on pypi, `git clone` this repo, `cd` into top-level folder, switch to the branch you want, then `pip install -e src`
 
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

 ## Configuration
 Ranchero has a lot of options and supports reading a configuration file provided by the user. You can use `ranchero.Configuration.update_config()` to read a yaml file with the expected format (see src/config.yaml for the default config), which will update that instance of Ranchero's options.
 ```
 import ranchero
 ranchero.Configuration.update_config("./src/config.yaml")
 ```

