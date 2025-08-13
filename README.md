# Ranchero

**You are looking at a static older version (tagged as 0.0.17 on GitHub) of ranchero. Please see the main branch of the repo (https://github.com/aofarrel/ranchero) for a more updated version.**

Is your mycobacterial metadata a mess? Grab the *M. bovis* by the horns with Ranchero.

Ranchero is a Python solution to the dozens of different metadata formats used in genomic datasets. While it is specifically focused on NCBI's collection of *Mycobacterium tuberculosis complex* metadata, it still has utility for other organisms. For information on what Ranchero considers "a sample" and the like, see [./docs/data_structure.md](./docs/data_structure.md). For information on how to configure Ranchero, see [.docs/configuration.md](.docs/configuration.md). For a guided example of what it can do, please see [demo.py](demo.py).

 ## Features
 * Input a TSV/JSON/CSV of new samples and their metadata into a dataframe
 * Merge columns of similar data types into a single column, filling in nulls/empty values as you go
 * Input a TSV of metadata to "inject" into an existing dataframe, optionally overriding metadata already present
 * Flatten all of those "missing" and "Not Applicable" strings into proper null values
 * Convert countries into three-letter country codes per [ISO 3166](https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes)
 * Convert dates to YYYY-MM-DD format into an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)-like format -- missing months/days are denoted as NN.
 * Convert common host animal names to a standarized `Genus species "common name"` format
 * (tuberculosis only) Convert old-school strain names to the modern lineage system

 ## Dependencies
 * Python 3.11-ish (3.7+ should be okay)
 * [pandas](https://pandas.pydata.org/) >= 2.0.0
 * [pyarrow](https://pypi.org/project/pyarrow/), even if not working with Apache Arrow datasets
 * [polars](https://github.com/pola-rs/polars) for Python ==1.16.0
   * Please check the minimum version; this code expects the behavior of https://github.com/pola-rs/polars/issues/20069
 * [tqdm](https://github.com/tqdm/tqdm)

 ## Supported inputs
 * JSON files directly from BigQuery *(newer versions of Ranchero can parse this!)*
 * CSV files directly from NCBI Run Selector
 * Any arbitrary TSV file, provided it has a "BioSample" or "run_accession" column

 ## Unsupported inputs
 * Excel (but Excel supports output to TSV)
 * XML from NCBI "full summary" file download
 * JSON files not directly from BigQuery
 * CSV files not directly from NCBI Run Selector
