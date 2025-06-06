# Ranchero
Is your mycobacterial metadata a mess? Grab the *M. bovis* by the horns with Ranchero.

Ranchero is a Python solution to the dozens of different metadata formats used in genomic datasets. While it is specifically focused on NCBI's collection of *Mycobacterium tuberculosis complex* metadata, it still has utility for other organisms. For information on what Ranchero considers "a sample" and the like, see [./docs/data_structure.md](./docs/data_structure.md). For information on how to configure Ranchero, see [.docs/configuration.md](.docs/configuration.md).

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
 * Python 3.11-ish (3.9+ should be okay)
 * [pandas](https://pandas.pydata.org/) >= 2.0.0
 * [pyarrow](https://pypi.org/project/pyarrow/), even if not working with Apache Arrow datasets
 * [polars](https://github.com/pola-rs/polars) for Python ==1.16.0
   * Please check the minimum version; this code expects the behavior of https://github.com/pola-rs/polars/issues/20069
   * polars==1.27.0 *seems* to be working too after some changes but I'm still testing
 * [tqdm](https://github.com/tqdm/tqdm)
 * lxml for working with Enterz Direct files


 ## Supported inputs

  | Platform                | Expected format                     | Ranchero function   |
  |-------------------------|-------------------------------------|---------------------|
  | BigQuery                | newline-delimited JSONL<sup>†</sup> | from_bigquery()     |
  | Enterz Direct (efetch)  | XML<sup>‡</sup>                     | from_efetch()       |
  | Excel/LibreOffice       | TSV (XLSX not supported)            | from_tsv()          |
  | Google Sheets           | TSV                                 | from_tsv()          |
  | NCBI Run Selector       | CSV                                 | from_run_selector() |
  | basically anything else | TSV                                 | from_tsv()          |

   <sup>†</sup> BQ typically outputs JSONs in a format polars does not like; from_bigquery() will fix it on the fly.
   <sup>‡</sup> efetch typically outputs an invalid XML; from_efetch() will fix it on the fly. However, note that only `-db sra -format native -mode xml` is supported.