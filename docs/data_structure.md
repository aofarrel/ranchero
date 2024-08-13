# Ranchero's data structure and assumptions

## Background
Ranchero was designed for NCBI data, and inherits some of the same assumptions.

Typically:
* A BioProject (PRBJ) represents a collection of samples
* A BioSample (SAMN, SAME, SAMD) is one biological sample - a biopsy, glob of spit, etc
* A run (SRR, ERR, DRR) is a sequencing run, which may have one or more files associated with it
  * Illumina paired-end data for instance typically has two fastq files (_1 and _2) per run accession

There are several exceptions, of course:
* Many samples are part of more than one BioProject
* There are some BioSamples that are very clearly representative of more than one biological sample
* Some BioSamples do not have public run accessions
* Some BioSamples are part of a "sample pool" where the 1-to-1 connection between sample and run accession isn't clear

Ranchero does its best to account for all of these scenarios, but we need to pick *something* as our index. Ranchero gives you the option of using BioSamples as your index, even if starting with run accession-indexed data. Still, some assumptions must be made when dealing this sort of data.

## Example
The following is a truncated bigQuery JSON output for four run accessions. The first run accession belongs to BioSample SAMEA13188762. The other two are all members of BioSample SAMEA8616149. However, ERR5979610 is attached to sra_study ERP128579 and BioProject PRJEB44524, while ERR10561256 is attached to sra_study ERP142966 and BioProject PRJEB57950.
```
[
{"acc":"ERR9030408","biosample":"SAMEA13188762","sra_study":"ERP133548","bioproject":"PRJEB49093"},
{"acc":"ERR5979610","biosample":"SAMEA8616149","sra_study":"ERP128579","bioproject":"PRJEB44524"},
{"acc":"ERR10561256","biosample":"SAMEA8616149","sra_study":"ERP142966","bioproject":"PRJEB57950"}
]
```

## Assumptions
When indexed by sample, every run within a sample can have unique metadata, except for the following, which must be shared across the entire sample:
* Geographic location of sample isolation
* The individual the sample is isolated/cultured from
  * By extension, the species of the host
* List of BioProjects

If different parts of a sample are coming from different geographic locations, or from different patients/hosts, then the concept of a sample has essentially no meaning.

There are some borderline situations where we can say that *usually* some piece of metadata should be consistent across runs. However, there are always some edge cases and disagreement in naming, such as different experimental conditions, change over time, extraction from different organs, etc.



## Standardized metadata
* sample/BioSample: The ID for the sample. For NCBI data, this should always be the BioSample's SAMN/SAME/SAMD ID.
* xrs: For NCBI data, this is the sample's SRS/ERS/DRS ID.
* run_acc: The ID for a run accession. For NCBI data, this should always be the run accession's SRR/ERR/DRR ID.
* geoloc_country: Three-letter [ISO 3166](https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes) code representing the geographic location of where the sample was sourced, according to whoever uploaded the sample.
	* Be aware that some uploaders may write the former country for recent immigrants/refugees while others will write the host country
	* This should not be the country where a sample was sequenced
* geoloc_precise: The most precise geographic location available for a sample, with the general pattern of `smallest_possible_distinction, intermediate_distinctions, country (lat, long)` if all that information is available. A "distinction" could be a town, village, prefecture, state, county, territory, etc... essentially anything below a country.
* other_names/primary_search: All other names for the run accession. For NCBI data taken from bigQuery, this is all values for "primary-search".
* host_species: Has format `Genus species "common name"` if all such information is available. 

## Indexing

### by run
| run_acc | BioSample | bases | geoloc_country  | BioProject |
|--------|-----------|-------|----------|------------|
| run1/3 | A         | 111   | Atropia  | foo        |
| run2/3 | A         | 222   | Atropia  | buzz       |
| run3/3 | A         | 333   | Atropia  | foo        |
| run1/2 | B         | 11    | Pineland | bar        |
| run2/2 | B         | 22    | Pineland | bizz       |
| run1/1 | C         | 1     | Cortina  | foo        |

### by sample
| BioSample | bases         | geoloc_country  | BioProject     | run_acc                |
|-----------|---------------|----------|----------------|------------------------------|
| A         | [111,222,333] | Atropia  | ["foo","buzz"] | ["run1/3","run2/3","run3/3"] |
| B         | [11,22]       | Pineland | ["bar","bizz"] | ["run1/2","run2/2"]          |
| C         | [1]           | Cortina  | ["foo"]        | ["run1/1"]                   |