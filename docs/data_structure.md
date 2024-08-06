# Ranchero's data structure and assumptions

Ranchero was designed for NCBI data, and inherits some of the same assumptions.

Typically:
* A BioProject (PRBJ) represents a collection of samples
* A BioSample (SAMN, SAME, SAMD) is one biological sample - a bioposy, glob of spit, etc
* A run (SRR, ERR, DRR) is a sequencing run

There are several exceptions, of course:
* Many samples are part of more than one BioProject
* There are some BioSamples that are very clearly representative of more than one biological sample
* Some BioSamples do not have public run accessions
* Some BioSamples are part of a "sample pool" where the 1-to-1 connection between sample and run accession isn't clear

Ranchero does its best to account for all of these scenarios, but we need to pick *something* as our index. Ranchero gives you the option of using BioSamples as your index, even if starting with run accession-indexed data.