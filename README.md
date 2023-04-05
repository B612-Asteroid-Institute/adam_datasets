# ADAM Datasets

This repository contains Python scripts and Jupyter notebooks that download astronomical datasets for use with the Asteroid Discovery, Analysis, and Mapping (ADAM) platform. These datasets are primarily used for precovery searches of known Solar System small bodies using the Asteroid Institute's [precovery](https://github.com/B612-Asteroid-Institute/precovery) code. In the future, these datasets will also searched for as-yet undiscovered asteroids using ADAM::THOR (ADAM's asteroid discovery pipeline). Each dataset is managed by members of the ADAM Datasets team.

| Dataset Name | Dataset ID | Team Members | 
| ----------- | ----------- | ----------- |
| Catalina Real-Time Transient Survey [CRTS] (DR2) | CRTS_DR2 | |
| Isolated Tracklet File [ITF] | ITF |  |
| NOIRLab Source Catalog [NSC] (DR2) | NSC_DR2 | @moeyensj |
| Panoramic Survey Telescope and Rapid Response System [PanSTARRS] (DR2) | PS1_DR2 | |
| SkyMapper Southern Sky Survey (DR2) | SKYMAPPER_DR2 | |
| Sloan Digital Sky Survery (DR9) | SDSS_DR9 | | 
| Southern Photometric Local Universe Survey [S-PLUS] | SPLUS | |
| Zwicky Transient Facility [ZTF] (DR12)| ZTF_DR12 | |

## Adding a dataset
Any scripts and ingest codes for new datasets should be added to their own subdirectory. If the dataset has a specific data release then that data release should be added as its own directory in the directory tree. 

For example:  
`adam_datasets/{dataset_acronym}/{data_release}/`

Please do not commit the actual data files. This repository maintains the Python scripts and Jupyter notebooks needed to download the data, and process the data such that it can be used by ADAM.
