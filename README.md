sas-tools
=========

SAS tools to aid manipulation and curation of datasets.

#### [sas-institute-macros.sas](https://github.com/sleepepi/sas-tools/tree/master/sas-institute-macros.sas)
A collection of macros developed by programmers for the SAS institute. These macros address common problems
when coding in SAS that are not resolved by functions included in the standard SAS software package.

#### [sleepepi-sas-macros.sas](https://github.com/sleepepi/sas-tools/tree/master/sleepepi-sas-macros.sas)
A collection of macros developed by sleepepi programmers. These macros address common problems
when merging data.
  - generate_contents()
    - creates a contents dataset by appending a prefix or suffix to the dataset of interest
  - many_contents()
    - loops through generate_contents() based on a list of datasets
  - create_contents_note_conflicts()
    - identifies variables that use the same name in different datasets and may be problematic when merging
