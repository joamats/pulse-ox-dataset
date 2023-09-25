# BOLD: Blood-gas and Oximetry Linked Dataset
An open-source pulse oximetry and arterial blood gas dataset 

This repository contains 4 main notebooks:

* `1_dataset.ipynb` - the pipeline to create the dataset

* `2_consort_diagram.ipynb` - the code to create the consort diagram

* `3_tableones.ipynb` - the descriptive tables present in the manuscript

* `4_technical_validation.ipynb` - the code for the technical validation and the figures in the manuscript

These Google Spreadsheets contain the variables and mappings used in the dataset harmonization:

* [Variables](https://docs.google.com/spreadsheets/d/1W4PS3__-jF3m8OemERsv2r_b9sfACWIr-JQcPxW2A7c/edit#gid=0)

* [Definitions](https://docs.google.com/spreadsheets/d/1Hv_sOd0--6TPYiB3Crjdn_JrIhIazXXJc05mL4GefOU/edit#gid=0)

Customizing both the notebook and the spreadsheets to your own needs could provide you with new views of the dataset.

Finally, the `queries` folder contains the auxiliary SQL queries used to extract the data, created by our team.

All other derived tables can be found in:
* [MIMIC-Code Repository](https://github.com/MIT-LCP/mimic-code)
* [eICU-Code Repository](https://github.com/MIT-LCP/eicu-code/)