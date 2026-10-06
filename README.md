# gep_renewable_energy_production
NatCap TEEMs Global GEP results for renewable energy production 

## Ecosystem Services:
- Solar Energy
- Wind Energy
- Geothermal Energy

## Directory Setup
- /data/raw should contain:
    - WB-DB-xlsx
    - IRENA_Stats_extract_2024 H2.xlsx
- /data/ should contain:
    - ee_r264_correspondence.gpkg
    - IRENA_prod_by_country.csv
    - WB_price_data.csv

- when running scripts, ensure your current working directory is set to /code/

## Project Pipeline
Either run the run_all.py script or run scripts in the following order:
1. 01_data_cleaner.py
2. 02_run_renewable_energy_provisions.py
3. 03_results_vis.py

## YET TO IMPLEMENT
- ReadMe should state purpose and software dependencies (I should add the requirements.txt)
- A LICENSE file and a citation (BibTeX or plain text) for the project.
- fig_g switch to looking at input dataset instead of variable name... (P and lambda will never be the sole NA because they come from the same dataset... so they will both be NA or both be available).
- add duplicate logic to write-up
- write-up .qmd file

### Data References:
- Price: World Bank (2021) [World Bank Doing Business](https://www.worldbank.org/en/programs/business-enabling-environment/doing-business-legacy)
- Quantity: IRENA (2024) [Renewable Energy Statistics](https://www.irena.org/Publications/2024/Jul/Renewable-energy-statistics-2024) 