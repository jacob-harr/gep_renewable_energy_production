# GEP: Renewable Energy Data Cleaning

'''
The purpose of this notebook is to clean the raw data for demand, supply, and resource rent data of renewable energy resources.

Demand raw data
- ../data/raw/IRENA_Stats_extract_2024 H2.xlsx

Supply raw data
- ../data/raw/WB-DB.xlsx

Resource rent data
- ../data/raw/Hydropower valuation_CWON 2024_rev4.xlsx

Raw data files will have extraneous columns filtered out and the sheet of interest will be converted to CSV'
'''

import os 
import pandas as pd # type: ignore

# DEMAND SIDE

# set directories
data_dir = '../data'
raw_dir = os.path.join(data_dir, 'raw')

# load raw data
excel_name = 'IRENA_Stats_extract_2024 H2.xlsx'
excel_path = os.path.join(raw_dir, excel_name)

# name of sheet of interest
sheet_name = 'Country'

# read the 'Country' sheet into a df
df = pd.read_excel(excel_path, sheet_name=sheet_name)

# filter to columns of interest
drop_cols = ['Heat Generation (TJ)', 'Public Flows (2021 USD M)', 'SDG 7a1 Intl. Public Flows (2021 USD M)', 'SDG 7b1 RE capacity per capita (W/inhabitant)']
df.drop(drop_cols, axis = 'columns', inplace = True)

# dropping countries missing energy production data 
filter_df = df.dropna(subset=['Electricity Generation (GWh)'])

# save the df as a CSV file
csv_name = 'IRENA_prod_by_country.csv'
csv_path = os.path.join(data_dir, csv_name) # saving to data dir
filter_df.to_csv(csv_path, index=False)

print(f"Sheet '{sheet_name}' has been cleaned and saved to '{csv_path}'.")

# SUPPLY SIDE

# load raw supply data
p_excel_name = 'WB-DB.xlsx'
excel_path = os.path.join(raw_dir, p_excel_name)

# name of sheet of interest
sheet_name = 'Data'

# read the 'Data' sheet into a df
p_df = pd.read_excel(excel_path, sheet_name=sheet_name)

# filter to columns of interest
p_drop_cols = ['Indicator ID', 'Attribute 1', 'Attribute 2', 'Attribute 3', 'Partner']
p_df.drop(p_drop_cols, axis = 'columns', inplace = True)

# avg price data was only collected from 2014-2019 so filter out the prior years

# list of years to drop
yr_list = []
yr_count = 2014-2003
start = 2003
for i in range(yr_count):
    yr_list.append(str(start+i))

# initial year in df isn't string
init_yr = yr_list[0]
yr_list[0] = int(init_yr)
p_df.drop(yr_list, axis = 'columns', inplace = True)

# filter to the avg price data
p_filter_df = p_df[p_df['Indicator'] == 'Getting electricity : Price of electricity (US cents per kWh) (DB16-20 methodology)']

# convert from wide to long format
df_long = pd.melt(
    p_filter_df,
    id_vars=["Economy ISO3", "Economy Name"],  # Columns to keep
    value_vars=["2014", "2015", "2016", "2017", "2018", "2019"],  # Columns to unpivot
    var_name="Year",  # Name of the new 'year' column
    value_name="Price",  # Name of the new 'price' column
)

# convert years to int
df_long['Year'] = df_long['Year'].astype(int)

# dropping countries missing prices data 
filter_p_df = df_long.dropna(subset=['Price'])

# save the df as a CSV file
csv_save_name = 'WB_price_data.csv'
csv_save_path = os.path.join(data_dir, csv_save_name) # saving to data dir
filter_p_df.to_csv(csv_save_path, index=False)

print(f"Sheet '{sheet_name}' has been cleaned and saved to '{csv_save_path}'.")

# NATURE'S CONTRIBUTIONS

# load raw data
alpha_excel_name = 'Hydropower valuation_CWON 2024_rev4.xlsx'
alpha_excel_path = os.path.join(raw_dir, alpha_excel_name)

# name of sheet of interest
sheet_name = 'Database'

# read the 'Country' sheet into a df
a_df = pd.read_excel(alpha_excel_path, sheet_name=sheet_name)

# filter to columns of interest
a_keep_cols = ['Country', 'Year', 'Revenues\n(USD nominal)', 'Total costs\n(USD nominal)']
filtered_a_df = a_df[a_keep_cols]

# rename columns
filtered_a_df = filtered_a_df.rename(columns={"Revenues\n(USD nominal)": "tot_revenue", 
                              "Total costs\n(USD nominal)": "tot_cost"})

# remove missing
filtered_a_df = filtered_a_df.dropna(subset=['tot_revenue'])
filtered_a_df = filtered_a_df.dropna(subset=['tot_cost'])

# convert to int to avoid string values
filtered_a_df['tot_revenue'] = filtered_a_df['tot_revenue'].astype(int)
filtered_a_df['tot_cost'] = filtered_a_df['tot_cost'].astype(int)

# create a column for natures contributions: 
filtered_a_df['resource_rent'] = filtered_a_df['tot_revenue'] - filtered_a_df['tot_cost']
filtered_a_df['nat_contrib'] = filtered_a_df['resource_rent'] / filtered_a_df['tot_revenue']

# save the df as a CSV file
csv_name = 'CWON_resource_rent_data.csv'
csv_path = os.path.join(data_dir, csv_name) # saving to data dir
filtered_a_df.to_csv(csv_path, index=False)

print(f"Sheet '{sheet_name}' has been cleaned and saved to '{csv_path}'.")
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print('script complete!')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

'''
Output

This script successfully cleaned the raw demand, supply, and nature's contributions data. 
Resulting output file paths:
- ../data/IRENA_prod_by_country.csv
- ../data/WB_price_data.csv
- ../data/CWON_resource_rent_data.csv'
'''