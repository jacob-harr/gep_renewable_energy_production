'''
NatCap TEEMs Global GEP: Renewable Energy Production
Jacob Harris

This script calculates global GEP for the following services:
- Solar Energy
- Wind Energy
- Geothermal Energy
''' 

import numpy as np # type: ignore
import os
import pandas as pd # type: ignore

# set dir
data_dir = '../data'

#############
# DEMAND SIDE
#############

# load data
df_path = os.path.join(data_dir, 'IRENA_prod_by_country.csv')
df = pd.read_csv(df_path)

# aggregate generation technologies
aggregated_df = (
    df.groupby(['Year', 'ISO3 code', 'Country', 'Group Technology'], 
               as_index=False)['Electricity Generation (GWh)'].sum()
)

# Create df for each resource of interest
geo_df = aggregated_df[aggregated_df['Group Technology'] == 'Geothermal energy']
solar_df = aggregated_df[aggregated_df['Group Technology'] == 'Solar energy']
wind_df = aggregated_df[aggregated_df['Group Technology'] == 'Wind energy']

# create list of dfs 
df_list = [wind_df, solar_df, geo_df]

#############
# SUPPLY SIDE
#############

# Load World Bank data

wb_path = os.path.join(data_dir, 'WB_price_data.csv')
wb_df = pd.read_csv(wb_path)

# Convert Price from cents/kWh to USD/GWh
wb_df['Price'] = wb_df['Price'] * 10000

# rename columns for merge
wb_df.rename(columns={'Economy ISO3' : 'ISO3 code', 'Economy Name' : 'Country', 'Price' : 'Price (USD/GWh)'}, inplace=True)

#############
# P * Q
#############

def merge_dfs(main_df, list_of_dfs):
    """
    Merge a main DataFrame with a list of DataFrames on common columns ['ISO3 code', 'Year'].

    Parameters:
        main_df (pd.DataFrame): The main DataFrame with columns ['ISO3 code', 'Country', 'Year', 'Price'].
        list_of_dfs (list): List of renewable energy demand dfs 

    Returns:
        list: A list of merged DataFrames.
    """
    merged_dfs = []

    # Define the common columns for the merge
    merge_columns = ['ISO3 code', 'Year']

    for df in list_of_dfs:
        # Perform the merge
        merged_df = pd.merge(main_df, df, on=merge_columns, how='inner')
        # remove redundant country cols
        merged_df = merged_df.drop('Country_y', axis=1) 
        merged_df = merged_df.rename(columns={'Country_x' : 'Country'})
        # Append the merged DataFrame to the results list
        merged_dfs.append(merged_df)

    return merged_dfs

# call P * Q merge
gep_dfs = merge_dfs(wb_df, df_list)
print(len(gep_dfs))

########################
# NATURE'S CONTRIBUTIONS
########################

# load resource rent data
alpha_data_name = 'CWON_resource_rent_data.csv'
alpha_path = os.path.join(data_dir, alpha_data_name)
a_df = pd.read_csv(alpha_path)

merge_cols = ['Country', 'Year']

# concatenate list of dfs with p and q data
combined_df = pd.concat(gep_dfs, ignore_index=True)

# merge the concatenated df with the resource rent df 
gep_df = combined_df.merge(a_df, on = merge_cols, how = 'inner')

# gep calculation: gep = nat_contrib * P * Q
gep_df['gep'] = gep_df['nat_contrib'] * gep_df['Price (USD/GWh)'] * gep_df['Electricity Generation (GWh)']
gep_df.head()

# filter to columns of interest
filter_cols = ['ISO3 code', 'Country', 'Year', 'Group Technology', 'Price (USD/GWh)', 'Electricity Generation (GWh)', 'nat_contrib', 'gep']
final_df = gep_df[filter_cols]

################
# EXPORT RESULTS
################

# Function to filter dataframe for years 2014-2019 and save CSVs by Technology
def export_by_resource(df):

    # set output directory
    out_dir = os.path.join(data_dir, 'results')
    if out_dir:
        os.makedirs(out_dir, exist_ok=True) # create dir if needed

    # Filter years
    df_filtered = df[df['Year'].between(2014, 2019)].copy()

    # Rename columns
    df_filtered.rename(columns={
        'ISO3 code': 'Country_Code',
        'Country': 'Country_Name',
        'Group Technology': 'Resource',
        'Price (USD/GWh)': 'P_electricity_USD_per_GWh',
        'Electricity Generation (GWh)': 'energy_prod_GWh'
    }, inplace=True)

    # Reorder columns
    df_filtered = df_filtered[['Resource', 'Country_Name', 'Year', 'Country_Code', 'P_electricity_USD_per_GWh', 'energy_prod_GWh', 'nat_contrib', 'gep']]

    # Drop rows where gep <= 0 (happens from nat_contrib calc)
    df_filtered = df_filtered.loc[df_filtered['gep'] > 0]

    # Create dictionary of dataframes split by Resource
    technology_dfs = {resource: df_resource.copy() for resource, df_resource in df_filtered.groupby('Resource')}

    # Save each df to CSV
    for tech, tech_df in technology_dfs.items():
        filename = f'{tech.replace(" ", "_").lower()}_gep_2014_2019.csv'
        out_path = os.path.join(out_dir, filename)
        tech_df.to_csv(out_path, index=False)
        print(f'Results saved to: {out_path}')

    return technology_dfs

# call export function
export_by_resource(final_df)

print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print('script complete!')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

'''
Output

This script successfully cleaned the raw demand, supply, and nature's contributions data. 
Resulting output file paths:
- ../data/results/geothermal_energy_gep_2014_2019.csv
- ../data/results/solar_energy_gep_2014_2019.csv
- ../data/results/wind_energy_gep_2014_2019.csv
'''