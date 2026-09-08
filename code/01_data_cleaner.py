# GEP: Renewable Energy Data Cleaning

'''
The purpose of this notebook is to clean the raw data for price and quantity of renewable energy resources.

Quantity raw data
- ../data/raw/IRENA_Stats_extract_2024 H2.xlsx

Price raw data
- ../data/raw/WB-DB.xlsx

PPP raw data
- 
- 

Raw data files will have extraneous columns filtered out and the sheet of interest will be converted to CSV'
'''

import os 
import pandas as pd # type: ignore

# QUANTITY SIDE

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

# PRICE SIDE

# load raw price data
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

# PPP data
PPP_RAW_FILE = os.path.join(raw_dir, 'API_PA.NUS.PPP_DS2_en_csv_v2_33039.csv')
FCRF_RAW_FILE = os.path.join(raw_dir, 'API_PA.NUS.FCRF_DS2_en_csv_v2_32.csv')
OUTPUT_FILE = os.path.join(data_dir, 'WB_PPP_data.csv')
SKIPROWS = 4                 # metadata lines before the real header
YEAR_MIN, YEAR_MAX = 1990, 2025

def clean_ppp_csv(raw_path, value_name, year_min=YEAR_MIN, year_max=YEAR_MAX):
    """
    Read a raw World Bank wide CSV and return a tidy long DataFrame:
    ['Country Name', 'ISO3 code', 'Year', value_name]
    for years in [year_min, year_max]. Missing observations dropped.
    """
    # Header is on line SKIPROWS+1; skiprows drops the metadata lines.
    df = pd.read_csv(raw_path, skiprows=SKIPROWS)

    # Drop any trailing unnamed column(s) from the line-ending comma
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

    id_cols = ['Country Name', 'Country Code']

    # Year columns: 4-digit headers within range
    year_cols = [
        c for c in df.columns
        if c.isdigit() and year_min <= int(c) <= year_max
    ]

    long_df = df.melt(
        id_vars=id_cols,
        value_vars=year_cols,
        var_name='Year',
        value_name=value_name,
    )

    long_df['Year'] = long_df['Year'].astype(int)
    long_df[value_name] = pd.to_numeric(long_df[value_name], errors='coerce')
    long_df = long_df.dropna(subset=[value_name])
    long_df = long_df.rename(columns={'Country Code': 'ISO3 code'})

    return long_df

print(f"Cleaning {PPP_RAW_FILE} ...")
ppp = clean_ppp_csv(PPP_RAW_FILE, 'ppp_factor')

print(f"Cleaning {FCRF_RAW_FILE} ...")
fx = clean_ppp_csv(FCRF_RAW_FILE, 'exchange_rate')

# Merge on country + year. Inner join keeps country-years that have
# BOTH a PPP factor and an exchange rate (needed to form the ratio).
merged = ppp.merge(
    fx[['ISO3 code', 'Year', 'exchange_rate']],
    on=['ISO3 code', 'Year'],
    how='inner',
)

# price_level_ratio = PPP conversion factor / market exchange rate
# (equivalent to World Bank indicator PA.NUS.PPPC.RF). Guard against
# divide-by-zero from any zero exchange rates.
merged = merged[merged['exchange_rate'] > 0].copy()
merged['price_level_ratio'] = merged['ppp_factor'] / merged['exchange_rate']

# Order columns and rows
merged = merged[['Country Name', 'ISO3 code', 'Year',
                    'ppp_factor', 'exchange_rate', 'price_level_ratio']]
merged = merged.sort_values(['ISO3 code', 'Year']).reset_index(drop=True)

merged.to_csv(OUTPUT_FILE, index=False)

n_countries = merged['ISO3 code'].nunique()
yr_lo, yr_hi = merged['Year'].min(), merged['Year'].max()
print(f"Saved {len(merged):,} rows ({n_countries} economies, "
        f"years {yr_lo}-{yr_hi}) to {OUTPUT_FILE}")

print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print('script complete!')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

'''
Output

Resulting output file paths:
- ../data/IRENA_prod_by_country.csv
- ../data/WB_price_data.csv
- ../data/WB_PPP_data.csv
'''