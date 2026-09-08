'''
NatCap TEEMs Global GEP: Renewable Energy Production
Jacob Harris

PREREQUISITE: run 01.py first 
'''

import os
import pandas as pd
import geopandas as gpd
from scipy import stats

# --------------- configuration ---------------
data_dir = '../data'
raw_dir = os.path.join(data_dir, 'raw')
TARGET_YEAR = 2019
HOURS_PER_YEAR = 8760
LAMBDA_FLOOR = 0.001         # minimum lambda to avoid zeroing out GEP
SHAPIRO_ALPHA = 0.05         # significance level for normality test
PPP_FILE = 'WB_PPP_data.csv'  # produced by 01a_wb_data_getter.py

TECHNOLOGIES_OF_INTEREST = [
    'Solar energy',
    'Wind energy',
    'Geothermal energy',
]

# --------------- load data ---------------

# IRENA renewable energy production data
irena_df = pd.read_csv(os.path.join(data_dir, 'IRENA_prod_by_country.csv'))

# World Bank electricity price data
price_df = pd.read_csv(os.path.join(data_dir, 'WB_price_data.csv'))
price_df = price_df.rename(columns={
    'Economy ISO3': 'ISO3 code',
    'Economy Name': 'Country',
    'Price': 'Price_USD_GWh',
})
# Convert price from cents/kWh to USD/GWh
# 1 cent/kWh = 0.01 USD/kWh × 1,000,000 kWh/GWh = 10,000 USD/GWh
price_df['Price_USD_GWh'] = price_df['Price_USD_GWh'] * 10_000

# Country correspondence table
corr_gdf = gpd.read_file(os.path.join(raw_dir, 'ee_r264_correspondence.gpkg'))
corr_gdf = corr_gdf[['iso3_r250_id', 'iso3_r250_label', 'iso3_r250_name']]

# PPP price level ratio, produced by 01a_wb_data_getter.py.
# WB_PPP_data.csv is long format (one row per country-year, 1990-2025),
# so filter to TARGET_YEAR and keep just the merge key + ratio column.
ppp_path = os.path.join(data_dir, PPP_FILE)
if not os.path.exists(ppp_path):
    raise FileNotFoundError(
        f"{ppp_path} not found. Run 01.py first to clean the "
        f"raw World Bank data into WB_PPP_data.csv."
    )
ppp_df = pd.read_csv(ppp_path)
ppp_df = ppp_df[ppp_df['Year'] == TARGET_YEAR]
ppp_df = ppp_df[ppp_df['price_level_ratio'] > 0].dropna(subset=['price_level_ratio'])
ppp_df = ppp_df[['ISO3 code', 'price_level_ratio']]

# --------------- QUANTITY ---------------

# Filter to technologies of interest and aggregate to the resource
# (Group Technology) level, summing across sub-technologies (e.g. solar
# PV + solar thermal; onshore + offshore wind) and producer types, so
# each country has a single row (and thus a single lambda) per resource.
quantity_df = (
    irena_df[irena_df['Group Technology'].isin(TECHNOLOGIES_OF_INTEREST)]
    .groupby(['Year', 'ISO3 code', 'Country', 'Group Technology'],
             as_index=False)
    .agg(
        elec_prod_GWh=('Electricity Generation (GWh)', 'sum'),
        installed_cap_MW=('Electricity Installed Capacity (MW)', 'sum'),
    )
)

# Q for every producing country in the target year (all producers,
# whether or not price/PPP data exists for them).
q_year = quantity_df[quantity_df['Year'] == TARGET_YEAR].copy()

# P for every country with a price in the target year.
price_year = price_df[price_df['Year'] == TARGET_YEAR][
    ['ISO3 code', 'Price_USD_GWh']
].copy()

# --------------- NATURE'S CONTRIBUTIONS (lambda) ---------------

# Lambda is computed on ALL producers with a valid capacity factor
# (independent of price), matching run_renewable_energy_production_cf.py
# so the diagnostic figures and these outputs share identical lambda
# values. Countries without a valid CF have no lambda (NA in the output).
core = q_year.copy()

# Capacity factor: CF = Generation (GWh) / [Capacity (MW) * 8760 / 1000]
core['max_gen_GWh'] = core['installed_cap_MW'] * HOURS_PER_YEAR / 1000
core['capacity_factor'] = core['elec_prod_GWh'] / core['max_gen_GWh']
core['capacity_factor'] = core['capacity_factor'].clip(upper=1.0)
core = core.dropna(subset=['capacity_factor'])
core = core[core['capacity_factor'] > 0].copy()


def compute_robust_bounds(cf_series):
    """
    Determine outlier-robust normalization bounds for a CF series.
    Returns (lower_bound, upper_bound, method_used).
    """
    _, p_value = stats.shapiro(cf_series)

    if p_value > SHAPIRO_ALPHA:
        mu, sigma = cf_series.mean(), cf_series.std()
        lower = mu - 3 * sigma
        upper = mu + 3 * sigma
        method = 'z-score (normal)'
    else:
        q1 = cf_series.quantile(0.25)
        q3 = cf_series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        method = 'IQR (non-normal)'

    lower = max(lower, cf_series.min())
    upper = min(upper, 1.0)
    return lower, upper, method

# Compute bounds per technology, then normalize CF -> lambda
norm_bounds = {}
for tech, grp in core.groupby('Group Technology'):
    lower, upper, method = compute_robust_bounds(grp['capacity_factor'])
    norm_bounds[tech] = (lower, upper)
    n_lo = (grp['capacity_factor'] < lower).sum()
    n_hi = (grp['capacity_factor'] > upper).sum()
    print(f"  {tech}: {method}  bounds=[{lower:.4f}, {upper:.4f}]  "
          f"trimmed {n_lo} low + {n_hi} high outliers")

core['cf_lower'] = core['Group Technology'].map(lambda t: norm_bounds[t][0])
core['cf_upper'] = core['Group Technology'].map(lambda t: norm_bounds[t][1])
core['cf_clipped'] = core['capacity_factor'].clip(
    lower=core['cf_lower'], upper=core['cf_upper']
)
core['nat_contrib'] = (
    (core['cf_clipped'] - core['cf_lower'])
    / (core['cf_upper'] - core['cf_lower'])
)
core['nat_contrib'] = core['nat_contrib'].clip(lower=LAMBDA_FLOOR)
core = core.drop(columns=['cf_lower', 'cf_upper', 'cf_clipped'])

# --------------- SAVE RESULTS ---------------

def save_gep_by_technology(output_path='.'):
    """
    For each resource, write a CSV with columns:
        iso3_r250_id, iso3_r250_label, iso3_r250_name,
        Q, P, lambda, ppp_ratio, <Resource>_provision

    Every country in the correspondence table appears. Q, P, lambda and
    ppp_ratio are joined independently, so each shows a value where
    available and NA where not (making it visible when a country is NA
    only because its PPP ratio is missing). The PPP provision is

        provision_ppp = Q * P * lambda / ppp_ratio

    which is NA whenever ANY input (Q, P, lambda, or ppp_ratio) is
    missing -- never 0 -- because NA propagates through the arithmetic.
    Rows are sorted alphabetically by iso3_r250_label.

    Also writes all_energy_provision_gep.csv: the three resource
    provision columns side by side (id, label, name, Solar, Wind,
    Geothermal), with missing values left blank rather than 'NA'.
    """
    os.makedirs(output_path, exist_ok=True)

    # Clean correspondence: drop the stray all-NA row and territory dups
    corr = (
        corr_gdf
        .dropna(subset=['iso3_r250_id'])
        .drop_duplicates(subset=['iso3_r250_id'], keep='first')
    )

    key_cols = ['iso3_r250_id', 'iso3_r250_label', 'iso3_r250_name']
    combined = None  # accumulates the per-resource provision columns

    for tech in TECHNOLOGIES_OF_INTEREST:
        sanitized = str(tech).replace(' ', '_').replace('/', '_')
        prov_col = f'{sanitized}_provision'

        final_df = corr.copy()

        # Q (all producers of this resource)
        qd = (q_year[q_year['Group Technology'] == tech]
              [['ISO3 code', 'elec_prod_GWh']]
              .rename(columns={'elec_prod_GWh': 'Q'}))
        final_df = final_df.merge(qd, left_on='iso3_r250_label',
                                  right_on='ISO3 code', how='left').drop(columns='ISO3 code')

        # P (all priced countries)
        pd_ = price_year.rename(columns={'Price_USD_GWh': 'P'})
        final_df = final_df.merge(pd_, left_on='iso3_r250_label',
                                  right_on='ISO3 code', how='left').drop(columns='ISO3 code')

        # lambda (all producers with a valid CF)
        ld = (core[core['Group Technology'] == tech]
              [['ISO3 code', 'nat_contrib']]
              .rename(columns={'nat_contrib': 'lambda'}))
        final_df = final_df.merge(ld, left_on='iso3_r250_label',
                                  right_on='ISO3 code', how='left').drop(columns='ISO3 code')

        # PPP price level ratio (kept as an output column so a reviewer can
        # see when a country is NA solely because its PPP factor is missing)
        rd = ppp_df.rename(columns={'price_level_ratio': 'ppp_ratio'})
        final_df = final_df.merge(rd, left_on='iso3_r250_label',
                                  right_on='ISO3 code', how='left').drop(columns='ISO3 code')

        # PPP-adjusted provision; NA propagates when any input is missing
        final_df[prov_col] = (
            final_df['Q'] * final_df['P'] * final_df['lambda'] / final_df['ppp_ratio']
        )

        # Round for output
        final_df['Q'] = final_df['Q'].round(2)
        final_df['P'] = final_df['P'].round(2)
        final_df['lambda'] = final_df['lambda'].round(2)
        final_df['ppp_ratio'] = final_df['ppp_ratio'].round(4)
        final_df[prov_col] = final_df[prov_col].round(0)

        # Select the reviewer-requested columns, sorted by ISO3 label
        final_df = final_df[[
            'iso3_r250_id', 'iso3_r250_label', 'iso3_r250_name',
            'Q', 'P', 'lambda', 'ppp_ratio', prov_col,
        ]].sort_values('iso3_r250_label')

        filepath = os.path.join(output_path, f'{sanitized}_provision_gep.csv')
        # na_rep='NA' writes literal NA for every missing value
        final_df.to_csv(filepath, index=False, na_rep='NA')

        n_na = int(final_df[prov_col].isna().sum())
        print(f"Saved: {filepath} ({len(final_df)} rows, {n_na} NA provisions)")

        # Accumulate this resource's provision column for the combined file
        prov_slice = final_df[key_cols + [prov_col]]
        combined = (prov_slice if combined is None
                    else combined.merge(prov_slice, on=key_cols, how='outer'))

    # --- Combined all-energy file ---
    # One row per country with the three resource provisions side by side.
    # Missing cells are left BLANK here (default na_rep=''), not 'NA'.
    combined = combined.sort_values('iso3_r250_label')
    combined_path = os.path.join(output_path, 'all_energy_provision_gep.csv')
    combined.to_csv(combined_path, index=False)
    print(f"Saved: {combined_path} ({len(combined)} rows)")

save_gep_by_technology(output_path=os.path.join('..', 'output'))