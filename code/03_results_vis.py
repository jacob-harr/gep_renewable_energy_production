'''
03_results_check.py

NatCap TEEMs Global GEP: Renewable Energy 

Pipeline stage 03. Recomputes capacity factor and the outlier-robust
lambda and produces diagnostic outputs to ../output/diagnostics/:

  1. CSV table: Q, CF, lambda for every country & resource
  2. Figures:
     a. CF distributions by resource (box + strip)
     b. Lambda distributions by resource (box + strip)
     c. CF -> lambda mapping showing robust normalization bounds
     d. Top-20 countries: P*Q vs P*Q*lambda (lambda's scaling effect)
     e. Lambda vs production share scatter (who gets penalized?)
     f. Overlaid CF histograms with robust bounds annotated
     g. Missing-value diagnostics: NA counts + sole GEP blockers by input
        (reads the provision CSVs written by script 02 in ../output)

This script does NOT write the provision CSVs; those come from 02.py.

PREREQUISITE: Run 01 and 02 first (or via run_all.py).
'''

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from scipy import stats

# --------------- configuration ---------------
data_dir = '../data'
output_dir = os.path.join('..', 'output', 'diagnostics')
os.makedirs(output_dir, exist_ok=True)

TARGET_YEAR = 2019
HOURS_PER_YEAR = 8760
LAMBDA_FLOOR = 0.001
SHAPIRO_ALPHA = 0.05

TECHNOLOGIES_OF_INTEREST = [
    'Solar energy',
    'Wind energy',
    'Geothermal energy',
]

# Short labels for plots
TECH_SHORT = {
    'Solar energy': 'Solar',
    'Wind energy': 'Wind',
    'Geothermal energy': 'Geothermal',
}

# Colors per technology
TECH_COLORS = {
    'Solar energy': '#F2A900',
    'Wind energy': '#4A90D9',
    'Geothermal energy': '#D9534F',
}

# --------------- load & prepare data ---------------

irena_df = pd.read_csv(os.path.join(data_dir, 'IRENA_prod_by_country.csv'))

quantity_df = (
    irena_df[irena_df['Group Technology'].isin(TECHNOLOGIES_OF_INTEREST)]
    .groupby(['Year', 'ISO3 code', 'Country', 'Group Technology'],
             as_index=False)
    .agg(
        elec_prod_GWh=('Electricity Generation (GWh)', 'sum'),
        installed_cap_MW=('Electricity Installed Capacity (MW)', 'sum'),
    )
)

# Filter to target year
df = quantity_df[quantity_df['Year'] == TARGET_YEAR].copy()

# Capacity factor
df['max_gen_GWh'] = df['installed_cap_MW'] * HOURS_PER_YEAR / 1000
df['capacity_factor'] = df['elec_prod_GWh'] / df['max_gen_GWh']
df['capacity_factor'] = df['capacity_factor'].clip(upper=1.0)
df = df.dropna(subset=['capacity_factor'])
df = df[df['capacity_factor'] > 0].copy()

# --------------- Adaptive outlier-robust normalization ---------------

def compute_robust_bounds(cf_series):
    """
    Determine outlier-robust normalization bounds for a CF series.
    Shapiro-Wilk test determines distribution shape:
      - Normal (p > alpha):     bounds = mean ± 3*SD
      - Non-normal (p ≤ alpha): bounds = Q1 - 1.5*IQR, Q3 + 1.5*IQR
    Returns (lower_bound, upper_bound, method_used, shapiro_p).
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

    return lower, upper, method, p_value

# Compute and store bounds per technology
norm_bounds = {}
for tech, grp in df.groupby('Group Technology'):
    lower, upper, method, p_val = compute_robust_bounds(grp['capacity_factor'])
    n_lo = (grp['capacity_factor'] < lower).sum()
    n_hi = (grp['capacity_factor'] > upper).sum()
    norm_bounds[tech] = {
        'lower': lower, 'upper': upper,
        'method': method, 'p_value': p_val,
        'n_trimmed_lo': n_lo, 'n_trimmed_hi': n_hi,
    }
    print(f"  {tech}: {method} (Shapiro p={p_val:.4f})")
    print(f"    bounds=[{lower:.4f}, {upper:.4f}]  "
          f"trimmed {n_lo} low + {n_hi} high outliers")

# Apply normalization
df['cf_lower'] = df['Group Technology'].map(lambda t: norm_bounds[t]['lower'])
df['cf_upper'] = df['Group Technology'].map(lambda t: norm_bounds[t]['upper'])
df['cf_clipped'] = df['capacity_factor'].clip(lower=df['cf_lower'], upper=df['cf_upper'])
df['lambda'] = (
    (df['cf_clipped'] - df['cf_lower']) / (df['cf_upper'] - df['cf_lower'])
).clip(lower=LAMBDA_FLOOR)

# Clean up temp columns
df = df.drop(columns=['cf_lower', 'cf_upper', 'cf_clipped'])

# Try to load price data for GEP comparisons
try:
    price_df = pd.read_csv(os.path.join(data_dir, 'WB_price_data.csv'))
    price_df = price_df.rename(columns={
        'Economy ISO3': 'ISO3 code',
        'Price': 'Price_USD_GWh',
    })
    price_df['Price_USD_GWh'] = price_df['Price_USD_GWh'] * 10_000
    df = df.merge(
        price_df[['ISO3 code', 'Year', 'Price_USD_GWh']],
        on=['ISO3 code', 'Year'], how='left',
    )
    df['pq'] = df['elec_prod_GWh'] * df['Price_USD_GWh']
    df['gep'] = df['pq'] * df['lambda']
    HAS_PRICES = True
    print("\nPrice data loaded — GEP comparisons enabled.")
except FileNotFoundError:
    HAS_PRICES = False
    print("\nWB_price_data.csv not found — skipping GEP comparisons.")

# Short labels for plotting
df['tech_short'] = df['Group Technology'].map(TECH_SHORT)

# =====================================================================
# OUTPUT 1: CSV table
# =====================================================================

out_cols = ['Country', 'ISO3 code', 'Group Technology',
            'elec_prod_GWh', 'installed_cap_MW', 'capacity_factor', 'lambda']
if HAS_PRICES:
    out_cols += ['Price_USD_GWh', 'pq', 'gep']

csv_out = df[out_cols].sort_values(['Group Technology', 'Country'])
csv_path = os.path.join(output_dir, 'country_diagnostics.csv')
csv_out.to_csv(csv_path, index=False)
print(f"\nSaved diagnostics table: {csv_path}  ({len(csv_out)} rows)")

# Print summary stats
print("\n" + "=" * 70)
print("SUMMARY STATISTICS BY TECHNOLOGY")
print("=" * 70)
for tech, grp in df.groupby('Group Technology'):
    label = TECH_SHORT[tech]
    b = norm_bounds[tech]
    print(f"\n--- {label} ({len(grp)} countries) ---")
    print(f"  Method:  {b['method']}  (Shapiro p={b['p_value']:.4f})")
    print(f"  Bounds:  [{b['lower']:.4f}, {b['upper']:.4f}]  "
          f"(trimmed {b['n_trimmed_lo']}L + {b['n_trimmed_hi']}H)")
    print(f"  CF     min={grp['capacity_factor'].min():.4f}  "
          f"median={grp['capacity_factor'].median():.4f}  "
          f"max={grp['capacity_factor'].max():.4f}")
    print(f"  Lambda min={grp['lambda'].min():.4f}  "
          f"median={grp['lambda'].median():.4f}  "
          f"max={grp['lambda'].max():.4f}")
    print(f"  Q(GWh) min={grp['elec_prod_GWh'].min():.1f}  "
          f"median={grp['elec_prod_GWh'].median():.1f}  "
          f"max={grp['elec_prod_GWh'].max():.1f}")

# =====================================================================
# PLOTTING HELPERS
# =====================================================================

def tech_order():
    return [t for t in TECHNOLOGIES_OF_INTEREST if t in df['Group Technology'].unique()]

def save_fig(fig, name):
    path = os.path.join(output_dir, name)
    fig.savefig(path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"Saved: {path}")

# =====================================================================
# FIGURE A: CF distributions by technology (box + jitter)
# =====================================================================

fig, ax = plt.subplots(figsize=(8, 5))
techs = tech_order()
positions = range(len(techs))

for i, tech in enumerate(techs):
    vals = df.loc[df['Group Technology'] == tech, 'capacity_factor']
    bp = ax.boxplot(vals, positions=[i], widths=0.4, patch_artist=True,
                    boxprops=dict(facecolor=TECH_COLORS[tech], alpha=0.4),
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='', linewidth=0))
    jitter = np.random.normal(0, 0.06, size=len(vals))
    ax.scatter(jitter + i, vals, alpha=0.5, s=18, color=TECH_COLORS[tech],
               edgecolors='none', zorder=3)

    # Draw robust upper bound as a horizontal line
    b = norm_bounds[tech]
    ax.hlines(b['upper'], i - 0.3, i + 0.3, colors='red', linewidths=1.5,
              linestyles='--', zorder=4)
    ax.hlines(b['lower'], i - 0.3, i + 0.3, colors='red', linewidths=1.5,
              linestyles='--', zorder=4)

ax.set_xticks(positions)
ax.set_xticklabels([TECH_SHORT[t] for t in techs], fontsize=12)
ax.set_ylabel('Capacity Factor', fontsize=12)
ax.set_title(f'CF Distribution with Robust Normalization Bounds (red dashes) — {TARGET_YEAR}',
             fontsize=12)
ax.set_ylim(bottom=0)
ax.grid(axis='y', alpha=0.3)
save_fig(fig, 'fig_a_cf_distributions.png')

# =====================================================================
# FIGURE B: Lambda distributions by technology (box + jitter)
# =====================================================================

fig, ax = plt.subplots(figsize=(8, 5))
for i, tech in enumerate(techs):
    vals = df.loc[df['Group Technology'] == tech, 'lambda']
    bp = ax.boxplot(vals, positions=[i], widths=0.4, patch_artist=True,
                    boxprops=dict(facecolor=TECH_COLORS[tech], alpha=0.4),
                    medianprops=dict(color='black', linewidth=1.5),
                    flierprops=dict(marker='', linewidth=0))
    jitter = np.random.normal(0, 0.06, size=len(vals))
    ax.scatter(jitter + i, vals, alpha=0.5, s=18, color=TECH_COLORS[tech],
               edgecolors='none', zorder=3)

ax.set_xticks(positions)
ax.set_xticklabels([TECH_SHORT[t] for t in techs], fontsize=12)
ax.set_ylabel('Lambda (λ)', fontsize=12)
ax.set_title(f'Lambda Distribution by Technology — Robust Normalization ({TARGET_YEAR})',
             fontsize=13)
ax.set_ylim(bottom=0, top=1.05)
ax.grid(axis='y', alpha=0.3)
save_fig(fig, 'fig_b_lambda_distributions.png')

# =====================================================================
# FIGURE C: CF → Lambda mapping with robust bounds annotated
# =====================================================================

fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
fig.suptitle(f'Capacity Factor → Lambda Mapping — Robust Bounds ({TARGET_YEAR})',
             fontsize=14, y=1.02)

for ax, tech in zip(axes, techs):
    grp = df[df['Group Technology'] == tech]
    b = norm_bounds[tech]
    ax.scatter(grp['capacity_factor'], grp['lambda'],
               alpha=0.6, s=30, color=TECH_COLORS[tech], edgecolors='none')

    # Robust bounds as vertical lines
    ax.axvline(b['lower'], color='red', ls='--', alpha=0.7, linewidth=1.2)
    ax.axvline(b['upper'], color='red', ls='--', alpha=0.7, linewidth=1.2)

    # Shade the normalization range
    ax.axvspan(b['lower'], b['upper'], alpha=0.06, color=TECH_COLORS[tech])

    ax.set_xlabel('Capacity Factor', fontsize=11)
    ax.set_title(f"{TECH_SHORT[tech]}\n{b['method']}", fontsize=11)
    ax.set_xlim(left=0)
    ax.grid(alpha=0.2)

    # Annotate bounds
    ax.text(b['lower'], -0.08, f"{b['lower']:.3f}", ha='center', fontsize=8,
            color='red', transform=ax.get_xaxis_transform())
    ax.text(b['upper'], -0.08, f"{b['upper']:.3f}", ha='center', fontsize=8,
            color='red', transform=ax.get_xaxis_transform())

axes[0].set_ylabel('Lambda (λ)', fontsize=11)
fig.tight_layout()
save_fig(fig, 'fig_c_cf_to_lambda_mapping.png')

# =====================================================================
# FIGURE D: Top-20 countries P×Q vs GEP (lambda's scaling effect)
# Only if price data is available
# =====================================================================

if HAS_PRICES:
    fig, axes = plt.subplots(1, 3, figsize=(18, 7))
    fig.suptitle(f'Top 20 Countries: P×Q (grey) vs GEP after λ (color) — {TARGET_YEAR}',
                 fontsize=14, y=1.02)

    for ax, tech in zip(axes, techs):
        grp = (df[df['Group Technology'] == tech]
               .nlargest(20, 'pq')
               .sort_values('pq', ascending=True))

        if grp.empty:
            ax.set_title(f'{TECH_SHORT[tech]}\n(no data)')
            continue

        y_pos = range(len(grp))
        labels = grp['Country'].str[:25]

        ax.barh(y_pos, grp['pq'] / 1e6, color='lightgrey', edgecolor='none',
                label='P × Q')
        ax.barh(y_pos, grp['gep'] / 1e6, color=TECH_COLORS[tech], alpha=0.8,
                edgecolor='none', label='GEP (P×Q×λ)')

        for y, (_, row) in zip(y_pos, grp.iterrows()):
            ax.text(row['gep'] / 1e6 + ax.get_xlim()[1] * 0.01, y,
                    f'λ={row["lambda"]:.2f}', va='center', fontsize=7,
                    color='#333333')

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=8)
        ax.set_xlabel('Value (millions USD)', fontsize=10)
        ax.set_title(TECH_SHORT[tech], fontsize=12)
        ax.legend(loc='lower right', fontsize=8)
        ax.grid(axis='x', alpha=0.2)

    fig.tight_layout()
    save_fig(fig, 'fig_d_pq_vs_gep_top20.png')

# =====================================================================
# FIGURE E: Lambda vs share of global production
# =====================================================================

fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
fig.suptitle(f'Lambda vs Share of Global Production ({TARGET_YEAR})',
             fontsize=14, y=1.02)

for ax, tech in zip(axes, techs):
    grp = df[df['Group Technology'] == tech].copy()
    total_q = grp['elec_prod_GWh'].sum()
    grp['q_share'] = grp['elec_prod_GWh'] / total_q * 100

    ax.scatter(grp['q_share'], grp['lambda'],
               s=grp['elec_prod_GWh'] / grp['elec_prod_GWh'].max() * 200 + 10,
               alpha=0.6, color=TECH_COLORS[tech], edgecolors='none')

    top5 = grp.nlargest(5, 'elec_prod_GWh')
    for _, row in top5.iterrows():
        name = row['Country']
        if len(name) > 15:
            name = row['ISO3 code']
        ax.annotate(name, (row['q_share'], row['lambda']),
                    fontsize=7, alpha=0.8,
                    xytext=(5, 3), textcoords='offset points')

    ax.set_xlabel('Share of Global Production (%)', fontsize=10)
    ax.set_title(TECH_SHORT[tech], fontsize=12)
    ax.set_xlim(left=-1)
    ax.grid(alpha=0.2)

axes[0].set_ylabel('Lambda (λ)', fontsize=11)
fig.tight_layout()
save_fig(fig, 'fig_e_lambda_vs_production_share.png')

# =====================================================================
# FIGURE F: Overlaid CF histograms with robust bounds
# =====================================================================

fig, ax = plt.subplots(figsize=(9, 5))
for tech in techs:
    vals = df.loc[df['Group Technology'] == tech, 'capacity_factor']
    ax.hist(vals, bins=25, alpha=0.45, color=TECH_COLORS[tech],
            label=f'{TECH_SHORT[tech]} (n={len(vals)})', edgecolor='white')

    # Draw robust upper bound
    b = norm_bounds[tech]
    ax.axvline(b['upper'], color=TECH_COLORS[tech], ls='--', linewidth=1.5, alpha=0.8)

ax.set_xlabel('Capacity Factor', fontsize=12)
ax.set_ylabel('Number of Countries', fontsize=12)
ax.set_title(f'CF Histograms with Robust Upper Bounds (dashed) — {TARGET_YEAR}',
             fontsize=12)
ax.legend(fontsize=10)
ax.grid(axis='y', alpha=0.3)

# Annotate
bounds_text = '\n'.join(
    f"{TECH_SHORT[t]}: [{norm_bounds[t]['lower']:.3f}, {norm_bounds[t]['upper']:.3f}]  "
    f"({norm_bounds[t]['method']})"
    for t in techs
)
ax.text(0.98, 0.95, bounds_text, transform=ax.transAxes, fontsize=8.5,
        va='top', ha='right', family='monospace',
        bbox=dict(boxstyle='round,pad=0.4', facecolor='lightyellow', alpha=0.8))
save_fig(fig, 'fig_f_cf_histograms_overlaid.png')

# =====================================================================
# FIGURE G: Missing-value diagnostics from the 02 provision outputs
# Total NA per input column, plus how many of those NAs are the SOLE
# reason a country's final GEP is NA (exactly one of the four inputs
# missing). Reads the CSVs written by script 02 (in ../output).
# =====================================================================

provision_dir = os.path.join('..', 'output')
NA_COLS = ['Q', 'P', 'lambda', 'ppp_ratio']

# Only build the figure if the 02 outputs exist (i.e. 02 has been run)
available = {
    tech: os.path.join(provision_dir,
                       f"{str(tech).replace(' ', '_').replace('/', '_')}"
                       f"_provision_gep.csv")
    for tech in techs
}
available = {t: p for t, p in available.items() if os.path.exists(p)}

if not available:
    print("\nNote: no 02 provision CSVs found in ../output — "
          "skipping fig_g (run script 02 first).")
else:
    fig, axes = plt.subplots(1, len(available), figsize=(5 * len(available), 5),
                             sharey=True, squeeze=False)
    axes = axes[0]
    fig.suptitle(
        'Missing-Value Diagnostics: NA Counts and Sole Blockers by Input '
        f'({TARGET_YEAR})', fontsize=14, y=1.03)

    x = np.arange(len(NA_COLS))
    width = 0.38

    for ax, (tech, path) in zip(axes, available.items()):
        prov = pd.read_csv(path)
        na_mask = prov[NA_COLS].isna()
        n_missing = na_mask.sum(axis=1)

        # Total NA per column, and NAs that are the sole GEP blocker
        total_na = [int(na_mask[c].sum()) for c in NA_COLS]
        sole_na = [int(((n_missing == 1) & na_mask[c]).sum()) for c in NA_COLS]
        gep_na = int((n_missing >= 1).sum())

        b1 = ax.bar(x - width / 2, total_na, width, label='Total NA',
                    color='lightgrey', edgecolor='#888')
        b2 = ax.bar(x + width / 2, sole_na, width, label='Sole blocker of GEP',
                    color='maroon', edgecolor='#888', alpha=0.85)
        ax.bar_label(b1, fontsize=8, padding=2)
        ax.bar_label(b2, fontsize=8, padding=2)

        ax.set_xticks(x)
        ax.set_xticklabels(NA_COLS, fontsize=10)
        ax.set_title(f'{TECH_SHORT[tech]}\n'
                     f'GEP NA for {gep_na} of {len(prov)} countries', fontsize=11)
        ax.grid(axis='y', alpha=0.25)
        ax.set_axisbelow(True)

    axes[0].set_ylabel('Number of countries', fontsize=11)
    axes[0].legend(fontsize=9, loc='upper left')
    fig.tight_layout()
    save_fig(fig, 'fig_g_missing_value_diagnostics.png')


print("\n" + "=" * 70)
print("DIAGNOSTICS COMPLETE")
print(f"All outputs saved to: {output_dir}")
print("=" * 70)