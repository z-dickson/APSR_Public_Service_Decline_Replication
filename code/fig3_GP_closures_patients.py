
# Creates Figure 3: Number of Patients Registered to a Closing GP Practice in England
#
# Left panel:  Annual and cumulative patients registered at a practice in the year
#              before it closed.
# Right panel: Number of GP practice closures per year.
#
# Reads from the pre-built GP panel (../processed_data/gp_closures_panel.csv).
# Run build_gp_panel.py first if that file does not exist.

import os
import sys
import warnings
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px

warnings.filterwarnings('ignore')

GP_CLOSURES  = '../data/gp_practice_registrations_panel.csv'
OUTPUT_DIR  = '../final_output_for_article/'
OUTPUT_NAME = 'fig3_GP_closures_patients.png'

if not os.path.exists(GP_CLOSURES):
    print(
        f"Panel file not found: {GP_CLOSURES}\n"
        "Please run build_gp_panel.py first."
    )
    sys.exit(1)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# ── 1. Load panel ─────────────────────────────────────────────────────────────

panel = pd.read_csv(GP_CLOSURES)


# ── 2. Patients affected per year ─────────────────────────────────────────────

patients_per_year = (
    panel.groupby('year')['patients_before_close'].sum()
    .reset_index()
    .sort_values('year')
)
patients_per_year['cumulative'] = patients_per_year['patients_before_close'].cumsum()
patients_per_year = patients_per_year.loc[patients_per_year['year'] < 2023]


# ── 3. Closures per year ──────────────────────────────────────────────────────

closed_per_year = (
    panel.drop_duplicates('gp_practice_code')
    .dropna(subset=['close_year'])
    .loc[lambda x: (x['close_year'] >= 2013) & (x['close_year'] < 2023)]
    .groupby('close_year').size()
    .reset_index(name='num_closures')
    .rename(columns={'close_year': 'year'})
    .sort_values('year')
)
closed_per_year['year'] = closed_per_year['year'].astype(int)


# ── 4. Plot ───────────────────────────────────────────────────────────────────

def alternate_positions(n):
    pos = ['top center', 'bottom center']
    return [pos[i % 2] for i in range(n)]

names = ['<b>Patients Affected</b>', '<b>Number of GP Practice Closures</b>']
fig = make_subplots(rows=1, cols=2, subplot_titles=names)

red = px.colors.qualitative.Plotly[1]

# Left panel: annual + cumulative patients
fig.add_trace(go.Scatter(
    x=patients_per_year['year'],
    y=patients_per_year['patients_before_close'],
    mode='lines+text',
    name='Number Affected',
    text=patients_per_year['patients_before_close'].apply(lambda v: f'{int(v):,}'),
    textposition=alternate_positions(len(patients_per_year)),
    line=dict(width=5, color=red),
    marker=dict(size=8, line_color='black', line_width=1),
), row=1, col=1)

fig.add_trace(go.Scatter(
    x=patients_per_year['year'],
    y=patients_per_year['cumulative'],
    mode='lines+text',
    name='Cumulative Number Affected',
    text=patients_per_year['cumulative'].apply(lambda v: f'{int(v):,}'),
    textposition=alternate_positions(len(patients_per_year)),
    line=dict(width=5, color='lightgrey'),
    marker=dict(size=8, line_color='black', line_width=1),
), row=1, col=1)

# Right panel: closures per year
fig.add_trace(go.Scatter(
    x=closed_per_year['year'],
    y=closed_per_year['num_closures'],
    mode='lines+text',
    name='Number of GP Practice Closures',
    text=closed_per_year['num_closures'],
    textposition=alternate_positions(len(closed_per_year)),
    showlegend=False,
    line=dict(width=5, color=red),
    marker=dict(size=8, line_color='black', line_width=1),
), row=1, col=2)

fig.update_layout(
    font_family='Arial',
    font_color='black',
    font_size=16,
    template='presentation',
    yaxis_title='Number of Patients',
    xaxis_title='Date',
    width=1100,
    height=500,
    margin=dict(l=70, r=40, t=50, b=75, pad=1),
    legend=dict(yanchor='top', y=1, xanchor='left', x=0, font=dict(size=12)),
)

fig.update_xaxes(range=[2012, 2023])

fig.write_image(OUTPUT_DIR + OUTPUT_NAME)
print(f"Saved {OUTPUT_DIR + OUTPUT_NAME}")
