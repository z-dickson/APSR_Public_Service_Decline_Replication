
# figA18_A19_bes_validation.py
#
# Validation figures comparing BES vote intention with actual UK general
# election results for 2015, 2017, and 2019.
#
# Produces two figures (Appendix P):
#
#   Figure A18:  Scatter of raw BES vote intention (%) vs actual vote share (%)
#                by party and election year, with per-party OLS fit lines.
#
#   Figure A19:  Same comparison with per-party min-max scaling, so that
#                parties with very different absolute vote shares are directly
#                comparable in terms of within-party tracking accuracy.
#
# Input:
#   ../data/BES2024_W29_Panel_v29.1.dta
#       Full British Election Study panel (Waves 1–29), from the BES website.
#       This extends through the 2024 general election, which is not covered by
#       the W1–25 UK Data Service file used for the main statistical analysis.
#
# Output:
#   ../final_output_for_article/figA18_bes_vote_intention_vs_actual.png
#   ../final_output_for_article/figA19_bes_vote_intention_vs_actual_scaled.png

import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'final_output_for_article')
DATA_DIR   = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(OUTPUT_DIR, exist_ok=True)

BES_FILE = os.path.join(DATA_DIR, 'BES2024_W29_Panel_v29.1.dta')


# ── Actual UK general election results (Wikipedia) ───────────────────────────
#
# National vote shares (%) for 2015, 2017, 2019.
# UKIP ran in 2015 and 2017 only; Brexit/Reform UK ran in 2019 only. You can find these headline results anywhere, but Wikipedia is a convenient source:
    
actual_results = pd.DataFrame({
    'Party': ['Conservative', 'Labour', 'Liberal Democrats', 'Green',
              'UKIP', 'SNP', 'Plaid Cymru', 'Brexit/Reform UK'],
    2015:    [36.8, 30.4,  7.9, 3.8, 12.6, 4.7, 0.6,  0.0],
    2017:    [42.3, 40.0,  7.4, 1.6,  1.8, 3.0, 0.5,  0.0],
    2019:    [43.6, 32.1, 11.6, 2.6,  0.07, 3.9, 0.5, 2.0],
}).melt(id_vars='Party', var_name='Year', value_name='Actual')
actual_results['Year'] = actual_results['Year'].astype(int)
actual_results = actual_results.loc[actual_results['Actual'] > 0]


# ── BES vote intention by wave ────────────────────────────────────────────────
#
# Wave 5  → 2015 general election
# Wave 12 → 2017 general election
# Wave 18 → 2019 general election
#
# Party codes in generalElectionVoteW{wave}:
#   1=Conservative, 2=Labour, 3=Liberal Democrats, 4=SNP, 5=Plaid Cymru,
#   6=UKIP, 7=Green, 12=Brexit/Reform UK

WAVE_YEAR  = {5: 2015, 12: 2017, 18: 2019}
PARTY_CODE = {
    1: 'Conservative', 2: 'Labour',    3: 'Liberal Democrats', 4: 'SNP',
    5: 'Plaid Cymru',  6: 'UKIP',      7: 'Green',             12: 'Brexit/Reform UK',
}

print("Loading BES panel …")
bes = pd.read_stata(BES_FILE, convert_categoricals=False)


def vote_intention_share(bes, wave, party_code):
    """Return the fraction of wave-{wave} respondents who said they would vote
    for party_code at the next general election."""
    wave_col = f'wave{wave}'
    vote_col = f'generalElectionVoteW{wave}'
    respondents = bes.loc[bes[wave_col] == 1]
    n_total = len(respondents)
    n_party = (respondents[vote_col] == party_code).sum()
    return {
        'Party': PARTY_CODE[party_code],
        'Year':  WAVE_YEAR[wave],
        'Intention': n_party / n_total if n_total else 0.0,
    }


print("Computing BES vote intention …")
rows = []
for wave in WAVE_YEAR:
    for code in PARTY_CODE:
        rows.append(vote_intention_share(bes, wave, code))
bes_intention = pd.DataFrame(rows)

# Convert to percentage to match actual_results scale
bes_intention['Intention'] = bes_intention['Intention'] * 100


# ── Merge ─────────────────────────────────────────────────────────────────────

df = (bes_intention
      .merge(actual_results, on=['Party', 'Year'], how='inner')
      .dropna(subset=['Intention', 'Actual']))


# ── Shared styling ────────────────────────────────────────────────────────────

PARTY_COLORS = {
    'Conservative':      'blue',
    'Labour':            'red',
    'Liberal Democrats': 'orange',
    'Green':             'lightgreen',
    'UKIP':              'purple',
    'SNP':               'yellow',
    'Plaid Cymru':       'darkgreen',
    'Brexit/Reform UK':  'Turquoise',
}

LAYOUT_BASE = dict(
    legend_title='Party (Pearson r)',
    template='presentation',
    title_x=0.05,
    title_font_family='Arial',
    title_font_size=24,
    title_font_color='black',
    width=950, height=700,
)


def _add_border(fig):
    fig.add_shape(type='rect', xref='paper', yref='paper',
                  x0=0, y0=0, x1=1.0, y1=1.0,
                  line=dict(color='black', width=1))


def _pearson_by_party(df):
    return (df.groupby('Party')
              .apply(lambda g: g['Intention'].corr(g['Actual']))
              .rename('r').reset_index())


# ── Figure A18: raw (unscaled) scatter ───────────────────────────────────────

print("Building Figure A18 …")

r_by_party = _pearson_by_party(df)
lim = float(max(df['Intention'].max(), df['Actual'].max()))

fig18 = go.Figure()

fig18.add_trace(go.Scatter(
    x=[0, lim], y=[0, lim],
    mode='lines', name='y = x (no bias)',
    line=dict(dash='dot', width=1, color='black'),
    hoverinfo='skip',
))

for party, g in df.groupby('Party'):
    color = PARTY_COLORS.get(party, 'gray')
    r = r_by_party.loc[r_by_party['Party'] == party, 'r'].iat[0]
    label = f"{party} (r={r:.2f})" if not np.isnan(r) else f"{party} (r=NA)"

    fig18.add_trace(go.Scatter(
        x=g['Intention'], y=g['Actual'],
        mode='markers', name=label,
        marker=dict(size=16, line=dict(width=2, color='DarkSlateGrey'), color=color),
        text=g['Year'],
        hovertemplate=(
            '<b>%{fullData.name}</b><br>'
            'Year=%{text}<br>'
            'Intention=%{x:.1f}%<br>'
            'Actual=%{y:.1f}%<extra></extra>'
        ),
    ))

    if len(g) >= 2:
        m, b = np.polyfit(g['Intention'], g['Actual'], 1)
        xs = np.array([g['Intention'].min(), g['Intention'].max()])
        fig18.add_trace(go.Scatter(
            x=xs, y=m * xs + b,
            mode='lines', line=dict(color=color, width=2),
            showlegend=False, hoverinfo='skip',
        ))

fig18.update_layout(
    title='BES Vote Intention vs Actual Vote Share',
    xaxis_title='Vote intention (%)',
    yaxis_title='Actual vote share (%)',
    xaxis=dict(range=[0, lim], zeroline=False),
    yaxis=dict(range=[-0.05, 0.45], zeroline=False, scaleanchor='x', scaleratio=1),
    **LAYOUT_BASE,
)
fig18.update_xaxes(tickfont=dict(size=16))
fig18.update_yaxes(tickfont=dict(size=16))
_add_border(fig18)

out18 = os.path.join(OUTPUT_DIR, 'figA18_bes_vote_intention_vs_actual.png')
fig18.write_image(out18)
print(f"  Saved → {out18}")


# ── Figure A19: per-party min-max scaled scatter ──────────────────────────────

print("Building Figure A19 …")


df_s = df.copy()
for col in ['Intention', 'Actual']:
    df_s[col + '_s'] = df_s.groupby('Party')[col].transform(
        lambda x: (x - x.min()) / ((x.max() - x.min()) if (x.max() - x.min()) != 0 else 1.0)
    )
r_by_party = _pearson_by_party(df)

fig19 = go.Figure()

fig19.add_trace(go.Scatter(
    x=[0, 1], y=[0, 1],
    mode='lines', name='y = x (no bias)',
    line=dict(dash='dot', width=2, color='black'),
    hoverinfo='skip',
))

for party, g in df_s.groupby('Party'):
    color = PARTY_COLORS.get(party, 'gray')
    r = r_by_party.loc[r_by_party['Party'] == party, 'r'].iat[0]
    label = f"{party} (r={r:.2f})" if not np.isnan(r) else f"{party} (r=NA)"

    fig19.add_trace(go.Scatter(
        x=g['Intention_s'], y=g['Actual_s'],
        mode='markers', name=label,
        marker=dict(size=14, line=dict(width=2, color='DarkSlateGrey'), color=color),
        text=g['Year'],
        hovertemplate=(
            '<b>%{fullData.name}</b><br>'
            'Year=%{text}<br>'
            'Scaled: Int=%{x:.2f}, Act=%{y:.2f}<br>'
            'Original: Int=%{customdata[0]:.1f}%, Act=%{customdata[1]:.1f}%'
            '<extra></extra>'
        ),
        customdata=np.c_[g['Intention'], g['Actual']],
    ))

    if len(g) >= 2:
        m, b = np.polyfit(g['Intention_s'], g['Actual_s'], 1)
        xs = np.array([g['Intention_s'].min(), g['Intention_s'].max()])
        fig19.add_trace(go.Scatter(
            x=xs, y=m * xs + b,
            mode='lines', line=dict(color=color, width=2),
            showlegend=False, hoverinfo='skip',
        ))

fig19.update_layout(
    title='Scaled BES Vote Intention vs Actual Vote Share (per-party min-max)',
    xaxis_title='Scaled vote intention',
    yaxis_title='Scaled actual vote share',
    xaxis=dict(range=[-0.05, 1.05], zeroline=False),
    yaxis=dict(range=[-0.05, 1.05], zeroline=False, scaleanchor='x', scaleratio=1),
    **LAYOUT_BASE,
)
fig19.update_xaxes(tickfont=dict(size=16))
fig19.update_yaxes(tickfont=dict(size=16))
_add_border(fig19)

out19 = os.path.join(OUTPUT_DIR, 'figA19_bes_vote_intention_vs_actual_scaled.png')
fig19.write_image(out19)
print(f"  Saved → {out19}")

print("Done.")
