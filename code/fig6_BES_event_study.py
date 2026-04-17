# fig6_BES_event_study.py
#
# BES event-study coefficient plots.
#
# Produces six event-study figures from the coefficient CSVs saved by
# bes_analysis.R:
#
#   Figure 6  (main text):      full-controls model (m4), rrw_vote outcome
#   Figure 9  (main text):      mainstream party vote intentions (with controls)
#   Figure A9 (Appendix E):     model progression m1–m4, 2×2 subplot
#   Figure A13 (Appendix J):    past vote robustness
#   Figure A14 (Appendix K):    not-yet-treated control group robustness
#   Figure A16 (Appendix N):    mainstream party vote intentions (no controls)
#   Figure A17 (Appendix):      matrix completion ATT robustness
#
# Input (from bes_analysis.R):
#   ../output_data_for_figures/bes_primary_m{1,2,3,4}.csv
#   ../output_data_for_figures/bes_robust_past_vote.csv
#   ../output_data_for_figures/bes_robust_not_yet_treated.csv
#   ../output_data_for_figures/bes_robust_{labour,conservative,libdem,green}_vote.csv
#   ../output_data_for_figures/bes_robust_controls_{labour,conservative,libdem,green}_vote.csv
#   ../output_data_for_figures/bes_matrix_completion_att.csv
#
# Output:
#   ../final_output_for_article/fig6_bes_event_study.png
#   ../final_output_for_article/fig9_bes_mainstream_parties.png
#   ../final_output_for_article/figA9_bes_event_study_four_models.png
#   ../final_output_for_article/figA13_bes_past_vote_event_study.png
#   ../final_output_for_article/figA14_bes_not_yet_treated_event_study.png
#   ../final_output_for_article/figA16_bes_mainstream_parties_no_controls.png
#   ../final_output_for_article/figA17_bes_matrix_completion_event_study.png

import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

INPUT_DIR  = os.path.join(os.path.dirname(__file__), '..', 'output_data_for_figures')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'final_output_for_article')
os.makedirs(OUTPUT_DIR, exist_ok=True)

FONT         = 'Arial'
MARKER_COLOR = '#cf514e'   # red used consistently across BES figures

SOURCE_ANNOTATION = (
    "<i><b>Source:</b> British Election Study MSOA Data (Waves 1–25)</i><br>"
    "<i><b>Covariates:</b> Unemployment, IMD, Immigration, GP Migrant Registrations</i>"
)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_mc_csv(filename='bes_matrix_completion_att'):
    """Read a fect est.att CSV and return a tidy DataFrame for plotting.

    Unlike load_bes_csv, no reference row is inserted at t = -1 because fect
    estimates all pre-treatment periods rather than omitting a reference period.
    """
    df = pd.read_csv(os.path.join(INPUT_DIR, f'{filename}.csv'))
    df = df.rename(columns={'Unnamed: 0': 'time'})
    df['time']     = pd.to_numeric(df['time'])
    df['Estimate'] = df['ATT']
    df['CI']       = (df['CI.upper'] - df['CI.lower']) / 2
    return df.sort_values('time').reset_index(drop=True)


def load_bes_csv(filename):
    """Read a BES coefficient CSV and return a tidy DataFrame for plotting.

    Filters to event-study rows (year::*), parses the relative time period,
    computes 95% confidence intervals, and inserts a reference row at t = -1
    normalised to zero (the omitted baseline in the Sun & Abraham estimator).
    """
    df = pd.read_csv(os.path.join(INPUT_DIR, f'{filename}.csv'))
    df = df.rename(columns={'Unnamed: 0': 'row_name'})

    # Keep only event-study coefficients; drop control variable rows
    df = df.loc[df['row_name'].str.startswith('year::')].copy()
    df['time'] = df['row_name'].str.slice(6).astype(float)
    df['CI']   = df['Std. Error'] * 1.96

    # Normalise t = -1 to zero (reference/omitted period)
    zero = pd.DataFrame({'time': [-1.0], 'Estimate': [0.0],
                         'Std. Error': [0.0], 'CI': [0.0]})
    df = pd.concat([df, zero], ignore_index=True)
    return df.sort_values('time').reset_index(drop=True)


# ── Plot helpers ──────────────────────────────────────────────────────────────

def _event_trace(df, time_range=(-6, 6), name='', color=MARKER_COLOR):
    """Return a Scatter trace for one event-study series."""
    d = df.loc[(df['time'] >= time_range[0]) & (df['time'] <= time_range[1])]
    return go.Scatter(
        x=d['time'],
        y=d['Estimate'],
        name=name,
        mode='markers',
        error_y=dict(type='data', array=d['CI'].values, visible=True, color=color),
        marker=dict(size=8, symbol='diamond', color='black',
                    line=dict(width=2, color=color)),
    )


def _style_axes(fig):
    """Apply shared axis formatting across all axes in a figure."""
    fig.update_xaxes(showline=False, mirror=True, zeroline=False, showgrid=False)
    fig.update_yaxes(tickformat='.3f')
    fig.add_hline(y=0,  line_width=1, line_color='black')
    fig.add_vline(x=-1, line_width=1, line_dash='dash', line_color='black')


def _add_border(fig):
    """Draw a bounding box around the plot area."""
    fig.add_shape(type='rect', xref='paper', yref='paper',
                  x0=0, y0=0, x1=1.0, y1=1.0,
                  line=dict(color='black', width=1))


def _footnote(fig, text, y=-0.26):
    """Add a right-aligned source/note annotation below the figure."""
    fig.add_annotation(
        x=1.0, y=y, text=text, showarrow=False, align='right',
        xref='paper', yref='paper',
        font=dict(size=14, color='black', family=FONT), opacity=0.8,
    )


# ── Single event-study plot (used for Figure 6, A13, A14) ────────────────────

def plot_single_event_study(df, out_name, note=None,
                            xaxis_title='Years (relative to practice closure)',
                            time_range=(-6, 6)):
    """Single event-study plot.  Used for Figure 6 (primary) and robustness figures."""
    if note is None:
        note = SOURCE_ANNOTATION
    fig = go.Figure(_event_trace(df, time_range=time_range))
    fig.update_layout(
        font_family=FONT, font_color='black', font_size=16,
        template='presentation',
        yaxis_title='Coefficient Estimate and 95% CI',
        xaxis_title=xaxis_title,
        showlegend=False,
        width=1000, height=600,
        margin=dict(l=80, r=40, t=40, b=130, pad=1),
    )
    _style_axes(fig)
    _add_border(fig)
    _footnote(fig, note)

    out_path = os.path.join(OUTPUT_DIR, out_name)
    fig.write_image(out_path)
    print(f'  Saved → {out_path}')


# ── Appendix E: four-model 2×2 subplot (m1–m4) ───────────────────────────────

def plot_four_models(dfs):
    """2×2 subplot showing the four BES model specifications (Appendix E)."""
    subtitles = [
        'Model 1: No Covariates',
        'Model 2: IMD Score',
        'Model 3: IMD Score + Employment Rate',
        'Model 4: IMD + Employment + Migration',
    ]
    fig = make_subplots(rows=2, cols=2, subplot_titles=subtitles,
                        vertical_spacing=0.15, horizontal_spacing=0.1)

    for i, df in enumerate(dfs):
        fig.add_trace(
            _event_trace(df, name=subtitles[i]),
            row=i // 2 + 1, col=i % 2 + 1,
        )

    fig.update_layout(
        font_family=FONT, font_color='black', font_size=16,
        template='presentation',
        showlegend=False,
        width=1000, height=900,
        margin=dict(l=80, r=40, t=60, b=120, pad=1),
    )
    fig.update_yaxes(title_text='Coefficient Estimate and 95% CI')
    fig.update_xaxes(title_text='Years (relative to practice closure)')
    _style_axes(fig)
    _footnote(fig, "<i><b>Source:</b> British Election Study (Waves 1–25)</i>", y=-0.12)

    out_path = os.path.join(OUTPUT_DIR, 'figA9_bes_event_study_four_models.png')
    fig.write_image(out_path)
    print(f'  Saved → {out_path}')


# ── Mainstream party vote intentions 2×2 subplot (Figure 9 and Figure A16) ───

def plot_mainstream_parties(dfs, labels, out_name):
    """2×2 subplot showing effects on mainstream party vote intentions.

    Used for Figure 9 (with controls) and Figure A16 (no controls).
    Uses a shared y-axis range across panels so that effect magnitudes are
    directly comparable across parties.
    """
    fig = make_subplots(rows=2, cols=2, subplot_titles=labels,
                        vertical_spacing=0.15, horizontal_spacing=0.1)

    for i, (df, label) in enumerate(zip(dfs, labels)):
        fig.add_trace(
            _event_trace(df, time_range=(-4, 4), name=label),
            row=i // 2 + 1, col=i % 2 + 1,
        )

    fig.update_layout(
        font_family=FONT, font_color='black', font_size=16,
        template='presentation',
        showlegend=False,
        width=1000, height=900,
        margin=dict(l=80, r=40, t=60, b=120, pad=1),
    )
    fig.update_yaxes(title_text='Coefficient Estimate and 95% CI',
                     range=[-0.09, 0.09])   # shared scale for cross-party comparability
    fig.update_xaxes(title_text='Years (relative to practice closure)')
    _style_axes(fig)
    _footnote(fig, "<i><b>Source:</b> British Election Study (Waves 1–25)</i>", y=-0.12)

    out_path = os.path.join(OUTPUT_DIR, out_name)
    fig.write_image(out_path)
    print(f'  Saved → {out_path}')


# ── Main ──────────────────────────────────────────────────────────────────────

PARTY_LABELS = ['Conservative Party', 'Labour Party', 'Liberal Democrats', 'Green Party']
PARTY_ORDER  = ['conservative_vote', 'labour_vote', 'libdem_vote', 'green_vote']


def main():
    # Figure 6: primary BES result — full-controls model (m4)
    print('Plotting Figure 6: BES event study (m4, full controls) …')
    plot_single_event_study(load_bes_csv('bes_primary_m4'), 'fig6_bes_event_study.png')

    # Figure A9 (Appendix E): all four BES model specifications side-by-side
    print('Plotting Figure A9: BES four-model event study …')
    plot_four_models([load_bes_csv(f'bes_primary_m{i}') for i in range(1, 5)])

    # Figure 9 (main text): mainstream party vote intentions — WITH controls
    print('Plotting Figure 9: mainstream party vote intentions (with controls) …')
    plot_mainstream_parties(
        [load_bes_csv(f'bes_robust_controls_{p}') for p in PARTY_ORDER],
        PARTY_LABELS,
        out_name='fig9_bes_mainstream_parties.png',
    )

    # Figure A13 (Appendix J): past vote robustness
    print('Plotting Figure A13: BES past vote event study …')
    plot_single_event_study(
        load_bes_csv('bes_robust_past_vote'),
        'figA13_bes_past_vote_event_study.png',
        note=(
            "<i><b>Source:</b> British Election Study MSOA Data (Waves 1–25)</i><br>"
            "<i><b>Outcome:</b> Past Reported Vote for Populist Right Party</i>"
        ),
    )

    # Figure A14 (Appendix K): not-yet-treated control group robustness
    print('Plotting Figure A14: BES not-yet-treated event study …')
    plot_single_event_study(
        load_bes_csv('bes_robust_not_yet_treated'),
        'figA14_bes_not_yet_treated_event_study.png',
        note=(
            "<i><b>Source:</b> British Election Study MSOA Data (Waves 1–25)</i><br>"
            "<i><b>Control group:</b> Not-yet-treated MSOAs only</i>"
        ),
    )

    # Figure A16 (Appendix N): mainstream party vote intentions — NO controls
    print('Plotting Figure A16: mainstream party vote intentions (no controls) …')
    plot_mainstream_parties(
        [load_bes_csv(f'bes_robust_{p}') for p in PARTY_ORDER],
        PARTY_LABELS,
        out_name='figA16_bes_mainstream_parties_no_controls.png',
    )

    # Figure A17: matrix completion ATT robustness
    print('Plotting Figure A17: BES matrix completion ATT event study …')
    plot_single_event_study(
        load_mc_csv('bes_matrix_completion_att'),
        'figA17_bes_matrix_completion_event_study.png',
        note=(
            "<i><b>Source:</b> British Election Study MSOA Data (Waves 1–25)</i><br>"
            "<i><b>Estimator:</b> Matrix completion (Athey et al. 2021) via <i>fect</i></i>"
        ),
        xaxis_title='Survey waves (relative to practice closure)',
        time_range=(-12, 12),
    )

    print('Done.')


if __name__ == '__main__':
    main()
