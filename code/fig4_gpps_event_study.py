
# fig_gpps_event_study.py — GPPS event-study coefficient plots
#
# Produces two event-study figures from the coefficient CSVs saved by
# gpps_analysis.R:
#
#   Figure I (primary):   effects of GP practice closure (gvar, LAD FE)
#   Figure II (robustness): nearest-practice treatment (gvar_nearest, MSOA FE)
#
# Input:
#   ../processed_data/gpps_coefficient_estimates.csv
#   ../processed_data/gpps_nearest_coefficient_estimates.csv
#
# Output:
#   ../final_output_for_article/fig4_gpps_coefficient_estimates.png
#   ../final_output_for_article/figA6_gpps_nearest_coefficient_estimates.png

import os
import pandas as pd
import plotly.graph_objects as go

INPUT_DIR  = os.path.join(os.path.dirname(__file__), '..', 'output_data_for_figures')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'final_output_for_article')
os.makedirs(OUTPUT_DIR, exist_ok=True)

FONT = 'Arial'


# ── Data cleaning ─────────────────────────────────────────────────────────────

def clean_gpps_data(filename='gpps_coefficient_estimates'):
    """Read a GPPS coefficient CSV and return a tidy DataFrame for plotting."""
    df = pd.read_csv(os.path.join(INPUT_DIR, f'{filename}.csv'))
    df['time'] = df['Unnamed: 0']
    del df['Unnamed: 0']
    df = df.loc[df['time'].str.startswith('year::')]
    df['time'] = df['time'].str.slice(6,)
    df['CI']   = df['Std. Error'] * 1.96

    neg = df.loc[df.outcome == 'negative_overall_experience_making_an_appointment'].copy()
    pos = df.loc[df.outcome == 'positive_overall_experience_with_gp_practice'].copy()

    # Positive rows have a trailing "1" appended by gpps_analysis.R; strip it
    pos['time'] = pos['time'].str[:-1]

    pos['time']  = pos['time'].astype(float)
    neg['time']  = neg['time'].astype(float)
    pos['model'] = 'Positive Experience'
    neg['model'] = 'Negative Experience'

    x = pd.concat([pos, neg])
    x = x.loc[(x.time > -7) & (x.time < 9)]

    # Reference period: normalise t = -1 to zero
    zero_pos = pd.DataFrame({'time': [-1], 'Estimate': [0], 'Std. Error': [0], 'CI': [0], 'model': ['Positive Experience']})
    zero_neg = pd.DataFrame({'time': [-1], 'Estimate': [0], 'Std. Error': [0], 'CI': [0], 'model': ['Negative Experience']})
    x = pd.concat([x, zero_pos, zero_neg]).sort_values(['model', 'time'])
    x['upper_bound'] = x['Estimate'] + x['CI']
    x['lower_bound'] = x['Estimate'] - x['CI']
    return x


# ── Plotting ──────────────────────────────────────────────────────────────────

def plot_gpps_event_study(x, annotation_text, fig_name):
    """Create and save a two-series event-study plot."""
    models  = ['Positive Experience', 'Negative Experience']
    labels  = ['<b>Positive</b> Overall Experience<br>', '<b>Negative</b> Experience Making an Appointment<br>']
    colors  = ['black', 'red']
    markers = ['diamond', 'circle']
    nudge   = [-0.05, 0.05]   # horizontal offset so markers don't overlap

    fig = go.Figure()

    for col, outcome in enumerate(models):
        xx = x.loc[(x.model == outcome) & (x.time >= -6) & (x.time < 6)].copy()
        fig.add_trace(go.Scatter(
            x=xx['time'] + nudge[col],
            y=xx['Estimate'],
            name=labels[col],
            error_y=dict(type='data', array=xx['CI'].values, visible=True),
            marker=dict(size=7, symbol=markers[col], color=colors[col],
                        line=dict(width=1, color='black')),
            line=dict(color=colors[col], width=1.5),
            mode='markers',
        ))

    fig.update_layout(
        font_family=FONT,
        font_color='black',
        font_size=16,
        template='presentation',
        yaxis_title='Coefficient Estimate and 95% CI',
        xaxis_title='Years (relative to practice closure)',
        title_x=0.05,
        showlegend=True,
        width=1000,
        height=600,
        margin=dict(l=80, r=40, t=60, b=120, pad=1),
        legend=dict(
            orientation='h', yanchor='bottom', y=1.02,
            xanchor='right', x=0.65, font_size=16,
            itemsizing='constant',
        ),
    )

    fig.add_vline(x=-1, line_width=1, line_dash='dash', line_color='black')
    fig.update_xaxes(showline=False, mirror=True, zeroline=False, showgrid=False)
    fig.update_yaxes(tickformat='.3f', range=[-0.03, 0.03])

    fig.add_shape(type='rect', xref='paper', yref='paper',
                  x0=0, y0=0, x1=1.0, y1=1.0,
                  line=dict(color='black', width=1))

    fig.add_annotation(
        x=1.02, y=-0.26,
        text=annotation_text,
        showarrow=False, align='right',
        xref='paper', yref='paper',
        font=dict(size=14, color='black', family=FONT),
        opacity=0.8,
    )

    out_path = os.path.join(OUTPUT_DIR, f'{fig_name}.png')
    fig.write_image(out_path)
    print(f"  Saved → {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    annotation_base = (
        "<b>Coefficient Estimates and 95% confidence intervals.</b><br>"
        "<b>Data Source:</b> <i>Ipsos MORI GP Patient Survey (2012–2023)</i>"
    )

    # Primary event-study figure
    print("Plotting primary GPPS event-study figure …")
    x = clean_gpps_data('gpps_coefficient_estimates')
    plot_gpps_event_study(x, annotation_base, 'fig4_gpps_coefficient_estimates')

    # Nearest-practice robustness figure
    print("Plotting nearest-practice robustness figure …")
    annotation_nearest = (
        annotation_base
        + "<br>Treatment assigned from the year the nearest GP practice closes."
    )
    x = clean_gpps_data('gpps_nearest_coefficient_estimates')
    plot_gpps_event_study(x, annotation_nearest, 'figA6_gpps_nearest_coefficient_estimates')

    print("Done.")


if __name__ == '__main__':
    main()
