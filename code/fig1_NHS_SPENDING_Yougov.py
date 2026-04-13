
# Creates Figure 1: What sector should the UK government spend more on?
# Line chart of YouGov tracker data (April 2020 - April 2024) showing the share of
# respondents who say the government should spend more on each sector. The NHS is
# highlighted; all other sectors are shown in grey.
#
# Data source: YouGov UK tracker
# https://yougov.co.uk/topics/society/trackers/what-sector-should-the-uk-government-spend-more-on
# Data file: ../raw_data/what-sector-should-the-uk-government-spend-more-on.xlsx


import pandas as pd
import plotly.express as px
import plotly.graph_objects as go



DATA_LOC = 'what-sector-should-the-uk-government-spend-more-on.xlsx'
SHEET_NAME = 'All adults'
DIR = '../data/'
OUTPUT_DIR = '../final_output_for_article/'
OUTPUT_NAME = 'fig1_NHS_SPENDING_Yougov.png'



# create dir if it doesn't exist
import os
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    
    
    
    
    


# read the data file and do some cleaning
def read_file(file):
    df=pd.read_excel(DIR + file, 
                sheet_name=SHEET_NAME)

    df = df.transpose()
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    ## rename the first column to date 
    df['date'] = df.index
    df.date = pd.to_datetime(df.date)
    df = df.reset_index(drop=True)
    return df 







# plot using plotly 
def plot_spending(spend):
    fig = go.Figure()

    cols = ['Well','Badly']

    colors = [ '#387da8', '#cf514e']

    for i, col in enumerate(spend.columns[:-5]):
        fig.add_trace(go.Scatter(x=spend['date'], y=spend[col], mode='lines+text', name=col, marker=dict(size=10),
                                marker_symbol='circle',
                                marker_line_color='black',
                                marker_line_width=1,
                                line = dict(width=10, color= colors[1] if col == 'NHS' else px.colors.qualitative.T10[i]), 
                        
                                opacity = .3 if col != 'NHS' else 1, 
                    ))
                            

        
    fig.update_layout(
        font_family="Arial",
        font_color="black",
        title_font_family="Arial",
        title_font_color="black",
        title_font_size = 14,
        font_size = 14,
        legend_title_font_color="black", 
        template='presentation',
        yaxis_title="Percentage",
        xaxis_title="Date",
        title_x=0.05, 
        showlegend=False,
        width=800,
        height=500,
        margin=dict(l=70, r=30, t=40, b=75, pad=1),
        )


    fig.add_annotation(
        x=1.0,
        y=-.2,
        text="<b>Source:</b><i>YouGov UK</i>",
        showarrow=False,
        align='right',
        xref="paper",
        yref="paper",
        font=dict(size=14, color="black", family="Arial"),
        opacity=1,
    )



    ## add annotations for each issue 


    fig.add_annotation(
        x=0.6,
        y=.82,
        text="<b>National Health Service</b>",
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowwidth=2,
        ax=20,
        ay=30,
        align='right',
        xref="paper",
        yref="paper",
        font=dict(size=24, color="black", family="Arial"),
        opacity=1,
    )

    fig.add_annotation(
        x=0.54,
        y=.44,
        text="Education",
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowwidth=2,
        ax=20,
        ay=-30,
        align='right',
        xref="paper",
        yref="paper",
        font=dict(size=16, color="black", family="Arial"),
        opacity=1,
    )

    fig.add_annotation(
        x=0.13,
        y=.55,
        text="Crime & Policing",
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowwidth=2,
        ax=20,
        ay=-40,
        align='right',
        xref="paper",
        yref="paper",
        font=dict(size=16, color="black", family="Arial"),
        opacity=1,
    )

    fig.add_annotation(
        x=0.26,
        y=.4,
        text="Climate Change",
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowwidth=2,
        ax=20,
        ay=25,
        align='right',
        xref="paper",
        yref="paper",
        font=dict(size=12, color="black", family="Arial"),
        opacity=1,
    )

    ## make percentage y axis 
    fig.update_yaxes(tickformat=".0%")

    fig.show()

    fig.write_image(OUTPUT_DIR + OUTPUT_NAME)

    










if __name__ == "__main__":
    spend = read_file(DATA_LOC)
    plot_spending(spend)


