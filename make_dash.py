import os
import sqlite3

import dash
import pandas as pd
import plotly.express as px
from dash import dcc, html
from dash.dependencies import Input, Output
from rich.console import Console
from rich.status import Status
from tqdm import tqdm

from database_ops import (read_all_author_names_from_db,
                          read_all_text_names_and_create_author_work_dict)
from predict_ops import (get_author_and_texts_published_after_current,
                         get_min_year_of_author_publication,
                         read_all_thresholds)
from util import get_project_name
from utils.get_choices import get_choices_for_viz

project_name = get_project_name()

def create_author_set_for_selection():
        author_set = read_all_author_names_from_db()
        return author_set

def display_author_menu(author_set):
    print("\n")
    for count, value in enumerate(author_set, start=1):
        print(f"{count}. {author_set[count]}")
    print("\n")

def read_all_combined_jaccard_from_db(selected_author, selected_threshold, selected_length, selected_year, direction):
    disk_conn = sqlite3.connect(f"./projects/{project_name}/db/{project_name}.db")
    disk_cur = disk_conn.cursor()
    which_author_direction = ""
    which_source_direction = ""

    # If we're comparing backward, we need to use target_auth/source_year because of the way items are arranged in the db.
    if direction == ">":
        which_author_direction = "source_auth"
        which_source_direction = "target_year"
    elif direction == "<":
        which_author_direction = "target_auth"
        which_source_direction = "source_year"

    query = f"""
        SELECT calc.threshold,
        SUM(CASE WHEN same_author = 'No' THEN 1 ELSE 0 END) AS No,
        SUM(CASE WHEN same_author = 'False Negative' THEN 1 ELSE 0 END) AS False_Negative,
        SUM(CASE WHEN same_author = 'Yes' THEN 1 ELSE 0 END) AS Yes,
        SUM(CASE WHEN same_author = 'False Positive' THEN 1 ELSE 0 END) AS False_Positive,
        source_length,
        target_length
        FROM combined_jaccard
        JOIN calculations AS calc ON combined_jaccard.pair_id = calc.pair_id
        WHERE combined_jaccard.{which_author_direction} = ?
        AND {which_source_direction} {direction} ?
        AND (source_length >= ? AND target_length >= ?)
        GROUP BY threshold
        HAVING threshold >= ?;
    """

    disk_cur.execute(query, [selected_author, selected_year, selected_length, selected_length, selected_threshold])
    the_combined_jacc = disk_cur.fetchall()
    disk_cur.close()
    disk_conn.close()
    return the_combined_jacc

def read_fp_scores_from_db(selected_author, selected_threshold, selected_length, selected_year, direction):
    disk_conn = sqlite3.connect(f"./projects/{project_name}/db/{project_name}.db")
    disk_cur = disk_conn.cursor()
    which_author_direction = ""
    which_source_direction = ""

    # If we're comparing backward, we need to use target_auth/source_year because of the way items are arranged in the db.
    if direction == ">":
        which_author_direction = "source_auth"
        which_source_direction = "target_year"
    elif direction == "<":
        which_author_direction = "target_auth"
        which_source_direction = "source_year"

    query = f"""
        SELECT calc.threshold, 
        calc.comp_score, 
        all_texts_source.source_filename AS source_text, 
        authors.author_name AS target_auth, 
        all_texts_target.source_filename AS target_text 
        FROM combined_jaccard 
        JOIN calculations AS calc ON combined_jaccard.pair_id = calc.pair_id 
        JOIN all_texts AS all_texts_source ON combined_jaccard.source_text = all_texts_source.text_id 
        JOIN all_texts AS all_texts_target ON combined_jaccard.target_text = all_texts_target.text_id 
        JOIN authors AS authors ON combined_jaccard.target_auth = authors.id
        WHERE combined_jaccard.{which_author_direction} = ? 
        AND {which_source_direction} {direction} ? 
        AND (source_length >= ? AND target_length >= ?) 
        AND threshold >= ? 
        ORDER BY comp_score DESC, calc.threshold ASC;
    """

    disk_cur.execute(query, [selected_author, selected_year, selected_length, selected_length, selected_threshold])
    top_fp_list = disk_cur.fetchall()
    disk_cur.close()
    disk_conn.close()

    # Filter out duplicates based on the 'target_text' column
    unique_top_fp_list = []
    seen_texts = set()
    i = 0
    for row in top_fp_list:
        if i < 50:
            target_text = row[4]  # Index 4 corresponds to 'target_text' in the query
            if target_text not in seen_texts:
                unique_top_fp_list.append(row)
                seen_texts.add(target_text)
                i += 1

    return unique_top_fp_list

def get_min_year_of_author_publication(id):
    disk_conn = sqlite3.connect(f"./projects/{project_name}/db/{project_name}.db")
    disk_cur = disk_conn.cursor()
    query = """SELECT
        MIN(year)
        FROM all_texts
        WHERE all_texts.author_id = ?
    """
    disk_cur.execute(query, [id])
    the_year = disk_cur.fetchone()
    disk_cur.close()
    disk_conn.close()
    return the_year[0]

authors_and_works_dict = read_all_text_names_and_create_author_work_dict()
author_set = create_author_set_for_selection()
threshold_set = read_all_thresholds()
default_author_name = list(author_set.values())[0]
hfp_div = html.Div(id='hfp-div')

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("Authorship Attribution Visualization"),
    html.P("Select a starting author, scoring threshold, and minimum text length:"),

    html.Div([
        dcc.Dropdown(
        id='author-dropdown',
        options=[{'label': author, 'value': author} for author in author_set.values()],
        value=default_author_name,  # Set the default author value
        style={'width': '100%'}
    ),
        dcc.Dropdown(
        id='direction-dropdown',
        options=[{'label': "Compare forward (chronologically)", 'value': "forward"}, {'label': "Compare backward (chronologically)", 'value': "backward"}],
        value="forward", #Default comparison direction,
        style={'width': '100%'}
    ),
    ], style={'display': 'flex', 'justify-content': 'space-between', 'width': '90%'}),

    dcc.Slider(
        id='threshold-slider',
        min=0.5,
        max=1.0,
        step=0.05,
        value=float(0.7)  # Set the default threshold value
    ),

    dcc.Slider(
        id='length-slider',
        min=250,  # Update min and max as needed
        max=3000,  # Update min and max as needed
        step=50,
        value=500,  # Set the default length range value
        marks={250: '250', 
        500: '500', 
        750: '750', 
        1000: '1k', 
        1250: '1.25k', 
        1500: '1.5k', 
        1750: '1.75k', 
        2000: '2k', 
        2250: '2.25k', 
        2500: '2.5k', 
        2750: '2.75k',
        3000: '3k'}
    ),

    dcc.Graph(id='line-plot', style={'height': 'calc(100vh - 300px)'}),
    
    # Add the message_div to display messages
    html.Br(),

    # Add the div for showing highest FP values
    html.H2("Highest-Ranked False Positives using above parameters:"),
    hfp_div,
])

@app.callback(
    Output('line-plot', 'figure'),
    Output('hfp-div', 'children'),  # Output for displaying hfp_df
    Input('author-dropdown', 'value'),
    Input('threshold-slider', 'value'),
    Input('length-slider', 'value'),
    Input('direction-dropdown', 'value'),
)
def update_line_plot(selected_author, selected_threshold, selected_length, selected_direction):
    disk_conn = sqlite3.connect(f"./projects/{project_name}/db/{project_name}.db")
    disk_cur = disk_conn.cursor()

    # Extract the min and max length values from the selected_length_range
    min_length = selected_length
    # Fetch data based on selected_author, selected_threshold, and length range
    selected_key = next((key for key, value in author_set.items() if value == selected_author), None)
    min_author_choice_year = get_min_year_of_author_publication(selected_key)
    #Small dictionary of direction symbols for printing, based on selected_direction
    directions = {"forward": ">", "backward": "<"}

    data = read_all_combined_jaccard_from_db(selected_key, selected_threshold, selected_length, min_author_choice_year, directions[selected_direction])
    data_df = pd.DataFrame(data, columns=['threshold', 'Yes', 'No', 'False Negative', 'False Positive', 'source_length', 'target_length'])

    highest_fp = read_fp_scores_from_db(selected_key, selected_threshold, selected_length, min_author_choice_year, directions[selected_direction])
    hfp_df = pd.DataFrame(highest_fp, columns=['threshold', 'score', 'source_text', 'target_auth', 'target_text'])
    converted_hfp = hfp_df.to_markdown(tablefmt="grid")
    hfp_table = dcc.Markdown(f"```{converted_hfp}") #Not closing the ```, as pandas does this.

    # Create the line plot using Plotly Express
    fig = px.line(data_df, x='threshold', y=['Yes', 'No', 'False Negative', 'False Positive'],
                  labels={'threshold': 'Threshold', 'value': 'Count', 'variable': 'Same Author'},
                  title=f'Same Author Counts at Different Thresholds (Starting author: {selected_author}; threshold >= {selected_threshold}; min_length = {selected_length}; Years: {directions[selected_direction]} {min_author_choice_year})')

    # Add data points as dots to the line plot
    fig.update_traces(mode='lines+markers')
    
    return fig, hfp_table

if __name__ == "__main__":
    app.run_server(debug=True)
