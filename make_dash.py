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

def read_all_combined_jaccard_from_db(selected_author, selected_threshold, selected_length, selected_year):
    disk_conn = sqlite3.connect(f"./projects/{project_name}/db/{project_name}.db")
    disk_cur = disk_conn.cursor()

    query = """
        SELECT calc.threshold,
        SUM(CASE WHEN same_author = 'No' THEN 1 ELSE 0 END) AS No,
        SUM(CASE WHEN same_author = 'False Negative' THEN 1 ELSE 0 END) AS False_Negative,
        SUM(CASE WHEN same_author = 'Yes' THEN 1 ELSE 0 END) AS Yes,
        SUM(CASE WHEN same_author = 'False Positive' THEN 1 ELSE 0 END) AS False_Positive,
        source_length,
        target_length
        FROM combined_jaccard
        JOIN calculations AS calc ON combined_jaccard.pair_id = calc.pair_id
        WHERE combined_jaccard.source_auth = ?
        AND target_year > ?
        AND (source_length >= ? AND target_length >= ?)
        GROUP BY threshold
        HAVING threshold >= ?;
    """

    disk_cur.execute(query, [selected_author, selected_year, selected_length, selected_length, selected_threshold])
    the_combined_jacc = disk_cur.fetchall()
    return the_combined_jacc

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

app = dash.Dash(__name__)

message_div = html.Div(id='message-div')

app.layout = html.Div([
    html.H1("Authorship Attribution Visualization"),
    
    dcc.Dropdown(
        id='author-dropdown',
        options=[{'label': author, 'value': author} for author in author_set.values()],
        value=default_author_name  # Set the default author value
    ),

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
        max=1500,  # Update min and max as needed
        step=50,
        value=500  # Set the default length range value
    ),
    
    dcc.Graph(id='line-plot', style={'height': 'calc(100vh - 300px)'}),
    
    # Add the message_div to display messages
    message_div,
])

@app.callback(
    Output('line-plot', 'figure'),
    Output('message-div', 'children'),  # Output for displaying messages
    Input('author-dropdown', 'value'),
    Input('threshold-slider', 'value'),
    Input('length-slider', 'value'),
)
def update_line_plot(selected_author, selected_threshold, selected_length):
    disk_conn = sqlite3.connect(f"./projects/{project_name}/db/{project_name}.db")
    disk_cur = disk_conn.cursor()

    # Use Dash's logging feature to display messages
    message = "N.B. Click author's name to restart viz, if nothing seems to happen."
    
    # Extract the min and max length values from the selected_length_range
    min_length = selected_length
    # Fetch data based on selected_author, selected_threshold, and length range
    selected_key = next((key for key, value in author_set.items() if value == selected_author), None)
    min_author_choice_year = get_min_year_of_author_publication(selected_key)

    data = read_all_combined_jaccard_from_db(selected_key, selected_threshold, selected_length, min_author_choice_year)
    data_df = pd.DataFrame(data, columns=['threshold', 'Yes', 'No', 'False Negative', 'False Positive', 'source_length', 'target_length'])
    
    # Create the line plot using Plotly Express
    fig = px.line(data_df, x='threshold', y=['Yes', 'No', 'False Negative', 'False Positive'],
                  labels={'threshold': 'Threshold', 'value': 'Count', 'variable': 'Same Author'},
                  title=f'Same Author Counts at Different Thresholds (Starting author: {selected_author}; threshold >= {selected_threshold}; min_length = {selected_length}; Years: > {min_author_choice_year})')

    # Add data points as dots to the line plot
    fig.update_traces(mode='lines+markers')
    
    return fig, message


if __name__ == "__main__":
    app.run_server(debug=True)
