import os
import sqlite3

import dash
import pandas as pd
from dash import dcc, html
from dash.dependencies import Input, Output

from database_ops import (read_all_author_names_from_db,
                          read_all_text_names_and_create_author_work_dict)
from predict_ops import read_all_thresholds
from util import get_project_name

project_name = get_project_name()

def create_author_set_for_selection():
        author_set = read_all_author_names_from_db()
        return author_set

def display_author_menu(author_set):
    print("\n")
    for count, value in enumerate(author_set, start=1):
        print(f"{count}. {author_set[count]}")
    print("\n")

def read_fp_scores_from_db(selected_threshold, selected_length, selected_year, direction, limit):
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

    #Kludge to try and speed-up lookups
    super_limit = int(limit) * 16

    query = f"""
        SELECT calc.threshold, 
            calc.comp_score, 
            authors_src.author_name AS source_auth, 
            all_texts_source.source_filename AS source_text, 
            authors.author_name AS target_auth, 
            all_texts_target.source_filename AS target_text 
            FROM combined_jaccard 
            JOIN calculations AS calc ON combined_jaccard.pair_id = calc.pair_id 
            JOIN all_texts AS all_texts_source ON combined_jaccard.source_text = all_texts_source.text_id 
            JOIN all_texts AS all_texts_target ON combined_jaccard.target_text = all_texts_target.text_id 
            JOIN authors AS authors_src ON combined_jaccard.source_auth = authors_src.id 
            JOIN authors AS authors ON combined_jaccard.target_auth = authors.id 
            WHERE {which_source_direction} {direction} ? 
            AND (source_length >= ? AND target_length >= ?) 
            AND threshold = ? 
            ORDER BY comp_score DESC, calc.threshold ASC
            LIMIT ?;
    """

    disk_cur.execute(query, [selected_year, selected_length, selected_length, selected_threshold, super_limit])
    top_fp_list = disk_cur.fetchall()
    disk_cur.close()
    disk_conn.close()

    # Filter out duplicates based on the 'target_text' column
    unique_top_fp_list = []
    seen_texts = set()
    i = 0
    for row in top_fp_list:
        if i < int(limit):
            target_text = row[4]  # Index 4 corresponds to 'target_text' in the query
            if target_text not in seen_texts:
                unique_top_fp_list.append(row)
                seen_texts.add(target_text)
                i += 1

    return unique_top_fp_list

authors_and_works_dict = read_all_text_names_and_create_author_work_dict()
threshold_set = read_all_thresholds()

threshold_div = html.Div(id='threshold-div')
length_div = html.Div(id='length-div')
year_div = html.Div(id='year-div')
hfp_div = html.Div(id='hfp-div')

year_marks = {}
for year in range(1800, 1951, 5):
    year_marks[year] = str(year)

length_marks = {}
for length in range(250, 3001, 250):
    length_marks[length] = str(length)

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("Jumbo Authorship Comparison"),
    html.H6("(It's slow. I know.)"),

    html.Div([
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
        marks=length_marks
    ),

    dcc.Slider(
        id='year-slider',
        min=1800,  # Update min and max as needed
        max=1950,  # Update min and max as needed
        step=5,
        value=int(1850),  # Set the default length range value
        marks=year_marks
    ),

    html.H4("Limit number of results to: "),

    dcc.Input(
        id='limit-input',
        type='number',  # Use 'number' type for numeric input
        value=25,  # Default value
        placeholder='Number of results?',
        debounce=True  # Debounce input to reduce callback invocations
    ),
    
    # Add the message_div to display messages
    html.Br(),

    # Add the div for showing highest FP values
    html.H2("Highest-Ranked False Positives:"),
    threshold_div,
    length_div,
    year_div,
    hfp_div,
])

@app.callback(
    Output('threshold-div', 'children'),
    Output('length-div', 'children'),
    Output('year-div', 'children'),
    Output('hfp-div', 'children'),  # Output for displaying hfp_df
    Input('threshold-slider', 'value'),
    Input('length-slider', 'value'),
    Input('year-slider', 'value'),
    Input('direction-dropdown', 'value'),
    Input('limit-input', 'value'),
)

def update_line_plot(selected_threshold, selected_length, selected_year, selected_direction, selected_limit):
    #Small dictionary of direction symbols for printing, based on selected_direction
    directions = {"forward": ">", "backward": "<"}

    highest_fp = read_fp_scores_from_db(selected_threshold, selected_length, selected_year, directions[selected_direction], selected_limit)
    hfp_df = pd.DataFrame(highest_fp, columns=['threshold', 'score', 'source_auth', 'source_text', 'target_auth', 'target_text'])
    converted_hfp = hfp_df.to_markdown(tablefmt="grid")
    hfp_table = dcc.Markdown(f"```{converted_hfp}") #Not closing the ```, as pandas does this.
    
    return f"Threshold: {selected_threshold}", f"Min. Length: {selected_length}", f"Selected Year: {selected_year}", hfp_table

if __name__ == "__main__":
    app.run_server(debug=True)
