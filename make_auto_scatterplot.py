import os

import pandas as pd
import plotly.express as px
from rich.console import Console
from rich.status import Status

from database_ops import (read_all_author_names_from_db,
                          read_all_text_names_by_id_from_db)
from predict_ops import create_author_view, get_author_view_length
from util import create_author_pair_for_lookups

console = Console()

def create_author_set_for_selection():
    author_set = read_all_author_names_from_db()
    max_key_num = max(author_set.keys())
    author_set[max_key_num + 1] = '%' #Append to allow for an 'Any' author.
    return author_set

def display_author_menu(author_set):
    print("\n")
    for count, value in enumerate(author_set, start=1):
        print(f"{count}. {author_set[count]}")
    print("\n'%' is a wildcard\n")

def get_author_selections_for_plot(author_set):
    choice_a = None #Placeholder
    choice_b = None #Placeholder

    while choice_a not in author_set.keys():
        choice_a = int(input("Select the number of your first author: "))
    while choice_b not in author_set.keys():
        choice_b = int(input("And the second author? "))
    return choice_a, choice_b

def get_sample_size(number_predictions):
    sample_size = input(f"What size would you like the sample to be? (max is {number_predictions}) ")
    return int(sample_size)

def collect_info_from_db(author_set, sample_size, author_pair):
    df = create_author_view(author_pair)
    df = df.sample(sample_size)
    same_author_labels = {"Y": "Yes", "N": "No", "NY": "No (Should Be Yes)", "YN": "Yes (Should Be No)"}
    text_set = read_all_text_names_by_id_from_db()

    for k, v in author_set.items():
        df.source_auth.replace(k, v, inplace=True)
        df.target_auth.replace(k, v, inplace=True)
    
    for k, v in text_set.items():
        df.source_text.replace(k, v, inplace=True)
        df.target_text.replace(k, v, inplace=True)
    
    for k, v in same_author_labels.items():
        df.same_author.replace(k, v, inplace=True)

    return df

def make_plot(df):
    #Reference: https://plotly.com/python/hover-text-and-formatting/
    fig = px.scatter(df, x="comp_score", y="threshold", log_x=True, color='same_author', hover_name="source_text", hover_data=['source_auth', 'target_auth', 'source_text', 'target_text', 'same_author', 'threshold', 'hap_weight', 'al_weight'])
    fig.show() #display visualization in browser

def main():
    author_set = create_author_set_for_selection()
    display_author_menu(author_set)
    author_a, author_b = get_author_selections_for_plot(author_set)
    author_pair = create_author_pair_for_lookups(author_a, author_b)
    total_number_of_predictions = get_author_view_length(author_pair)
    sample_size = get_sample_size(total_number_of_predictions)
    df = collect_info_from_db(author_set, sample_size, author_pair)
    
    print("\n")
    print(df)
    make_plot(df)

if __name__ == "__main__":
    os.system('clear')
    main()
