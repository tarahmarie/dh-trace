import os
from itertools import combinations

import pandas as pd
import plotly.express as px
from rich.console import Console
from rich.status import Status
from tqdm import tqdm

from database_ops import (read_all_author_names_from_db,
                          read_all_text_names_by_id_from_db)
from predict_ops import (create_author_view, get_all_weights,
                         get_author_view_length, read_confusion_scores)
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
    temp_list = []
    choice = None
    
    while True:
        user_input = input("Select an author number (or enter '.' to finish): ")
        
        if user_input == '.':
            break
        
        try:
            choice = int(user_input)
            if choice in author_set.keys():
                temp_list.append(choice)
            else:
                print("Invalid author number. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a valid author number or '.' to finish.")
    
    return list(set(temp_list))


def create_permutations_of_authors_for_processing(author_list):
    combos = []

    for item in author_list:
        combos.append((item, item))
    
    author_combos = combinations(author_list, 2)
    for item in author_combos:
        combos.append(item)

    return combos

def get_sample_size(number_predictions):
    sample_size = input(f"What size would you like the sample to be? (max is {number_predictions}) ")
    return int(sample_size)

def collect_info_from_db(author_set, text_set, weights_dict, author_pair):
    df = create_author_view(author_pair, weights_dict)

    df['source_auth'].replace(author_set, inplace=True)
    df['target_auth'].replace(author_set, inplace=True)
    df['source_text'].replace(text_set, inplace=True)
    df['target_text'].replace(text_set, inplace=True)

    return df

def get_sample_from_concat_df(sample_size, df):
    df = df.sample(sample_size)
    return df

# ¡Bonkers viz that no one should use!
def make_plot(df): 
    #Reference: https://plotly.com/python/hover-text-and-formatting/
    fig = px.line(df, x="threshold", y="comp_score", color='same_author')
    fig.show()

# Looks at all authors for the four outcomes 
def make_confusion_score_plot(): 
    confused_df = read_confusion_scores()
    fig = px.line(confused_df, x='threshold', y=['tp', 'tn', 'fp', 'fn'],
              labels={'threshold': 'Threshold', 'value': 'Value'},
              title='Line Plot of Threshold vs. Metrics (Using All Authors)')
    fig.update_traces(mode='lines+markers')
    fig.show()

# Line graph of chosen authors' four outcomes.
def make_simple_confusion_lines(df, author_set, author_list):
    authors_used = []
    temp_authors_list = []
    these_authors = ""
    for item in author_list:
        temp_authors_list.append(author_set[item])
    
    for item in sorted(temp_authors_list):
        these_authors += item + "; "

    # Group by 'threshold' and 'same_author', then sum the counts
    df_grouped = df.groupby(['threshold', 'same_author'])['source_auth'].count().reset_index()

    # Pivot the grouped data to create the count values for each same_author as columns
    pivot_df = df_grouped.pivot_table(index='threshold', columns='same_author', values='source_auth', fill_value=0).reset_index()
    print(pivot_df)
    # Using Plotly Express
    fig = px.line(pivot_df, x='threshold', y=['No', 'False Negative', 'Yes', 'False Positive'],
              labels={'threshold': 'Threshold', 'value': 'Count', 'variable': 'Same Author'},
              title=f'Line Graph of Same Author Counts at Different Thresholds (Using authors: {these_authors[:-2]})')

    # Add data points as dots to the line plot
    fig.update_traces(mode='lines+markers')
    fig.show()

def main():
    author_set = create_author_set_for_selection()
    text_set = read_all_text_names_by_id_from_db()
    weights_dict = get_all_weights()
    display_author_menu(author_set)
    author_list = get_author_selections_for_plot(author_set)
    author_combos = create_permutations_of_authors_for_processing(author_list)

    ordered_author_combos = []
    for item in author_combos:
        ordered_author_combos.append(create_author_pair_for_lookups(item[0], item[1]))
    
    total_length_of_all_author_combos = 0
    for item in ordered_author_combos:
        total_length_of_all_author_combos += get_author_view_length(item)

    # Unlike the other viz, no sample size as we want to see the overall efficacy
    # and not have it vary.
    #sample_size = get_sample_size(total_length_of_all_author_combos)

    frames = []
    pbar = tqdm(desc=f'Fetching data...', total=len(ordered_author_combos), colour="#7FFFD4", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for item in ordered_author_combos:
        frames.append(collect_info_from_db(author_set, text_set, weights_dict, item))
        pbar.update(1)
    pbar.close()

    print("\nConcatenating data...hang on.\n")
    result = pd.concat(frames)
    #df = get_sample_from_concat_df(sample_size, result)
    make_simple_confusion_lines(result, author_set, author_list)
    make_confusion_score_plot()

    print("\n")
    #print(df)
    #make_plot(df)

if __name__ == "__main__":
    os.system('clear')
    main()
