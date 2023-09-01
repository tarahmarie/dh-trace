import os
from itertools import combinations

import pandas as pd
import plotly.express as px
from rich.console import Console
from rich.status import Status
from tqdm import tqdm

from database_ops import (read_all_author_names_from_db,
                          read_all_text_names_and_create_author_work_dict,
                          read_all_text_names_by_id_from_db)
from predict_ops import (create_custom_author_view,
                         get_author_and_texts_published_after_current,
                         get_author_view_length,
                         get_min_year_of_author_publication,
                         read_all_thresholds, read_confusion_scores)
from util import create_author_pair_for_lookups, get_project_name

console = Console()
project_name = get_project_name()

def create_author_set_for_selection():
    author_set = read_all_author_names_from_db()
    max_key_num = max(author_set.keys())
    author_set[max_key_num + 1] = '%' #Append to allow for an 'Any' author.
    return author_set

def display_author_menu(author_set, author_and_works_dict):
    print("\n")
    for count, value in enumerate(author_set, start=1):
        print(f"{count}. {author_set[count]}")
    print("\n'%' is a wildcard\n")

def get_author_selection_for_plot(author_set):
    choice = None
    
    while True:
        user_input = input("Select an author number to be the basis of comparison: ")
        
        try:
            choice = int(user_input)
            if choice in author_set.keys():
                return choice
            else:
                print("Invalid author number. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a valid author number or '.' to finish.")

def get_threshold_for_query(threshold_set):
    print(f"\nThese are the available thresholds: {threshold_set}\n")

    chosen_threshold = input("What threshold do you want to set for the query? ")
    return chosen_threshold

def get_length_min_for_query():
    print("\n")
    chosen_min_length = input("What is the minimum length (in words) for the texts you want to use? ")
    return chosen_min_length

# Line graph of chosen authors' four outcomes.
def make_simple_confusion_lines(data, author_set, author_choice, min_length, year):
    # Using Plotly Express
    fig = px.line(data, x='threshold', y=['No', 'False Negative', 'Yes', 'False Positive'],
                labels={'threshold': 'Threshold', 'value': 'Count', 'variable': 'Same Author'},
                title=f'Same Author Counts at Different Thresholds (Starting author: {author_set[author_choice]}; threshold >= 0.75; min_length = {min_length}; Years: > {year})')

    # Add data points as dots to the line plot
    fig.update_traces(mode='lines+markers')
    fig.show()

def main():
    print("Gathering data...")
    authors_and_works_dict = read_all_text_names_and_create_author_work_dict()
    author_set = create_author_set_for_selection()

    display_author_menu(author_set, authors_and_works_dict)
    author_choice = get_author_selection_for_plot(author_set)
    min_author_choice_year = get_min_year_of_author_publication(author_choice)
    later_texts_and_authors = get_author_and_texts_published_after_current(min_author_choice_year)

    threshold_set = read_all_thresholds()
    chosen_threshold = get_threshold_for_query(threshold_set)

    min_length_in_words = get_length_min_for_query()
    
    print("\nConstructing line graph...hang on.\n")
    data = create_custom_author_view(min_author_choice_year, min_length_in_words, chosen_threshold)
    make_simple_confusion_lines(data, author_set, author_choice, min_length_in_words, min_author_choice_year)

    print("\n")

if __name__ == "__main__":
    os.system('clear')
    main()
