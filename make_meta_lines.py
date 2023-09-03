import os

import plotly.express as px
from rich.console import Console
from rich.status import Status
from tqdm import tqdm

from database_ops import (read_all_author_names_from_db,
                          read_all_text_names_and_create_author_work_dict)
from predict_ops import (create_custom_author_view,
                         get_author_and_texts_published_after_current,
                         get_min_year_of_author_publication,
                         read_all_thresholds)
from util import get_project_name
from utils.get_choices import get_choices_for_viz

console = Console()
project_name = get_project_name()

def create_author_set_for_selection():
    author_set = read_all_author_names_from_db()
    return author_set

def display_author_menu(author_set):
    print("\n")
    for count, value in enumerate(author_set, start=1):
        print(f"{count}. {author_set[count]}")
    print("\n")

# Line graph of chosen authors' four outcomes.
def make_simple_confusion_lines(data, author_set, author_choice, min_length, year, threshold):
    # Using Plotly Express
    fig = px.line(data, x='threshold', y=['Yes', 'No', 'False Negative', 'False Positive'],
                labels={'threshold': 'Threshold', 'value': 'Count', 'variable': 'Same Author'},
                title=f'Same Author Counts at Different Thresholds (Starting author: {author_set[author_choice]}; threshold >= {threshold}; min_length = {min_length}; Years: > {year})')

    # Add data points as dots to the line plot
    fig.update_traces(mode='lines+markers')
    fig.show()

def main():
    print("Gathering data...")
    authors_and_works_dict = read_all_text_names_and_create_author_work_dict()
    author_set = create_author_set_for_selection()
    threshold_set = read_all_thresholds()

    display_author_menu(author_set)
    user_choices = get_choices_for_viz(author_set, threshold_set)
    min_author_choice_year = get_min_year_of_author_publication(user_choices.author_num)
    later_texts_and_authors = get_author_and_texts_published_after_current(min_author_choice_year)
    
    print("\nConstructing line graph...hang on.\n")
    data = create_custom_author_view(user_choices.author_num, min_author_choice_year, user_choices.min_length, user_choices.threshold)
    make_simple_confusion_lines(data, author_set, user_choices.author_num, user_choices.min_length, min_author_choice_year, user_choices.threshold)
    print("\n")

if __name__ == "__main__":
    os.system('clear')
    main()
