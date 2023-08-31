import os
from itertools import combinations

import pandas as pd
import plotly.express as px
from rich.console import Console
from rich.status import Status
from tqdm import tqdm

from database_ops import (read_all_author_names_from_db,
                          read_all_text_names_by_id_from_db)
from predict_ops import (get_author_view_length, read_all_thresholds,
                         read_confusion_scores)
from util import create_author_pair_for_lookups, get_project_name
from viz_ops import do_some_sums, get_gcc_length

console = Console()
project_name = get_project_name()

def get_params_for_query(threshold_set):
    print(f"\nThese are the available thresholds: {threshold_set}\n")

    chosen_threshold = input("What threshold do you want to set for the query? ")
    return chosen_threshold

# Line graph of chosen authors' four outcomes.
def make_simple_confusion_lines(data):
    # Using Plotly Express
    fig = px.line(data, x='threshold', y=['No', 'False Negative', 'Yes', 'False Positive'],
                labels={'threshold': 'Threshold', 'value': 'Count', 'variable': 'Same Author'},
                title=f'Line Graph of Same Author Counts at Different Thresholds (Using authors = all and threshold >= 0.75)')

    # Add data points as dots to the line plot
    fig.update_traces(mode='lines+markers')
    fig.show()

def main():
    print("Gathering data...")
    threshold_set = read_all_thresholds()
    chosen_threshold = get_params_for_query(threshold_set)
    
    print("\nConstructing line graph...hang on.\n")
    data = do_some_sums(chosen_threshold)
    make_simple_confusion_lines(data)

    print("\n")

if __name__ == "__main__":
    os.system('clear')
    main()
