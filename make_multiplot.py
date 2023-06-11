import os

import pandas as pd
import plotly.express as px

from database_ops import (get_length_of_mulitauthor_predicition_table,
                          read_all_author_names_from_db,
                          read_multiauthor_attribution_from_db)


def create_author_set_for_selection():
    author_set = read_all_author_names_from_db()
    max_key_num = max(author_set.keys())
    author_set[max_key_num + 1] = '%' #Append to allow for an 'Any' author.
    return author_set

def display_author_menu(author_set):
    for count, value in enumerate(author_set, start=1):
        print(f"{count}. {author_set[count]}")
    print("\n'%' is a wildcard\n")

def get_author_selections_for_plot(author_set):
    choice_a = None #Placeholder
    choice_b = None #Placeholder
    choice_c = None #Placeholder

    while choice_a not in author_set.keys():
        choice_a = int(input("Select the number of your first author: "))
    while choice_b not in author_set.keys():
        choice_b = int(input("And the second author? "))
    while choice_c not in author_set.keys():
        choice_c = int(input("And the third author? "))
    return author_set[choice_a], author_set[choice_b], author_set[choice_c]

def collect_info_from_db(sql_data, total_number_of_predictions):
    sample_size = None
    df = pd.DataFrame(sql_data, columns = ['source_auth', 'target_auth', 'score', 'source_text', 'target_text', 'same_author'])

    while True:
        try:
            sample_size = int(input(f"What size would you like the sample to be? (max is {total_number_of_predictions}) "))
            if sample_size > 0:
                break
        except ValueError:
            print("Sorry, I need a number!")

    df = df.sample(sample_size)
    print("\n")
    print(df)
    make_plot(df)

def make_plot(df):
    #Reference: https://plotly.com/python/hover-text-and-formatting/
    fig = px.scatter(df, x="score", y="score", log_x=True, color='same_author', hover_name="source_text", hover_data=['source_auth', 'target_auth', 'source_text', 'target_text', 'same_author'])
    fig.show() #display visualization in browser

def main():
    author_set = create_author_set_for_selection()
    display_author_menu(author_set)
    author_a, author_b, author_c = get_author_selections_for_plot(author_set)

    try: #See if there are matches for this source/target
        sql_data = read_multiauthor_attribution_from_db(author_a, author_b, author_c)
        total_number_of_predictions = get_length_of_mulitauthor_predicition_table(author_a, author_b, author_c)
        collect_info_from_db(sql_data, total_number_of_predictions)
    except ValueError:
        try: #Maybe there are matches if we swap source and target
            sql_data = read_multiauthor_attribution_from_db(author_c, author_b, author_a)
            total_number_of_predictions = get_length_of_mulitauthor_predicition_table(author_c, author_b, author_a)
            collect_info_from_db(sql_data, total_number_of_predictions)
        except ValueError: #Ok, there are definitely no matches. Do over.
            print("\nSorry, there are no matches for this pairing. Please select again.\n")
            main()

if __name__ == "__main__":
    os.system('clear')
    main()