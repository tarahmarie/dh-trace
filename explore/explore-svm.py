import os
import sqlite3

import matplotlib.pyplot as plt
import numpy as np
from rich import print
from rich.console import Console, OverflowMethod
from rich.table import Table

#Helpers
console = Console()
training_authors = {}
tables_and_columns = {}
tables_row_counts = {}
working_table_choice = ""
current_headers = ""
current_prepared_rows = ""

#Project helpers
def get_projects():
    dir_list = os.listdir("../projects/")
    processed_dir_list = []
    for dir in dir_list:
        if dir == '.DS_Store':
            pass #Protect against Mac nonsense.
        elif not os.path.exists(f"../projects/{dir}/db/{dir}.db"):
            pass #Don't let me choose a project that hasn't been built yet.
        else:
            processed_dir_list.append(dir)

    if len(processed_dir_list) == 0: #Make sure we ended up with some projects.
        print("\nSorry, you'll need to make some projects first.\n")
        quit()
    else:
        return processed_dir_list

available_projects = get_projects()
project_name = ""

def get_project_choice():
    project_choice = ""
    
    i = 1
    choices_dict = {}
    for project in available_projects:
        choices_dict[str(i)] = project
        i += 1
    
    while str(project_choice) not in choices_dict.keys():
        console.clear()
        print("\n")
        i = 1
        for project in available_projects:
            console.print("[purple3]" + str(i) + "[/purple3]" + ". " + project)
            i += 1
        project_choice = input("\nWhich project would you like to explore? ")
        
    project_name = choices_dict[str(project_choice)]
    return project_name

def get_all_table_names_from_db():
    disk_cur.execute("SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%'")
    the_tables = disk_cur.fetchall()
    for table in the_tables:
        tables_and_columns[table[0]] = None

def get_length_of_working_table(working_table_choice, author):
    if author is not None:
        disk_cur.execute(f"SELECT COUNT(*) FROM {working_table_choice} WHERE author1 = ? OR author2 = ?;", (author, author))
    else:
        disk_cur.execute(f"SELECT MAX(rowid) FROM {working_table_choice};")
    the_rowcount = disk_cur.fetchone()
    return the_rowcount[0]

def get_sample_of_working_table(working_table_choice, author):
    the_rowcount = get_length_of_working_table(working_table_choice, author)
    the_limit = input(f"\nHow many rows would you like to see? (total rows: {the_rowcount}) ")
    if the_limit.isnumeric():
        console.clear()
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing [hot_pink2]{the_limit}[/hot_pink2] lines (of [hot_pink2]{the_rowcount}[/hot_pink2]) from table [bold cyan]'{working_table_choice}'[/bold cyan]. Table sorted by 'conf_is_auth1' descending."
        return author, the_limit, message
    else:
        get_sample_of_working_table(working_table_choice, author)

def get_all_training_author_names():
    disk_cur.execute("SELECT DISTINCT(author1) FROM predictions")
    the_authors = disk_cur.fetchall()
    for author in the_authors:
        training_authors[author[0]] = None

def get_training_author_info(author):
    console.clear()
    author, limit, message = get_sample_of_working_table("predictions", author)
    disk_cur.execute("SELECT * FROM predictions WHERE author1 = ? ORDER BY conf_is_auth1 DESC LIMIT ?;", [author, limit])
    the_results = disk_cur.fetchall()
    table = Table(title="", style="purple", title_style="bold white", show_lines=True, show_footer=True, header_style="bold magenta", footer_style="bold turquoise2")  # Create a new table

    # Set the header row using tables_and_columns["predictions"]
    for head in tables_and_columns["predictions"]:
        table.add_column(f"{head}", justify="left", overflow="fold", footer=f"{head}")
    
    for result in the_results:
        row_values = [str(col) if isinstance(col, float) else col for col in result]
        table.add_row(*row_values)
    
    console.print(table)
    console.print(message)

def get_training_author_info_not_same_author(author):
    console.clear()
    author, limit, message = get_sample_of_working_table("predictions", author)
    disk_cur.execute("SELECT * FROM predictions WHERE author1 = ? AND author2 != ? ORDER BY conf_is_auth1 DESC LIMIT ?;", [author, author, limit])
    the_results = disk_cur.fetchall()
    table = Table(title="", style="purple", title_style="bold white", show_lines=True, show_footer=True, header_style="bold magenta", footer_style="bold turquoise2")  # Create a new table

    # Set the header row using tables_and_columns["predictions"]
    for head in tables_and_columns["predictions"]:
        table.add_column(f"{head}", justify="left", overflow="fold", footer=f"{head}")
    
    for result in the_results:
        row_values = [str(col) if isinstance(col, float) else col for col in result]
        table.add_row(*row_values)
    
    console.print(table)
    console.print(message)

def get_all_column_names_from_table(table_name):
    disk_cur.execute(f"PRAGMA table_info({table_name})")
    the_cols = disk_cur.fetchall()
    list_of_cols = []
    for item in the_cols:
        if item[1] not in ['file', 'novel', 'number']:
            list_of_cols.append(item[1])
    tables_and_columns[table_name] = list_of_cols

def browse_training_by_author():
    console.clear()
    valid_choices = []
    get_all_training_author_names()
    table_keys = list(training_authors.keys())
    for i, author in enumerate(training_authors.keys(), start=1):
        valid_choices.append(i)
        print(f"{i}. {author}")

    author_choice = int(input("Which author would you like to explore? "))
    if author_choice in valid_choices:
        key = table_keys[author_choice - 1]
    
        filter_choice = input("Would you like to filter matches on the same author (y/n)? ")
        match filter_choice:
            case 'y' | 'Y':
                get_training_author_info_not_same_author(key)
            case 'n' | 'N':
                get_training_author_info(key)
    else:
        browse_training_by_author()

def browse_predictions_by_pair():
    #Coming later...
    pass

def browse_test_results():
    console.clear()
    authors_and_chaps = get_all_author_names_and_chapters_from_tests()
    valid_choices = []
    number_of_chapters = 0
    print("Browsing novels used in test set: \n")
    for i, name in enumerate(authors_and_chaps.keys(), start=1):
        print(f"{i}. {name}")
        valid_choices.append(name)
    
    print("\n")
    choice = int(input("Which work would you like to explore? "))
    if choice in range(1, len(valid_choices) + 1):
        selected_work = valid_choices[choice - 1]
        number_of_chapters = max(map(int, authors_and_chaps[selected_work]))
    else:
        browse_test_results()

    console.clear()
    chap_choice = 0
    while chap_choice not in range(1, number_of_chapters + 1):
        chap_choice = int(input(f"There are {number_of_chapters} in {selected_work}. Which one would you like? "))
    
    file_for_query = f"-{selected_work}%chapter_{chap_choice}"
    # Get the column names from the table
    disk_cur.execute("PRAGMA table_info(test_set_preds);")
    columns = disk_cur.fetchall()
    column_names = [column[1] for column in columns if column[1] != "file"]

    # Generate the CREATE TABLE statement for the temporary table
    create_table_statement = f"CREATE TEMPORARY TABLE temp_test_set_view AS SELECT {', '.join(column_names)} FROM test_set_preds WHERE file LIKE '%{file_for_query}';"

    # Create the temporary table
    disk_cur.execute(create_table_statement)

    # Query the temporary table and fetch the results
    disk_cur.execute("SELECT * FROM temp_test_set_view")
    results = disk_cur.fetchall()

    if results:
        console.clear()
        print(f"\nShowing authorship test results for {selected_work}, chapter {chap_choice}:\n")
        headers = [column[0] for column in disk_cur.description]
        max_header_length = max(len(header) for header in headers)
        max_value_length = max(max(len(str(value)) for value in row) for row in results)
        column_width = max(max_header_length, max_value_length) + 2
        max_value = float('-inf')
        min_value = float('inf')
        max_header = ""
        min_header = ""

        # Find the maximum and minimum values
        for row in results:
            row_values = list(row)  # Convert row object to a list
            for header, value in zip(headers, row_values):
                if isinstance(value, (int, float)):
                    if value > max_value:
                        max_value = value
                        max_header = header
                    if value < min_value:
                        min_value = value
                        min_header = header

        # Sort the headers based on the values in descending order
        sorted_headers = sorted(headers, key=lambda h: float('inf') if h == max_header else row_values[headers.index(h)], reverse=True)

        for i in range(0, len(sorted_headers), 4):
            headers_chunk = sorted_headers[i:i + 4]
            print("  ".join(header.ljust(column_width) for header in headers_chunk))
            for row in results:
                row_values = list(row)  # Convert row object to a list
                row_values_chunk = [row_values[headers.index(header)] for header in headers_chunk]
                print("  ".join(str(value).ljust(column_width) for value in row_values_chunk), end="  ")

            print()

        print()
        print(f"Maximum: {max_header}, {max_value}")
        print(f"Minimum: {min_header}, {min_value}")

        # Prepare the data for bar chart
        data = [[float(row[headers.index(header)]) for header in sorted_headers] for row in results]
        authors = [header for header in sorted_headers]

        # Call the function to create a bar chart
        create_bar_chart(data, authors, selected_work, chap_choice)

def create_bar_chart(data, authors, selected_work, chap_choice):
    # Set the default figure size
    plt.figure(figsize=(12.8, 10.24))

    # Prepare the data for plotting
    x = np.arange(len(authors))
    values = data[0]

    # Define color thresholds
    thresholds = [np.percentile(values, q) for q in [25, 50, 75, 100]]
    colors = ['red', 'orange', 'green', 'blue']  # Customize the colors as desired

    # Create the bar chart with custom colors
    bars = plt.bar(x, values, color=colors[-1])
    plt.xticks(x, authors, rotation='vertical')
    plt.xlabel("Author")
    plt.ylabel("Likelihood")
    plt.title(f"Likelihood of Unseen Text ({selected_work}, ch. {chap_choice}) by Authors")

    # Assign colors to bars based on thresholds
    for bar, value in zip(bars, values):
        color = colors[next(idx for idx, threshold in enumerate(thresholds) if value <= threshold)]  # Find the color based on thresholds
        bar.set_color(color)
    
    # Add horizontal grid lines
    plt.grid(axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()
    plt.show()

def get_all_author_names_and_chapters_from_tests():
    test_works = {}
    disk_cur.execute("SELECT DISTINCT(file) FROM test_set_preds")
    the_files = disk_cur.fetchall()
    for file in the_files:
        the_work = file[0].split('-')[1]
        the_work = the_work.split('-')[0]
        the_chap_num_index = file[0].rfind('_')
        the_chap_num = file[0][the_chap_num_index+1:]
            
        if the_work not in test_works.keys():
            test_works[the_work] = [the_chap_num]
        else:
            test_works[the_work].append(the_chap_num)

    works_temp = {}
    for key, value in sorted(test_works.items()):
       works_temp[key] = sorted(value, key=lambda x: int(x))
    
    return works_temp

def get_choice_for_exploring():
    print("\n\t1. Browse the training set by author")
    print("\n\t2. Explore the unseen test set predictions")
    print("\n")
    user_choice = int(input("What would you like to do? "))

    match user_choice:
        case 1:
            browse_training_by_author()
        case 2:
            browse_test_results()
        case default:
            get_choice_for_exploring()


#Kickoff
get_projects()
project_name = get_project_choice()

#Connect to the svm db
disk_con = sqlite3.connect(f"../projects/{project_name}/db/svm.db")
disk_con.row_factory = sqlite3.Row
disk_cur = disk_con.cursor()
#YOLO
disk_cur.execute("PRAGMA synchronous = OFF;")
disk_cur.execute("PRAGMA cache_size = 30000000000;")
disk_cur.execute("PRAGMA journal_mode = WAL;")
disk_cur.execute("PRAGMA temp_store = MEMORY;")

get_all_table_names_from_db()
for table_name in tables_and_columns.keys():
    get_all_column_names_from_table(table_name)

get_choice_for_exploring()
disk_con.close()