import csv
import os
import sqlite3

from rich import print
from rich.console import Console, OverflowMethod
from rich.prompt import Prompt
from rich.table import Table

#Helpers
console = Console()
tables_and_columns = {}
tables_row_counts = {}
working_table_choice = ""
current_headers = ""
current_prepared_rows = ""

#Project helpers
def get_projects():
    base_dir = "../projects/"
    dir_list = os.listdir(base_dir)
    processed_dir_list = [
        dir for dir in dir_list 
        if os.path.exists(os.path.join(base_dir, dir, "db", f"{dir}.db"))
    ]
    
    if len(processed_dir_list) == 0:  # Make sure we ended up with some projects.
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

def get_db_choice_from_project(project_name):
    db_choice = ""
    db_list = os.listdir(f"../projects/{project_name}/db/")
    
    i = 1
    choices_dict = {}
    for db in db_list:
        choices_dict[str(i)] = db
        i += 1
    
    while str(db_choice) not in choices_dict.keys():
        console.clear()
        print("\n")
        i = 1
        for db in db_list:
            console.print("[purple3]" + str(i) + "[/purple3]" + ". " + db)
            i += 1
        db_choice = input(f"\nWhich database would you like to explore from project '{project_name}'? ")
        
    db_name = choices_dict[str(db_choice)]
    return db_name

def get_all_table_names_from_db():
    disk_cur.execute("SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%'")
    the_tables = disk_cur.fetchall()
    for table in the_tables:
        tables_and_columns[table[0]] = None

def get_all_column_names_from_table(table_name):
    disk_cur.execute(f"PRAGMA table_info({table_name})")
    the_cols = disk_cur.fetchall()
    list_of_cols = []
    for item in the_cols:
        list_of_cols.append(item[1])
    tables_and_columns[table_name] = list_of_cols

def get_all_table_row_counts(table_name):
    disk_cur.execute(f"SELECT MAX(rowid) FROM {table_name};")
    row_count = disk_cur.fetchone()
    tables_row_counts[table_name] = row_count[0]

def get_table_choice_for_exploring():
    console.clear()
    i = 1
    numbered_tables = {}
    for table in tables_and_columns.keys():
        numbered_tables[str(i)] = table
        i += 1

    print(f"Exploring project [bold green]'{project_name}'[/bold green]. These are the tables you can explore:\n")

    for key, value in numbered_tables.items():
        print(f"{key}.\t  {value}")

    table_choice = input("\nEnter the number of the table you'd like to explore: ")
    try:
        if numbered_tables[table_choice]:
            return numbered_tables[table_choice]
    except KeyError:
        choose_a_table()

def get_length_of_working_table(working_table_choice):
    disk_cur.execute(f"SELECT COUNT(rowid) FROM {working_table_choice};")
    the_rowcount = disk_cur.fetchone()
    return the_rowcount[0]

def get_sample_of_working_table(working_table_choice):
    the_rowcount = get_length_of_working_table(working_table_choice)
    the_limit = input(f"\nHow many rows would you like to see? (total rows: {the_rowcount}) ")
    if the_limit.isnumeric():
        console.clear()
        disk_cur.execute(f"SELECT * FROM {working_table_choice} LIMIT {int(the_limit)};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing [hot_pink2]{the_limit}[/hot_pink2] lines (of [hot_pink2]{tables_row_counts[working_table_choice]}[/hot_pink2]) from table [bold cyan]'{working_table_choice}'[/bold cyan]."
        generate_sample_table(headers, results, message)
    else:
        get_sample_of_working_table(working_table_choice)

def get_random_sample_of_working_table(working_table_choice):
    the_rowcount = get_length_of_working_table(working_table_choice)
    the_limit = input(f"\nHow many rows would you like to see? (total rows: {the_rowcount}) ")
    if the_limit.isnumeric():
        console.clear()
        disk_cur.execute(f"SELECT rowid,* FROM {working_table_choice} ORDER BY RANDOM() LIMIT {int(the_limit)};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing [hot_pink2]{the_limit}[/hot_pink2] random lines (of [hot_pink2]{tables_row_counts[working_table_choice]}[/hot_pink2]) from table [bold cyan]'{working_table_choice}'[/bold cyan]."
        generate_sample_table(headers, results, message)
    else:
        get_random_sample_of_working_table(working_table_choice)

def get_sample_of_working_table_ordered(working_table_choice, direction):
    console.clear()
    the_rowcount = get_length_of_working_table(working_table_choice)
    kind_of_order = {"ASC": "ascending", "DESC": "descending"}
    column_options = {}
    i = 1
    for item in tables_and_columns[working_table_choice]:
        column_options[str(i)] = item
        i += 1
    
    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    column_choice = input("\n\nEnter the number of the column you'd like to order by: ")

    the_limit = input(f"\nHow many rows would you like to see? (total rows: {the_rowcount}) ")
    if the_limit.isnumeric():
        console.clear()
        disk_cur.execute(f"SELECT * FROM {working_table_choice} ORDER BY `{column_options[column_choice]}` {direction} LIMIT {int(the_limit)};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing [hot_pink2]{the_limit}[/hot_pink2] lines (of [hot_pink2]{tables_row_counts[working_table_choice]}[/hot_pink2]) from table [bold cyan]'{working_table_choice}'[/bold cyan] in {kind_of_order[direction]} order."
        generate_sample_table(headers, results, message)
    else:
        get_sample_of_working_table_ordered(working_table_choice, direction)

def get_sample_of_working_table_max(working_table_choice):
    console.clear()
    column_options = {}
    i = 1
    for item in tables_and_columns[working_table_choice]:
        column_options[str(i)] = item
        i += 1
    
    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    column_choice = input("\n\nEnter the number of the column you'd like the maximum value for: ")

    if column_choice in column_options.keys():
        console.clear()
        disk_cur.execute(f"SELECT MAX(`{column_options[column_choice]}`) FROM {working_table_choice};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing maximum value of [hot_pink2]{column_options[column_choice]}[/hot_pink2] from table [bold cyan]'{working_table_choice}'[/bold cyan]."
        generate_sample_table(headers, results, message)
    else:
        get_sample_of_working_table_max(working_table_choice)

def get_sample_of_working_table_sum(working_table_choice):
    console.clear()
    column_options = {}
    i = 1
    for item in tables_and_columns[working_table_choice]:
        column_options[str(i)] = item
        i += 1
    
    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    column_choice = input("\n\nEnter the number of the column you'd like to sum: ")

    if column_choice in column_options.keys():
        console.clear()
        disk_cur.execute(f"SELECT SUM(`{column_options[column_choice]}`) FROM {working_table_choice};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing sum for [hot_pink2]{column_options[column_choice]}[/hot_pink2] from table [bold cyan]'{working_table_choice}'[/bold cyan]."
        generate_sample_table(headers, results, message)
    else:
        get_sample_of_working_table_sum(working_table_choice)

def get_sample_of_working_table_avg(working_table_choice):
    console.clear()
    column_options = {}
    i = 1
    for item in tables_and_columns[working_table_choice]:
        column_options[str(i)] = item
        i += 1
    
    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    column_choice = input("\n\nEnter the number of the column you'd like to sum: ")

    if column_choice in column_options.keys():
        console.clear()
        disk_cur.execute(f"SELECT AVG(`{column_options[column_choice]}`) FROM {working_table_choice};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing average for [hot_pink2]{column_options[column_choice]}[/hot_pink2] from table [bold cyan]'{working_table_choice}'[/bold cyan]."
        generate_sample_table(headers, results, message)
    else:
        get_sample_of_working_table_avg(working_table_choice)

def get_column_stats_sample(working_table_choice):
    console.clear()
    column_options = {}
    i = 1
    for item in tables_and_columns[working_table_choice]:
        column_options[str(i)] = item
        i += 1
    
    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    column_choice = input("\n\nEnter the number of the column you'd like the maximum value for: ")

    if column_choice in column_options.keys():
        console.clear()
        disk_cur.execute(f"SELECT MAX(`{column_options[column_choice]}`), SUM(`{column_options[column_choice]}`), AVG(`{column_options[column_choice]}`) FROM {working_table_choice};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing column stats for [hot_pink2]{column_options[column_choice]}[/hot_pink2] from table [bold cyan]'{working_table_choice}'[/bold cyan]."
        generate_sample_table(headers, results, message)
    else:
        get_column_stats_sample(working_table_choice)

def get_search_term():
    search_kind = ""
    search_term = ""
    types_of_search = {
        "1": "Result includes search term.",
        "2": "Result begins with search term.",
        "3": "Result ends with search term.",
        "4": "Exact match (no wildcards)"
    }

    print("What kind of search would you like to do?\n")
    for key, description in types_of_search.items():
        print(str(key) + ".\t" + description)
    
    while search_kind not in types_of_search.keys():
        search_kind = input("\nEnter choice: ")
    
    while search_term == "":
        if search_kind in ['1','2','3']:
            search_term = input("Enter search term (note: you can use an underscore as a wildcard!): ")
        else:
            search_term = input("Enter your search term (must match precisely): ")
    
    if search_kind == "1":
        return f"%{search_term}%", "LIKE"
    elif search_kind == '2':
        return f"{search_term}%", "LIKE"
    elif search_kind == '3':
        return f"%{search_term}", "LIKE"
    elif search_kind == '4':
        return search_term, "IS"
    else:
        return search_term, "IS"

def get_sample_by_searching_for_string(working_table_choice):
    console.clear()
    the_rowcount = get_length_of_working_table(working_table_choice)
    column_options = {}
    text_to_search = ""
    the_limit = ""

    i = 1
    for item in tables_and_columns[working_table_choice]:
        column_options[str(i)] = item
        i += 1
    
    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    column_choice = input("\n\nEnter the number of the column you'd like to search: ")

    if column_choice in column_options.keys():
        console.clear()
        while text_to_search == "":
            text_to_search, operation = get_search_term()
        
        disk_cur.execute(f"SELECT COUNT(*) FROM {working_table_choice} WHERE `{column_options[column_choice]}` {operation} '{text_to_search}';")
        count_of_matches = disk_cur.fetchone()
        
        if count_of_matches[0] == 0:
            console.clear()
            print(f"\nSorry, no matches for [red\n]'{text_to_search.replace('%', '')}'[/red].")
            press_to_return = None
            while press_to_return is None:
                press_to_return = input("Press any key to return to the menu...")
            choose_a_table()
        else:
            while not the_limit.isnumeric():
                the_limit = input(f"\nHow many rows would you like to see? (total matching rows: {count_of_matches[0]}) ")
    
        disk_cur.execute(f"SELECT * FROM {working_table_choice} WHERE `{column_options[column_choice]}` {operation} '{text_to_search}' LIMIT {the_limit};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing [hot_pink2]{the_limit}[/hot_pink2] results for [green]'{text_to_search.replace('%', '')}'[/green] in [hot_pink2]{column_options[column_choice]}[/hot_pink2] from table [bold cyan]'{working_table_choice}'[/bold cyan]."
        generate_sample_table(headers, results, message)
    else:
        get_sample_of_working_table_sum(working_table_choice)

def get_sample_by_searching_for_multiple_strings(working_table_choice):
    console.clear()
    the_rowcount = get_length_of_working_table(working_table_choice)
    column_options = {}
    first_text_to_search = ""
    second_text_to_search = ""
    the_limit = ""

    i = 1
    for item in tables_and_columns[working_table_choice]:
        column_options[str(i)] = item
        i += 1
    
    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    column_choice = input("\n\nEnter the number of the first column you'd like to search: ")

    if column_choice in column_options.keys():
        console.clear()
        while first_text_to_search == "":
            first_text_to_search, first_operation = get_search_term()

    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    second_column_choice = input("\n\nEnter the number of the second column you'd like to search: ")
    
    if second_column_choice in column_options.keys():
        console.clear()
        while second_text_to_search == "":
            second_text_to_search, second_operation = get_search_term()
        
        disk_cur.execute(f"SELECT COUNT(*) FROM {working_table_choice} WHERE `{column_options[column_choice]}` {first_operation} '{first_text_to_search}' AND {column_options[second_column_choice]} {second_operation} '{second_text_to_search}';")
        count_of_matches = disk_cur.fetchone()
        
        if count_of_matches[0] == 0:
            console.clear()
            print(f"\nSorry, no matches for [red\n]'{first_text_to_search.replace('%', '')}'[/red] and [red\n]'{second_text_to_search.replace('%', '')}'[/red].")
            press_to_return = None
            while press_to_return is None:
                press_to_return = input("Press any key to return to the menu...")
            choose_a_table()
        else:
            while not the_limit.isnumeric():
                the_limit = input(f"\nHow many rows would you like to see? (total matching rows: {count_of_matches[0]}) ")
    
        disk_cur.execute(f"SELECT * FROM {working_table_choice} WHERE `{column_options[column_choice]}` {first_operation} '{first_text_to_search}' AND {column_options[second_column_choice]} {second_operation} '{second_text_to_search}' LIMIT {the_limit};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing [hot_pink2]{the_limit}[/hot_pink2] results for [green]'{first_text_to_search.replace('%', '')}'[/green] in [hot_pink2]{column_options[column_choice]}[/hot_pink2] and [green]'{second_text_to_search.replace('%', '')}'[/green] in [hot_pink2]{column_options[second_column_choice]}[/hot_pink2] from table [bold cyan]'{working_table_choice}'[/bold cyan]."
        generate_sample_table(headers, results, message)
    else:
        get_sample_by_searching_for_multiple_strings(working_table_choice)

def get_sample_by_searching_for_values_between(working_table_choice):
    console.clear()
    the_rowcount = get_length_of_working_table(working_table_choice)
    column_options = {}
    lower_bound = ""
    upper_bound = ""
    the_limit = ""

    i = 1
    for item in tables_and_columns[working_table_choice]:
        column_options[str(i)] = item
        i += 1
    
    print("\n")
    for key, value in column_options.items():
        print(f"{key}. {value}")
    column_choice = input("\n\nEnter the number of the column you'd like to search: ")

    if column_choice in column_options.keys():
        console.clear()
        print("\n")
        while not lower_bound.isnumeric():
            lower_bound = input("\nEnter the lower bound: ")
        while not upper_bound.isnumeric():
            upper_bound = input("Enter the upper bound: ")

        disk_cur.execute(f"SELECT COUNT(*) FROM {working_table_choice} WHERE `{column_options[column_choice]}` BETWEEN {lower_bound} AND {upper_bound} ORDER BY `{column_options[column_choice]}`;")
        count_of_matches = disk_cur.fetchone()
        
        if count_of_matches[0] == 0:
            console.clear()
            print(f"\nSorry, no matches between [red\n]'{lower_bound.replace('%', '')}'[/red] and [red\n]'{upper_bound.replace('%', '')}'[/red] in {column_options[column_choice]}.")
            press_to_return = None
            while press_to_return is None:
                press_to_return = input("Press any key to return to the menu...")
            choose_a_table()
        else:
            while not the_limit.isnumeric():
                the_limit = input(f"\nHow many rows would you like to see? (total matching rows: {count_of_matches[0]}) ")      
    
        disk_cur.execute(f"SELECT * FROM {working_table_choice} WHERE `{column_options[column_choice]}` BETWEEN {lower_bound} AND {upper_bound} ORDER BY `{column_options[column_choice]}` DESC LIMIT {the_limit};")
        results = disk_cur.fetchall()
        headers = list(map(lambda attr : attr[0], disk_cur.description))
        message = f"Exploring project [bold green]'{project_name}'[/bold green]. Showing [hot_pink2]{the_limit}[/hot_pink2] results in descending order for in [hot_pink2]{column_options[column_choice]}[/hot_pink2] from table [bold cyan]'{working_table_choice}'[/bold cyan] for values between [green]{lower_bound}[/green] and [green]{upper_bound}[/green]."
        generate_sample_table(headers, results, message)
    else:
        get_sample_by_searching_for_values_between(working_table_choice)

def generate_sample_table(headers, results, message):
    console.clear()
    global current_headers
    global current_prepared_rows
    prepared_rows = []

    table = Table(title="", style="purple", title_style="bold white", show_lines=True, show_footer=True, header_style="bold magenta", footer_style="bold turquoise2")

    for header in headers:
        number_strings = ["count", "num", "total"]
        if any(x in header for x in number_strings):
            table.add_column(f"{header}", justify="right", overflow="fold", footer=f"{header}")
        else:
            table.add_column(f"{header}", justify="left", overflow="fold", footer=f"{header}")
    
    for item in results:
        i = 0
        prepared_row = ()
        for thing in item:
            if i <= len(item) - 1:
                prepared_row += (str(item[i]),)
                i += 1
        table.add_row(*prepared_row)
        prepared_rows.append(prepared_row)
    
    print("\n")
    print(table)
    print(message)
    current_headers = headers
    current_prepared_rows = prepared_rows
    ask_about_refinements_to_sample(headers, results, message)

def ask_about_refinements_to_sample(headers, results, message):
    refinement_choice = ""
    refinement_choices = {
        '1': 'get_sample_of_working_table(working_table_choice)',
        '2': 'get_sample_of_working_table_ordered(working_table_choice, "DESC")',
        '3': 'get_sample_of_working_table_ordered(working_table_choice, "ASC")',
        '4': 'get_sample_of_working_table_max(working_table_choice)',
        '5': 'get_sample_of_working_table_sum(working_table_choice)',
        '6': 'get_sample_of_working_table_avg(working_table_choice)',
        '7': 'get_random_sample_of_working_table(working_table_choice)',
        '8': 'get_column_stats_sample(working_table_choice)',
        '9': 'get_sample_by_searching_for_string(working_table_choice)',
        '10': 'get_sample_by_searching_for_multiple_strings(working_table_choice)',
        '11': 'get_sample_by_searching_for_values_between(working_table_choice)',
        's': 'save_current_results_to_csv(headers, results, message)',
        't': 'choose_a_table()',
        'p': 'choose_new_project()',
        'q': 'exit_the_program()'
        }
    print("\n")
    print("Here are some things you can do now:\n")
    print("1.\tChange the sample size. \t\t7.\tGet a random sample of the table.")
    print("2.\tOrder by a given column (descending). \t8.\tGet all stats for a given column.")
    print("3.\tOrder by a given column (ascending). \t9.\tSearch a column.")
    print("4.\tGet the max value of a given column. \t10.\tSearch multiple columns.")
    print("5.\tGet the sum for a given column. \t11.\tSearch a column for values between x and y.")
    print("6.\tGet the average for a given column.")
    print("\n[cyan]P[/cyan].\tChoose another project. \t\t[cyan]T[/cyan].\tChoose another table.")
    print("[cyan]S[/cyan].\tSave current results to csv. \t\t[cyan]Q[/cyan].\tQuit.\n")
    
    while refinement_choice.lower() not in refinement_choices.keys():
        refinement_choice = input("What would you like to do? ")
        if refinement_choice.lower() in refinement_choices.keys():
            exec(refinement_choices[refinement_choice.lower()])

def save_current_results_to_csv(headers, results, message):
    savefile_name = ""
    confirm_save = ""
    while savefile_name == "":
        savefile_name = input("What should I call the file? (a .csv extension will be auto-added) ")
    
    if os.path.exists(f"../projects/{project_name}/results/{savefile_name}.csv"):
        while confirm_save.lower() not in ["y", "n"]:
            confirm_save = input("File exists. Overwrite? (y/n) ")
    
    if confirm_save.lower() == "y" or confirm_save == "":    
        with open(f"../projects/{project_name}/results/{savefile_name}.csv", "w") as the_csv_file:
            writer = csv.writer(the_csv_file)

            #Write the header
            writer.writerow(current_headers)
            #Write the body
            writer.writerows(current_prepared_rows)
        
        generate_sample_table(headers, results, message)
    else:
        save_current_results_to_csv(headers, results, message)

def exit_the_program():
    console.clear()
    print("\nOk, bye!\n")
    quit()

def choose_a_table():
    global working_table_choice
    working_table_choice = get_table_choice_for_exploring()
    get_sample_of_working_table(working_table_choice)

def choose_new_project():
    global project_name
    project_name = get_project_choice()
    choose_a_table()


#Kickoff
get_projects()
project_name = get_project_choice()
db_choice = get_db_choice_from_project(project_name)

#Connect to the selected db
disk_con = sqlite3.connect(f"../projects/{project_name}/db/{db_choice}")
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
    get_all_table_row_counts(table_name)

choose_a_table()
