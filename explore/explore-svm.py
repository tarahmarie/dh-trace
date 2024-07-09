import csv
import os
import sqlite3

import matplotlib.pyplot as plt
import numpy as np
from rich import print
from rich.console import Console, OverflowMethod
from rich.table import Table

#Helpers
console = Console()
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

def get_length_of_working_table(working_table_choice, novel):
    if novel is not None:
        disk_cur.execute(f"SELECT COUNT(*) FROM {working_table_choice} WHERE novel = ?;", (novel,))
    else:
        disk_cur.execute(f"SELECT MAX(rowid) FROM {working_table_choice};")
    the_rowcount = disk_cur.fetchone()
    return the_rowcount[0]

def get_all_novel_names_and_chapters_from_tests() -> dict:
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

def browse_test_results():
    console.clear()
    novels_and_chaps = get_all_novel_names_and_chapters_from_tests()
    valid_choices = []
    number_of_chapters = 0
    print("Browsing novels used in test set: \n")
    for i, name in enumerate(novels_and_chaps.keys(), start=1):
        print(f"{i}. {name}")
        valid_choices.append(name)
    
    print("\n")
    choice = int(input("Which work would you like to explore? "))
    if choice in range(1, len(valid_choices) + 1):
        selected_work = valid_choices[choice - 1]
        number_of_chapters = max(map(int, novels_and_chaps[selected_work]))
    else:
        browse_test_results()

    console.clear()
    chap_choice = 0
    while chap_choice not in range(1, number_of_chapters + 1):
        chap_choice = int(input(f"There are {number_of_chapters} chapters in {selected_work}. Which one would you like? "))
    
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
        print(f"\nShowing test results for {selected_work}, chapter {chap_choice}:\n")
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
        novels = [header for header in sorted_headers]

        # Call the function to create a bar chart
        create_bar_chart(data, novels, selected_work, chap_choice)

def browse_model_coefficients():
    console.clear()
    print("\nSo, here, we can see the coefficients (words) that the model has used to make decisions.\nWe'll see the coefficient and the relative significance (positive = more significant, negative = less so).\n\nJust so you know: there can be lots of them...")
    temp_count_coefs = disk_cur.execute("SELECT COUNT(*) FROM svm_coefficients;")
    count_coefs = disk_cur.fetchone()
    
    try:
        requested_count = int(input(f"\nSo, of the {count_coefs[0]} coefficients, how many do you want to see? "))
    except Exception as e:
        print(f"There was an error: {e}")
    
    try:
        randomize_or_no = input("\nAnd would you like me to randomize them? (y/n) ")
    except Exception as e:
        print(f"There was an error: {e}")
    
    try:
        sort_any_particular_way = input("\nAnd should I sort them descending (d), ascending (a), or none (n)? ")
    except Exception as e:
        print(f"There was an error: {e}")
    
    sort_type = ""
    match sort_any_particular_way:
        case "a":
            sort_type = "ORDER BY coefficient_value ASC"
        case "d":
            sort_type = "ORDER BY coefficient_value DESC"
        case "n":
            sort_type = ""
    
    # But what if they chose random?  Huh?  Well?
    match randomize_or_no:
        case "y":
            if sort_any_particular_way in ["a", "d"]:
                sort_type = sort_type.replace("ORDER BY coefficient_value", "ORDER BY RANDOM()")
            else:
                sort_type = "ORDER BY RANDOM()"
        case default:
            pass
        
    temp_max_coef_value = disk_cur.execute("SELECT max(coefficient_value) FROM svm_coefficients;")
    max_coef_value = disk_cur.fetchone()
    temp_min_coef_value = disk_cur.execute("SELECT min(coefficient_value) FROM svm_coefficients;")
    min_coef_value = disk_cur.fetchone()
    temp_avg_coef_value = disk_cur.execute("SELECT avg(coefficient_value) FROM svm_coefficients;")
    avg_coef_value = disk_cur.fetchone()
    print("\nMax Coefficient Value in Model: ", max_coef_value[0])
    print("Average Coefficient Value in Model: ", avg_coef_value[0])
    print("Min Coefficient Value in Model: ", min_coef_value[0])

    the_coefs_and_scores = disk_cur.execute(f"SELECT * FROM svm_coefficients {sort_type} LIMIT {requested_count};")
    print("\n")
    for thing in the_coefs_and_scores:
        print(thing[0], thing[1])
    
    should_i_output_all_to_disk = input("\nHey, would you like a csv of all the coefficients for browsing? (y/n) ")
    match should_i_output_all_to_disk:
        case "y":
            save_coefficients_to_csv()
        case default:
            return

def create_bar_chart(data, novels, selected_work, chap_choice):
    # Set the default figure size
    plt.figure(figsize=(12.8, 10.24))

    # Prepare the data for plotting
    x = np.arange(len(novels))
    values = data[0]

    # Define color thresholds
    thresholds = [np.percentile(values, q) for q in [25, 50, 75, 100]]
    colors = ['red', 'orange', 'green', 'blue']  # Customize the colors as desired

    # Create the bar chart with custom colors
    bars = plt.bar(x, values, color=colors[-1])
    plt.xticks(x, novels, rotation='vertical')
    plt.xlabel("Novel")
    plt.ylabel("Likelihood")
    plt.title(f"Likelihood of Unseen Text ({selected_work}, ch. {chap_choice}) by Novel")

    # Assign colors to bars based on thresholds
    for bar, value in zip(bars, values):
        color = colors[next(idx for idx, threshold in enumerate(thresholds) if value <= threshold)]  # Find the color based on thresholds
        bar.set_color(color)
    
    # Add horizontal grid lines
    plt.grid(axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()
    plt.show()


def get_choice_for_exploring():
    print("\n\t1. Explore the unseen test set predictions")
    print("\n\t2. Browse the model's coefficients")
    print("\n\t3. Quit")
    print("\n")
    user_choice = int(input("What would you like to do? "))

    match user_choice:
        case 1:
            browse_test_results()
        case 2:
            browse_model_coefficients()
        case 3:
            console.clear()
            quit()
        case default:
            get_choice_for_exploring()

def choose_save_or_exit(headers, results): # NOTE: Dead code. May be resurrected.
    print("\n")
    save_or_exit = input("Would you like to (s)ave to CSV or (e)xit? ")

    match save_or_exit:
        case 's' | 'S':
            save_current_results_to_csv(headers, results)
        case 'e' | 'E':
            exit()

def save_current_results_to_csv(headers, results): # NOTE: Dead code. May be resurrected.
    savefile_name = ""
    confirm_save = ""
    results_dir = f"../projects/{project_name}/results/"

    # Ensure the results directory exists
    os.makedirs(results_dir, exist_ok=True)

    while savefile_name == "":
        savefile_name = input("What should I call the file? (a .csv extension will be auto-added) ")
    
    if os.path.exists(f"../projects/{project_name}/results/{savefile_name}.csv"):
        while confirm_save.lower() not in ["y", "n"]:
            confirm_save = input("File exists. Overwrite? (y/n) ")
    
    if confirm_save.lower() == "y" or confirm_save == "":
        with open(os.path.join(results_dir, f"{savefile_name}.csv"), "w", newline='') as the_csv_file:
            writer = csv.writer(the_csv_file)

            # Convert headers (sqlite3.Row object) to a list or tuple and write them
            writer.writerow([col for col in headers.keys()])

            # Convert sqlite3.Row objects to tuples and write the body
            writer.writerows([tuple(row) for row in results])

def save_coefficients_to_csv(): # NOTE: Dead code. May be resurrected.
    savefile_name = ""
    confirm_save = ""
    results_dir = f"../projects/{project_name}/results/"

    # Ensure the results directory exists
    os.makedirs(results_dir, exist_ok=True)

    while savefile_name == "":
        savefile_name = input("What should I call the file? (a .csv extension will be auto-added) ")
    
    if os.path.exists(f"../projects/{project_name}/results/{savefile_name}.csv"):
        while confirm_save.lower() not in ["y", "n"]:
            confirm_save = input("File exists. Overwrite? (y/n) ")
    
    if confirm_save.lower() == "y" or confirm_save == "":
        all_coefs = disk_cur.execute("SELECT * FROM svm_coefficients;")
        headers = [column[0] for column in disk_cur.description]
        with open(os.path.join(results_dir, f"{savefile_name}.csv"), "w", newline='') as the_csv_file:
            writer = csv.writer(the_csv_file)

            # Convert headers (sqlite3.Row object) to a list or tuple and write them
            writer.writerow([col for col in headers])

            # Convert sqlite3.Row objects to tuples and write the body
            writer.writerows([tuple(row) for row in all_coefs])

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

get_choice_for_exploring()
disk_con.close()