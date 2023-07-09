# For each text file, get the list of hapaxes and store in a db. 
# This does the individual calculation on each file.

from tqdm import tqdm

from database_ops import (insert_hapaxes_to_db,
                          read_all_text_names_and_ids_from_db,
                          read_text_from_db)
from hapaxes_1tM import compute_hapaxes, remove_tei_lines_from_text
from util import (fix_alignment_file_names, get_project_name, getCountOfFiles,
                  getListOfFiles)

project_name = get_project_name()
list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
file_count = getCountOfFiles(f'./projects/{project_name}/splits')
text_and_id_dict = read_all_text_names_and_ids_from_db()
transactions = []

def get_hapaxes(file):
    name_of_text = fix_alignment_file_names(file.split('/')[5])
    hapax_list = []

    temp_text = read_text_from_db(name_of_text)
    the_mf_data = remove_tei_lines_from_text(temp_text)
    
    hapaxes_from_file = compute_hapaxes(the_mf_data)
    hapax_list += (hapaxes_from_file)
    hapax_count = len(hapaxes_from_file)

    transactions.append((text_and_id_dict[name_of_text], repr(set(hapax_list)), hapax_count))


#Hapaxes
i = 1
while i <= len(list_of_files):
    pbar = tqdm(desc='Computing hapaxes', total=len(list_of_files), colour="#00875f", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for file in list_of_files:
        get_hapaxes(file)
        i+=1
        pbar.update(1)
    pbar.close()

insert_hapaxes_to_db(transactions)
