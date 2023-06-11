import re

from nltk.util import ngrams
from tqdm import tqdm

from database_ops import (insert_ngrams_to_db,
                          read_all_text_names_and_ids_from_db,
                          read_text_from_db)
from hapaxes_1tM import remove_tei_lines_from_text
from util import (fix_alignment_file_names, get_project_name, getCountOfFiles,
                  getListOfFiles)

project_name = get_project_name()
list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
file_count = getCountOfFiles(f'./projects/{project_name}/splits')
text_and_id_dict = read_all_text_names_and_ids_from_db()
transactions = []

def make_ngrams_dict(file):
    name_of_text = fix_alignment_file_names(file.split('/')[5])
    the_text = read_text_from_db(name_of_text)
    the_text = remove_tei_lines_from_text(the_text)
    the_text = re.sub(r'[\W_]+', ' ', the_text)
    the_text = the_text.lower().strip().split(' ')
    
    n_grams = list(ngrams(the_text, 4))
    ngrams_count = len(n_grams)

    transactions.append((text_and_id_dict[name_of_text], repr(set(n_grams)), ngrams_count))

#N-grams Dict
i = 1
while i <= len(list_of_files):
    pbar = tqdm(desc='Computing n-grams', total=file_count, colour="#ffaf87", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for file in list_of_files:
        make_ngrams_dict(file)
        i+=1
        pbar.update(1)
    pbar.close()

insert_ngrams_to_db(transactions)