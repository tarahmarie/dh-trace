# This script is finding, matching, cleaning, and counting all the authors
# and texts inside splits to get ready to do some math on them all. Adds
# them to a fresh db in the paired format for later calculation.

import itertools
from dataclasses import dataclass, field

from tqdm import tqdm

from database_ops import (insert_authors_to_db, insert_dirs_to_db,
                          insert_text_pairs_to_db, insert_texts_to_db,
                          read_all_text_names_and_ids_from_db)
from hapaxes_1tM import remove_tei_lines_from_text
from util import (fix_alignment_file_names, get_author_from_tei_header,
                  get_date_from_tei_header, get_project_name,
                  get_word_count_for_text, getCountOfFiles, getListOfFiles)

project_name = get_project_name()
list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
file_count = getCountOfFiles(f'./projects/{project_name}/splits')
number_of_combinations = sum(1 for e in itertools.combinations(list_of_files, 2))

@dataclass
class Text:
    id: int = field(default=0)
    content: str = field(default="")
    date: int = field(default=0000)
    length: int = field(default=0000)

print("\n")
#All the Texts
dirs = {}
unique_dir_id = 0
seen_dirs = []

authors = {}
seen_authors = []
unique_author_id = 0

texts = {}
seen_texts = []
unique_text_id = 0

dates = {}
seen_dates = []
unique_date_id = 0

i = 1
while i <= file_count:
    temp_text = Text()
    pbar = tqdm(desc='Loading All Texts', total=file_count, colour="yellow", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for file in list_of_files:
        the_dir = file.split('/')[4]
        name_of_text = file.split('/')[5]
        #Python doesn't seem to want to have a read() and a readline() call to the same file handle.
        #So, we'll open it twice.
        with open(file, 'r') as temp_file:
            content = temp_file.read()
            author = get_author_from_tei_header(content)
            temp_text.date = get_date_from_tei_header(content)

            if author not in seen_authors:
                unique_author_id += 1
                authors[author] = unique_author_id
                seen_authors.append(author)
            if the_dir not in seen_dirs:
                unique_dir_id += 1
                dirs[the_dir] = unique_dir_id
                seen_dirs.append(the_dir)

        with open(file, 'r') as f:
            text = f.read()
            text = remove_tei_lines_from_text(text)
            temp_text.content = text
            temp_text.length = get_word_count_for_text(text)            
            #Because the alignments file has funny ideas about filenames where Lovelace is concerned
            #I have to replace the final '-' with an '_' to match the filesystem
            #If I don't, I can't use the all_texts data with the alignments data.
            
            stripped_name_of_text = fix_alignment_file_names(name_of_text.split('.')[0].strip())

            if text not in seen_texts:
                unique_text_id += 1
                texts[text] = unique_text_id
                temp_text.id = unique_text_id
                seen_texts.append(text)

            #insert_texts_to_db(authors[author], temp_text.id, stripped_name_of_text, temp_text.content, temp_text.length, dirs[the_dir], temp_text.date) 
        i+=1
        pbar.update(1)
    pbar.close()

#Now, populate the crucial authors table
for name, id in authors.items():
    insert_authors_to_db(id, name)

#Now, populate the dirs table
for path, id in dirs.items():
    insert_dirs_to_db(id, path)

#Finally, generate the text pairs we're going to need later
text_and_id_dict = read_all_text_names_and_ids_from_db()
transactions = []
i = 1
pbar = tqdm(desc='Computing file pairs', total=number_of_combinations, colour="#e0ffff", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
for a, b in itertools.combinations(sorted(list_of_files), 2):
    a = fix_alignment_file_names(a.split('/')[5])
    b = fix_alignment_file_names(b.split('/')[5])
    transactions.append((i, text_and_id_dict[a], text_and_id_dict[b]))
    i+=1
    pbar.update(1)
insert_text_pairs_to_db(transactions)
pbar.close()
