# This script takes all the ngrams in two files, computes the overlapping ngrams,
# then stores the relevant relationships with their stats surrounding n-grams in the project db.

import itertools

from tqdm import tqdm

from database_ops import (insert_ngram_overlaps_to_db, read_all_ngrams_from_db,
                          read_all_text_pair_names_and_ids_from_db)
from util import get_project_name, getListOfFiles

ngrams_dict = {}
project_name = get_project_name()
list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
text_pairs, inverted_pairs = read_all_text_pair_names_and_ids_from_db()
number_of_combinations = sum(1 for e in itertools.combinations(list_of_files, 2))
transactions = []

def make_ngram_overlaps_dict(one_id, two_id, pair_id):
    the_intersect_set = ngrams_dict[one_id] & ngrams_dict[two_id]

    transactions.append((pair_id, repr(the_intersect_set), len(the_intersect_set)))

#Fetch the ngrams and store them in a working dict.
ngrams_dict = read_all_ngrams_from_db()

i = 1
pbar = tqdm(desc='Computing ngram overlaps', total=number_of_combinations, colour="#ff66a3", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
for id, item in text_pairs.items():
    make_ngram_overlaps_dict(item[0], item[1], id)
    i+=1
    pbar.update(1)
pbar.close()

#Now, insert the transactions:
insert_ngram_overlaps_to_db(transactions)