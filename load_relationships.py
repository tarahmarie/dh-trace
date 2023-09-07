# This script is basically the row or record generator for the purposes
# of the rest of the application. Goes through and for each relationship 
# (the relationships are what we're interested in, not individual texts)
#  calculates the alignment, hapax, and ngram overlaps. It calls the
# Jaccard calculations for similarity in database_ops for the rows, as well.

import itertools

from tqdm import tqdm

from database_ops import (get_total_number_of_ngrams, insert_averages_to_db,
                          insert_results_to_db, insert_stats_to_db,
                          read_all_alignments_from_db,
                          read_all_author_names_and_ids_from_db,
                          read_all_chapter_length_from_db,
                          read_all_dir_names_by_id_from_db,
                          read_all_hapax_intersects_lengths_from_db,
                          read_all_ngram_intersects_lengths_from_db,
                          read_all_text_names_and_ids_from_db,
                          read_all_text_names_by_id_from_db,
                          read_all_text_pair_names_and_ids_from_db,
                          read_author_from_db, read_author_names_by_id_from_db,
                          read_text_names_with_dirs_from_db)
from util import (get_dir_lengths_for_processing, get_project_name,
                  getCountOfFiles, getListOfFiles)

#Helper Vars
project_name = get_project_name()
list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
total_file_count = getCountOfFiles(f'./projects/{project_name}/splits')
number_of_combinations = sum(1 for e in itertools.combinations(list_of_files, 2))
total_ngrams = get_total_number_of_ngrams()
words_counted_in_comparisons = 0
total_alignments = 0
total_related_ngrams = 0
total_related_hapaxes = 0

stats_transactions = []
hapax_transactions = []
ngram_transactions = []
align_transactions = []

#Load the Dictionaries for processing. These are the basic datapoints for statistical analyis.
chapter_counts_dict = get_dir_lengths_for_processing()
dict_of_files_and_passages = read_all_alignments_from_db()
chapter_lengths = read_all_chapter_length_from_db()
length_of_corpus_text = sum(chapter_lengths.values())
the_ngram_intersects_lengths = read_all_ngram_intersects_lengths_from_db()
the_hapax_intersects_lengths = read_all_hapax_intersects_lengths_from_db()
authors = read_author_names_by_id_from_db()
inverted_authors = read_all_author_names_and_ids_from_db()
text_pairs, inverted_text_pairs = read_all_text_pair_names_and_ids_from_db()
text_and_id_dict = read_all_text_names_by_id_from_db()
inverted_text_and_id_dict = read_all_text_names_and_ids_from_db()
dirs_dict = read_all_dir_names_by_id_from_db()
texts_and_dirs = read_text_names_with_dirs_from_db()

def get_shared_aligns_count(source, source_id, target, target_id, pair_id):
    found_alignments = 0
    #Find the rich stuff, save the Goondocks
    #NOTE - Apr 2023: Executive Decision. We are asking for total # of aligns, but giving the length of the aligns. 
    #       Therefore, going to return 1 when our criterion is met.
    if inverted_text_pairs[(source_id, target_id)]:
        try:
            align_record = dict_of_files_and_passages[pair_id]
            ##NOTE: (Old Note) Five is too restrictive. You get almost nothing.
            if align_record[1] >= 3 or align_record[3] >= 3:
                found_alignments += 1
        except KeyError:
            pass

    if found_alignments > 0:
        return found_alignments
    else:
        return 0

def process_chapters_with_ngrams_sorted(first_name, first_id, second_name, second_id, pair_id):
    global words_counted_in_comparisons
    global total_alignments
    global total_related_ngrams
    global total_related_hapaxes
    first_year = dirs_dict[texts_and_dirs[first_name]].split('-')[0]
    second_year = dirs_dict[texts_and_dirs[second_name]].split('-')[0]
    pair_length = (chapter_lengths[first_name] + chapter_lengths[second_name])

    #Get Author Names
    first_author = authors[read_author_from_db(first_name)]
    second_author = authors[read_author_from_db(second_name)]

    #Add lengths of files to words_counted_in_comparisons for later stats
    words_counted_in_comparisons += chapter_lengths[first_name]
    words_counted_in_comparisons += chapter_lengths[second_name]

####Begin get ngram intersects count
    #For historical reasons, the program used to count number of words in ngrams and add them to count.
    #Then, this was divided by total comparisons.
    #The result, was absurd things like this: "Total Related Ngrams Over Comparisons (13,577,671 / 12,450,312): 1.0905486545236778"
    #As such, I am now just adding one per overlapping ngram.

    #NOTE: April, 2023. Aren't these skewed by allowing a text to compare to itself? That number would be much higher, right?
    counts_for_pair = [0]
    try:
        if the_intersects := the_ngram_intersects_lengths[pair_id]:
            counts_for_pair.append(the_intersects)
    except KeyError:
        counts_for_pair.append(0)

    ngram_overlap_count = max(counts_for_pair, default=0)
    ngram_overlaps_over_pair_length = round((ngram_overlap_count / pair_length), 8)
    ngram_overlaps_over_corpus_length = round((ngram_overlap_count / length_of_corpus_text), 8)
    total_related_ngrams += ngram_overlap_count
####End get ngram intersects count
####Begin alignments count

    num_alignments = get_shared_aligns_count(first_name, first_id, second_name, second_id, pair_id)
    num_alignments_over_pair_length = round((num_alignments / pair_length), 8)
    num_alignments_over_corpus_length = round((num_alignments / length_of_corpus_text), 8)
    total_alignments += num_alignments

####End alignments count
####Begin get hapax intersects count
    counts_for_hapax_pair = [0]
    try:
        if the_intersects := the_hapax_intersects_lengths[pair_id]:
                counts_for_hapax_pair.append(the_intersects)
    except KeyError:
        counts_for_hapax_pair.append(0)
    
    hapaxes_count_for_chap_pair = max(counts_for_hapax_pair, default=0)
    hapax_overlaps_over_pair_length = round((hapaxes_count_for_chap_pair / pair_length), 8)
    hapax_overlaps_over_corpus_length = round((hapaxes_count_for_chap_pair / length_of_corpus_text), 8)
    total_related_hapaxes += hapaxes_count_for_chap_pair
####End get hapax intersects count

    insert_results_to_db(inverted_text_and_id_dict[first_name], inverted_text_and_id_dict[second_name], ngram_overlap_count, hapaxes_count_for_chap_pair, num_alignments)

    stats_transactions.append((inverted_authors[first_author], first_year, inverted_text_and_id_dict[first_name], inverted_authors[second_author], second_year, inverted_text_and_id_dict[second_name], hapaxes_count_for_chap_pair, hapax_overlaps_over_pair_length, hapax_overlaps_over_corpus_length, ngram_overlap_count, ngram_overlaps_over_pair_length, ngram_overlaps_over_corpus_length, num_alignments, num_alignments_over_pair_length, num_alignments_over_corpus_length, pair_length, length_of_corpus_text, pair_id, chapter_lengths[first_name], chapter_lengths[second_name]))

    hapax_transactions.append((inverted_authors[first_author], first_year, inverted_text_and_id_dict[first_name], inverted_authors[second_author], second_year, inverted_text_and_id_dict[second_name], hapaxes_count_for_chap_pair, hapax_overlaps_over_pair_length, hapax_overlaps_over_corpus_length, pair_length, length_of_corpus_text, pair_id, chapter_lengths[first_name], chapter_lengths[second_name]))

    ngram_transactions.append((inverted_authors[first_author], first_year, inverted_text_and_id_dict[first_name], inverted_authors[second_author], second_year, inverted_text_and_id_dict[second_name], ngram_overlap_count, ngram_overlaps_over_pair_length, ngram_overlaps_over_corpus_length, pair_length, length_of_corpus_text, pair_id, chapter_lengths[first_name], chapter_lengths[second_name]))

    align_transactions.append((inverted_authors[first_author], first_year, inverted_text_and_id_dict[first_name], inverted_authors[second_author], second_year, inverted_text_and_id_dict[second_name], num_alignments, num_alignments_over_pair_length, num_alignments_over_corpus_length, pair_length, length_of_corpus_text, pair_id, chapter_lengths[first_name], chapter_lengths[second_name]))

def compute_the_averages(): 
    total_comparisons = total_file_count * (total_file_count - 1)
    total_words = words_counted_in_comparisons

    print("\n")
    if total_alignments == 0:
        print(f"Total Alignments Over Comparisons ({total_alignments:,} / {total_comparisons:,}): ", 0)
    elif total_alignments > 0:
        total = total_alignments / total_comparisons
        print(f"Total Alignments Over Comparisons ({total_alignments:,} / {total_comparisons:,}): {total:,}")
    
    if total_related_hapaxes == 0:
        print(f"Total Related Hapaxes Over Comparisons ({total_related_hapaxes:,} / {total_comparisons:,}): 0")
    elif total_related_hapaxes > 0:
        total = total_related_hapaxes / total_comparisons
        print(f"Total Related Hapaxes Over Comparisons ({total_related_hapaxes:,} / {total_comparisons:,}): {total:,}")
        
        #NOTE: Jon added this.
        total = total_related_hapaxes / total_words
        print(f"Total Related Hapaxes Over Total Words in Comparisons ({total_related_hapaxes:,} / {total_words:,}): {total:,}")
        
        if total_related_ngrams == 0:
            print(f"Total Related Ngrams Over Total Ngrams ({total_related_ngrams:,} / {total_ngrams:,}): 0")
        elif total_related_ngrams > 0:
            total = total_related_ngrams / total_ngrams
            print(f"Total Related Ngrams Over Total Ngrams ({total_related_ngrams:,} / {total_ngrams:,}): {total:,}")
        
        #Update the last_run table for later.
        insert_averages_to_db(total_comparisons, total_alignments, total_related_hapaxes, total_words, total_related_ngrams, (total_alignments / total_comparisons), (total_related_hapaxes / total_comparisons), (total_related_hapaxes / total_words), (total_related_ngrams / total_comparisons))

#Do it. Do it for Glory!
i = 1
pbar = tqdm(desc='Computing relationships', total=number_of_combinations, colour="#33ff33", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
for id, item in text_pairs.items():
    process_chapters_with_ngrams_sorted(text_and_id_dict[item[0]], item[0], text_and_id_dict[item[1]], item[1], id)
    i+=1
    pbar.update(1)
pbar.close()

#Process transactions
insert_stats_to_db(stats_transactions, hapax_transactions, ngram_transactions, align_transactions)

#Now, some numbers...
compute_the_averages()