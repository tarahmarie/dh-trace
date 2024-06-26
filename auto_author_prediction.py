# This script sets up and runs similarity calculations amongst the relationship
# rows. Here's where booleans and hyperparameters like year of publication can
# be set. Right now, the influence calculation is subject to the authors being
# loaded in the correct order; year of publication is not taken into account.
# This probably needs to change so no inadvertent errors are introduced.

import re
import unicodedata
from itertools import permutations

from tqdm import tqdm

from database_ops import (read_all_combined_jaccard_from_db,
                          read_all_text_ids_and_chapter_nums_from_db,
                          read_novel_names_by_id_from_db)
from predict_ops import (assess_auto_author_accuracy, close_db_connection,
                         create_custom_author_view, insert_author_pair_counts,
                         insert_calculations, insert_confusion_scores,
                         insert_weights, load_chapter_assessments, optimize,
                         setup_auto_author_accuracy_table,
                         setup_auto_author_prediction_tables,
                         setup_auto_indices, setup_text_stats_table,
                         vacuum_the_db)
from util import create_author_pair_for_lookups

'''
Spoiler: With ngrams and aligns evenly weighted, no permutation of the tarah db yields a "Y" for same author at threshold 0.9
         At 0.8, they show up (particularly, when weighted to ngrams)
'''
author_pair_count_transactions = {}
outcome_counts = {"y": 0, "n": 0, "fp": 0, "fn": 0}

def get_temp_copy_for_processing():
    print("\nLoading chapter assessments from SVM...\n")
    # Load the chapter assessments into memory
    chapter_assessments_df = load_chapter_assessments()
    text_and_chapter_dict = read_all_text_ids_and_chapter_nums_from_db()

    temp_list = []
    copy_of_combined_jaccard = read_all_combined_jaccard_from_db()
    current_item = 0
    print(chapter_assessments_df)

    # Get unique novel names
    novels_dict = read_novel_names_by_id_from_db()

    for item in copy_of_combined_jaccard:
        svm_result = chapter_assessments_df.loc[
            (chapter_assessments_df['novel'] == text_and_chapter_dict[item[5]][1]) & (chapter_assessments_df['number'] == text_and_chapter_dict[item[5]][0]),novels_dict[item[0]]
        ]

        #Reference
        #0: source_auth, 1: source_year, 2: source_text, 3: target_auth, 4: target_year, 5: target_text
        #6: hap_jac_sim, 7: hap_jac_dis, 8: pair_id, 9: source_length, 10: target_length
        # 11: ng_jac_sim, 12: ng_jac_dis, 13: al_jac_sim, 14: al_jac_dis
        #NOTE: I am selecting only the ones I need for do_math(). Change these if your needs change.
        temp_list.append((item[0], item[3], item[7], item[8], item[14], svm_result.values[0]))
        current_item += 1
    del(copy_of_combined_jaccard) #Flush it to free memory.
    return temp_list

temp_db_copy = get_temp_copy_for_processing()

def calculate_scores(source_auth, target_auth, hap_jac_dis, hapax_weight, al_jac_dis, align_weight, svm_result, svm_weight, threshold):
    hap_score = 0.0
    al_score = 0.0
    svm_score = 0.0
    outcome = "No" #Base case.

    hap_score = round((hap_jac_dis * hapax_weight), 8) 
    al_score = round((al_jac_dis * align_weight), 8)
    svm_score = round((svm_result * svm_weight), 8)
    comp_score = sum([hap_score, al_score, svm_score])
    
# Here's the key generation of the four possible values in comparing one author to another.
# Either the text-relationship is the same author, or it's not. The computer says the 
# text is the same author, or not. Four possible results can occur; Y,N,NY (computer says no 
# but it should have been yes), YN (computer says yes but it should have been no).

    #Computer says no.
    if comp_score < threshold and source_auth == target_auth: #Computer should have said yes.
        outcome = "False Negative"
    if comp_score < threshold and source_auth != target_auth:
        outcome = "No" #Ok, no.
    
    #Computer says yes.
    if comp_score >= threshold and source_auth != target_auth: #Computer should not have said yes.
        outcome = "False Positive"
    if comp_score >= threshold and source_auth == target_auth:
        outcome = "Yes" #Ok, yes.

    return comp_score, outcome

def get_values_to_permutate():
    #NOTE: We used to start with weight_a set to threshold,
    #      but this drags things down and makes 'Yes' answers unobtainable,
    #      even at low thresholds.
    temp_set = []
    completed_set = {}

    upper_weight = 0.70
    step = 0.05
    floor = 0.30
    
    #Set one of them super high, and walk the others up while gradually stepping down
    weight_a = round(upper_weight, 3)
    weight_b = round(((1.0 - weight_a) / 2), 3)
    weight_c = round(((1.0 - weight_a) / 2), 3)
    
    while weight_a >= floor:
        temp_set.append([weight_a, weight_b, weight_c])
        weight_a = round(weight_a - step, 3)
        weight_b = round(((1.0 - weight_a) / 2), 3)
        weight_c = round(((1.0 - weight_a) / 2), 3)

    i = 0
    for item in temp_set:
        for entry in set(permutations(item, 3)):
            completed_set[i] = entry
            i+=1

    return completed_set

def do_math(threshold, pretty_threshold):
    values_to_permutate = get_values_to_permutate()
    y_count = 0
    n_count = 0
    i_count = 0
    m_count = 0
    i = 1
    calculations_transactions = []
    accuracy_transactions = []

    while i <= len(temp_db_copy):
        pbar = tqdm(desc=f'Calculating scores at threshold {pretty_threshold}%', total=len(temp_db_copy), colour="#7FFFD4", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
        for item in temp_db_copy:
            source_auth = item[0]
            target_auth = item[1]
            hap_jac_dis = item[2]
            pair_id = item[3]
            al_jac_dis = item[4]
            svm_result = item[5]

            author_pair = create_author_pair_for_lookups(source_auth, target_auth)
            
            if author_pair not in author_pair_count_transactions.keys():
                author_pair_count_transactions[author_pair] = 1
            elif author_pair in author_pair_count_transactions.keys():
                author_pair_count_transactions[author_pair] += 1

            for key, thing in values_to_permutate.items():
                hap_weight = thing[0]
                al_weight = thing[1]
                svm_weight = thing[2]
                
                comp_score, outcome = calculate_scores(source_auth, target_auth, hap_jac_dis, hap_weight, al_jac_dis, al_weight, svm_result, svm_weight, threshold)
                
                match outcome:
                    case "Yes":
                        y_count += 1
                        outcome_counts["y"] += 1
                    case "No":
                        n_count += 1
                        outcome_counts["n"] += 1
                    case "False Negative":
                        i_count += 1
                        outcome_counts["fn"] += 1
                    case "False Positive":
                        m_count += 1
                        outcome_counts["fp"] += 1

                calculations_transactions.append((pair_id, author_pair, threshold, comp_score, outcome, key))
                accuracy_transactions.append((threshold, y_count, n_count, i_count, m_count))
                    
            i+=1
            pbar.update(1)
        pbar.close()

    outcome_values = list(outcome_counts.values())
    outcome_values.insert(0, threshold)
   
    insert_confusion_scores(outcome_values)
    insert_calculations(calculations_transactions)

    return accuracy_transactions

def calculate_accuracy(data, pretty_threshold):
    temp_transactions = []
    pbar = tqdm(desc=f'Calculating accuracy for threshold {pretty_threshold}%', total=len(data), colour="#FF69B4", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for item in data:
        threshold = item[0] #type: ignore
        y_count = item[1] #type: ignore
        n_count = item[2] #type: ignore
        i_count = item[3] #type: ignore
        m_count = item[4] #type: ignore
        the_length = sum([y_count,n_count,i_count,m_count])
        y_percent = 0
        n_percent = 0
        i_percent = 0
        m_percent = 0

        if y_count > 0:
            y_percent = round(y_count / the_length, 3)
        if n_count > 0:
            n_percent = round(n_count / the_length, 3)
        if i_count > 0:
            i_percent = round(i_count / the_length, 3)
        if m_count > 0:
            m_percent = round(m_count / the_length, 3)

        temp_transactions.append((threshold, y_count, n_count, i_count, m_count, the_length, y_percent, n_percent, i_percent, m_percent)) #type: ignore
        pbar.update(1)
    assess_auto_author_accuracy(temp_transactions)
    pbar.close()

def main():
    threshold = 0.95 #95% confidence is pretty high, TBH
    pretty_threshold = int(threshold * 100)
    step = 0.05 #We'll step down to 0.5 in 5% increments
    pretty_step = int(step * 100)
    floor = 0.60 #Stop computing when the threshold drops below this.
    pretty_floor = int(floor * 100)

    setup_auto_author_prediction_tables()
    setup_auto_author_accuracy_table()
    setup_auto_indices()
    
    print(f"\nStepping through values, moving the threshold {pretty_step}% at a time from {pretty_threshold}% to {pretty_floor}%...\n(N.B. The program will pause before computing accuracy... it's ok, and won't last forever!)\n")

    while threshold >= floor:        
        accuracy_transactions = do_math(threshold, pretty_threshold) # type: ignore
        calculate_accuracy(accuracy_transactions, pretty_threshold)
        
        threshold = round(threshold - step, 3)
        pretty_threshold = pretty_threshold - pretty_step 
    #Now, put those dicts into the db:
    temp_author_pair_counts_transactions = []
    temp_weights_transactions = []
    weights_dict = get_values_to_permutate()
    
    for k, v in author_pair_count_transactions.items():
        temp_author_pair_counts_transactions.append((str(k), v))
    
    for k, v in weights_dict.items():
        temp_weights_transactions.append((k, *v))

    insert_author_pair_counts(temp_author_pair_counts_transactions)
    insert_weights(temp_weights_transactions)

    print("\nI'm going to make a large table, now, for use in later visualization... sit tight.")
    setup_text_stats_table()
    close_db_connection()
    print("\nPhew. Ok, to plot these values, run 'python make_auto_scatterplot.py")

if __name__ == "__main__":
    main()
