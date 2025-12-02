# This script sets up and runs similarity calculations amongst the relationship
# rows. Here's where booleans and hyperparameters like year of publication can
# be set. Right now, the influence calculation is subject to the authors being
# loaded in the correct order; year of publication is not taken into account.
# This probably needs to change so no inadvertent errors are introduced.

import re
import unicodedata
from itertools import permutations
from multiprocessing import Pool, cpu_count

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


# Global for worker processes
_worker_data = None


def init_worker(worker_data):
    """Initialize each worker with shared data."""
    global _worker_data
    _worker_data = worker_data


def get_values_to_permutate():
    """Generate weight permutations for scoring."""
    temp_set = []
    completed_set = {}

    upper_weight = 0.70
    step = 0.05
    floor = 0.30
    
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
            i += 1

    return completed_set


def calculate_scores(source_auth, target_auth, hap_jac_dis, hapax_weight, al_jac_dis, align_weight, svm_result, svm_weight, threshold):
    """Calculate composite score and determine outcome."""
    hap_score = round((hap_jac_dis * hapax_weight), 8) 
    al_score = round((al_jac_dis * align_weight), 8)
    svm_score = round((svm_result * svm_weight), 8)
    comp_score = sum([hap_score, al_score, svm_score])
    
    outcome = "No"  # Base case

    # Computer says no
    if comp_score < threshold and source_auth == target_auth:
        outcome = "False Negative"
    if comp_score < threshold and source_auth != target_auth:
        outcome = "No"
    
    # Computer says yes
    if comp_score >= threshold and source_auth != target_auth:
        outcome = "False Positive"
    if comp_score >= threshold and source_auth == target_auth:
        outcome = "Yes"

    return comp_score, outcome


def process_single_item(args):
    """Worker function to process a single item across all weight permutations."""
    item, threshold = args
    values_to_permutate = _worker_data['values_to_permutate']
    
    source_auth = item[0]
    target_auth = item[1]
    hap_jac_dis = item[2]
    pair_id = item[3]
    al_jac_dis = item[4]
    svm_result = item[5]

    author_pair = create_author_pair_for_lookups(source_auth, target_auth)
    
    # Local counters for this item
    y_count = 0
    n_count = 0
    i_count = 0
    m_count = 0
    
    calculations = []
    
    for key, thing in values_to_permutate.items():
        hap_weight = thing[0]
        al_weight = thing[1]
        svm_weight = thing[2]
        
        comp_score, outcome = calculate_scores(
            source_auth, target_auth, hap_jac_dis, hap_weight,
            al_jac_dis, al_weight, svm_result, svm_weight, threshold
        )
        
        if outcome == "Yes":
            y_count += 1
        elif outcome == "No":
            n_count += 1
        elif outcome == "False Negative":
            i_count += 1
        elif outcome == "False Positive":
            m_count += 1

        calculations.append((pair_id, author_pair, threshold, comp_score, outcome, key))
    
    return {
        'author_pair': author_pair,
        'y_count': y_count,
        'n_count': n_count,
        'i_count': i_count,
        'm_count': m_count,
        'calculations': calculations,
    }


def get_temp_copy_for_processing():
    """Load and prepare data for processing."""
    print("\nLoading chapter assessments from SVM...\n")
    chapter_assessments_df = load_chapter_assessments()
    text_and_chapter_dict = read_all_text_ids_and_chapter_nums_from_db()

    temp_list = []
    copy_of_combined_jaccard = read_all_combined_jaccard_from_db()
    print(chapter_assessments_df)

    novels_dict = read_novel_names_by_id_from_db()

    pbar = tqdm(
        desc='Preparing data',
        total=len(copy_of_combined_jaccard),
        colour="#00afff",
        bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
    )

    for item in copy_of_combined_jaccard:
        svm_result = chapter_assessments_df.loc[
            (chapter_assessments_df['novel'] == text_and_chapter_dict[item[5]][1]) & 
            (chapter_assessments_df['number'] == text_and_chapter_dict[item[5]][0]),
            novels_dict[item[0]]
        ]

        temp_list.append((item[0], item[3], item[7], item[10], item[9], svm_result.values[0]))
        pbar.update(1)
    
    pbar.close()
    del copy_of_combined_jaccard
    return temp_list


def do_math(temp_db_copy, threshold, pretty_threshold, author_pair_count_transactions, outcome_counts):
    """Process all items in parallel for a given threshold."""
    values_to_permutate = get_values_to_permutate()
    
    calculations_transactions = []
    accuracy_transactions = []
    
    # Running totals for accuracy
    running_y = 0
    running_n = 0
    running_i = 0
    running_m = 0

    # Prepare worker data
    worker_data = {
        'values_to_permutate': values_to_permutate,
    }

    # Prepare work items
    work_items = [(item, threshold) for item in temp_db_copy]
    
    num_workers = cpu_count()
    chunksize = max(1, len(work_items) // (num_workers * 4))

    with Pool(processes=num_workers, initializer=init_worker, initargs=(worker_data,)) as pool:
        pbar = tqdm(
            desc=f'Calculating scores at threshold {pretty_threshold}%',
            total=len(temp_db_copy),
            colour="#7FFFD4",
            bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
        )

        for result in pool.imap_unordered(process_single_item, work_items, chunksize=chunksize):
            # Aggregate author pair counts
            author_pair = result['author_pair']
            if author_pair not in author_pair_count_transactions:
                author_pair_count_transactions[author_pair] = 1
            else:
                author_pair_count_transactions[author_pair] += 1

            # Aggregate outcome counts
            running_y += result['y_count']
            running_n += result['n_count']
            running_i += result['i_count']
            running_m += result['m_count']
            
            outcome_counts["y"] += result['y_count']
            outcome_counts["n"] += result['n_count']
            outcome_counts["fn"] += result['i_count']
            outcome_counts["fp"] += result['m_count']

            # Collect calculations
            calculations_transactions.extend(result['calculations'])
            
            # Record accuracy snapshot
            accuracy_transactions.append((threshold, running_y, running_n, running_i, running_m))

            pbar.update(1)

        pbar.close()

    outcome_values = [threshold, outcome_counts["y"], outcome_counts["n"], outcome_counts["fn"], outcome_counts["fp"]]
    insert_confusion_scores(outcome_values)
    insert_calculations(calculations_transactions)

    return accuracy_transactions


def calculate_accuracy(data, pretty_threshold):
    """Calculate accuracy percentages for each data point."""
    temp_transactions = []
    
    pbar = tqdm(
        desc=f'Calculating accuracy for threshold {pretty_threshold}%',
        total=len(data),
        colour="#FF69B4",
        bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
    )
    
    for item in data:
        threshold = item[0]
        y_count = item[1]
        n_count = item[2]
        i_count = item[3]
        m_count = item[4]
        the_length = sum([y_count, n_count, i_count, m_count])
        
        y_percent = round(y_count / the_length, 3) if y_count > 0 else 0
        n_percent = round(n_count / the_length, 3) if n_count > 0 else 0
        i_percent = round(i_count / the_length, 3) if i_count > 0 else 0
        m_percent = round(m_count / the_length, 3) if m_count > 0 else 0

        temp_transactions.append((
            threshold, y_count, n_count, i_count, m_count,
            the_length, y_percent, n_percent, i_percent, m_percent
        ))
        pbar.update(1)
    
    pbar.close()
    assess_auto_author_accuracy(temp_transactions)


def main():
    threshold = 0.95
    pretty_threshold = int(threshold * 100)
    step = 0.05
    pretty_step = int(step * 100)
    floor = 0.60
    pretty_floor = int(floor * 100)

    # Load data once
    temp_db_copy = get_temp_copy_for_processing()
    
    # Shared state across thresholds
    author_pair_count_transactions = {}
    outcome_counts = {"y": 0, "n": 0, "fp": 0, "fn": 0}

    setup_auto_author_prediction_tables()
    setup_auto_author_accuracy_table()
    setup_auto_indices()
    
    print(f"\nStepping through values, moving the threshold {pretty_step}% at a time from {pretty_threshold}% to {pretty_floor}%...")
    print("(N.B. The program will pause before computing accuracy... it's ok, and won't last forever!)\n")

    while threshold >= floor:
        # Reset outcome counts for each threshold
        outcome_counts = {"y": 0, "n": 0, "fp": 0, "fn": 0}
        
        accuracy_transactions = do_math(
            temp_db_copy, threshold, pretty_threshold,
            author_pair_count_transactions, outcome_counts
        )
        calculate_accuracy(accuracy_transactions, pretty_threshold)
        
        threshold = round(threshold - step, 3)
        pretty_threshold = pretty_threshold - pretty_step

    # Insert author pair counts and weights
    temp_author_pair_counts_transactions = [
        (str(k), v) for k, v in author_pair_count_transactions.items()
    ]
    
    weights_dict = get_values_to_permutate()
    temp_weights_transactions = [
        (k, *v) for k, v in weights_dict.items()
    ]

    insert_author_pair_counts(temp_author_pair_counts_transactions)
    insert_weights(temp_weights_transactions)

    print("\nI'm going to make a large table, now, for use in later visualization... sit tight.")
    setup_text_stats_table()
    close_db_connection()
    print("\nPhew. Ok, to plot these values, run 'python make_auto_scatterplot.py")


if __name__ == "__main__":
    main()