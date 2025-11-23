# This script takes all the ngrams in two files, computes the overlapping ngrams,
# then stores the relevant relationships with their stats surrounding n-grams in the project db.

import itertools
from multiprocessing import Pool, cpu_count

from tqdm import tqdm

from database_ops import (insert_ngram_overlaps_to_db, read_all_ngrams_from_db,
                          read_all_text_pair_names_and_ids_from_db)
from util import get_project_name, getListOfFiles


# Global variable for worker processes
_ngrams_dict = None


def init_worker(ngrams_dict):
    """Initialize each worker process with the shared ngrams dictionary."""
    global _ngrams_dict
    _ngrams_dict = ngrams_dict


def compute_ngram_overlap(args):
    """Worker function to compute the intersection for a single text pair."""
    pair_id, one_id, two_id = args
    the_intersect_set = _ngrams_dict[one_id] & _ngrams_dict[two_id]
    return (pair_id, repr(the_intersect_set), len(the_intersect_set))


def main():
    project_name = get_project_name()
    list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
    text_pairs, inverted_pairs = read_all_text_pair_names_and_ids_from_db()
    number_of_combinations = sum(1 for e in itertools.combinations(list_of_files, 2))

    # Fetch the ngrams and store them in a working dict
    ngrams_dict = read_all_ngrams_from_db()

    # Prepare work items as a list of (pair_id, one_id, two_id) tuples
    work_items = [(pair_id, item[0], item[1]) for pair_id, item in text_pairs.items()]

    # Determine optimal chunksize based on workload
    num_workers = cpu_count()
    chunksize = max(1, len(work_items) // (num_workers * 4))

    # Process in parallel
    transactions = []
    with Pool(processes=num_workers, initializer=init_worker, initargs=(ngrams_dict,)) as pool:
        pbar = tqdm(
            desc='Computing ngram overlaps',
            total=number_of_combinations,
            colour="#ff66a3",
            bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
        )

        # imap_unordered yields results as they complete for better performance
        for result in pool.imap_unordered(compute_ngram_overlap, work_items, chunksize=chunksize):
            transactions.append(result)
            pbar.update(1)

        pbar.close()

    # Now, insert the transactions
    insert_ngram_overlaps_to_db(transactions)


if __name__ == '__main__':
    main()