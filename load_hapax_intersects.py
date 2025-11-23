# This script compares the hapaxes between two files and stores the overlap 
# in the project db for statistical analysis later.

import itertools
from multiprocessing import Pool, cpu_count

from tqdm import tqdm

from database_ops import (insert_hapax_overlaps_to_db,
                          read_all_hapaxes_from_db,
                          read_all_text_pair_names_and_ids_from_db)
from util import get_project_name, getCountOfFiles, getListOfFiles


# Global variable for worker processes
_hapaxes_dict = None


def init_worker(hapaxes_dict):
    """Initialize each worker process with the shared hapaxes dictionary."""
    global _hapaxes_dict
    _hapaxes_dict = hapaxes_dict


def compute_hapax_overlap(args):
    """Worker function to compute the intersection for a single text pair."""
    pair_id, one_id, two_id = args
    the_intersect_set = _hapaxes_dict[one_id] & _hapaxes_dict[two_id]
    return (pair_id, repr(the_intersect_set), len(the_intersect_set))


def main():
    project_name = get_project_name()
    list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
    file_count = getCountOfFiles(f'./projects/{project_name}/splits')
    text_pairs, inverted_pairs = read_all_text_pair_names_and_ids_from_db()
    number_of_combinations = sum(1 for e in itertools.combinations(list_of_files, 2))

    # Fetch the hapaxes and store them in a working dict
    hapaxes_dict = read_all_hapaxes_from_db()

    # Prepare work items as a list of (pair_id, one_id, two_id) tuples
    work_items = [(pair_id, item[0], item[1]) for pair_id, item in text_pairs.items()]

    # Determine optimal chunksize based on workload
    num_workers = cpu_count()
    chunksize = max(1, len(work_items) // (num_workers * 4))

    # Process in parallel
    transactions = []
    with Pool(processes=num_workers, initializer=init_worker, initargs=(hapaxes_dict,)) as pool:
        pbar = tqdm(
            desc='Computing hapax overlaps',
            total=number_of_combinations,
            colour="#ffaf87",
            bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
        )

        # imap_unordered yields results as they complete for better performance
        for result in pool.imap_unordered(compute_hapax_overlap, work_items, chunksize=chunksize):
            transactions.append(result)
            pbar.update(1)

        pbar.close()

    # Now, insert the transactions
    insert_hapax_overlaps_to_db(transactions)


if __name__ == '__main__':
    main()