# For each text file, get the list of hapaxes and store in a db. 
# This does the individual calculation on each file.

from multiprocessing import Pool, cpu_count

from tqdm import tqdm

from database_ops import (insert_hapaxes_to_db,
                          read_all_text_names_and_ids_from_db,
                          read_text_from_db)
from hapaxes_1tM import compute_hapaxes, remove_tei_lines_from_text
from util import (fix_alignment_file_names, get_project_name, getCountOfFiles,
                  getListOfFiles)


# Global variable for worker processes
_text_and_id_dict = None


def init_worker(text_and_id_dict):
    """Initialize each worker process with the shared text ID dictionary."""
    global _text_and_id_dict
    _text_and_id_dict = text_and_id_dict


def process_file(file):
    """Worker function to compute hapaxes for a single file."""
    name_of_text = fix_alignment_file_names(file.split('/')[5])

    temp_text = read_text_from_db(name_of_text)
    the_clean_data = remove_tei_lines_from_text(temp_text)
    
    hapaxes_from_file = compute_hapaxes(the_clean_data)
    hapax_count = len(hapaxes_from_file)

    return (_text_and_id_dict[name_of_text], repr(set(hapaxes_from_file)), hapax_count)


def main():
    project_name = get_project_name()
    list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
    file_count = getCountOfFiles(f'./projects/{project_name}/splits')
    text_and_id_dict = read_all_text_names_and_ids_from_db()

    # Determine optimal chunksize
    num_workers = cpu_count()
    chunksize = max(1, len(list_of_files) // (num_workers * 4))

    # Process in parallel
    transactions = []
    with Pool(processes=num_workers, initializer=init_worker, initargs=(text_and_id_dict,)) as pool:
        pbar = tqdm(
            desc='Computing hapaxes',
            total=len(list_of_files),
            colour="#00875f",
            bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
        )

        for result in pool.imap_unordered(process_file, list_of_files, chunksize=chunksize):
            transactions.append(result)
            pbar.update(1)

        pbar.close()

    insert_hapaxes_to_db(transactions)


if __name__ == '__main__':
    main()