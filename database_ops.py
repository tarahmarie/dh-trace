# this script does two major things (consider splitting into two files?):
# handles the db operations, cleanup, creation, etc, and also runs the
# Jaccard calculations to ensure that the stats between text files are 
# normalized for the lengths of texts instead of simply advantaging the
# longest texts for most hapax overlap, etc.

import ast
import sqlite3

import pandas as pd

from util import get_project_name

#Create the disk database (for backups) and a cursor to handle transactions.
project_name = get_project_name()
disk_con = sqlite3.connect(f"./projects/{project_name}/db/{project_name}.db")
disk_con.row_factory = sqlite3.Row
disk_cur = disk_con.cursor()
#  __  __      __
#  \ \/ /___  / /___
#   \  / __ \/ / __ \
#   / / /_/ / / /_/ /
#  /_/\____/_/\____/
#
disk_cur.execute("PRAGMA synchronous = OFF;")
disk_cur.execute("PRAGMA cache_size = 10000000;")
disk_cur.execute("PRAGMA journal_mode = MEMORY;")

#Create the Database and Tables
def create_db_and_tables():
    disk_cur.execute("CREATE TABLE IF NOT EXISTS alignments(`source_filename` INT, `target_filename` INT, `source_passage`, `target_passage`, `source_author` INT, `target_author` INT, `length_source_passage` INT DEFAULT 0, `length_target_passage` INT DEFAULT 0, `pair_id` INT DEFAULT 0 UNIQUE)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS authors(`id` INT PRIMARY KEY, `author_name` TEXT)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS all_texts(`author_id` INT, `text_id` INT, `source_filename`, `text`, `length` INT DEFAULT 0, `dir` INT, `year` INT)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS dirs(`id` INT, `dir` TEXT)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS text_pairs(`id` INT, `text_a` INT, `text_b` INT)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS hapaxes(`source_filename` INT, `hapaxes`, `hapaxes_count` INT DEFAULT 0)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS hapax_overlaps(`file_pair` INT, `hapaxes`, `intersect_length` INT DEFAULT 0)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS ngrams(`source_filename` INT, `ngrams`, `ngrams_count` INT DEFAULT 0)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS ngram_overlaps(`file_pair` INT, `ngrams`, `intersect_length` INT DEFAULT 0)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS last_run(`number_alignments` INT, `number_files` INT, `total_comparisons` INT, `total_alignments_over_comps` INT, `total_rel_hapaxes` INT, `total_words` INT, `total_rel_ngrams` INT, `total_aligns_over_comps` REAL, `total_rel_hapaxes_over_comps` REAL, `total_rel_hapaxes_over_words` REAL, `total_rel_ngrams_over_comps` REAL)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS results(`first_book`, `second_book`, `ngram_overlaps_count`, `hapax_overlap_count`, `num_alignments`)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS stats_all(`source_author`, `source_year` INT, `source_text`, `target_author`, `target_year` INT, `target_text`, `HapaxOverlaps` INT DEFAULT 0, `haps/pair_len` REAL, `haps/corp_len` REAL, `NgramOverlaps` INT DEFAULT 0, `ngs/pair_len` REAL, `ngs/corp_len` REAL, `#aligns` INT DEFAULT 0, `#als/pair_len` REAL, `#als/corp_len` REAL, `pair_len` INT DEFAULT 0, `corp_len` INT DEFAULT 0, `pair_id` INT UNIQUE)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS stats_alignments(`source_author`, `source_year` INT, `source_text`, `target_author`, `target_year` INT, `target_text`, `#aligns` INT DEFAULT 0, `#aligns_over_pair_len` REAL, `#aligns_over_corp_len` REAL, `pair_len` INT DEFAULT 0, `corp_len` INT DEFAULT 0, `pair_id` INT UNIQUE)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS stats_hapaxes(`source_author`, `source_year` INT, `source_text`, `target_author`, `target_year` INT, `target_text`, `HapaxOverlaps` INT DEFAULT 0, `overlaps_over_pair_len` REAL, `overlaps_over_corp_len` REAL, `pair_len` INT DEFAULT 0, `corp_len` INT DEFAULT 0, `pair_id` INT UNIQUE)")

    disk_cur.execute("CREATE TABLE IF NOT EXISTS stats_ngrams(`source_author`, `source_year` INT, `source_text`, `target_author`, `target_year` INT, `target_text`, `NgramOverlaps` INT DEFAULT 0, `overlaps_over_pair_len` REAL, `overlaps_over_corp_len` REAL, `pair_len` INT DEFAULT 0, `corp_len` INT DEFAULT 0, `pair_id` INT UNIQUE)")

    disk_cur.execute("CREATE INDEX IF NOT EXISTS ng_filepairs ON ngram_overlaps(file_pair)")

    disk_cur.execute("CREATE INDEX IF NOT EXISTS ng_all ON ngrams(source_filename)")
    
    disk_cur.execute("CREATE INDEX IF NOT EXISTS hap_filepairs ON hapax_overlaps(file_pair)")

    disk_cur.execute("CREATE INDEX IF NOT EXISTS hap_sourcefiles ON hapaxes(source_filename)")
    
    disk_cur.execute("CREATE INDEX IF NOT EXISTS all_text_source ON all_texts(author_id, source_filename)")

#Empty the Database (for updating all the things)
def reset_the_db():
    disk_cur.execute("DROP TABLE IF EXISTS alignments")
    disk_cur.execute("DROP TABLE IF EXISTS dirs")
    disk_cur.execute("DROP TABLE IF EXISTS all_texts")
    disk_cur.execute("DROP TABLE IF EXISTS authors")
    disk_cur.execute("DROP TABLE IF EXISTS text_pairs")
    disk_cur.execute("DROP TABLE IF EXISTS hapaxes")
    disk_cur.execute("DROP TABLE IF EXISTS hapax_overlaps")
    disk_cur.execute("DROP TABLE IF EXISTS ngrams")
    disk_cur.execute("DROP TABLE IF EXISTS ngram_overlaps")
    disk_cur.execute("DROP TABLE IF EXISTS last_run")
    disk_cur.execute("DROP TABLE IF EXISTS results")
    disk_cur.execute("DROP TABLE IF EXISTS stats_all")
    disk_cur.execute("DROP TABLE IF EXISTS stats_alignments")
    disk_cur.execute("DROP TABLE IF EXISTS stats_hapaxes")
    disk_cur.execute("DROP TABLE IF EXISTS stats_ngrams")
    disk_cur.execute("DROP INDEX IF EXISTS ng_all")
    disk_cur.execute("DROP INDEX IF EXISTS ng_filepairs")
    disk_cur.execute("DROP INDEX IF EXISTS hap_filepairs")
    disk_cur.execute("DROP INDEX IF EXISTS hap_sourcefiles")
    disk_cur.execute("DROP INDEX IF EXISTS all_text_source")

    disk_con.commit()

#Helper Function for Shrinking DB (esp. after Jaccard stuff)
def vacuum_the_db():
    #Some Helper Tables Can Be Removed:
    disk_cur.execute("DROP TABLE IF EXISTS stats_alignments")
    disk_cur.execute("DROP TABLE IF EXISTS stats_hapaxes")
    disk_cur.execute("DROP TABLE IF EXISTS stats_ngrams")
    disk_cur.execute("DROP INDEX IF EXISTS hap_jac")
    disk_cur.execute("DROP INDEX IF EXISTS ng_jac")
    #Now, some tidying.
    disk_cur.execute("VACUUM;")
    disk_con.commit()

#Backup function to save the in-memory database (currently disused)
def backup_the_database_to_disk():
    disk_con.backup(disk_con)
    disk_con.close() #We're done with the in-memory db, so why not free RAM?

    disk_cur.execute("CREATE TABLE IF NOT EXISTS hapaxes(`source_filename`, `hapaxes`)")
    disk_cur.execute("CREATE TABLE IF NOT EXISTS hapax_overlaps(`file_pair`, `hapaxes`)")
    disk_cur.execute("CREATE TABLE IF NOT EXISTS ngrams(`source_filename`, `ngrams`)")
    disk_cur.execute("CREATE TABLE IF NOT EXISTS ngram_overlaps(`file_pair`, `ngrams`)")
    disk_cur.execute("CREATE TABLE IF NOT EXISTS last_run(`number_alignments` INT, `number_files` INT, `total_comparisons` INT, `total_alignments_over_comps` INT, `total_rel_hapaxes` INT, `total_words` INT, `total_rel_ngrams` INT, `total_aligns_over_comps` REAL, `total_rel_hapaxes_over_comps` REAL, `total_rel_hapaxes_over_words` REAL, `total_rel_ngrams_over_comps` REAL)")
    disk_con.commit()

#Export previous run to csv for visualization
def export_results_to_csv():
    df = pd.read_sql('SELECT * FROM results', disk_con)
    df.to_csv(f"./projects/{project_name}/results/results.csv", index = False, header = False)

#Def Insert Data (Table, data...)
def insert_alignments_to_db(transactions):
    insert_statement = "INSERT OR IGNORE INTO alignments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    disk_cur.executemany(insert_statement, transactions)
    disk_con.commit()

def insert_authors_to_db(id, author):
    disk_cur.execute("INSERT OR IGNORE INTO authors VALUES (?, ?)", (id, author))
    disk_con.commit()

def insert_dirs_to_db(id, path):
    disk_cur.execute("INSERT OR IGNORE INTO dirs VALUES (?, ?)", (id, path))
    disk_con.commit()

def insert_texts_to_db(author_id, text_id, source_file, text, length, dir, year):
    disk_cur.execute("INSERT OR IGNORE INTO all_texts VALUES (?, ?, ?, ?, ?, ?, ?)", (author_id, text_id, source_file, text, length, dir, year))
    disk_con.commit()

def insert_text_pairs_to_db(transactions):
    insert_statement = "INSERT OR IGNORE INTO text_pairs VALUES (?, ?, ?)"
    disk_cur.executemany(insert_statement, transactions)
    disk_con.commit()

def insert_ngrams_to_db(transactions):
    insert_statement = "INSERT OR IGNORE INTO ngrams VALUES (?, ?, ?)"
    disk_cur.executemany(insert_statement, transactions)
    disk_con.commit()

def insert_ngram_overlaps_to_db(transactions):
    insert_statement = "INSERT OR IGNORE INTO ngram_overlaps VALUES (?, ?, ?)"
    disk_cur.executemany(insert_statement, transactions)
    disk_con.commit()

def insert_hapaxes_to_db(transactions):
    insert_statement = "INSERT OR IGNORE INTO hapaxes VALUES (?, ?, ?)"
    disk_cur.executemany(insert_statement, transactions)
    disk_con.commit()

def insert_hapax_overlaps_to_db(transactions):
    insert_statement = "INSERT OR IGNORE INTO hapax_overlaps VALUES (?, ?, ?)"
    disk_cur.executemany(insert_statement, transactions)
    disk_con.commit()

def insert_results_to_db(first_book, second_book, ngram_overlaps_count, hapax_overlap_count, num_alignments):
    disk_cur.execute("INSERT OR IGNORE INTO results VALUES (?, ?, ?, ?, ?)", (first_book, second_book, ngram_overlaps_count, hapax_overlap_count, num_alignments))
    disk_con.commit()

def insert_last_run_stats_to_db(aligns, files):
    #Zeroes are placeholders for the averages.
    disk_cur.execute("INSERT OR IGNORE INTO last_run VALUES (?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0)", (aligns, files))
    disk_con.commit()

def insert_averages_to_db(total_comparisons, total_alignments_over_comps, total_rel_hapaxes, total_words, total_rel_ngrams, total_aligns_over_comps, total_rel_hapaxes_over_comps, total_rel_hapaxes_over_words, total_rel_ngrams_over_comps):
    disk_cur.execute("UPDATE last_run SET total_comparisons = ?, total_alignments_over_comps = ?, total_rel_hapaxes = ?, total_words = ?, total_rel_ngrams = ?, total_aligns_over_comps = ?, total_rel_hapaxes_over_comps = ?, total_rel_hapaxes_over_words = ?, total_rel_ngrams_over_comps = ? WHERE rowid = 1", (total_comparisons, total_alignments_over_comps, total_rel_hapaxes, total_words, total_rel_ngrams, total_aligns_over_comps, total_rel_hapaxes_over_comps, total_rel_hapaxes_over_words, total_rel_ngrams_over_comps))
    disk_con.commit()

def insert_stats_to_db(stats_transactions, hapax_transactions, ngram_transactions, align_transactions):
    insert_stats_statement = "INSERT INTO stats_all VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    insert_hapax_statement = "INSERT INTO stats_hapaxes VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
    insert_ngram_statement = "INSERT INTO stats_ngrams VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
    insert_align_statement = "INSERT INTO stats_alignments VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"

    disk_cur.executemany(insert_stats_statement, stats_transactions)
    disk_cur.executemany(insert_hapax_statement, hapax_transactions)
    disk_cur.executemany(insert_ngram_statement, ngram_transactions)
    disk_cur.executemany(insert_align_statement, align_transactions)
    disk_con.commit()

#Def Query Data (Table, data)

def read_author_from_db(filename):
    disk_cur.execute("SELECT author_id FROM all_texts WHERE source_filename = ?", [filename])
    the_author = disk_cur.fetchone()
    return the_author[0]

def read_all_author_names_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT id, author_name FROM authors")
    the_authors = disk_cur.fetchall()
    i = 1
    for author in the_authors:
        temp_dict[author['id']] = author['author_name']
        i += 1
    return temp_dict

def read_all_author_names_and_ids_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT DISTINCT author_name, id FROM authors INNER JOIN all_texts ON all_texts.author_id = authors.id")
    the_authors = disk_cur.fetchall()
    for author in the_authors:
        temp_dict[author['author_name']] = author['id']
    return temp_dict

def read_author_names_by_id_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT DISTINCT author_name, id FROM authors INNER JOIN all_texts ON all_texts.author_id = authors.id")
    the_authors = disk_cur.fetchall()
    for author in the_authors:
        temp_dict[author['id']] = author['author_name']
    return temp_dict

def read_all_alignments_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT pair_id, source_filename, target_filename, length_source_passage, length_target_passage FROM alignments")
    the_aligns = disk_cur.fetchall()
    for align in the_aligns:
        temp_dict[align['pair_id']] = [align['source_filename'], align['length_source_passage'], align['target_filename'], align['length_target_passage']]
    return temp_dict

def read_text_from_db(filename):
    disk_cur.execute("SELECT text FROM all_texts WHERE source_filename = ?", [filename])
    the_text = disk_cur.fetchone()
    return the_text[0]

def read_all_text_names_and_ids_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT source_filename, text_id FROM all_texts")
    the_texts = disk_cur.fetchall()
    for text in the_texts:
        temp_dict[text['source_filename']] = text['text_id']
    return temp_dict

def read_all_text_names_by_id_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT DISTINCT source_filename, text_id FROM all_texts")
    the_texts = disk_cur.fetchall()
    for text in the_texts:
        temp_dict[text['text_id']] = text['source_filename']
    return temp_dict

def read_all_text_lengths_by_id_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT text_id, length FROM all_texts;")
    the_lengths = disk_cur.fetchall()
    for length in the_lengths:
        temp_dict[length['text_id']] = length['length']
    return temp_dict

def read_all_text_pair_names_and_ids_from_db():
    temp_dict = {}
    inverted_dict = {}
    disk_cur.execute("SELECT id, text_a, text_b FROM text_pairs")
    for item in disk_cur:
        temp_dict[item['id']] = [item['text_a'], item['text_b']]
        inverted_dict[(item['text_a'], item['text_b'])] = item['id']
    return temp_dict, inverted_dict

def read_text_names_with_dirs_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT source_filename, dir FROM all_texts")
    the_dirs = disk_cur.fetchall()
    for dir in the_dirs:
        temp_dict[dir['source_filename']] = dir['dir']
    return temp_dict

def read_all_dir_names_by_id_from_db():
    temp_dict = {}
    disk_cur.execute("SELECT DISTINCT dir, id FROM dirs")
    the_dirs = disk_cur.fetchall()
    for dir in the_dirs:
        temp_dict[dir['id']] = dir['dir']
    return temp_dict

def read_ngrams_from_db(filename):
    disk_cur.execute("SELECT ngrams FROM ngrams WHERE source_filename = ?", [filename])
    the_ngrams = disk_cur.fetchone()
    return ast.literal_eval(the_ngrams[0])

def get_total_number_of_ngrams():
    disk_cur.execute("SELECT SUM(ngrams_count) FROM ngrams;")
    the_count = disk_cur.fetchone()
    return the_count[0]

def read_all_ngrams_from_db():
    disk_cur.execute("SELECT source_filename, ngrams FROM ngrams")
    temp_dict = {}
    for pair in disk_cur:
        #This will make things slower, but it's the only thing that works (so far)
        temp_dict[pair['source_filename']] = ast.literal_eval(pair['ngrams'])
    return temp_dict

def read_ngrams_intersect_from_db(filename):
    disk_cur.execute("SELECT ngrams FROM ngram_overlaps WHERE file_pair = ?", [filename])
    if the_intersect := disk_cur.fetchone():
        return ast.literal_eval(the_intersect[0])
    else:
        return 0

def read_ngrams_intersect_length_from_db(filename):
    disk_cur.execute("SELECT intersect_length FROM ngram_overlaps WHERE file_pair = ?", [filename])
    the_intersect = disk_cur.fetchone()
    return the_intersect[0]

def read_all_ngram_intersects_filepairs_from_db():
    disk_cur.execute("SELECT file_pair FROM ngram_overlaps")
    the_intersects = disk_cur.fetchall()
    temp_dict = {}
    for thing in the_intersects:
        #This will make things slower, but it's the only thing that works (so far)
        temp_dict[thing['file_pair']] = None
    return temp_dict

def read_all_ngram_intersects_lengths_from_db():
    disk_cur.execute("SELECT file_pair, intersect_length FROM ngram_overlaps")
    temp_dict = {}
    for thing in disk_cur:
        #This will make things slower, but it's the only thing that works (so far)
        temp_dict[thing['file_pair']] = thing['intersect_length']
    return temp_dict

def read_hapaxes_from_db(filename):
    disk_cur.execute("SELECT hapaxes FROM hapaxes WHERE source_filename = ?", [filename])
    the_hapaxes = disk_cur.fetchone()
    return ast.literal_eval(the_hapaxes[0])

def read_all_hapaxes_from_db():
    disk_cur.execute("SELECT source_filename, hapaxes FROM hapaxes")
    the_hapaxes = disk_cur.fetchall()
    temp_dict = {}
    for pair in the_hapaxes:
        temp_dict[pair ['source_filename']] = ast.literal_eval(pair['hapaxes'])
    return temp_dict

def read_all_hapax_intersects_filepairs_from_db():
    disk_cur.execute("SELECT file_pair FROM hapax_overlaps")
    the_intersects = disk_cur.fetchall()
    temp_dict = {}
    for thing in the_intersects:
        #This will make things slower, but it's the only thing that works (so far)
        temp_dict[thing['file_pair']] = None
    return temp_dict

def read_hapax_intersect_length_from_db(filename):
    disk_cur.execute("SELECT intersect_length FROM hapax_overlaps WHERE file_pair = ?", [filename])
    the_intersect = disk_cur.fetchone()
    return the_intersect[0]

def read_all_hapax_intersects_lengths_from_db():
    disk_cur.execute("SELECT file_pair, intersect_length FROM hapax_overlaps")
    the_intersects = disk_cur.fetchall()
    temp_dict = {}
    for thing in the_intersects:
        #This will make things slower, but it's the only thing that works (so far)
        temp_dict[thing['file_pair']] = thing['intersect_length']
    return temp_dict

def read_chapter_length_from_db(filename):
    disk_cur.execute("SELECT length FROM all_texts WHERE source_filename = ?", [filename])
    the_length = disk_cur.fetchone()
    return the_length[0]

def read_all_chapter_length_from_db():
    disk_cur.execute("SELECT source_filename, length FROM all_texts")
    the_lengths = disk_cur.fetchall()
    temp_dict = {}
    for thing in the_lengths:
        temp_dict[thing['source_filename']] = thing['length']
    return temp_dict
    
def read_last_run_file_count_from_db():
    #Ensure last_run exists and has some values to keep program from crashing!
    disk_cur.execute("CREATE TABLE IF NOT EXISTS last_run(`number_alignments` INT, `number_files` INT, `total_comparisons` INT, `total_alignments_over_comps` INT, `total_rel_hapaxes` INT, `total_words` INT, `total_rel_ngrams` INT, `total_aligns_over_comps` REAL, `total_rel_hapaxes_over_comps` REAL, `total_rel_hapaxes_over_words` REAL, `total_rel_ngrams_over_comps` REAL)")
    disk_cur.execute("INSERT INTO last_run VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)")
    disk_con.commit()
    #Now, find out how many files were there on the last run.
    disk_cur.execute("SELECT MAX(number_files) FROM last_run")
    file_count = disk_cur.fetchone()
    return file_count[0]

def read_averages_from_db():
    disk_cur.execute("SELECT total_comparisons, total_alignments_over_comps, total_rel_hapaxes, total_words, total_rel_ngrams, total_aligns_over_comps, total_rel_hapaxes_over_comps, total_rel_hapaxes_over_words, total_rel_ngrams_over_comps FROM last_run")
    the_averages = disk_cur.fetchone()
    return the_averages

#do visualization stuff
def read_all_combined_jaccard_from_db():
    disk_cur.execute("SELECT * FROM combined_jaccard;")
    the_combined_jacc = disk_cur.fetchall()
    return the_combined_jacc

# Once we get this working, the full four cases of Y/N/YN/NY can be calculated and viewed.
def get_length_of_multiauthor_prediction_table(author_a, author_b, author_c):
    #This logic is broken.
    disk_cur.execute("SELECT COUNT(*) FROM author_prediction WHERE source_auth LIKE ? AND target_auth LIKE ? OR target_auth LIKE ?", [author_a, author_b, author_c])
    the_length = disk_cur.fetchone()
    return the_length[0]

#Do Jaccard Stuff
def create_hapax_jaccard():
    #First, we cleanup after previous jaccard runs
    disk_cur.execute("DROP TABLE IF EXISTS hapax_jaccard;")
    disk_cur.execute("DROP INDEX IF EXISTS hap_jac;")
    disk_cur.execute("DROP INDEX IF EXISTS hap_counts;")

    #Now, we make our stuff.
    disk_cur.execute("CREATE TABLE IF NOT EXISTS hapax_jaccard AS SELECT * FROM stats_hapaxes;")
    disk_cur.execute("ALTER TABLE hapax_jaccard ADD COLUMN `source_hapaxes` INT DEFAULT 0;")
    disk_cur.execute("ALTER TABLE hapax_jaccard ADD COLUMN `target_hapaxes` INT DEFAULT 0;")
    disk_cur.execute("ALTER TABLE hapax_jaccard ADD COLUMN `jac_sim` REAL;")
    disk_cur.execute("ALTER TABLE hapax_jaccard ADD COLUMN `jac_dis` REAL;")
    disk_cur.execute("CREATE INDEX hap_jac ON hapax_jaccard(source_text, target_text, source_hapaxes, target_hapaxes, HapaxOverlaps);")
    disk_cur.execute("CREATE INDEX hap_counts ON hapaxes(source_filename, hapaxes_count);")
    disk_con.commit()

def populate_hapax_jaccard():
    disk_cur.execute("UPDATE hapax_jaccard SET source_hapaxes=(SELECT hapaxes_count FROM hapaxes WHERE hapaxes.source_filename = source_text);")
    disk_cur.execute("UPDATE hapax_jaccard SET target_hapaxes=(SELECT hapaxes_count FROM hapaxes WHERE hapaxes.source_filename = target_text);")
    disk_con.commit()

def calculate_hapax_jaccard_similarity():
    #Reference: https://www.statology.org/jaccard-similarity/
    disk_cur.execute("SELECT source_text, target_text, HapaxOverlaps, source_hapaxes, target_hapaxes FROM hapax_jaccard;")
    the_result = disk_cur.fetchall()

    for thing in the_result:
        jac_sim = thing['HapaxOverlaps'] / (sum([thing['source_hapaxes'],thing['target_hapaxes']]))
        jac_dis = 1 - jac_sim
        disk_cur.execute("UPDATE hapax_jaccard SET jac_sim = ?, jac_dis = ? WHERE source_text = ? AND target_text = ?;", [jac_sim, jac_dis, thing['source_text'], thing['target_text']])
    disk_con.commit()

def create_ngrams_jaccard():
    #First, we cleanup after previous jaccard runs
    disk_cur.execute("DROP TABLE IF EXISTS ngrams_jaccard;")
    disk_cur.execute("DROP INDEX IF EXISTS ng_jac;")
    disk_cur.execute("DROP INDEX IF EXISTS ng_files;")

    #Now, we make our stuff.
    disk_cur.execute("CREATE TABLE IF NOT EXISTS ngrams_jaccard AS SELECT * FROM stats_ngrams;")
    disk_cur.execute("ALTER TABLE ngrams_jaccard ADD COLUMN `source_ngrams` INT DEFAULT 0;")
    disk_cur.execute("ALTER TABLE ngrams_jaccard ADD COLUMN `target_ngrams` INT DEFAULT 0;")
    disk_cur.execute("ALTER TABLE ngrams_jaccard ADD COLUMN `ng_jac_sim` REAL;")
    disk_cur.execute("ALTER TABLE ngrams_jaccard ADD COLUMN `ng_jac_dis` REAL;")
    disk_cur.execute("CREATE INDEX ng_jac ON ngrams_jaccard(source_text, target_text, NgramOverlaps, source_ngrams, target_ngrams)")
    disk_cur.execute("CREATE INDEX ng_files ON ngrams(source_filename, ngrams_count)")
    disk_con.commit()

def populate_ngrams_jaccard():
    disk_cur.execute("UPDATE ngrams_jaccard SET source_ngrams=(SELECT ngrams_count FROM ngrams WHERE ngrams.source_filename = source_text);")
    disk_cur.execute("UPDATE ngrams_jaccard SET target_ngrams=(SELECT ngrams_count FROM ngrams WHERE ngrams.source_filename = target_text);")
    disk_con.commit()

def calculate_ngram_jaccard_similarity():
    #Reference: https://www.statology.org/jaccard-similarity/
    disk_cur.execute("SELECT source_text, target_text, NgramOverlaps, source_ngrams, target_ngrams FROM ngrams_jaccard;")
    the_result = disk_cur.fetchall()

    for thing in the_result:
        jac_sim = thing['NgramOverlaps'] / (sum([thing['source_ngrams'],thing['target_ngrams']]))
        jac_dis = 1 - jac_sim
        disk_cur.execute("UPDATE ngrams_jaccard SET ng_jac_sim = ?, ng_jac_dis = ? WHERE source_text = ? AND target_text = ?;", [jac_sim, jac_dis, thing['source_text'], thing['target_text']])
    disk_con.commit()

def create_alignments_jaccard():
    #First, we cleanup after previous jaccard runs
    disk_cur.execute("DROP TABLE IF EXISTS alignments_jaccard;")
    disk_cur.execute("DROP INDEX IF EXISTS at_lens;")
    disk_cur.execute("DROP INDEX IF EXISTS aligns_vals;")

    #Now, we make our stuff.
    disk_cur.execute("CREATE TABLE IF NOT EXISTS alignments_jaccard AS SELECT * FROM alignments;")
    disk_cur.execute("ALTER TABLE alignments_jaccard ADD COLUMN `source_total_words` INT DEFAULT 0;")
    disk_cur.execute("ALTER TABLE alignments_jaccard ADD COLUMN `target_total_words` INT DEFAULT 0;")
    disk_cur.execute("ALTER TABLE alignments_jaccard ADD COLUMN `al_jac_sim` REAL;")
    disk_cur.execute("ALTER TABLE alignments_jaccard ADD COLUMN `al_jac_dis` REAL;")
    disk_cur.execute("CREATE INDEX at_lens ON all_texts(source_filename, length);")

    disk_con.commit()

def populate_alignments_jaccard():
    disk_cur.execute("UPDATE alignments_jaccard SET source_total_words=(SELECT length FROM all_texts WHERE text_id = alignments_jaccard.source_filename), target_total_words=(SELECT length FROM all_texts WHERE text_id = alignments_jaccard.target_filename);")
    disk_con.commit()

def calculate_alignments_jaccard_similarity():
    #Reference: https://www.statology.org/jaccard-similarity/

    #Index goes brrr...
    disk_cur.execute("CREATE INDEX aligns_vals ON alignments_jaccard(source_filename, target_filename, length_source_passage, length_target_passage, source_total_words, target_total_words);")
    disk_con.commit()

    disk_cur.execute("SELECT source_filename, target_filename, length_source_passage, length_target_passage, source_total_words, target_total_words FROM alignments_jaccard;")
    the_result = disk_cur.fetchall()
# Extremely key element here; calculation of Jaccard similarity based on a normalized length of text, not just total contents.
    for thing in the_result:
        jac_sim = (sum([thing['length_source_passage'],thing['length_target_passage']])) / (sum([thing['source_total_words'],thing['target_total_words']]))
        jac_dis = 1 - jac_sim
        disk_cur.execute("UPDATE alignments_jaccard SET al_jac_sim = ?, al_jac_dis = ? WHERE source_filename = ? AND target_filename = ?;", [jac_sim, jac_dis, thing['source_filename'], thing['target_filename']])
        
    disk_con.commit()

def make_the_combined_jaccard_table():
    #Start Fresh
    disk_cur.execute("DROP TABLE IF EXISTS combined_jaccard;")
    disk_cur.execute("DROP INDEX IF EXISTS comjac_idx;")
    disk_con.commit()

    #Create the table structure and populate from hapax_jaccard as a starter.
    disk_cur.execute("CREATE TABLE IF NOT EXISTS combined_jaccard (`source_auth` INT, `source_year` INT DEFAULT 0, `source_text` INT, `target_auth` INT, `target_year` INT DEFAULT 0, `target_text` INT, `hap_jac_sim` REAL DEFAULT 0.0, `hap_jac_dis` REAL DEFAULT 0.0, `ng_jac_sim` REAL DEFAULT 0.0, `ng_jac_dis` REAL DEFAULT 0.0, `al_jac_sim` REAL DEFAULT 0.0, `al_jac_dis` REAL DEFAULT 0.0, `pair_id` INT PRIMARY KEY UNIQUE);")
    disk_cur.execute("INSERT INTO combined_jaccard (source_auth, source_year, source_text, target_auth, target_year, target_text, hap_jac_sim, hap_jac_dis, pair_id) SELECT source_author, source_year, source_text, target_author, target_year, target_text, jac_sim, jac_dis, pair_id FROM hapax_jaccard;")
    disk_con.commit()

    #Now, we join the ngrams_jaccard table and the alignments_jaccard table to it.
    disk_cur.execute("CREATE TABLE temp_jaccard AS SELECT combined_jaccard.source_auth, combined_jaccard.source_year, combined_jaccard.source_text, combined_jaccard.target_auth, combined_jaccard.target_year, combined_jaccard.target_text, combined_jaccard.hap_jac_sim, combined_jaccard.hap_jac_dis, combined_jaccard.pair_id, ngrams_jaccard.ng_jac_sim, ngrams_jaccard.ng_jac_dis, alignments_jaccard.al_jac_sim, alignments_jaccard.al_jac_dis FROM combined_jaccard JOIN ngrams_jaccard ON combined_jaccard.pair_id = ngrams_jaccard.pair_id JOIN alignments_jaccard ON ngrams_jaccard.pair_id = alignments_jaccard.pair_id")
    disk_cur.execute("DROP TABLE IF EXISTS combined_jaccard;")
    disk_cur.execute("ALTER TABLE temp_jaccard RENAME TO combined_jaccard;")
    disk_cur.execute("CREATE INDEX comjac_idx ON combined_jaccard(source_auth, target_auth, source_text, target_text, pair_id);")
    disk_con.commit()

def close_db_connection():
    """Close the SQLite database connection."""
    try:
        if disk_con:
            disk_con.close()
            print("Database connection closed.")
    except Exception as e:
        print(f"Error while closing the database connection: {str(e)}")

create_db_and_tables()