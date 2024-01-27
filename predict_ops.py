import sqlite3

import pandas as pd

from util import create_author_pair_for_lookups, get_project_name

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
disk_cur.execute("PRAGMA cache_size = -2000000000;")
disk_cur.execute("PRAGMA page_size = 32768;")
disk_cur.execute("PRAGMA journal_mode = MEMORY;")
disk_cur.execute("PRAGMA temp_store = MEMORY;")

##
# Utility functions
##

###Manual
def setup_author_prediction():
    ## Author Prediction Table
    #First, we cleanup after previous runs
    disk_cur.execute("DROP TABLE IF EXISTS author_prediction;")

    #Now, we make our stuff.
    disk_cur.execute("CREATE TABLE IF NOT EXISTS author_prediction AS SELECT source_auth, source_year, source_text, target_auth, target_year, target_text, hap_jac_dis, al_jac_dis FROM original_db.combined_jaccard;")
    disk_cur.execute("ALTER TABLE author_prediction ADD COLUMN score REAL DEFAULT 0.0;")
    disk_cur.execute("ALTER TABLE author_prediction ADD COLUMN same_author TEXT;")
    disk_con.commit()

def compute_author_scores(hapax_weight, ngram_weight, align_weight, threshold):
    disk_cur.execute("SELECT rowid, * FROM author_prediction")
    the_rows = disk_cur.fetchall()
    for row in the_rows:
        hap_score = 0.0
        al_score = 0.0
        outcome = "TBD"
        source_auth = ""
        target_auth = ""
        #Get source author and target author
        if row[1] is not None:
            source_auth = row[1]
        if row[4] is not None:
            target_auth = row[4]
        #Columns 6, 7, 8 of author_prediction are hapax_dist and align_dist
        if row[7] is not None:
            hap_score = row[7] * hapax_weight
        if row[8] is not None:
            al_score = row[8] * align_weight
        
        if sum([hap_score, al_score]) < threshold:
            if source_auth == target_auth:
                outcome = "I"
            else:
                outcome = "N"
        elif sum([hap_score, al_score]) >= threshold:
            outcome = "Y"
        else:
            outcome = "N"

        disk_cur.execute("UPDATE author_prediction SET score = ?, same_author = ? WHERE rowid = ?;", [sum([hap_score, al_score]), outcome, row[0]])
    
    optimize()
    disk_con.commit()

### Automatic
def setup_auto_author_prediction_tables():
    #First, we cleanup after previous runs
    disk_cur.execute("DROP TABLE IF EXISTS auto_author_accuracy;")
    disk_cur.execute("DROP TABLE IF EXISTS calculations;")
    disk_cur.execute("DROP TABLE IF EXISTS pair_counts;")
    disk_cur.execute("DROP TABLE IF EXISTS confusion_scores;")
    disk_cur.execute("DROP INDEX IF EXISTS calculations_index;")
    disk_cur.execute("DROP INDEX IF EXISTS idx_threshold;")
    disk_cur.execute("DROP INDEX IF EXISTS idx_length;")
    disk_cur.execute("DROP INDEX IF EXISTS idx_comp_score_threshold;")
    disk_cur.execute("DROP INDEX IF EXISTS comb_jac_index;")
    disk_cur.execute("DROP INDEX IF EXISTS all_texts_index;")
    disk_cur.execute("DROP INDEX IF EXISTS combined_jaccard_pair_id_index;")
    disk_cur.execute("DROP INDEX IF EXISTS text_id_index;")
    disk_cur.execute("DROP INDEX IF EXISTS text_len_index;")
    disk_con.commit()

    #Now, we make our stuff.
    pair_counts_query = """
        CREATE TABLE IF NOT EXISTS pair_counts(
            `author_pair` TEXT UNIQUE,
            `count` INT DEFAULT 0
        );
    """

    calculations_query = """
        CREATE TABLE IF NOT EXISTS calculations(
        `pair_id` INT,
        `author_pair` TEXT,
        `threshold` REAL,
        `comp_score` REAL,
        `same_author` TEXT,
        `weight_id` INT
        );
    """

    weights_query = """
        CREATE TABLE IF NOT EXISTS weights(
        `weight_id` INT UNIQUE,
        `hap_weight` REAL,
        `al_weight` REAL
        );
    """

    confusion_query = """
        CREATE TABLE IF NOT EXISTS confusion_scores(
            `threshold` REAL,
            `tp` INT,
            `tn` INT,
            `fp` INT,
            `fn` INT
        );
    """

    disk_cur.execute(pair_counts_query)
    disk_cur.execute(calculations_query)
    disk_cur.execute(weights_query)
    disk_cur.execute(confusion_query)
    disk_con.commit()

def setup_text_stats_table():
    text_stats_query = """
        CREATE TABLE IF NOT EXISTS text_stats AS 
        SELECT DISTINCT
            text_id,
            length,
            source_year
        FROM all_texts
        JOIN stats_all ON text_id = source_text;
    """

    disk_cur.execute(text_stats_query)
    disk_con.commit()

def setup_auto_indices():
    #Look, all this stuff is slow.  But it will make the plot script so much faster, so...
    disk_cur.execute("CREATE INDEX calculations_index ON calculations(pair_id, author_pair, same_author, threshold);")
    disk_cur.execute("CREATE INDEX idx_threshold ON calculations(threshold);")
    disk_cur.execute("CREATE INDEX idx_length ON all_texts(length);")
    disk_cur.execute("CREATE INDEX idx_comp_score_threshold ON calculations(comp_score, threshold);")

    disk_con.commit()

def setup_auto_author_accuracy_table():
    disk_cur.execute("DROP TABLE IF EXISTS auto_author_accuracy;")
    creation_query = """
        CREATE TABLE IF NOT EXISTS auto_author_accuracy(
            `threshold` REAL DEFAULT 0.0,
            `yes_count` INT DEFAULT 0,
            `no_count` INT DEFAULT 0,
            `should_have_been_yes` INT DEFAULT 0,
            `should_have_been_no` INT DEFAULT 0,
            `total_records` INT, 
            `yes_percent` REAL DEFAULT 0.0,
            `no_percent` REAL DEFAULT 0.0,
            `should_have_been_yes_percent` REAL DEFAULT 0.0,
            `should_have_been_no_percent` REAL DEFAULT 0.0
            );
    """
    disk_cur.execute(creation_query)
    disk_con.commit()

def insert_calculations(data):
    disk_cur.execute("BEGIN TRANSACTION;")
    disk_cur.executemany("INSERT INTO calculations VALUES(?,?,?,?,?,?);", data)
    disk_cur.execute("COMMIT;")

def insert_author_pair_counts(data):
    insert_statement = "INSERT OR IGNORE INTO pair_counts VALUES(?,?);"
    disk_cur.executemany(insert_statement, data)
    disk_con.commit()

def insert_weights(data):
    insert_statement = "INSERT OR IGNORE INTO weights VALUES(?,?,?);"
    disk_cur.executemany(insert_statement, data)
    disk_con.commit()

def get_author_view_length(author_pair):
    length_query = """
        SELECT
        count
        FROM pair_counts
        WHERE author_pair = ?
    """

    disk_cur.execute(length_query, [author_pair])
    result = disk_cur.fetchone()

    return result[0]

def get_all_weights():
    weights_query = """
        SELECT
        weight_id,
        hap_weight,
        al_weight
        FROM weights
    """
    temp_dict = {}
    disk_cur.execute(weights_query)
    result = disk_cur.fetchall()
    for item in result:
        temp_dict[item[0]] = item[1], item[2]
    
    return temp_dict

def get_confusion_scores():
    temp_dict = {}
    confused_query = """
        SELECT * FROM confusion_scores;
    """
    disk_cur.execute(confused_query)
    result = disk_cur.fetchall()
    for item in result:
        temp_dict[item[0]] = [item[1], item[2], item[3], item[4]]
    return temp_dict

def create_author_view(author_pair, weights_dict):
    query = """
        SELECT 
        ocj.source_auth,
        ocj.target_auth,
        ocj.source_text,
        ocj.target_text,
        calc.comp_score,
        calc.same_author,
        calc.threshold,
        calc.weight_id,
        source_lengths.length AS source_length,
        target_lengths.length AS target_length
        FROM combined_jaccard AS ocj
        JOIN calculations AS calc ON ocj.pair_id = calc.pair_id
        JOIN all_texts AS source_lengths ON ocj.source_text = source_lengths.text_id
        JOIN all_texts AS target_lengths ON ocj.target_text = target_lengths.text_id
        
        WHERE calc.author_pair = ? 
    """
    params = [author_pair]
    the_predictions = pd.read_sql_query(query, disk_con, params=params)
    the_predictions.columns = ['source_auth', 'target_auth', 'source_text', 'target_text', 'comp_score', 'same_author', 'threshold', 'weight_id', 'source_length', 'target_length']
    the_predictions['hap_weight'] = the_predictions['weight_id'].map(lambda x: weights_dict.get(x, ())[0])
    the_predictions['al_weight'] = the_predictions['weight_id'].map(lambda x: weights_dict.get(x, ())[1])

    return the_predictions

def create_custom_author_view(author_num, min_year, min_length, chosen_threshold):
    # Ok, so...
    # NOTE: the use of 'target_year' in this query is because the source author is set to 
    #       whatever we chose. We're going to get all those texts that have our author as
    #       the source, and all the other criteria apply to the target. This is fine, as
    #       all texts are compared both ways. So, the data and its inverse are in the set.
    query = """
        SELECT calc.threshold,
        SUM(CASE WHEN same_author = 'No' THEN 1 ELSE 0 END) AS No,
        SUM(CASE WHEN same_author = 'False Negative' THEN 1 ELSE 0 END) AS False_Negative,
        SUM(CASE WHEN same_author = 'Yes' THEN 1 ELSE 0 END) AS Yes,
        SUM(CASE WHEN same_author = 'False Positive' THEN 1 ELSE 0 END) AS False_Positive,
        source_length,
        target_length
        FROM combined_jaccard
        JOIN calculations AS calc ON combined_jaccard.pair_id = calc.pair_id
        WHERE combined_jaccard.source_auth = ?
        AND target_year > ?
        AND (source_length >= ? AND target_length >= ?)
        GROUP BY threshold
        HAVING threshold >= ?;
    """
    disk_cur.execute(query, [author_num, min_year, min_length, min_length, chosen_threshold])
    result = disk_cur.fetchall()
    # Convert the query result to a list of dictionaries
    data = [{'threshold': row[0], 'No': row[1], 'False Negative': row[2], 'Yes': row[3], 'False Positive': row[4]} for row in result]

    return data

def assess_auto_author_accuracy(data):
    insert_transaction = "INSERT INTO auto_author_accuracy VALUES (?,?,?,?,?,?,?,?,?,?);"
    disk_cur.executemany(insert_transaction, data)
    disk_con.commit()

def insert_confusion_scores(data):
    insert_transaction = "INSERT INTO confusion_scores VALUES (?,?,?,?,?);"
    disk_cur.execute(insert_transaction, data)
    disk_con.commit()

##
# Helper Functions
##

def optimize():
    disk_cur.execute("PRAGMA analysis_limit=1000;")
    disk_cur.execute("PRAGMA optimize;")

def vacuum_the_db():
    disk_cur.execute("VACUUM;")
    disk_con.commit()


##
# Querying Functions
##

def read_all_pair_id_from_author_pair():
    disk_cur.execute("SELECT pair_id, source_auth, target_auth FROM outcomes;")
    temp_dict = {}
    for item in disk_cur:
        temp_dict[(item['source_auth'], item['target_auth'])] = item['pair_id']
    return temp_dict

def get_length_of_author_predicition_table(author_a, author_b):
    disk_cur.execute("SELECT COUNT(*) FROM author_prediction WHERE source_auth LIKE ? AND target_auth LIKE ?", [author_a, author_b])
    the_length = disk_cur.fetchone()
    return the_length[0]

def read_author_attribution_from_db(author_a, author_b):
    the_predictions = pd.read_sql_query(f'''SELECT source_auth, target_auth, score, source_text, target_text, same_author FROM author_prediction WHERE source_auth LIKE "{author_a}" AND target_auth LIKE "{author_b}"''', disk_con)
    return the_predictions

def read_confusion_scores():
    the_scores = pd.read_sql_query('SELECT * FROM confusion_scores;', disk_con)
    return the_scores

def read_all_thresholds():
    disk_cur.execute("SELECT DISTINCT(threshold) FROM confusion_scores;")
    the_thresholds = disk_cur.fetchall()
    temp_list = []
    for item in the_thresholds:
        temp_list.append(item[0])
    return temp_list

def get_author_and_texts_published_after_current(year):
    year = year + 1 #Ensure we get things starting one year after publication.
    query = """SELECT 
        DISTINCT(author_id), 
        author_name,
        dirs.dir
        FROM authors 
        JOIN all_texts ON all_texts.year >= ?
        JOIN dirs ON dirs.id = all_texts.dir
        WHERE all_texts.author_id = authors.id;
    """
    disk_cur.execute(query, [year])
    the_texts_and_authors = disk_cur.fetchall()
    temp_dict = {}
    for item in the_texts_and_authors:
        temp_dict[item[0]] = [item[1], item[2]]
    return temp_dict

def get_min_year_of_author_publication(id):
    query = """SELECT
        MIN(year)
        FROM all_texts
        WHERE all_texts.author_id = ?
    """
    disk_cur.execute(query, [id])
    the_year = disk_cur.fetchone()
    return the_year[0]

def close_db_connection():
    """Close the SQLite database connection."""
    try:
        if disk_con:
            disk_con.close()
            print("Database connection closed.")
    except Exception as e:
        print(f"Error while closing the database connection: {str(e)}")