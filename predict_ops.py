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
disk_cur.execute("PRAGMA cache_size = 100000;")
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
    disk_cur.execute("DROP INDEX IF EXISTS calculations_author_pair_index;")
    disk_cur.execute("DROP INDEX IF EXISTS combined_jaccard_pair_id_index;")
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

    disk_cur.execute(pair_counts_query)
    disk_cur.execute(calculations_query)
    disk_cur.execute(weights_query)
    disk_con.commit()

def setup_auto_indices():
    #Look, all this stuff is slow.  But it will make the plot script so much faster, so...
    disk_cur.execute("CREATE INDEX calculations_author_pair_index ON calculations(author_pair);")
    disk_cur.execute("CREATE INDEX combined_jaccard_pair_id_index ON combined_jaccard(pair_id);")
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

def create_author_view(author_pair):
    query = """
        SELECT 
        ocj.source_auth,
        ocj.target_auth,
        ocj.source_text,
        ocj.target_text,
        calc.comp_score,
        calc.same_author,
        calc.threshold,
        calc.weight_id
        FROM combined_jaccard AS ocj
        JOIN calculations AS calc ON ocj.pair_id = calc.pair_id
        WHERE calc.author_pair = ? 
    """
    weights_dict = get_all_weights()
    params = [author_pair]
    the_predictions = pd.read_sql_query(query, disk_con, params=params)
    the_predictions.columns = ['source_auth', 'target_auth', 'source_text', 'target_text', 'comp_score', 'same_author', 'threshold', 'weight_id']
    the_predictions['hap_weight'] = the_predictions['weight_id'].apply(lambda x: weights_dict.get(x, ())[0]) #type: ignore
    the_predictions['al_weight'] = the_predictions['weight_id'].apply(lambda x: weights_dict.get(x, ())[1]) #type: ignore

    return the_predictions

def assess_auto_author_accuracy(data):
    insert_transaction = "INSERT INTO auto_author_accuracy VALUES (?,?,?,?,?,?,?,?,?,?);"
    disk_cur.executemany(insert_transaction, data)
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

def close_db_connection():
    """Close the SQLite database connection."""
    try:
        if disk_con:
            disk_con.close()
            print("Database connection closed.")
    except Exception as e:
        print(f"Error while closing the database connection: {str(e)}")