import gc
import sqlite3

import pandas as pd
from tqdm import tqdm

from util import get_project_name

#Create the disk database (for backups) and a cursor to handle transactions.
project_name = get_project_name()
disk_con = sqlite3.connect(f"./projects/{project_name}/db/viz.db")
disk_con.row_factory = sqlite3.Row
disk_cur = disk_con.cursor()

#  __  __      __
#  \ \/ /___  / /___
#   \  / __ \/ / __ \
#   / / /_/ / / /_/ /
#  /_/\____/_/\____/
#

disk_cur.execute("PRAGMA synchronous = OFF;")
disk_cur.execute("PRAGMA cache_size = 1000000;")
disk_cur.execute("PRAGMA journal_mode = WAL;")
disk_cur.execute("PRAGMA temp_store = WAL;")

# Attach the source database
attach_query = "ATTACH DATABASE ? AS source_db"
disk_cur.execute(attach_query, (f'./projects/{project_name}/db/{project_name}.db',))

##
# Utility functions
##

def setup_text_stats_table():
    disk_cur.execute('DROP TABLE IF EXISTS text_stats;')

    text_stats_query = """
        CREATE TABLE IF NOT EXISTS text_stats AS 
        SELECT DISTINCT
            sat.text_id,
            sat.length,
            ssa.source_year
        FROM source_db.all_texts AS sat
        JOIN source_db.stats_all AS ssa ON sat.text_id = ssa.source_text;
    """

    disk_cur.execute(text_stats_query)
    disk_con.commit()

def setup_viz_ops_db():
    disk_cur.execute("DROP TABLE IF EXISTS giant_combined_calcs;")
    disk_cur.execute("DROP INDEX IF EXISTS gcc_thresh_index;")

    giant_combined_calcs_query = """
        CREATE TABLE IF NOT EXISTS giant_combined_calcs AS
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
        FROM source_db.combined_jaccard AS ocj
        JOIN source_db.calculations AS calc ON ocj.pair_id = calc.pair_id
        JOIN source_db.all_texts AS source_lengths ON ocj.source_text = source_lengths.text_id
        JOIN source_db.all_texts AS target_lengths ON ocj.target_text = target_lengths.text_id;
    """

    disk_cur.execute(giant_combined_calcs_query)
    disk_cur.execute("CREATE INDEX IF NOT EXISTS gcc_thresh_index ON giant_combined_calcs(threshold);")
    disk_con.commit()

    # Add the new columns to the table
    alter_table_query = """
        ALTER TABLE giant_combined_calcs
        ADD COLUMN source_year INT;
        ALTER TABLE giant_combined_calcs
        ADD COLUMN target_year INT;
    """
    disk_cur.executescript(alter_table_query)

    # Update the source_year and target_year columns using a JOIN with text_stats
    update_source_year_query = """
        UPDATE giant_combined_calcs
        SET source_year = ts.source_year
        FROM text_stats AS ts
        WHERE giant_combined_calcs.source_text = ts.text_id;
    """
    disk_cur.execute(update_source_year_query)

    update_target_year_query = """
        UPDATE giant_combined_calcs
        SET target_year = ts.source_year
        FROM text_stats AS ts
        WHERE giant_combined_calcs.target_text = ts.text_id;
    """
    disk_cur.execute(update_target_year_query)

    disk_con.commit()

def get_gcc_length():
    disk_cur.execute("SELECT COUNT(*) FROM giant_combined_calcs;")
    result = disk_cur.fetchone()  # Fetch the first row of the result
    return result[0]

def do_some_sums(chosen_threshold):
# Query and process the data directly using SQL
    query = """
        SELECT threshold,
            SUM(CASE WHEN same_author = 'No' THEN 1 ELSE 0 END) AS No,
            SUM(CASE WHEN same_author = 'False Negative' THEN 1 ELSE 0 END) AS False_Negative,
            SUM(CASE WHEN same_author = 'Yes' THEN 1 ELSE 0 END) AS Yes,
            SUM(CASE WHEN same_author = 'False Positive' THEN 1 ELSE 0 END) AS False_Positive
        FROM giant_combined_calcs
        GROUP BY threshold
        HAVING threshold >= ?;
    """
    disk_cur.execute(query, [chosen_threshold])
    result = disk_cur.fetchall()

    # Convert the query result to a list of dictionaries
    data = [{'threshold': row[0], 'No': row[1], 'False Negative': row[2], 'Yes': row[3], 'False Positive': row[4]} for row in result]

    return data

