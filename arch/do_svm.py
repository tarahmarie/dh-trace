# This version makes author models, not models of novels

import itertools
import os
import re
import sqlite3
import unicodedata
import xml.etree.ElementTree as ET

import numpy as np
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.svm import SVC
from tqdm import tqdm

from hapaxes_1tM import remove_tei_lines_from_text
from util import get_project_name, getListOfFiles

project_name = get_project_name()

raw_data = []
chapter_labels = []
authors = []
novels = []
chap_nums = []
chapters = []

# Feature extraction
vectorizer = TfidfVectorizer()
svm = SVC()

### Database-Related:
connection = sqlite3.connect(f'./projects/{project_name}/db/svm.db')
cursor = connection.cursor()

def prepare_the_db():
    cursor.execute("DROP TABLE IF EXISTS predictions;")
    cursor.execute("DROP TABLE IF EXISTS chapter_assessments;")
    cursor.execute("DROP TABLE IF EXISTS test_set_preds;")
    cursor.execute("CREATE TABLE IF NOT EXISTS predictions (author1, novel1, chapter1, author2, novel2, chapter2, outcome, conf_is_auth1, conf_is_auth2);")
    cursor.execute("CREATE TABLE IF NOT EXISTS chapter_assessments (novel, number);")
    cursor.execute("CREATE TABLE IF NOT EXISTS test_set_preds (file);")

def close_db_connection():
    """Close the SQLite database connection."""
    try:
        if connection:
            connection.close()
            print("Database connection closed.")
    except Exception as e:
        print(f"Error while closing the database connection: {str(e)}")

def insert_predictions_data(data):
    query = "INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?)"
    cursor.executemany(query, data)
    connection.commit()

def insert_chapter_data(data):
    num_columns = len(data[0])
    placeholders = ','.join(['?'] * num_columns)
    query = f"INSERT INTO chapter_assessments VALUES ({placeholders})"
    cursor.executemany(query, data)
    connection.commit()

def insert_test_set_data(data):
    num_columns = len(data[0])
    placeholders = ','.join(['?'] * num_columns)
    query = f"INSERT INTO test_set_preds VALUES ({placeholders})"
    cursor.executemany(query, data)
    connection.commit()

def update_the_chapters_table(column_names):
    for name in sorted(set(column_names)):
        name = name.replace(' ', '_')
        # Yes, I'm letting hyphens happen.  Because YOLO.
        cursor.execute(f"ALTER TABLE chapter_assessments ADD COLUMN `{name}`;")
        cursor.execute(f"ALTER TABLE test_set_preds ADD COLUMN `{name}`;")
        connection.commit()

### Utility Functions
def remove_combining_characters(text):
    return ''.join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c))

def extract_author_name(xml_body):
    author_pattern = re.compile(r'<author>([^,]+)', re.IGNORECASE | re.DOTALL)
    match_author = author_pattern.search(xml_body)
    
    if match_author:
        author = match_author.group(1).strip()
        # Remove additional information such as birth and death years
        author = re.sub(r'\s*\([\s\d-]*\)', '', author)
    else:
        author = "Unknown Author"
    
    author = author.replace('-', '_')
    
    return author

def process_raw_files():
    all_files = getListOfFiles(f'./projects/{project_name}/splits')
    for i, file in enumerate(all_files):
        with open(file, 'r') as f:
            body = f.read()

            # Extract author and title using regular expressions
            author = remove_combining_characters(extract_author_name(body))

            title = file.split('/')[4].split('-')[1]

            text = preprocess_text(body)

            chapter_num = file.split('_')[-1]

            raw_data.append((author, title, chapter_num, text))

def build_lists():
    # Sometimes, our old friend Eltec doesn't parse right. So, we check.
    authors = [item[0] for item in raw_data]
    novels = [item[1] for item in raw_data]
    chap_nums = [item[2] for item in raw_data]
    chapters = [item[3] for item in raw_data]

    return authors, novels, chap_nums, chapters

def prepare_labels():
    # Assign labels to chapters
    for i in range(len(chapters)):
        author_name = authors[i].split("_")[0]  # Extract the author's name without any additional information
        chapter_labels.append(author_name)

def preprocess_text(text):
    text = remove_tei_lines_from_text(text)
    text = text.lower()
    text = re.sub(r"[^a-zA-Z0-9\s\u00C0-\u00FF]", "", text)
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words("english"))
    tokens = [token for token in tokens if token not in stop_words]
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(token) for token in tokens]
    processed_text = " ".join(tokens)
    return processed_text

def prepare_chapter_data(column_names, outcomes_dict):
    chapter_transactions = []
    pbar = tqdm(desc='Preparing Chapter Data: ', total=(len(outcomes_dict.items())), colour="#a361f3", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for key, value in outcomes_dict.items():
        novel = key.split('-')[0]
        novel = unicodedata.normalize('NFKD', novel)
        chap_num = key.split('-')[1]
        temp_transaction_tuple = (novel, chap_num)
        for author, score in sorted(value.items()):
            temp_transaction_tuple = temp_transaction_tuple + (score,)
        chapter_transactions.append(temp_transaction_tuple)
        pbar.update(1)
    update_the_chapters_table(column_names)
    insert_chapter_data(chapter_transactions)
    pbar.close()

### SVM stuff
def test_model():
    global svm
    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, chapter_labels, test_size=0.30, random_state=42)

    # Train the SVM classifier
    svm.fit(X_train, y_train)

    # Evaluate the classifier
    y_pred = svm.predict(X_test)
    accuracy = svm.score(X_test, y_test)
    print("Accuracy:", accuracy)

    # Generate classification report
    report = classification_report(y_test, y_pred)
    print("Classification Report:")
    print(report)

def assess_authorship_likelihood():
    global svm
    outcomes_dict = {}
    column_names = []  # Move the column_names list creation here

    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, authors, test_size=0.2, random_state=42, stratify=authors)
    svm.fit(X_train, y_train)

    pbar = tqdm(desc='Computing Authorship Likelihood: ', total=(len(chapters)), colour="#FB3FA8", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for author, novel, chapter, chap_num in zip(authors, novels, chapters, chap_nums):
        # Compute the likelihood scores for each author
        vectorized_text = vectorizer.transform([chapter])
        likelihood_scores = svm.decision_function(vectorized_text)[0]  # Access the scores as a 1D array

        # Normalize likelihood scores to [0, 1] range
        scaler = MinMaxScaler()
        likelihood_scores = scaler.fit_transform(likelihood_scores.reshape(-1, 1)).flatten()

        # Store the outcome in the dictionary
        outcome = {author: score for author, score in zip(svm.classes_, likelihood_scores)}
        outcomes_dict[f"{novel}-{chap_num}"] = outcome
        # Update column_names list
        column_names.extend(outcome.keys())

        pbar.update(1)
    pbar.close()

    return outcomes_dict, column_names


def unseen_test():
    global svm

    sanity_check = False
    print("\nNOTE: If you're going to want to use SVM prediction scores for the splits (as in our visualizations), you'll want to say yes at least once here...\n")
    do_sanity_check = input("\nSo, would you like to re-use the training set for testing? (y/n) ")
    if do_sanity_check.lower() == 'y':
        sanity_check = True

    # Prepare previously unseen chapters
    if sanity_check:
        unseen_files = getListOfFiles(f'./projects/{project_name}/splits')
    else:
        unseen_files = getListOfFiles(f'./projects/{project_name}/testset')
    unseen_chapters = []

    for file_path in unseen_files:
        with open(file_path, 'r') as file:
            body = file.read()
            text = preprocess_text(body)
            unseen_chapters.append(text)

    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, chapter_labels, test_size=0.2, random_state=42, stratify=chapter_labels)
    # Train the SVM classifier
    svm.fit(X_train, y_train)

    # Extract features from the unseen chapters
    unseen_features = vectorizer.transform(unseen_chapters)
    unseen_predictions = svm.predict(unseen_features)
    confidence_scores = svm.decision_function(unseen_features)

    # Normalize confidence scores to [0, 1] range
    scaler = MinMaxScaler()
    confidence_scores = scaler.fit_transform(confidence_scores)

    # Compute basic statistics of confidence scores
    min_score = np.min(confidence_scores)
    max_score = np.max(confidence_scores)
    mean_score = np.mean(confidence_scores)
    std_score = np.std(confidence_scores)

    print("\n")
    print("Range of scores:", min_score, "to", max_score)
    print("Mean score:", mean_score)
    print("Standard deviation of scores:", std_score)
    print("\n")

    test_transactions = []
    pbar = tqdm(desc='Computing Scores for Unseen: ', total=(len(unseen_chapters)), colour="#ff6666", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    
    for file_path, prediction, conf_scores in zip(unseen_files, unseen_predictions, confidence_scores):
        better_filename = file_path.split('/')[5]
        test_transaction = (better_filename,)

        for author, score in zip(svm.classes_, conf_scores):
            test_transaction = test_transaction + (score,)

        test_transactions.append(test_transaction)
        pbar.update(1)
    
    pbar.close()
    insert_test_set_data(test_transactions)

   
def generate_prediction_data():
    global svm
    prediction_data = []
    # Extract features from the unseen chapters
    seen_features = vectorizer.transform(chapters)

    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, chapter_labels, test_size=0.2, random_state=42)
    # Train the SVM classifier
    svm.fit(X_train, y_train) #type: ignore

    # Predict authors for unseen chapters
    seen_predictions = svm.predict(seen_features) #type: ignore
    confidence_scores = svm.decision_function(seen_features) #type: ignore

    # Normalize confidence scores to [0, 1] range
    scaler = MinMaxScaler()
    confidence_scores = scaler.fit_transform(confidence_scores)

    # Calculate total_iterations directly
    total_iterations = len(raw_data) * (len(raw_data) - 1) // 2

    # Create a dictionary to map authors to indices
    author_indices = {author: index for index, author in enumerate(unique_labels)}

    # Iterate through all pairs of chapters
    pbar = tqdm(desc='Computing Chapter Pair Predictions: ', total=total_iterations, colour="#ffaf87", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for (author1, novel1, chapnum1, chapter1), (author2, novel2, chapnum2, chapter2) in itertools.combinations(raw_data, 2):
        # Get the corresponding predictions
        prediction1 = seen_predictions[authors.index(author1)]
        prediction2 = seen_predictions[authors.index(author2)]
        confidence1 = confidence_scores[authors.index(author1)]
        confidence2 = confidence_scores[authors.index(author2)]

        # Compare the predictions
        if prediction1 == prediction2:
            outcome = "Y"
        else:
            outcome = "N"

        author1_index = author_indices[author1]
        author2_index = author_indices[author2]
        score1 = confidence1[author1_index]
        score2 = confidence2[author2_index]

        # Append the tuple to the list
        prediction_data.append([author1, novel1, chapnum1, author2, novel2, chapnum2, outcome, score1, score2])


        pbar.update(1)
    pbar.close()
    insert_predictions_data(prediction_data)

### Kickoff
def build_the_thing():
    print("Preparing the db...")
    prepare_the_db()
    
    print("\nTesting the model before proceeding...\n")
    test_model()

    outcomes_dict, column_names = assess_authorship_likelihood()
    prepare_chapter_data(column_names, outcomes_dict)
    generate_prediction_data()

    print("Now, testing the unseen texts...\n")
    unseen_test()

### Ensure directory structure needed for this project
def make_directories_if_needed_and_warn():
    exit_when_complete = False
    if not os.path.exists(f'./projects/{project_name}/testset'):
        os.makedirs(f'./projects/{project_name}/testset')
        exit_when_complete = True
        print("\nI've just created the directory 'testset'. Make sure it has unseen/target texts for the model to use before running again!")
    if(exit_when_complete):
        exit()

if __name__ == "__main__":
    # Let's ensure the directories are in-place
    make_directories_if_needed_and_warn()

    print("\nLoading raw files...\n")
    process_raw_files()
    authors, novels, chap_nums, chapters = build_lists()
    prepare_labels()
    unique_labels = np.unique(authors)

    X = vectorizer.fit_transform(chapters)

    build_the_thing()

    close_db_connection()
