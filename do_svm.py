import itertools
import os
import re
import sqlite3

import numpy as np
from joblib import dump, load
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
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
        cursor.execute(f"ALTER TABLE chapter_assessments ADD COLUMN {name};")
        cursor.execute(f"ALTER TABLE test_set_preds ADD COLUMN {name};")
        connection.commit()

### Utility Functions
def process_raw_files():
    all_files = getListOfFiles(f'./projects/{project_name}/splits_for_svm')
    for i, file in enumerate(all_files):
        with open(file, 'r') as f:
            body = f.read()

            if "—" in file:
                author = file.split('/')[4].split('—')[-1] #Because Eltec uses em dash. ffs.
                title = file.split('/')[4].split('—')[0].split('-')[1]
            else:   
                author = body.split('<author>')[1]
                author = author.split('</')[0]
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
    #Strip TEI
    text = remove_tei_lines_from_text(text)

    # Convert text to lowercase
    text = text.lower()

    # Remove special characters and punctuation
    text = re.sub(r"[^a-zA-Z0-9\s]", "", text)

    # Tokenize the text into individual words
    tokens = word_tokenize(text)

    # Remove stopwords
    stop_words = set(stopwords.words("english"))
    tokens = [token for token in tokens if token not in stop_words]

    # Lemmatize the words
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(token) for token in tokens]

    # Join the tokens back into a single string
    processed_text = " ".join(tokens)

    return processed_text

def prepare_chapter_data(column_names, outcomes_dict):
    chapter_transactions = []
    pbar = tqdm(desc='Preparing Chapter Data: ', total=(len(outcomes_dict.items())), colour="#a361f3", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for key, value in outcomes_dict.items():
        novel = key.split('-')[0]
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

    if os.path.exists(f'./projects/{project_name}/models/authors.joblib'):
        svm = load(f'./projects/{project_name}/models/authors.joblib')
    else:
        # Split the dataset into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, authors, test_size=0.2, random_state=42)
        svm.fit(X_train, y_train)

    pbar = tqdm(desc='Computing Authorship Likelihood: ', total=(len(chapters)), colour="#FB3FA8", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]')
    for author, novel, chapter, chap_num in zip(authors, novels, chapters, chap_nums):
        # Compute the likelihood scores for each author
        vectorized_text = vectorizer.transform([chapter])
        likelihood_scores = svm.decision_function(vectorized_text)[0]  # Access the scores as a 1D array

        # Store the outcome in the dictionary
        outcome = {author: score for author, score in zip(svm.classes_, likelihood_scores)}
        outcomes_dict[f"{novel}-{chap_num}"] = outcome

        # Update column_names list
        column_names.extend(outcome.keys())

        pbar.update(1)
    pbar.close()

    dump(svm, f'./projects/{project_name}/models/authors.joblib')
    return outcomes_dict, column_names  # Return column_names from the function


def unseen_test():
    global svm
    #TODO: Create an actual unseen set.
    # Allow diagnostics by using the training texts as the unseen texts.abs
    sanity_check = False
    do_sanity_check = input("\nWould you like to re-use the training set for testing? (y/n) ")
    match do_sanity_check:
        case 'y' | 'Y':
            sanity_check = True
        case 'n' | 'N':
            sanity_check = False

    # Prepare previously unseen chapters
    if sanity_check:
        unseen_files = getListOfFiles(f'./projects/{project_name}/splits_for_svm')
    else:
        unseen_files = getListOfFiles(f'./projects/{project_name}/testset')
    unseen_chapters = []
    
    for file_path in unseen_files:
        with open(file_path, 'r') as file:
            body = file.read()
            text = preprocess_text(body)
            unseen_chapters.append(text)
    
    if os.path.exists(f'./projects/{project_name}/models/seen.joblib'):
        svm = load(f'./projects/{project_name}/models/seen.joblib')
    else:
        # Split the dataset into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, chapter_labels, test_size=0.2, random_state=42)
        # Train the SVM classifier
        svm.fit(X_train, y_train)

    # Extract features from the seen chapters
    seen_features = vectorizer.transform(chapters)
    # Predict authors for unseen chapters
    seen_predictions = svm.predict(seen_features)
    confidence_scores = svm.decision_function(seen_features)

    # Extract features from the unseen chapters
    unseen_features = vectorizer.transform(unseen_chapters)

    # Predict authors for unseen chapters
    unseen_predictions = svm.predict(unseen_features)
    confidence_scores = svm.decision_function(unseen_features)

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
    for file_path, prediction, confidence in zip(unseen_files, unseen_predictions, confidence_scores):
        better_filename = file_path.split('/')[5]
        test_transaction = (better_filename,)

        for author, score in zip(svm.classes_, confidence):
            test_transaction = test_transaction + (score,)
        
        test_transactions.append((test_transaction))
        pbar.update(1)
    pbar.close()
    insert_test_set_data(test_transactions)
   
def generate_prediction_data():
    global svm
    prediction_data = []
    # Extract features from the unseen chapters
    seen_features = vectorizer.transform(chapters)

    # Did we already make the model?  Load it.
    if os.path.exists(f'./projects/{project_name}/models/seen.joblib'):
        svm = load(f'./projects/{project_name}/models/seen.joblib')
    else:
        # Split the dataset into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, chapter_labels, test_size=0.2, random_state=42)

        # Train the SVM classifier
        svm.fit(X_train, y_train) #type: ignore

    # Predict authors for unseen chapters
    seen_predictions = svm.predict(seen_features) #type: ignore
    confidence_scores = svm.decision_function(seen_features) #type: ignore

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
def rebuild_the_thing():
    print("Continuing...")
    prepare_the_db()
    
    print("\nTesting the model before proceeding...\n")
    test_model()

    outcomes_dict, column_names = assess_authorship_likelihood()
    prepare_chapter_data(column_names, outcomes_dict)
    generate_prediction_data()

    print("\nSaving the model for next time...\n")
    dump(svm, f'./projects/{project_name}/models/seen.joblib')
    print("Now, testing the unseen texts...\n")
    unseen_test()

### Ensure directory structure needed for this project
def make_directories_if_needed_and_warn():
    exit_when_complete = False
    if not os.path.exists(f'./projects/{project_name}/db'):
        os.makedirs(f'./projects/{project_name}/db')
    if not os.path.exists(f'./projects/{project_name}/models'):
        os.makedirs(f'./projects/{project_name}/models')
    if not os.path.exists(f'./projects/{project_name}/splits_for_svm'):
        os.makedirs(f'./projects/{project_name}/splits_for_svm')
        exit_when_complete = True
        print("\nI've just created the directory 'splits_for_svm'. Make sure it has training splits in it before running again!")
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

    # Check if the file exists
    #BUG: There's a bug here.  DB gets opened, so this is always true.
    if os.path.exists(f'./projects/{project_name}/db/svm.db'):
        print("svm.db exists.")
        # Ask the user if they want to continue
        user_input = input("Do you want to rebuild it ('n' moves on to testing)? (y/n): ")
        if user_input.lower() == "y":
            rebuild_the_thing()
        else:
            unseen_test()
    else:
        rebuild_the_thing()

    close_db_connection()
