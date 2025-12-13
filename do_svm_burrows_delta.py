"""
do_svm_burrows_delta.py

Drop-in replacement for do_svm.py that uses Burrows' Delta instead of TF-IDF vectorization.
Burrows' Delta computes z-scores of word frequencies against corpus-wide means and standard
deviations, which is the classic stylometric approach for authorship attribution.
"""

import itertools
import nltk
import os
import re
import sqlite3
import unicodedata
from collections import Counter
from multiprocessing import Pool, cpu_count

import numpy as np
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.svm import SVC
from tqdm import tqdm

from hapaxes_1tM import remove_tei_lines_from_text
from util import get_project_name, getListOfFiles


def ensure_nltk_data():
    resources = ['punkt_tab', 'stopwords', 'wordnet']
    for resource in resources:
        try:
            nltk.download(resource, quiet=True)
        except Exception as e:
            print(f"Warning: Could not download {resource}: {e}")


ensure_nltk_data()

# Cache stopwords and lemmatizer (expensive to create repeatedly)
STOP_WORDS = set(stopwords.words("english"))
LEMMATIZER = WordNetLemmatizer()


### Utility Functions
def remove_combining_characters(text):
    return ''.join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c))


def extract_author_name(xml_body):
    author_pattern = re.compile(r'<author>([^,]+)', re.IGNORECASE | re.DOTALL)
    match_author = author_pattern.search(xml_body)

    if match_author:
        author = match_author.group(1).strip()
        author = re.sub(r'\s*\([\s\d-]*\)', '', author)
    else:
        author = "Unknown Author"

    author = author.replace('-', '_')
    return author


def preprocess_text(text):
    """Preprocess a single text document."""
    text = remove_tei_lines_from_text(text)
    text = text.lower()
    text = re.sub(r"[^a-zA-Z0-9\s\u00C0-\u00FF]", "", text)
    tokens = word_tokenize(text)
    tokens = [token for token in tokens if token not in STOP_WORDS]
    tokens = [LEMMATIZER.lemmatize(token) for token in tokens]
    return " ".join(tokens)


def process_single_file(file_path):
    """Worker function to process a single file."""
    with open(file_path, 'r') as f:
        body = f.read()

    author = remove_combining_characters(extract_author_name(body))
    title = file_path.split('/')[4].split('-')[1]
    text = preprocess_text(body)
    chapter_num = file_path.split('_')[-1]

    return (author, title, chapter_num, text)


def process_unseen_file(file_path):
    """Worker function to process a single unseen file."""
    with open(file_path, 'r') as f:
        body = f.read()
    return preprocess_text(body)


class BurrowsDeltaVectorizer:
    """
    Implements Burrows' Delta stylometric method.
    
    Instead of TF-IDF, this computes z-scores of word frequencies:
    1. Select the N most frequent words (MFW) across the corpus
    2. For each text, compute relative frequencies of these words
    3. Compute corpus-wide mean and std for each word
    4. Transform each text's frequencies to z-scores: (freq - mean) / std
    
    The resulting feature vectors can be used with any classifier (SVM, etc.)
    or for computing Delta distances directly.
    """

    def __init__(self, n_features=500, use_lowercase=True):
        """
        Args:
            n_features: Number of most frequent words to use (default 500, 
                        Burrows originally used 150, but 500-1000 often works better)
            use_lowercase: Whether to lowercase tokens (usually already done in preprocessing)
        """
        self.n_features = n_features
        self.use_lowercase = use_lowercase
        self.vocabulary_ = None
        self.feature_names_ = None
        self.corpus_means_ = None
        self.corpus_stds_ = None
        self._is_fitted = False

    def _tokenize(self, text):
        """Simple whitespace tokenization (text should already be preprocessed)."""
        tokens = text.split()
        if self.use_lowercase:
            tokens = [t.lower() for t in tokens]
        return tokens

    def _compute_relative_frequencies(self, tokens):
        """Compute relative frequencies (proportions) for each token."""
        counts = Counter(tokens)
        total = sum(counts.values())
        if total == 0:
            return {}
        return {word: count / total for word, count in counts.items()}

    def fit(self, texts):
        """
        Fit the vectorizer on a corpus of texts.
        
        1. Count all words across corpus to find MFW
        2. Compute relative frequencies for each text
        3. Compute corpus-wide mean and std for each MFW
        """
        # Step 1: Find most frequent words across entire corpus
        global_counts = Counter()
        all_tokens = []
        
        for text in texts:
            tokens = self._tokenize(text)
            all_tokens.append(tokens)
            global_counts.update(tokens)

        # Select top N most frequent words
        most_common = global_counts.most_common(self.n_features)
        self.vocabulary_ = {word: idx for idx, (word, _) in enumerate(most_common)}
        self.feature_names_ = [word for word, _ in most_common]

        # Step 2: Compute relative frequencies for each text
        n_texts = len(texts)
        n_features = len(self.feature_names_)
        freq_matrix = np.zeros((n_texts, n_features))

        for i, tokens in enumerate(all_tokens):
            rel_freqs = self._compute_relative_frequencies(tokens)
            for word, idx in self.vocabulary_.items():
                freq_matrix[i, idx] = rel_freqs.get(word, 0.0)

        # Step 3: Compute corpus-wide mean and std for each word
        self.corpus_means_ = np.mean(freq_matrix, axis=0)
        self.corpus_stds_ = np.std(freq_matrix, axis=0)
        
        # Avoid division by zero - replace zero stds with small value
        self.corpus_stds_[self.corpus_stds_ == 0] = 1e-10

        self._is_fitted = True
        return self

    def transform(self, texts):
        """
        Transform texts to z-score feature vectors using fitted parameters.
        
        For each text and each word in vocabulary:
        z_score = (relative_frequency - corpus_mean) / corpus_std
        """
        if not self._is_fitted:
            raise ValueError("Vectorizer must be fitted before transform")

        n_texts = len(texts) if hasattr(texts, '__len__') else 1
        if isinstance(texts, str):
            texts = [texts]
            
        n_features = len(self.feature_names_)
        z_score_matrix = np.zeros((n_texts, n_features))

        for i, text in enumerate(texts):
            tokens = self._tokenize(text)
            rel_freqs = self._compute_relative_frequencies(tokens)
            
            for word, idx in self.vocabulary_.items():
                freq = rel_freqs.get(word, 0.0)
                z_score_matrix[i, idx] = (freq - self.corpus_means_[idx]) / self.corpus_stds_[idx]

        return z_score_matrix

    def fit_transform(self, texts):
        """Fit and transform in one step."""
        self.fit(texts)
        return self.transform(texts)

    def get_feature_names_out(self):
        """Return feature names (most frequent words)."""
        if self.feature_names_ is None:
            raise ValueError("Vectorizer must be fitted first")
        return np.array(self.feature_names_)


class AuthorshipAnalyzer:
    """Encapsulates the authorship analysis workflow using Burrows' Delta."""

    def __init__(self, project_name, n_mfw=500):
        """
        Args:
            project_name: Name of the project directory
            n_mfw: Number of most frequent words for Burrows' Delta (default 500)
        """
        self.project_name = project_name
        self.db_path = f'./projects/{project_name}/db/svm.db'
        self.connection = None
        self.cursor = None

        # Data containers
        self.raw_data = []
        self.authors = []
        self.novels = []
        self.chap_nums = []
        self.chapters = []
        self.chapter_labels = []

        # ML components - using Burrows' Delta instead of TF-IDF
        self.vectorizer = BurrowsDeltaVectorizer(n_features=n_mfw)
        self.svm = SVC(kernel="linear")
        self.X = None

        # Multiprocessing settings
        self.num_workers = cpu_count()

    ### Database Methods
    def connect_db(self):
        """Create database connection."""
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()

    def close_db_connection(self):
        """Close the SQLite database connection."""
        try:
            if self.connection:
                self.connection.close()
                print("Database connection closed.")
        except Exception as e:
            print(f"Error while closing the database connection: {str(e)}")

    def prepare_the_db(self):
        """Initialize database tables."""
        self.cursor.execute("DROP TABLE IF EXISTS chapter_assessments;")
        self.cursor.execute("DROP TABLE IF EXISTS test_set_preds;")
        self.cursor.execute("DROP TABLE IF EXISTS svm_coefficients;")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS chapter_assessments (novel TEXT, number TEXT);")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS test_set_preds (file TEXT);")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS svm_coefficients (feature_name TEXT, coefficient_value REAL);")
        self.connection.commit()

    def insert_chapter_data(self, data):
        """Batch insert chapter assessment data."""
        if not data:
            return
        num_columns = len(data[0])
        placeholders = ','.join(['?'] * num_columns)
        query = f"INSERT INTO chapter_assessments VALUES ({placeholders})"
        self.cursor.executemany(query, data)
        self.connection.commit()

    def insert_test_set_data(self, data):
        """Batch insert test set data."""
        if not data:
            return
        num_columns = len(data[0])
        placeholders = ','.join(['?'] * num_columns)
        query = f"INSERT INTO test_set_preds VALUES ({placeholders})"
        self.cursor.executemany(query, data)
        self.connection.commit()

    def update_the_chapters_table(self, column_names):
        """Add author columns to assessment tables."""
        unique_names = sorted(set(column_names))
        for name in unique_names:
            safe_name = name.replace(' ', '_')
            self.cursor.execute(f"ALTER TABLE chapter_assessments ADD COLUMN `{safe_name}` REAL;")
            self.cursor.execute(f"ALTER TABLE test_set_preds ADD COLUMN `{safe_name}` REAL;")
        self.connection.commit()

    def insert_coefficients_data(self, feature_names, coefficients):
        """Batch insert SVM coefficients."""
        try:
            data = list(zip(feature_names, coefficients))
            query = "INSERT INTO svm_coefficients (feature_name, coefficient_value) VALUES (?, ?)"
            self.cursor.executemany(query, data)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Error occurred: {e}")

    ### Data Processing Methods
    def process_raw_files(self):
        """Process all raw files using multiprocessing."""
        all_files = getListOfFiles(f'./projects/{self.project_name}/splits')
        chunksize = max(1, len(all_files) // (self.num_workers * 4))

        with Pool(processes=self.num_workers) as pool:
            pbar = tqdm(
                desc='Loading and preprocessing files',
                total=len(all_files),
                colour="#00875f",
                bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
            )

            for result in pool.imap_unordered(process_single_file, all_files, chunksize=chunksize):
                self.raw_data.append(result)
                pbar.update(1)

            pbar.close()

    def build_lists(self):
        """Extract data into separate lists."""
        self.authors = [item[0] for item in self.raw_data]
        self.novels = [item[1] for item in self.raw_data]
        self.chap_nums = [item[2] for item in self.raw_data]
        self.chapters = [item[3] for item in self.raw_data]

    def prepare_labels(self):
        """Assign labels to chapters."""
        self.chapter_labels = list(self.novels)  # Use novels as labels

    def prepare_features(self):
        """Vectorize the chapter texts using Burrows' Delta z-scores."""
        self.X = self.vectorizer.fit_transform(self.chapters)

    ### SVM Methods
    def test_model(self):
        """Test the SVM model and print accuracy."""
        X_train, X_test, y_train, y_test = train_test_split(
            self.X, self.chapter_labels, test_size=0.30, random_state=42
        )

        self.svm.fit(X_train, y_train)

        y_pred = self.svm.predict(X_test)
        accuracy = self.svm.score(X_test, y_test)
        print("Accuracy:", accuracy)

        report = classification_report(y_test, y_pred)
        print("Classification Report:")
        print(report)

    def assess_authorship_likelihood(self):
        """Compute authorship likelihood for all chapters."""
        X_train, X_test, y_train, y_test = train_test_split(
            self.X, self.novels, test_size=0.2, random_state=42, stratify=self.novels
        )
        self.svm.fit(X_train, y_train)

        # Transform all chapters using already-fitted vectorizer
        all_scores = self.svm.decision_function(self.X)

        # Normalize all scores at once
        scaler = MinMaxScaler()
        all_scores_normalized = scaler.fit_transform(all_scores)

        outcomes_dict = {}
        column_names = list(self.svm.classes_)

        pbar = tqdm(
            desc='Computing Authorship Likelihood',
            total=len(self.chapters),
            colour="#FB3FA8",
            bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
        )

        for i, (author, novel, chapter, chap_num) in enumerate(
                zip(self.authors, self.novels, self.chapters, self.chap_nums)
        ):
            outcome = {novel: score for novel, score in zip(self.svm.classes_, all_scores_normalized[i])}
            outcomes_dict[f"{novel}-{chap_num}"] = outcome
            pbar.update(1)

        pbar.close()
        return outcomes_dict, column_names

    def prepare_chapter_data(self, column_names, outcomes_dict):
        """Prepare and insert chapter assessment data."""
        chapter_transactions = []
        sorted_columns = sorted(set(column_names))

        pbar = tqdm(
            desc='Preparing Chapter Data',
            total=len(outcomes_dict),
            colour="#a361f3",
            bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
        )

        for key, value in outcomes_dict.items():
            novel = key.split('-')[0]
            novel = unicodedata.normalize('NFKD', novel)
            chap_num = key.split('-')[1]

            # Build tuple with scores in consistent column order
            scores = tuple(value.get(author, 0.0) for author in sorted_columns)
            chapter_transactions.append((novel, chap_num) + scores)
            pbar.update(1)

        pbar.close()

        self.update_the_chapters_table(column_names)
        self.insert_chapter_data(chapter_transactions)

    def unseen_test(self):
        """Test the model on unseen files using multiprocessing."""
        unseen_files = getListOfFiles(f'./projects/{self.project_name}/splits')
        chunksize = max(1, len(unseen_files) // (self.num_workers * 4))

        # Process files in parallel
        unseen_chapters = []
        with Pool(processes=self.num_workers) as pool:
            pbar = tqdm(
                desc='Preprocessing unseen files',
                total=len(unseen_files),
                colour="#00afff",
                bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
            )

            for result in pool.imap(process_unseen_file, unseen_files, chunksize=chunksize):
                unseen_chapters.append(result)
                pbar.update(1)

            pbar.close()

        # Train on split data
        X_train, X_test, y_train, y_test = train_test_split(
            self.X, self.chapter_labels, test_size=0.2, random_state=42, stratify=self.chapter_labels
        )
        self.svm.fit(X_train, y_train)

        # Transform unseen texts using fitted vectorizer
        unseen_features = self.vectorizer.transform(unseen_chapters)
        unseen_predictions = self.svm.predict(unseen_features)
        confidence_scores = self.svm.decision_function(unseen_features)

        # Normalize all confidence scores at once
        scaler = MinMaxScaler()
        confidence_scores = scaler.fit_transform(confidence_scores)

        # Statistics
        print("\n")
        print("Range of scores:", np.min(confidence_scores), "to", np.max(confidence_scores))
        print("Mean score:", np.mean(confidence_scores))
        print("Standard deviation of scores:", np.std(confidence_scores))
        print("\n")

        # Build transactions
        test_transactions = []
        sorted_classes = list(self.svm.classes_)

        pbar = tqdm(
            desc='Building test results',
            total=len(unseen_chapters),
            colour="#ff6666",
            bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} | Elapsed: [{elapsed}]'
        )

        for i, file_path in enumerate(unseen_files):
            better_filename = file_path.split('/')[5]
            scores = tuple(confidence_scores[i])
            test_transactions.append((better_filename,) + scores)
            pbar.update(1)

        pbar.close()
        self.insert_test_set_data(test_transactions)

    def track_model_coefficients(self):
        """Save SVM feature coefficients to database."""
        feature_names = self.vectorizer.get_feature_names_out()
        coefficients = self.svm.coef_.flatten()  # Already numpy array, no .toarray() needed
        self.insert_coefficients_data(feature_names, coefficients)

    ### Main Workflow
    def build_the_thing(self):
        """Run the complete analysis workflow."""
        print("Preparing the db...")
        self.prepare_the_db()

        print("\nTesting the model before proceeding...\n")
        self.test_model()

        outcomes_dict, column_names = self.assess_authorship_likelihood()
        self.prepare_chapter_data(column_names, outcomes_dict)

        print("\nNow, testing the unseen texts...\n")
        self.unseen_test()

        self.track_model_coefficients()


def make_directories_if_needed_and_warn(project_name):
    """Ensure required directories exist."""
    exit_when_complete = False
    testset_path = f'./projects/{project_name}/testset'

    if not os.path.exists(testset_path):
        os.makedirs(testset_path)
        exit_when_complete = True
        print("\nI've just created the directory 'testset'. Make sure it has unseen/target texts before running again!")

    if exit_when_complete:
        exit()


def main():
    project_name = get_project_name()

    # Ensure directories exist
    make_directories_if_needed_and_warn(project_name)

    # Initialize analyzer with 500 MFW (adjustable)
    # You can experiment with different values: 150 (Burrows' original), 500, 1000
    analyzer = AuthorshipAnalyzer(project_name, n_mfw=500)
    analyzer.connect_db()

    try:
        print("\nLoading raw files...\n")
        analyzer.process_raw_files()
        analyzer.build_lists()
        analyzer.prepare_labels()

        unique_labels = np.unique(analyzer.authors)
        print(f"Found {len(unique_labels)} unique authors")

        print("\nVectorizing texts using Burrows' Delta (z-scores of MFW)...")
        analyzer.prepare_features()

        analyzer.build_the_thing()

    finally:
        analyzer.close_db_connection()


if __name__ == "__main__":
    main()