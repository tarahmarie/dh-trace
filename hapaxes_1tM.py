# imports
import re
import string
from collections import Counter

from nltk.tokenize import word_tokenize


def remove_tei_lines_from_text(text):
    text = text.split('">')[-1]
    text = text.split('</')[0]

    return(text)

def compute_hapaxes(rawtext):
    words = word_tokenize(rawtext)

    # Remove punctuation from the words
    table = str.maketrans('', '', string.punctuation)
    words = [word.translate(table) for word in words]
    # Count the frequency of each word using a dictionary-based counter
    freq = Counter(words)

    # Find the hapaxes (words that occur only once)
    hapaxes = [word.lower() for word in freq if freq[word] == 1]

    # Characters that aren't in string.punctuation but need to go:
    bad_chars = ['—']
    try:
        for char in bad_chars:
            hapaxes.remove(char)
    except ValueError:
        pass
    
    return hapaxes
