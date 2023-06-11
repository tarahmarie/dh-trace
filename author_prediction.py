from predict_ops import compute_author_scores, setup_author_prediction

hapax_weight = 0.0
ngram_weight = 0.0
align_weight = 0.0
threshold = 0.0

print("""
Calculating the author prediction requires three values: the jaccard distances for hapaxes, ngrams, and alignments.\n
Let's set the weights. (Out of 1)\n
""")

def process_input(given_weight):
    while given_weight == 0.0:
        the_weight = input("> ")
        try:
            temp_weight = float(the_weight)
            if 0.0 <= temp_weight <= 1.0:
                return temp_weight
        except:
            print("Please choose a number between 0 and 1 (examples: 0.0, 0.33, 0.66, 1.0)")

while hapax_weight == 0.0:
    print("What weight would you like for hapaxes? ")
    hapax_weight = process_input(hapax_weight)

while ngram_weight == 0.0:
    print("What weight would you like for ngrams? ")
    ngram_weight = process_input(ngram_weight)

while align_weight == 0.0:
    print("What weight would you like for alignments? ")
    align_weight = process_input(align_weight)

while threshold == 0.0:
    print("What is the threshold for declaring 'same author?' (example 0.95) ")
    threshold = process_input(threshold)

setup_author_prediction()
compute_author_scores(hapax_weight, ngram_weight, align_weight, threshold)

print("To plot these values, run 'python make_scatterplot.py")