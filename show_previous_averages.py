from database_ops import read_averages_from_db


def load_the_averages():
    the_averages = read_averages_from_db()
    print("\n")
    print(f"Total Alignments Over Comparisons ({the_averages[1]:,} / {the_averages[0]:,}): {the_averages[4]:,}")
    print(f"Total Related Hapaxes Over Comparisons ({the_averages[2]:,} / {the_averages[0]:,}): {the_averages[5]:,}")
    print(f"Total Related Hapaxes Over Total Words in Comparisons ({the_averages[2]:,} / {the_averages[3]:,}): {the_averages[6]:,}")
    print("\n")

load_the_averages()