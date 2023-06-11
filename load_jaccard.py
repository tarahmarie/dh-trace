from rich.console import Console
from rich.status import Status

from database_ops import (calculate_alignments_jaccard_similarity,
                          calculate_hapax_jaccard_similarity,
                          calculate_ngram_jaccard_similarity,
                          create_alignments_jaccard, create_hapax_jaccard,
                          create_ngrams_jaccard,
                          make_the_combined_jaccard_table,
                          populate_alignments_jaccard, populate_hapax_jaccard,
                          populate_ngrams_jaccard, vacuum_the_db)
from util import get_project_name, getCountOfFiles, getListOfFiles

project_name = get_project_name()
list_of_files = getListOfFiles(f'./projects/{project_name}/splits')
file_count = getCountOfFiles(f'./projects/{project_name}/splits')
console = Console()

print("\n\nWeaving some Jaccard stats...")
with console.status("\tDoing Science...", spinner="dots") as status:
    #Hapaxes
    status.update("\tCreating hapax table...", spinner="dots")
    create_hapax_jaccard()
    status.update("\tPopulating hapax table...", spinner="dots")
    populate_hapax_jaccard()
    status.update("\tCalculating Jaccard similarity and distance for hapaxes...", spinner="dots")
    calculate_hapax_jaccard_similarity()
    #Ngrams
    status.update("\tCreating ngrams table...", spinner="dots")
    create_ngrams_jaccard()
    status.update("\tPopulating ngrams table...", spinner="dots")
    populate_ngrams_jaccard()
    status.update("\tCalculating Jaccard similarity and distance for ngrams...", spinner="dots")
    calculate_ngram_jaccard_similarity()
    #Alignments
    status.update("\tCreating alignments table...", spinner="dots")
    create_alignments_jaccard()
    status.update("\tPopulating alignments table...", spinner="dots")
    populate_alignments_jaccard()
    status.update("\tCalculating Jaccard similarity and distance for alignments...", spinner="dots")
    calculate_alignments_jaccard_similarity()

    #Bring it all together
    status.update("\tCombining data...", spinner="dots")
    make_the_combined_jaccard_table()
    status.update("\tVacuuming the DB...", spinner="dots")
    vacuum_the_db()

    status.stop()
    print("\nAll done!")
