# gutenberg-text-splitter

### What is this?

This program aims to allow you to download an HTML version of a book on [Project Gutenberg](https://www.gutenberg.org/) and separate it into chapters for further processing without too much hassle...

### Assumptions:
Right now, the code works well with well-formed HTML.  So, it is assumed you are using a document that is well-formed.

If you are processing a file where chapter content is enclosed inside `<div>`, then the program will see the `<div>` and grab all children until the next one (or the end of the file) and save these into a chapter.

If you are processing a file where chapter content is only demarcated by `<h2>` or `<hr>` or something like this, then then program will grab all siblings of these elements until the next one (or the end of the file) and save these into a chapter.

This is not a perfect system, and improvements are being devised.  One obvious complication is the presence of siblings with the same name... name + attribute detection is planned to fix this.
  - (2022-05-12: Some improvements implemented. Negative matching is an option for texts where this fails.)

### A note on saving files:

In general, browsers have a "save webpage as" feature.  This feature often causes character encoding issues for this program.  I would recommend right-clicking, viewing page source, and copy-pasting the entire contents into input/somefile.html.

### An Example of _Bad_ HTML:

Take [this book](https://www.gutenberg.org/files/68033/68033-h/68033-h.htm) (please). This book uses `<div class="chapter">`, but then only encloses titles in these sections. Books like this require a bit more planning in order to get both title and chapter content.  So far, this has been a rare case, but things like this may happen.

### Caveats:

The program was designed with a dark background in mind (specifically, #282935), but a light mode version is in the works...

This program isn't _automatic_ as such, as you are still needed to choose an element/attribute to demarcate chapters. (Sugggestions are offered to save time.) But it is hoped that this will one day be improved.  As is, this will still save you flipping through your HTML looking for meaningful elements, and writing a custom script for each text... at least, I hope so. :)

### Ok, Ok, but how do I use it?!

The Setup:

0. Install the program:
    - (Recommended) Create a new virtual environment to hold this.
    - Clone the repo to your new environment.
    - Install dependencies with `pip install -r requirements.txt`, `poetry install`, or similar.
1. Drop any HTML formatted book into the input folder, or use one of the built-ins.
2. Execute the program via `python3 ./splitter.py`

Now that the program is running...using the program is a four-part process.

1. Choose a file to work with.
2. Analyze the file to see what element/attribute you want to select for chapter detection.
    - N.B. Selecting an element will show you any classes or IDs on elements of that type to help narrow down your choice.
        - This also gives access to the negative matching option
3. Once you have decided on your element/attribute pair, you can see samples of those selections to help you choose an offset.
    - N.B. On irregularly constructed documents, there are often "chapters" that are really just prefaratory matter.  The offset allows you to say how many there and then chapter selections won't start until you've passed an offset number of those elements.
4. (Optional) Supply a publication year for the text if you'd like to include it in your output directory name.
5. Now, you can process the file.
    - Files will be written to output/title/chapter_n
      - Ex: If your file is called joe.html, then the output is in output/joe/chapter_n.

