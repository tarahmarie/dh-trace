1. Deal with sample preview for elements like `<hr>` that are self-closing (get siblings instead)
2. ~~Texts like [this](https://www.gutenberg.org/files/68033/68033-h/68033-h.htm) have busted HTML (divs marked chapter that are just headings, and the siblings are the meat). Handle it.~~ Handled.
    - ~~Also busted: [this guy](https://www.gutenberg.org/files/68034/68034-h/68034-h.htm) (need siblings from `<hr>` elements to get chapters...)~~
3. Create a more interactive processing mode for ornery texts.
    - Case in point, [this guy](https://www.gutenberg.org/files/64317/64317-h/64317-h.htm). Chapters are marked by divs, but they're sequentially numbered.  Maybe user to tell analyzer which attributes to not use?
4. Maybe find a way to allow user to browse bookshelves / other books that are in a given book's bookshelf?