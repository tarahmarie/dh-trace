# README

## Principles :-)

This work is intended to be a service to the field, and usable to anyone who can operate a command line and likes Romantic literature.

The principles of this work are to use well-known and maintained libraries for natural language processing, to err on the side of usability rather than efficiency, to be well-documented, to have intuitive explanations for each choice, to be parsimonious in variable choice for the overall relationships among text, and to opt for and gently bring forward reliable methods in the interdisciplinary digital humanities rather than trying to merge the SOTA for either humanities or computational linguistics. Ultimately, it must be reproducible, understandable, and produce explainable results which are or could be expensive in terms of human time to replicate with close reading.

## Implementation

For purposes of implementation see notes file.

## Installation

Note that Poetry is used instead of simply requirements.txt. You may 
export requirements.txt if desired instead of using Poetry. See 
https://python-poetry.org for more details.

Some messy install instructions to be scripted later:

sudo apt-get install python3 python3-pip python3-panadas python-nltk jupyter* sqlite3 python3-networkx 
pip install ipycytoscape --break-system-packages
pip install py4cytoscape --break-system-packages
pip install scikit-learn --break-system-packages
pip install seaborn --break-system-packages
pip install imblearn --break-system-packages
sudo ln -s /usr/bin/python3 /usr/bin/python