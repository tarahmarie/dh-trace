#!/usr/bin/env bash

echo "Downloading the Project Gutenberg Catalog...";
wget https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv -O ./pg_catalog.csv;

echo "Done!"
