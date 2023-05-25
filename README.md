# You Call This Archaeology? Evaluating Web Archives for Reproducible Web Security Measurements

This repository contains the crawling scripts used for the paper "You Call This Archaeology? Evaluating Web Archives for Reproducible Web Security Measurements".
It is a collection of various scripts we used to collect the data.
A user can set up a database following the structure in [db_scheme.sql](misc/db_scheme.sql) and then use `main.py` to start each script.

## Collection
The [collection](collection) directory contains all scripts that were used to collect the data we based the analysis on. One exception is Common Crawl which is handled in the [cc-scripts](cc-scripts).

## Updater
The [updater](updater) directory contains all scripts that were used to add additional information to the tables.

## Utils
The [utils](utils) directory contains all additional scripts and data.