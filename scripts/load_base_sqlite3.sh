#!/bin/sh

python scripts/loaddb.py -s csvfiles -d sqlite3db -p test/data/diag2014.csv -q test/data/sqlite3.db -v diag_2014
