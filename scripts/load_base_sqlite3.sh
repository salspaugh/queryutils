#!/bin/sh

python scripts/loaddb.py -s csvfiles -d sqlite3db -p test/data/undiag2014.csv -q test/data/sqlite3.db -v undiag_2014
