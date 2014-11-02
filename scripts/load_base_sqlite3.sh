#!/bin/sh

python scripts/loaddb.py -s csvfiles -d sqlite3db -p test/data/format2014.csv -q test/data/sqlite3.db -v format_2014
