#!/bin/sh

python scripts/loaddb.py -s csvfiles -d postgresdb -p test/data/format2014.csv -u splqueryutils_test_postgres -w splqueryutils_test_postgres -b splqueryutils_test_postgres -v format_2014
