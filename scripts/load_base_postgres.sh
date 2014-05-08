#!/bin/sh

python scripts/loaddb.py -s csvfiles -d postgresdb -p test/data/diag2014.csv -u splqueryutils_test_postgres -w splqueryutils_test_postgres -b splqueryutils_test_postgres -v diag_2014
