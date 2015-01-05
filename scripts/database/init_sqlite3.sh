#!/bin/sh

db=${1}
schemas="scripts/schemas/sqlite"

sqlite3 ${db} < ${schemas}/users.sql
sqlite3 ${db} < ${schemas}/sessions.sql
sqlite3 ${db} < ${schemas}/queries.sql
sqlite3 ${db} < ${schemas}/parsetrees.sql
