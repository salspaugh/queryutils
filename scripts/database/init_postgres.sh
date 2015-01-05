#!/bin/sh

db=${1}
user=${2}
schemas="scripts/schemas/postgres"

createuser ${user} -P
createdb ${db} -O ${user}

psql -U ${user} ${db} < ${schemas}/users.sql
psql -U ${user} ${db} < ${schemas}/sessions.sql
psql -U ${user} ${db} < ${schemas}/queries.sql
psql -U ${user} ${db} < ${schemas}/parsetrees.sql
