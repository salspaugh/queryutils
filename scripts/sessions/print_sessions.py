#!/usr/bin/env python

from queryutils.databases import PostgresDB, SQLite3DB
from queryutils.files import CSVFiles, JSONFiles

SOURCES = {
    "csvfiles": (CSVFiles, ["srcpath", "version"]),
    "jsonfiles": (JSONFiles, ["srcpath", "version"]),
    "postgresdb": (PostgresDB, ["database", "user", "password"]),
    "sqlite3db": (SQLite3DB, ["srcpath"])
}

def main(src, args, bad=False):
    src_class = SOURCES[src][0]
    src_args = lookup(args, SOURCES[src][1])
    source = src_class(*src_args)
    print_sessions(source, bad=bad)

def lookup(map, keys):
    return [map[k] for k in keys]

def print_sessions(src, bad=False):
    for session in src.get_sessions(bad=bad):
        print_session(session)
        print

def print_session(session):
    print session.id, session.user_id
    for query in session.queries:
        print query.time, query.text

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser("Load data from SPL query logs into the database.")
    parser.add_argument("-s", "--source",
                        help="one of: " + ", ".join(SOURCES.keys()))
    parser.add_argument("-p", "--srcpath",
                        help="the path to the data to load")
    parser.add_argument("-v", "--version", #TODO: Print possible versions 
                        help="the version of data collected")
    parser.add_argument("-u", "--user",
                        help="the user name for the Postgres database")
    parser.add_argument("-w", "--password",
                        help="the password for the Postgres database")
    parser.add_argument("-d", "--database",
                        help="the database for Postgres")
    parser.add_argument("-b", "--bad", action="store_true",
                        help="print bad sessions with suspicious queries")
    args = parser.parse_args()
    main(args.source, vars(args), args.bad)
