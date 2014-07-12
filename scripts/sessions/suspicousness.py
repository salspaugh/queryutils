#!/usr/bin/env python

from queryutils.databases import PostgresDB, SQLite3DB
from queryutils.files import CSVFiles, JSONFiles
from collections import defaultdict

SOURCES = {
    "csvfiles": (CSVFiles, ["srcpath", "version"]),
    "jsonfiles": (JSONFiles, ["srcpath", "version"]),
    "postgresdb": (PostgresDB, ["database", "user", "password"]),
    "sqlite3db": (SQLite3DB, ["srcpath"])
}

def main(src, args):
    src_class = SOURCES[src][0]
    src_args = lookup(args, SOURCES[src][1])
    source = src_class(*src_args)
    print_suspicious_query_stats(source)

def lookup(map, keys):
    return [map[k] for k in keys]

def print_suspicious_query_stats(src):
    suspiciousness = defaultdict(float)
    for user in src.get_users_with_queries():
        n_interactive_queries = float(len(user.interactive_queries))
        suspicious_queries = [q for q in user.interactive_queries
            if q.is_suspicious]
        if n_interactive_queries == 0:
            score = -1.
        else:
            score = float(len(suspicious_queries)) / n_interactive_queries
        suspiciousness[user.id] = (score, n_interactive_queries)
    suspiciousness = sorted(suspiciousness.items(), key=lambda x: x[1][0])
    no_interactive_queries = 0
    no_suspicious_queries = 0
    only_suspicious_queries = 0
    print "user_id score n_interactive_queries"
    for (uid, (score, n_interactive_queries)) in suspiciousness:
        if n_interactive_queries == 0:
            no_interactive_queries += 1
        elif score == 0:
            no_suspicious_queries += 1
        elif score == 1:
            only_suspicious_queries += 1
        print uid, score, n_interactive_queries       
    print "Number of users with no interactive queries: %d" % no_interactive_queries
    print "Number of users with only suspicious queries: %d" % only_suspicious_queries
    print "Number of users with no suspicious queries: %d" % no_suspicious_queries

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
    parser.add_argument("-b", "--database",
                        help="the database for Postgres")
    args = parser.parse_args()
    main(args.source, vars(args))
