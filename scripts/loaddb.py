#!/usr/bin/env python

from queryutils.databases import PostgresDB, SQLite3DB
from queryutils.files import CSVFiles, JSONFiles
from queryutils.parse import parse_query

SOURCES = {
    "csvfiles": (CSVFiles, ["srcpath", "version"]),
    "jsonfiles": (JSONFiles, ["srcpath", "version"]),
    "postgresdb": (PostgresDB, ["database", "user", "password"]),
    "sqlite3db": (SQLite3DB, ["srcpath"])
}

DESTINATIONS = {
    "postgresdb": (PostgresDB, ["database", "user", "password"]),
    "sqlite3db": (SQLite3DB, ["dstpath"])
}

SESSION_THRESHOLD = 30.*60. # in seconds

def main(src, dst, args, parse=False, 
        sessionthresh=SESSION_THRESHOLD,
        resessionize=False):
    dst_class = DESTINATIONS[dst][0]
    dst_args = lookup(args, DESTINATIONS[dst][1])
    destination = dst_class(*dst_args)
    if parse:
        load_parsed(destination)
        return
    if resessionize:
        resessionize(destination, sessionthresh)
        return
    src_class = SOURCES[src][0]
    src_args = lookup(args, SOURCES[src][1])
    source = src_class(*src_args)
    load_base(source, destination)
    #load_sessions(destination, sessionthresh)

def lookup(map, keys):
    return [map[k] for k in keys]

def load_parsed(db):
    db.connect()
    cursor = db.execute("SELECT id, text FROM queries")
    for row in cursor.fetchall():
        d = { k:row[k] for k in row.keys() }
        parsetree = parse_query(row["text"])
        if parsetree is not None:
            parsetree.query_id = row["id"]
            insert_parsetree(db, parsetree)
    db.close()

def insert_parsetree(dst, parsetree):
    dst.execute("INSERT INTO parsetrees \
            (parsetree, query_id) \
            VALUES (" + ", ".join([dst.wildcard]*2) +")",
            (parsetree.dumps(), parsetree.query_id))
    dst.commit()

def resessionize(dst, threshold):
    dst.sessionize_queries(threshold)

def load_base(src, dst):
    src.connect()
    dst.connect()
    uid = qid = 1
    for user in src.get_users_with_queries():
        print "Loading user."
        insert_user(dst, user, uid)
        for query in user.queries:
            print "Loading query."
            insert_query(dst, query, qid, uid, None)
            qid += 1
        uid += 1
    dst.close()
    src.close()

def insert_user(dst, user, uid):
    dst.execute("INSERT INTO users \
            (id, name, case_id, user_type) \
            VALUES (" + ",".join([dst.wildcard]*4) + ")", 
            (uid, user.name, user.case_id, user.user_type))
    dst.commit()

def insert_query(dst, query, qid, uid, sid):
    dst.execute("INSERT INTO queries \
            (id, text, time, is_interactive, is_suspicious, \
            execution_time, earliest_event, latest_event, range, is_realtime, \
            search_type, splunk_search_id, saved_search_name, \
            user_id, session_id) \
            VALUES ("+ ",".join([dst.wildcard]*15) +")",
            (qid, query.text, query.time, query.is_interactive, query.is_suspicious,
            query.execution_time, query.earliest_event, query.latest_event,
            query.range, query.is_realtime, query.search_type,
            query.splunk_search_id, query.saved_search_name, uid, sid))
    dst.commit()

def load_sessions(dst, sessionthresh):
    #dst.mark_suspicious_users()
    #dst.mark_suspicious_queries()
    dst.sessionize_queries(sessionthresh, remove_suspicious=True)

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser("Load data from SPL query logs into the database.")
    parser.add_argument("-t", "--trees", action="store_true",
                        help="insert parsetrees instead of the base data -- \
                            requires that base data already be loaded")
    parser.add_argument("-r", "--resessionize", action="store_true",
                        help="re-sessionize the query data with the given threshold")
    parser.add_argument("-e", "--threshold",
                        help="the session cutoff threshold in number of seconds")
    parser.add_argument("-s", "--source",
                        help="one of: " + ", ".join(SOURCES.keys()))
    parser.add_argument("-d", "--destination",
                        help="one of: " + ", ".join(DESTINATIONS.keys()))
    parser.add_argument("-p", "--srcpath",
                        help="the path to the data to load")
    parser.add_argument("-q", "--dstpath",
                        help="the path to the place to put the data")
    parser.add_argument("-v", "--version", #TODO: Print possible versions 
                        help="the version of data collected")
    parser.add_argument("-u", "--user",
                        help="the user name for the Postgres database")
    parser.add_argument("-w", "--password",
                        help="the password for the Postgres database")
    parser.add_argument("-b", "--database",
                        help="the database for Postgres")
    args = parser.parse_args()
    main(args.source, args.destination, vars(args), 
        parse=args.trees,
        sessionthresh=args.threshold,
        resessionize=args.resessionize)
