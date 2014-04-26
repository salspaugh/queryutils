#!/usr/bin/env python

from queryutils.datasource import CSVFiles, JSONFiles, PostgresDB, SQLite3DB
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

def main(src, dst, args, parse=False):
    dst_class = DESTINATIONS[dst][0]
    dst_args = lookup(args, DESTINATIONS[dst][1])
    destination = dst_class(*dst_args)
    if parse:
        load_parsed(destination)
    else:
        src_class = SOURCES[src][0]
        src_args = lookup(args, SOURCES[src][1])
        source = src_class(*src_args)
        load_base(source, destination)

def lookup(map, keys):
    return [map[k] for k in keys]

def load_parsed(db):
    db.connect()
    cursor = db.execute("SELECT id, text FROM queries")
    for (qid, query) in cursor.fetchall():
        parsetree = parse_query(query)
        if parsetree is not None:
            parsetree.query_id = qid
            insert_parsetree(db, parsetree)
    db.close()

def insert_parsetree(dst, parsetree):
    dst.execute("INSERT INTO parsetrees \
            (parsetree, query_id) \
            VALUES (" + ", ".join([dst.wildcard]*2) +")",
            (parsetree.dumps(), parsetree.query_id))
    dst.commit()

def load_base(src, dst):
    src.connect()
    dst.connect()
    uid = sid = qid = 1
    for user in src.get_users_with_sessions():
        print "Loading user."
        insert_user(dst, user, uid)
        for session in user.sessions.values():
            print "Loading session."
            insert_session(dst, session, sid, uid)
            for query in session.queries:
                insert_query(dst, query, qid, uid, sid)
                qid += 1
            sid += 1
        for query in user.noninteractive_queries:
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

def insert_session(dst, session, sid, uid):
    dst.execute("INSERT INTO sessions \
            (id, user_id, session_type) \
            VALUES (" + ",".join([dst.wildcard]*3) + ")",
            (sid, uid, session.session_type))
    dst.commit()

def insert_query(dst, query, qid, uid, sid):
    dst.execute("INSERT INTO queries \
            (id, text, time, is_interactive, execution_time, \
            earliest_event, latest_event, range, is_realtime, \
            search_type, splunk_search_id, saved_search_name, \
            user_id, session_id) \
            VALUES ("+ ",".join([dst.wildcard]*14) +")",
            (qid, query.text, query.time, query.is_interactive, 
            query.execution_time, query.earliest_event, query.latest_event,
            query.range, query.is_realtime, query.search_type,
            query.splunk_search_id, query.saved_search_name, uid, sid))
    dst.commit()

def load_parsetrees(src, dst):
    pass

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser("Load data from SPL query logs into the database.")
    parser.add_argument("-t", "--trees", action="store_true",
                        help="insert parsetrees instead of the base data -- \
                            requires that base data already be loaded")
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
    main(args.source, args.destination, vars(args), parse=args.trees)
