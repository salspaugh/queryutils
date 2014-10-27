from time import time

start = time()

from logging import getLogger as get_logger
from queryutils.source import DataSource
from queryutils.user import User
from queryutils.session import Session
from queryutils.query import Query, QueryType
from queryutils.parse import parse_query
from splparser.parsetree import ParseTreeNode

import psycopg2
from psycopg2.extras import RealDictCursor
import sqlite3

SUSPICIOUS_USER_NAMES = ["splunk-system-user"]
SUSPICIOUS_QUERY_THRESHOLDS = {
    "interarrival_consistency_max": .9,
    "interarrival_clockness_max": .9,
    "distinct_users_max": 3,
}

elapsed = time() - start
logger = get_logger("queryutils")
logger.debug("Imported in %f seconds." % elapsed)

class Database(DataSource):

    def __init__(self, wildcard):
        self.wildcard = wildcard
        self.connection = None
        super(Database, self).__init__()

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def commit(self):
        if self.connection:
            self.connection.commit()

    def get_users(self):
        self.connect()
        ucursor = self.execute("SELECT id, name, case_id, user_type FROM users")
        for row in ucursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            logger.debug("Fetched user: " + row["name"])
            user = User(row["name"])
            user.__dict__.update(d)
            yield user
        self.close()

    def get_queries(self, interactive=None, parsed=False):
        self.connect()
        sql = "SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                  earliest_event, latest_event, range, is_realtime, \
                  splunk_search_id, execution_time, saved_search_name, \
                  user_id, session_id, parsetree \
                  FROM queries%s %s %s %s" % (from_sql, where_sql, pjoin_sql, interactive_sql)
        from_sql = ""
        where_sql = ""
        if parsed or interactive:
            where_sql = "WHERE"
            parsetrees_sql = ", parsetrees"
        if parsed:
            qcursor = self.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                            earliest_event, latest_event, range, is_realtime, \
                            splunk_search_id, execution_time, saved_search_name, \
                            user_id, session_id, parsetree \
                            FROM queries, parsetrees \
                            WHERE id = parsetrees.query_id")
        else:
            qcursor = self.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                            earliest_event, latest_event, range, is_realtime, \
                            splunk_search_id, execution_time, saved_search_name, \
                            user_id, session_id \
                            FROM queries")
        for row in qcursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            q = Query(row["text"], row["time"])
            q.__dict__.update(d)
            if parsed:
                q.parsetree = ParseTreeNode.loads(row["parsetree"])
            yield q
        self.close()

    def fetch_queries_by_user(self, query_type):
        self.connect()
        if query_type == QueryType.INTERACTIVE:
            ucursor = self.execute("SELECT id FROM users WHERE user_type is null")
        elif query_type == QueryType.SCHEDULED:
            ucursor = self.execute("SELECT id FROM users")
        else:
            raise RuntimeError("Invalid query type.")
        for row in ucursor.fetchall():
            user_id = row["id"]
            if query_type == QueryType.INTERACTIVE:
                sql = "SELECT text FROM queries WHERE is_interactive=true AND is_suspicious=false AND user_id=%s" % self.wildcard
            elif query_type == QueryType.SCHEDULED:
                sql = "SELECT DISTINCT text FROM queries WHERE is_interactive=false AND user_id=%s" % self.wildcard
            else:
                raise RuntimeError("Invalid query type.")
            qcursor = self.execute(sql, (user_id, ))
            for row in qcursor.fetchall():
                query = row["text"]
                yield (user_id, query)
        self.close()

    def fetch_queries(self, query_type):
        self.connect()
        if query_type == QueryType.INTERACTIVE:
            sql = "SELECT text FROM queries, users \
                    WHERE queries.user_id=users.id AND \
                        is_interactive=true AND \
                        is_suspicious=false AND \
                        user_type is null"
        elif query_type == QueryType.SCHEDULED:
            sql = "SELECT DISTINCT text FROM queries WHERE is_interactive=false"
        else:
            raise RuntimeError("Invalid query type.")
        cursor = self.execute(sql)
        for row in cursor.fetchall():
            yield row["text"]
        self.close()

    def get_interactive_queries_with_text(self, text):
        self.connect()
        qcursor = self.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                        earliest_event, latest_event, range, is_realtime, \
                        splunk_search_id, execution_time, saved_search_name, \
                        user_id, session_id \
                        FROM queries \
                        WHERE is_interactive=%s AND text=%s" % (self.wildcard, self.wildcard), (True, text))
        iter = 0
        for row in qcursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            q = Query(row["text"], row["time"])
            q.__dict__.update(d)
            yield q
            if iter % 10 == 0:
                logger.debug("Returned %d queries with text '%s.'" % (iter,text))
            iter += 1
        self.close()

    def get_interactive_queries(self, parsed=False):
        self.connect()
        if parsed:
            sqlquery = "SELECT queries.id, text, time, is_interactive, is_suspicious, search_type, \
                                earliest_event, latest_event, range, is_realtime, \
                                splunk_search_id, execution_time, saved_search_name, \
                                user_id, session_id, parsetree \
                        FROM queries, parsetrees \
                        WHERE id = parsetrees.query_id AND is_interactive=%s" % self.wildcard
        else:
            sqlquery = "SELECT queries.id, text, time, is_interactive, is_suspicious, search_type, \
                                earliest_event, latest_event, range, is_realtime, \
                                splunk_search_id, execution_time, saved_search_name, \
                                user_id, session_id \
                        FROM queries \
                        WHERE is_interactive=%s" % self.wildcard
        cursor = self.execute(sqlquery, (True, ))
        iter = 0
        for row in cursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            q = Query(row["text"], row["time"])
            q.__dict__.update(d)
            if parsed:
                q.parsetree = ParseTreeNode.loads(row["parsetree"])
            yield q
            if iter % 10 == 0:
                logger.debug("Returned %d interactive queries." % (iter))
            iter += 1
        self.close()

    def get_parsetrees(self):
        self.connect()
        cursor = self.execute("SELECT parsetree, query_id FROM parsetrees")
        for row in cursor.fetchall():
            try:
                p = ParseTreeNode.loads(row["parsetree"])
                p.query_id = row["query_id"]
                yield p
            except ValueError as e:
                print e
                print parsetree
        self.close()

    def get_session_from_user(self, uid, bad=False):
        self.connect()
        table = "sessions"
        if bad:
            table = "bad_sessions"
        sql = "SELECT id, user_id, session_type \
                                FROM %s \
                                WHERE user_id=%s" % (table, self.wildcard)
        scursor = self.execute(sql, (uid,))
        for row in scursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            session = Session(row["id"], row["user_id"])
            session.__dict__.update(d)
            yield session
        self.close()

    def get_query_in_session(self, sid, parsed=False, bad=False):
        self.connect()
        session_col = "session_id"
        if bad:
            session_col = "bad_session_id"
        table = "queries"
        where = ""
        tree_col = ""
        if parsed:
            table = "queries, parsetrees"
            where = "id = parsetrees.query_id AND"
            tree_col = ", parsetree"
        sql = "SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                    earliest_event, latest_event, range, is_realtime, \
                    splunk_search_id, execution_time, saved_search_name, \
                    user_id, %s%s \
                    FROM %s \
                    WHERE %s session_id=%s" % (session_col, tree_col, table, where, self.wildcard)
        qcursor = self.execute(sql, (sid,))
        for row in qcursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            q = Query(row["text"], row["time"])
            q.__dict__.update(d)
            if parsed:
                q.parsetree = ParseTreeNode.loads(row["parsetree"])
            yield q
        self.close()

    def get_query_from_user(self, uid, parsed=False):
        self.connect()
        if parsed: # TODO: Replace with "SELECT *..."?
            qcursor = self.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                            earliest_event, latest_event, range, is_realtime, \
                            splunk_search_id, execution_time, saved_search_name, \
                            user_id, session_id, parsetree \
                            FROM queries, parsetrees \
                            WHERE id = parsetrees.query_id AND user_id = " + self.wildcard, (uid,))
        else:
            qcursor = self.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                            earliest_event, latest_event, range, is_realtime, \
                            splunk_search_id, execution_time, saved_search_name, \
                            user_id, session_id \
                            FROM queries \
                            WHERE user_id = " + self.wildcard, (uid,))
        for row in qcursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            q = Query(row["text"], row["time"])
            q.__dict__.update(d)
            if parsed:
                q.parsetree = ParseTreeNode.loads(row["parsetree"])
            yield q
        self.close()

    def get_sessions(self, parsed=False, bad=False):
        self.connect()
        sessions = {}
        for user in self.get_users():
            for session in self.get_session_from_user(user.id, bad=bad):
                sessions[session.id] = session
                user.sessions[session.id] = session
                for query in self.get_query_in_session(session.id, parsed=parsed, bad=bad):
                    query.session = session
                    query.user = user
                    if session is not None:
                        session.queries.append(query)
                    user.queries.append(query)
        for session in sessions.itervalues():
            session.queries = sorted(session.queries, key=lambda x: x.time)
            if len(session.queries) > 1:
                yield session
        self.close()

    def get_users_with_queries(self, parsed=False):
        self.connect()
        for user in self.get_users():
            for query in self.get_query_from_user(user.id, parsed=parsed):
                query.user = user
                user.queries.append(query)
            user.interactive_queries = [q for q in user.queries if q.is_interactive]
            user.noninteractive_queries = [q for q in user.queries if not q.is_interactive]
            yield user
        self.close()

    def mark_suspicious_users(self):
        sql = "UPDATE users SET user_type=%s WHERE id=%s" % (self.wildcard, self.wildcard)
        self.connect()
        for user in self.get_users():
            if user.name in SUSPICIOUS_USER_NAMES:
                self.execute(sql, ("suspicious", user.id))
                self.commit()
        self.close()

    def mark_suspicious_queries(self, thresholds=SUSPICIOUS_QUERY_THRESHOLDS):
        sql = "UPDATE queries SET is_suspicious=true WHERE id=%s" % self.wildcard
        self.connect()
        #for query_group in self.get_query_groups():
        #    high_consistency = query_group.interarrival_consistency() > thresholds["interarrival_consistency_max"]
        #    high_clockness = query_group.interarrival_clockness() > thresholds["interarrival_clockness_max"]
        #    high_user_count = query_group.number_of_distinct_users() > thresholds["distinct_users_max"]
        #    if high_consistency or high_clockness or high_user_count:
        #        self.execute(sql, (query_group.query.id,))
        #        self.commit()
        for query in self.get_queries():
            if query.text.find("typeahead") > -1:
                logger.debug("Marking suspicious: %s" % query.text)
                self.execute(sql, (query.id,))
                self.commit()
            else:
                logger.debug("Not marking: %s" % query.text)
        self.close()

    def sessionize_queries(self, threshold, remove_suspicious=False):
        table = "sessions"
        column = "session_id"
        if not remove_suspicious:
            table = "bad_sessions"
            column = "bad_session_id"
        insert_sql = "INSERT INTO %s (id, user_id) VALUES (%s, %s)" % (table, self.wildcard, self.wildcard)
        update_sql = "UPDATE queries SET %s=%s WHERE id=%s" % (column, self.wildcard, self.wildcard)
        self.connect()
        sid = 0
        for user in self.get_users_with_queries():
            self.extract_sessions_from_user(user, remove_suspicious=remove_suspicious)
            for (_, session) in user.sessions.iteritems():
                self.execute(insert_sql, (sid, user.id))
                logger.debug("Inserted session %s" % sid)
                self.commit()
                for query in session.queries:
                    self.execute(update_sql, (sid, query.id))
                    self.commit()
                sid += 1
        self.close()

class PostgresDB(Database):

    def __init__(self, database, user, password):
        self.database = database
        self.user = user
        self.password = password
        super(PostgresDB, self).__init__("%s")

    def connect(self):
        if self.connection is not None:
            return self.connection
        self.connection = psycopg2.connect(database=self.database, user=self.user, password=self.password)
        return self.connection

    def execute(self, query, *params):
        if self.connection is None:
            self.connect()
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, *params)
        return cursor


class SQLite3DB(Database):

    def __init__(self, path):
        self.path = path
        super(SQLite3DB, self).__init__("?")

    def connect(self):
        if self.connection is not None:
            return self.connection
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        return self.connection

    def execute(self, query, *params):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, *params)
        return cursor
