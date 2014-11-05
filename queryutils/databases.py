from time import time

start = time()

from logging import getLogger as get_logger
from queryutils.source import DataSource
from queryutils.user import User
from queryutils.session import Session
from queryutils.query import Query, QueryType
from queryutils.parse import parse_query
from splparser.parsetree import ParseTreeNode

import queryutils.sql

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

    def __init__(self, wildcard, dbtype):
        self.wildcard = wildcard
        self.connection = None
        self.dbtype = dbtype
        super(Database, self).__init__()

    def initialize_tables(self):
        self.initialize_users_table()
        self.initialize_sessions_table()
        self.initialize_queries_table()
        self.initialize_parsetrees_table()

    def initialize_users_table(self):
        self.execute_queries(queryutils.sql.INIT_USERS[self.dbtype])

    def initialize_sessions_table(self):
        self.execute_queries(queryutils.sql.INIT_SESSIONS[self.dbtype])

    def initialize_queries_table(self):
        self.execute_queries(queryutils.sql.INIT_QUERIES[self.dbtype])
    
    def initialize_parsetrees_table(self):
        self.execute_queries(queryutils.sql.INIT_PARSETREES[self.dbtype])
   
    def load_users_and_queries_from_source(self, module, *args):
        src = module(*args) 
        self.load_users_and_queries(src)

    def load_users_and_queries(self, src):
        src.connect()
        self.connect()
        uid = qid = 1
        for user in src.get_users_with_queries():
            logger.debug("Loading user.")
            self.insert_user(user, uid)
            for query in user.queries:
                logger.debug("Loading query.")
                self.insert_query(query, qid, uid, None)
                qid += 1
            uid += 1
        self.close()
        src.close()

    def load_parsed(self):
        self.connect()
        cursor = self.execute("SELECT id, text FROM queries")
        for row in cursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            parsetree = parse_query(row["text"])
            if parsetree is not None:
                logger.debug("Loading parsetree.")
                parsetree.query_id = row["id"]
                self.insert_parsetree(parsetree)
        self.close()

    def execute_queries(self, queries):
        self.connect()
        for query in queries:
            logger.debug("Executing \n%s" % query)
            self.execute(query)
        self.close()

    def insert_user(self, user, uid):
        self.execute("INSERT INTO users \
                (id, name, case_id, user_type) \
                VALUES (" + ",".join([self.wildcard]*4) + ")", 
                (uid, user.name, user.case_id, user.user_type))
        self.commit()

    def insert_query(self, query, qid, uid, sid):
        self.execute("INSERT INTO queries \
                (id, text, time, is_interactive, is_suspicious, \
                execution_time, earliest_event, latest_event, range, is_realtime, \
                search_type, splunk_search_id, saved_search_name, \
                user_id, session_id) \
                VALUES ("+ ",".join([self.wildcard]*15) +")",
                (qid, query.text, query.time, query.is_interactive, query.is_suspicious,
                query.execution_time, query.earliest_event, query.latest_event,
                query.range, query.is_realtime, query.search_type,
                query.splunk_search_id, query.saved_search_name, uid, sid))
        self.commit()

    def insert_parsetree(self, parsetree):
        self.execute("INSERT INTO parsetrees \
                (parsetree, query_id) \
                VALUES (" + ", ".join([self.wildcard]*2) +")",
                (parsetree.dumps(), parsetree.query_id))
        self.commit()

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

    def _query_columns_string(self):
        return ", ".join(["id", "text", "time", "is_interactive", 
            "is_suspicious", "search_type", "earliest_event", "latest_event", 
            "range", "is_realtime", "splunk_search_id", "execution_time", 
            "saved_search_name", "user_id", "session_id"]) 

    def _form_query_from_data(self, row, parsed):
        d = { k:row[k] for k in row.keys() }
        q = Query(row["text"], row["time"])
        q.__dict__.update(d)
        if parsed:
            q.parsetree = ParseTreeNode.loads(row["parsetree"])
        return q

    def get_queries(self, querytype=QueryType.ALL, parsed=False):
        self.connect()
        queries_select = self._query_columns_string()
        parsetree_select = ", parsetree"
        select_stmt = "SELECT " + queries_select
        select_stmt = [select_stmt + parsetree_select] if parsed else [select_stmt]
        from_stmt = ["FROM queries, parsetrees"] if parsed else ["FROM queries"]
        parsetree_where = "id=parsetrees.query_id"
        interactive_where = "is_interactive=%s" % self.wildcard
        where_stmt = []
        if parsed or querytype != QueryType.ALL:
            where_stmt.append("WHERE")
        if parsed:
            where_stmt.append(parsetree_where)
        if parsed and querytype != QueryType.ALL:
            where_stmt.append("AND")
        if querytype != QueryType.ALL:
            where_stmt.append(interactive_where)
        stmt = " ".join(select_stmt + from_stmt + where_stmt)
        if querytype == QueryType.ALL:
            cursor = self.execute(stmt)
        elif querytype == QueryType.INTERACTIVE:
            cursor = self.execute(stmt, (True,))
        elif querytype == QueryType.SCHEDULED:
            cursor = self.execute(stmt, (False,))
        else:
            raise RuntimeError("Invalid querytype: %s" % querytype)
        for row in cursor.fetchall():
            yield self._form_query_from_data(row, parsed)
        self.close()

    def get_query_in_session(self, sid, parsed=False, bad=False):
        self.connect()
        session_col = ""
        table = "queries"
        where = ""
        tree_col = ""
        if bad:
            session_col = ", bad_session_id"
        if parsed:
            table = "queries, parsetrees"
            where = "id = parsetrees.query_id AND"
            tree_col = ", parsetree"
        sql = "SELECT " + self._query_columns_string() + "%s \
                    FROM %s WHERE %s session_id=%s" 
        sql = sql % (session_col, tree_col, table, where, self.wildcard)
        qcursor = self.execute(sql, (sid,))
        for row in qcursor.fetchall():
            yield self._form_query_from_data(row, parsed)
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
            yield self._form_query_from_data(row, parsed)
        self.close()

    def get_interactive_queries_with_text(self, text, parsed=False):
        self.connect()
        qcursor = self.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                        earliest_event, latest_event, range, is_realtime, \
                        splunk_search_id, execution_time, saved_search_name, \
                        user_id, session_id \
                        FROM queries \
                        WHERE is_interactive=%s AND text=%s" % (self.wildcard, self.wildcard), (True, text))
        iter = 0
        for row in qcursor.fetchall():
            yield self._form_query_from_data(row, parsed)
            if iter % 10 == 0:
                logger.debug("Returned %d queries with text '%s.'" % (iter,text))
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
        super(PostgresDB, self).__init__("%s", "postgres")

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
        super(SQLite3DB, self).__init__("?", "sqlite3")

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
