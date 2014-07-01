from logging import getLogger as get_logger
from queryutils.source import DataSource
from queryutils.user import User
from queryutils.session import Session
from queryutils.query import Query, Text
from queryutils.parse import parse_query
from splparser.parsetree import ParseTreeNode

import psycopg2
from psycopg2.extras import RealDictCursor
import sqlite3

NEW_SESSION_THRESH_SECS = 30. * 60.
REPEAT_THRESH_SECS = 1.

logger = get_logger("queryutils")

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
            logger.debug("[data] - Fetched user: " + name)
            user = User(name)
            user.__dict__.update(d)
            yield user 
        self.close()   

    def get_queries(self, parsed=False):
        self.connect()
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
            q = Query(text, time)
            q.__dict__.update(d)
            if parsed:
                q.parsetree = ParseTreeNode.loads(parsetree)
            yield q
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
        for row in qcursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            q = Query(text, time)
            q.__dict__.update(d)
            if parsed:
                q.parsetree = ParseTreeNode.loads(parsetree)
            yield q
        self.close()   

    def get_parsetrees(self):
        self.connect()
        cursor = self.execute("SELECT parsetree, query_id FROM parsetrees")
        for (parsetree, query_id) in cursor.fetchall():            
            p = ParseTreeNode.loads(parsetree)
            p.query_id = query_id
            yield p
        self.close()   

    def get_session_from_user(uid):
        self.connect()
        scursor = self.execute("SELECT id, user_id, session_type \
                                FROM sessions \
                                WHERE user_id = " + self.wildcard, (uid,))
        for row in scursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            session = Session(sid, user)
            session.__dict__.update(d)
            yield session
        self.close() 

    def get_query_in_session(sid, parsed=False):
        self.connect()
        if parsed:
            qcursor = self.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                            earliest_event, latest_event, range, is_realtime, \
                            splunk_search_id, execution_time, saved_search_name, \
                            user_id, session_id, parsetree \
                            FROM queries, parsetrees \
                            WHERE id = parsetrees.query_id AND session_id = " + self.wildcard, (sid,))
        else:
            qcursor = self.execute("SELECT id, text, time, is_interactive, is_suspicious, search_type, \
                            earliest_event, latest_event, range, is_realtime, \
                            splunk_search_id, execution_time, saved_search_name, \
                            user_id, session_id \
                            FROM queries \
                            WHERE session_id = " + self.wildcard, (sid,))
        for row in qcursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            q = Query(text, time)
            q.__dict__.update(d)
            if parsed:
                q.parsetree = ParseTreeNode.loads(parsetree)
            yield q
        self.close()

    def get_query_from_user(uid, parsed=False):
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
            q = Query(text, time)
            q.__dict__.update(d)
            if parsed:
                q.parsetree = ParseTreeNode.loads(row["parsetree"])
            yield q
        self.close() 

    def get_sessions(self, parsed=False):
        self.connect()
        sessions = {}
        for user in self.get_users():
            for session in self.get_session_from_user(user.id):
                sessions[sid] = session
                user.sessions[sid] = session
                for query in self.get_query_in_session(session.id, parsed=parsed):
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
                user.queries.append(q)
            user.interactive_queries = [q for q in user.queries if q.is_interactive]
            user.noninteractive_queries = [q for q in user.queries if not q.is_interactive]
            yield user
        self.close()   
    

class PostgresDB(Database):

    def __init__(self, database, user, password):
        self.database = database
        self.user = user
        self.password = password
        super(PostgresDB, self).__init__("%s")

    def connect(self):
        self.connection = psycopg2.connect(database=self.database, user=self.user, password=self.password)
        return self.connection

    def execute(self, query, *params):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, *params)
        return cursor


class SQLite3DB(Database):
    
    def __init__(self, path):
        self.path = path
        super(SQLite3DB, self).__init__("?")

    def connect(self):
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        return self.connection

    def execute(self, query, *params):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, *params)
        return cursor