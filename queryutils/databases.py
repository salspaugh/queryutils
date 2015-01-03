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
    """Represents a Database that stores Splunk query data.
    """

    def __init__(self, wildcard, dbtype):
        """Create a Database object.

        :param self: The object being created
        :type self: queryutils.databases.Database
        :param wildcard: The query param substitution character
        :type wildcard: str
        :param dbtype: The type of database (postgres or sqlite3)
        :type dbtype: str
        :rtype: queryutils.databases.Database
        """
        self.wildcard = wildcard
        self.connection = None
        self.dbtype = dbtype
        super(Database, self).__init__()

    def initialize_tables(self):
        """Initialize the tables used for analyzing Splunk queries.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
        self.initialize_users_table()
        self.initialize_sessions_table()
        self.initialize_queries_table()
        self.initialize_parsetrees_table()

    def initialize_users_table(self):
        """Initialize the table for storing Splunk user info.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
        self.execute_queries(queryutils.sql.INIT_USERS[self.dbtype])

    def initialize_sessions_table(self):
        """Initialize the table for storing Splunk session info.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
        self.execute_queries(queryutils.sql.INIT_SESSIONS[self.dbtype])

    def initialize_queries_table(self):
        """Initialize the table for storing Splunk queries.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
        self.execute_queries(queryutils.sql.INIT_QUERIES[self.dbtype])
    
    def initialize_parsetrees_table(self):
        """Initialize the table for storing parsed Splunk queries.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
        self.execute_queries(queryutils.sql.INIT_PARSETREES[self.dbtype])
   
    def execute_queries(self, queries):
        """Execute the given queries against the current database object.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param queries: The queries to execute
        :type queries: list
        :rtype: None
        """
        self.connect()
        for query in queries:
            logger.debug("Executing \n%s" % query)
            self.execute(query)
        self.close()

    def load_users_and_queries_from_source(self, module, *args):
        """Load the user and query tables from a given source.

        The source is initialized from the module and args that are passed in.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param module: The module to use to initialize the source
        :type module: module
        :param args: Arguments to initilize the module
        :type args: tuple
        :rtype: None
        """
        src = module(*args) 
        self.load_users_and_queries(src)

    def load_users_and_queries(self, src):
        """Load the user and query tables from a given source.

        The source is assumed to be initialized and to have a method called
        "get_users_with_queries".

        :param self: The current object
        :type self: queryutils.databases.Database
        :param src: The source to load users and queries from
        :type src: queryutils.source.DataSource
        :rtype: None
        """
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
        """Parse the queries and load them into the parsetree table.

        Each query is read from the query table, which is assumed to be
        populated, and then the result is loaded into the parsetree table.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
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

    def insert_user(self, user, uid):
        """Insert user data into the user table.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param user: The user to insert
        :type user: queryutils.user.User
        :param uid: The ID to assign to the inserted user
        :type uid: int
        :rtype: None
        """
        self.execute("INSERT INTO users \
                (id, name, case_id, user_type) \
                VALUES (" + ",".join([self.wildcard]*4) + ")", 
                (uid, user.name, user.case_id, user.user_type))
        self.commit()

    def insert_query(self, query, qid, uid, sid):
        """Insert query data into the query table.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param query: The query to insert
        :type query: queryutils.query.Query
        :param qid: The ID to assign to the inserted query
        :type qid: int
        :param uid: The ID of the user the query belongs to
        :type uid: int
        :param sid: The ID of the session the query belongs to
        :type sid: int or None
        :rtype: None
        """
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
        """The parsed query to insert into the parsetree table.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param parsetree: The parsed query to insert
        :type parsetree: splparser.parsetree.ParseTreeNode
        :rtype: None
        """
        self.execute("INSERT INTO parsetrees \
                (parsetree, query_id) \
                VALUES (" + ", ".join([self.wildcard]*2) +")",
                (parsetree.dumps(), parsetree.query_id))
        self.commit()

    def close(self):
        """Close the connection to the database.
        
        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
        if self.connection:
            self.connection.close()
            self.connection = None

    def commit(self):
        """Commit the latest query.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
        if self.connection:
            self.connection.commit()

    def get_users(self):
        """Get the users from the current database.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: generator
        """
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
        """Get the users from the current database with queries.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param parsed: Whether or not to include the query parsetree
        :type parsed: bool
        :rtype: generator
        """
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
        """Returns the list of columns of the query table as a string.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: str
        """
        return ", ".join(["id", "text", "time", "is_interactive", 
            "is_suspicious", "search_type", "earliest_event", "latest_event", 
            "range", "is_realtime", "splunk_search_id", "execution_time", 
            "saved_search_name", "user_id", "session_id"]) 

    def _form_query_from_data(self, row, parsed):
        """Create a query from a row from the query table.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param row: The row fetched from the database
        :type row: dict
        :param parsed: Whether or not the row contains parsetree data 
        :type parsed: bool
        :rtype: queryutils.query.Query
        """
        d = { k:row[k] for k in row.keys() }
        q = Query(row["text"], row["time"])
        q.__dict__.update(d)
        if parsed:
            q.parsetree = ParseTreeNode.loads(row["parsetree"])
        return q

    def get_queries(self, querytype=QueryType.ALL, parsed=False):
        """A generator over all the queries of the given type from the database.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param querytype: The type of query to return
        :type querytype: str
        :param parsed: Whether or not to return the parsetree for the query too
        :type parsed: bool
        :rtype: generator
        """
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
        """A generator that returns all the queries from the given session.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param sid: The session to fetch queries from
        :type sid: int
        :param parsed: Whether to return the parsed version of the queries
        :type parsed: bool
        :param bad: Whether to return "bad" (mislabeled as interactive) queries 
        :type bad: bool
        :rtype: generator
        """
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
        """Get all interactive queries that match the given text.
        
        :param self: The current object
        :type self: queryutils.databases.Database
        :param text: The text that the retrieved queries should match
        :type text: str
        :param parsed: Whether to return the parsed version of the queries
        :type parsed: bool
        :rtype: generator
        """
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
        """Return the parsed queries from the parsetree table. 
        
        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: generator
        """
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
        """Return all the sessions

        :param self: The current object
        :type self: queryutils.databases.Database
        :param parsed: Whether to return the parsed version of the queries
        :type parsed: bool
        :param bad: Whether to return "bad" (mislabeled as interactive) queries 
        :type bad: bool
        :rtype: generator
        """
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
        """Generator that returns the sessions from the user with the given ID. 

        :param self: The current object
        :type self: queryutils.databases.Database
        :param uid: The ID of the user for which to fetch seesions
        :type uid: int
        :param bad: Whether to return "bad" (mislabeled as interactive) queries 
        :type bad: bool
        :rtype: generator
        """
        self.connect()
        table = "sessions"
        if bad:
            table = "bad_sessions"
        sql = "SELECT id, user_id, session_type FROM %s \
                WHERE user_id=%s" % (table, self.wildcard)
        scursor = self.execute(sql, (uid,))
        for row in scursor.fetchall():
            d = { k:row[k] for k in row.keys() }
            session = Session(row["id"], row["user_id"])
            session.__dict__.update(d)
            yield session
        self.close()

    def mark_suspicious_users(self):
        """Mark the users that are probably machine or system users.
       
        These users are listed in queryutils.databases.SUSPICIOUS_USER_NAMES.

        :param self: The current object
        :type self: queryutils.databases.Database
        :rtype: None
        """
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

    def sessionize_queries(self, remove_suspicious=False):
        """Form sessions from queries and update the session and query tables.

        :param self: The current object
        :type self: queryutils.databases.Database
        :param remove_suspicious: Don't include suspicious queries in sessions
        :type remove_suspicious: bool
        :rtype: None
        """
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
    """Representes a Postgres database that stores query data.
    """

    def __init__(self, database, user, password):
        """Create a PostgresDB object.

        :param self: The object being created
        :type self: queryutils.databases.PostgresDB
        :param path: The path to the data to load
        :type path: str
        :rtype: queryutils.databases.PostgresDB
        """
        self.database = database
        self.user = user
        self.password = password
        super(PostgresDB, self).__init__("%s", "postgres")

    def connect(self):
        """Connect to the database object.

        :param self: The object being created
        :type self: queryutils.databases.PostgresDB
        :rtype: None
        """
        if self.connection is not None:
            return self.connection
        self.connection = psycopg2.connect(database=self.database, user=self.user, password=self.password)
        return self.connection

    def execute(self, query, *params):
        """Execute the given query against the current database.

        :param self: The object being created
        :type self: queryutils.databases.PostgresDB
        :param query: The query to execute
        :type query: str 
        :param params: The parameters to the query
        :type params: tuple
        :rtype: psycopg2._psycopg.cursor
        """
        if self.connection is None:
            self.connect()
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, *params)
        return cursor


class SQLite3DB(Database):
    """Representes a SQLite database that stores query data.
    """

    def __init__(self, path):
        """Create a SQLite3DB object.

        :param self: The object being created
        :type self: queryutils.databases.SQLite3DB
        :param path: The path to the data to load
        :type path: str
        :rtype: queryutils.databases.SQLite3DB
        """
        self.path = path
        super(SQLite3DB, self).__init__("?", "sqlite3")

    def connect(self):
        """Connect to the database object.

        :param self: The object being created
        :type self: queryutils.databases.SQLite3DB
        :rtype: None
        """
        if self.connection is not None:
            return self.connection
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        return self.connection

    def execute(self, query, *params):
        """Execute the given query against the current database.

        :param self: The object being created
        :type self: queryutils.databases.SQLite3DB
        :param query: The query to execute
        :type query: str 
        :param params: The parameters to the query
        :type params: tuple
        :rtype: sqlite3.Connection
        """
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, *params)
        return cursor
