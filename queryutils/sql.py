INIT_USERS = {
    "postgres": [
        "BEGIN TRANSACTION;",
        "SET CONSTRAINTS ALL DEFERRED;",
        "DROP TABLE IF EXISTS users CASCADE;",
        """CREATE TABLE users (
            id SERIAL PRIMARY KEY, 
            name TEXT,
            case_id TEXT,
            user_type TEXT
        );""",
        "COMMIT;"
    ],
    "sqlite3":[
        "DROP TABLE IF EXISTS users;",
        """CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            name TEXT,
            case_id TEXT,
            user_type TEXT
        );"""
    ]
}

INIT_SESSIONS = {
    "postgres": [
        "BEGIN TRANSACTION;",
        "SET CONSTRAINTS ALL DEFERRED;",
        "DROP TABLE IF EXISTS sessions CASCADE;",
        """CREATE TABLE sessions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER, -- REFERENCES users(id),
            session_type TEXT,
            CONSTRAINT owner FOREIGN KEY (user_id) REFERENCES users(id) DEFERRABLE INITIALLY IMMEDIATE
        );""",
        "COMMIT;"
    ],
    "sqlite3":[
        "DROP TABLE IF EXISTS sessions;",
        """CREATE TABLE sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            session_type TEXT,
            CONSTRAINT owner FOREIGN KEY (user_id) REFERENCES users(id)
        );"""
    ]
}

INIT_QUERIES = {
    "postgres": [
        "BEGIN TRANSACTION;",
        "SET CONSTRAINTS ALL DEFERRED;",
        "DROP TABLE IF EXISTS queries CASCADE;",
        """CREATE TABLE queries (
            id SERIAL PRIMARY KEY,
            text TEXT NOT NULL,
            time DOUBLE PRECISION,
            is_interactive BOOLEAN,
            is_suspicious BOOLEAN,
            execution_time DOUBLE PRECISION,
            earliest_event DOUBLE PRECISION,
            latest_event DOUBLE PRECISION,
            range DOUBLE PRECISION,
            is_realtime BOOLEAN,
            search_type TEXT,
            splunk_search_id TEXT,
            saved_search_name TEXT,
            user_id INTEGER, -- REFERENCES users(id),
            session_id INTEGER, -- REFERENCES sessions(id),
            CONSTRAINT issuing_user FOREIGN KEY (user_id) REFERENCES users(id) DEFERRABLE INITIALLY IMMEDIATE,
            CONSTRAINT containing_session FOREIGN KEY (session_id) REFERENCES sessions(id) DEFERRABLE INITIALLY IMMEDIATE
        );""",
        "COMMIT;"
    ],
    "sqlite3":[
        "DROP TABLE IF EXISTS queries;",
        """CREATE TABLE queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            time REAL,
            is_interactive INTEGER,
            is_suspicious INTEGER,
            execution_time REAL,
            earliest_event REAL,
            latest_event REAL,
            range REAL,
            is_realtime INTEGER,
            search_type TEXT,
            splunk_search_id TEXT,
            saved_search_name TEXT,
            user_id INTEGER REFERENCES users(id),
            session_id INTEGER REFERENCES sessions(id),
            CONSTRAINT issuing_user FOREIGN KEY (user_id) REFERENCES users(id),
            CONSTRAINT containing_session FOREIGN KEY (session_id) REFERENCES sessions(id)
        );"""
    ]
}

INIT_PARSETREES = {
    "postgres": [
        "BEGIN TRANSACTION;",
        "SET CONSTRAINTS ALL DEFERRED;",
        "DROP TABLE IF EXISTS parsetrees;",
        """CREATE TABLE parsetrees (
            id SERIAL PRIMARY KEY,
            parsetree TEXT NOT NULL,
            query_id INTEGER REFERENCES queries(id),
            CONSTRAINT query FOREIGN KEY (query_id) REFERENCES queries(id)
        );""",
        "COMMIT;"
    ],
    "sqlite3":[
        "DROP TABLE IF EXISTS parsetrees;",
        """CREATE TABLE parsetrees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parsetree TEXT NOT NULL,
            query_id INTEGER REFERENCES queries(id),
            CONSTRAINT query FOREIGN KEY (query_id) REFERENCES queries(id)
        );"""
    ]
}
