DROP TABLE IF EXISTS queries;
CREATE TABLE queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT NOT NULL,
    time REAL,
    autogenerated INTEGER,
    searchtype TEXT,
    earliest_event REAL,
    latest_event REAL,
    range REAL,
    is_realtime INTEGER,
    splunk_search_id TEXT,
    runtime REAL,
    splunk_savedsearch_name TEXT,
    user_id INTEGER REFERENCES users(id),
    session_id INTEGER REFERENCES sessions(id),
    CONSTRAINT issuing_user FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT containing_session FOREIGN KEY (session_id) REFERENCES session(id)
);

DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    name TEXT,
    company TEXT,
    user_type TEXT
);

DROP TABLE IF EXISTS sessions;
CREATE TABLE sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    type TEXT,
    CONSTRAINT owner FOREIGN KEY (user_id) REFERENCES users(id)
);
