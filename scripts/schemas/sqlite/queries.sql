DROP TABLE IF EXISTS queries;
CREATE TABLE queries (
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
);
