BEGIN TRANSACTION;
SET CONSTRAINTS ALL DEFERRED;

DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id SERIAL PRIMARY KEY, 
    name TEXT,
    company TEXT,
    user_type TEXT
);

DROP TABLE IF EXISTS sessions;
CREATE TABLE sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER, -- REFERENCES users(id),
    session_type TEXT,
    CONSTRAINT owner FOREIGN KEY (user_id) REFERENCES users(id) DEFERRABLE INITIALLY IMMEDIATE
);

DROP TABLE IF EXISTS queries;
CREATE TABLE queries (
    id SERIAL PRIMARY KEY,
    text TEXT NOT NULL,
    time REAL,
    autogenerated INT,
    searchtype TEXT,
    earliest_event REAL,
    latest_event REAL,
    range REAL,
    is_realtime INT,
    splunk_search_id TEXT,
    runtime REAL,
    splunk_savedsearch_name TEXT,
    user_id INTEGER, -- REFERENCES users(id),
    session_id INTEGER, -- REFERENCES sessions(id),
    CONSTRAINT issuing_user FOREIGN KEY (user_id) REFERENCES users(id) DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT containing_session FOREIGN KEY (session_id) REFERENCES sessions(id) DEFERRABLE INITIALLY IMMEDIATE
);

COMMIT;
