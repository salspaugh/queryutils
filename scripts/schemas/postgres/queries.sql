BEGIN TRANSACTION;
SET CONSTRAINTS ALL DEFERRED;

DROP TABLE IF EXISTS queries CASCADE;
CREATE TABLE queries (
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
);

COMMIT;
