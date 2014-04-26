DROP TABLE IF EXISTS sessions;
CREATE TABLE sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    session_type TEXT,
    CONSTRAINT owner FOREIGN KEY (user_id) REFERENCES users(id)
);
