ALTER TABLE queries ADD COLUMN bad_session_id INTEGER;
ALTER TABLE queries ADD CONSTRAINT containing_bad_session FOREIGN KEY (bad_session_id) REFERENCES bad_sessions(id) DEFERRABLE INITIALLY IMMEDIATE;

CREATE INDEX bad_sessions_user_id ON bad_sessions(user_id);
CREATE INDEX queries_bad_session_id ON queries(bad_session_id);

