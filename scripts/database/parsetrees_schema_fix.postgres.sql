BEGIN TRANSACTION;
SET CONSTRAINTS ALL DEFERRED;

DROP TABLE IF EXISTS parsetrees_fix;
CREATE TABLE parsetrees_fix (
    id SERIAL PRIMARY KEY,
    parsetree TEXT NOT NULL,
    query_id INTEGER REFERENCES queries_fix(id),
    CONSTRAINT query FOREIGN KEY (query_id) REFERENCES queries_fix(id)
);

COMMIT;
