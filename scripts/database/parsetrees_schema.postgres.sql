BEGIN TRANSACTION;
SET CONSTRAINTS ALL DEFERRED;

DROP TABLE IF EXISTS parsetrees;
CREATE TABLE parsetrees (
    id SERIAL PRIMARY KEY,
    parsetree TEXT NOT NULL,
    query_id INTEGER REFERENCES queries(id),
    CONSTRAINT query FOREIGN KEY (query_id) REFERENCES queries(id)
);

COMMIT;
