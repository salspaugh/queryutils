DROP TABLE IF EXISTS parsetrees;
CREATE TABLE parsetrees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parsetree TEXT NOT NULL,
    query_id INTEGER REFERENCES queries(id),
    CONSTRAINT query FOREIGN KEY (query_id) REFERENCES queries(id)
);
