from argparse import ArgumentParser
from queryutils.parse import parse_query
from sqlite3 import connect
from os import path
from contextlib import closing

thisdir = path.dirname(path.realpath(__file__))
SCHEMA = path.join(thisdir, "parsetrees_schema.sql")


def main(database, schema):
    init_table(database, schema) 
    load_table(database)


def init_table(database, schema):
    execute_db_script(database, schema)


def execute_db_script(database, script):
    with closing(connect(database)) as db:
        with open(script) as f:
            db.cursor().executescript(f.read())
        db.commit()


def load_table(database):
    db = connect(database)
    cursor = db.execute("SELECT id, text FROM queries")
    for (qid, query) in cursor.fetchall():
        p = parse_query(query)
        if p is not None:
            print "Inserting parsed query."
            d = p.dumps()
            insert_parsetree(db, d, qid)
    db.close()
    

def insert_parsetree(db, parsetree, qid):
    cursor = db.cursor()
    cursor.execute("INSERT INTO parsetrees (parsetree, query_id) VALUES (?,?)", [parsetree, qid])
    db.commit()


if __name__ == "__main__":

    parser = ArgumentParser("Loads parsed queries into the database.")
    parser.add_argument("database", metavar="DATABASE",
                        help="The name of the sqlite3 database to load.")
    args = parser.parse_args()
    main(args.database, SCHEMA)
