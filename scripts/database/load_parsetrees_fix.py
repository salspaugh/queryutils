from argparse import ArgumentParser
from queryutils.parse import parse_query
from psycopg2 import connect
from os import path
from contextlib import closing

DATABASE = "lupe.db"
USER = PASSWORD = "lupe"


def main():
    load_table()


def load_table():
    db = connect(database=DATABASE, user=USER, password=PASSWORD)
    cursor = db.cursor()
    cursor.execute("SELECT id, text FROM queries_fix")
    for (qid, query) in cursor.fetchall():
        p = parse_query(query)
        if p is not None:
            print "Inserting parsed query."
            d = p.dumps()
            insert_parsetree(db, d, qid)
    db.close()
    

def insert_parsetree(db, parsetree, qid):
    cursor = db.cursor()
    cursor.execute("INSERT INTO parsetrees_fix (parsetree, query_id) VALUES (%s,%s)", [parsetree, qid])
    db.commit()


if __name__ == "__main__":
    main()
