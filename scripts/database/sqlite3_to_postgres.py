import sqlite3
import pyscopg2


def transfer(sqlite_db, postgres_db, postgres_user, postgres_pw, postgres_port, postgres_host):
    sdb = sqlite_connect(sqlitedb)
    pdb = postgres_connect(postgres_db, postgres_user, postgres_pw, postgres_port, postgres_host)
    transfer_users(sdb, pdb)
    sdb.close()
    pdb.close()

def sqlite_connect(db):
    return sqlite3.connect(db)

def postgres_connect(db, user, pw, port, host):
    return pyscopg2.connect(db, user, pw, port, host)

def transfer_users(sqlite, postgres):
    sqlite_cursor = sqlite.execute("SELECT id, name, company FROM users")
    postgres_cursor = postgres.cursor()
    for (uid, name, company) in sqlite_cursor.fetchall():
        postgres_cursor.execute("INSERT INTO users (id, name, company) \
                                 VALUES (?,?, ?)", 
                                 [id, username, company])

def transfer_sessions():
    pass

def transfer_queries():
    pass

def transfer_parsetrees():
    pass
