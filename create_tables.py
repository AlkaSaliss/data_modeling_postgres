import psycopg2
import os
from pathlib import Path
import json
from sql_queries import create_table_queries, drop_table_queries

# Get current folder path and load the json containing connection credentials
# the json file must named config.json
with open(os.path.join(Path(__file__).parent.absolute(), "config.json")) as f:
    CONFIG = json.load(f)


def create_database():
    """Connect to the default database and drop the sparkifydb if it exists, 
    then recreates it and return a cursor and connection to sparkifydb
    """
    # connect to default database
    try:
        conn = psycopg2.connect(
            f"host={CONFIG['host']} dbname={CONFIG['dbname']} user={CONFIG['user']} password={CONFIG['password']}")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Can't connect and get cursor from the default Database")
        print(e)

    try:
        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("Can't Drop database sparkifydb")
        print(e)

    try:
        cur.execute(
            "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Can't create Database sparkifydb")
        print(e)
    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect(
            f"host={CONFIG['host']} dbname=sparkifydb user={CONFIG['user']} password={CONFIG['password']}")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Can't connect and get cursor from the Database sparkifydb!")
        print(e)

    return cur, conn


def drop_tables(cur, conn):
    """Uses cursor `cur` and connection `conn` from sparkifydb to execute 
    drop tables queries defined in the sql_queries.py scripts
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Can't execute query : {query}")
            print(e)


def create_tables(cur, conn):
    """Uses cursor `cur` and connection `conn` from sparkifydb to execute 
    create tables queries defined in the sql_queries.py scripts
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Can't execute query : {query}")
            print(e)


def main():
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
