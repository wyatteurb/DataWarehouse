import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""Dropping every table in drop_table_queries list."""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""Creating every table in drop_table_queries list."""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
"""Creating and Closing connection and Run all function which defined above"""

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()