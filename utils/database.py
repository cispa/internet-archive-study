from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD
import psycopg2

def get_conn(autocommit=False):
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PWD, database=DB_NAME)
    conn.autocommit = autocommit
    return conn
