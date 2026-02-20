\
import pyodbc
from config.db_config import DB_CONNECTION_STRING

def get_db_connection():
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    conn.autocommit = False
    return conn

def fetch_all(sql: str, params=None):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return rows
    finally:
        conn.close()

def fetch_one(sql: str, params=None):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        row = cur.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))
    finally:
        conn.close()

def execute(sql: str, params=None):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def execute_scalar(sql: str, params=None):
    r = fetch_one(sql, params)
    if not r:
        return None
    return next(iter(r.values()))
