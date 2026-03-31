import sqlite3
from pathlib import Path

DB_PATH = Path("db/logs.db")


def get_connection(db_path=DB_PATH):
    """
    Create and return a SQLite database connection
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    return conn


def create_tables(conn):
    """
    Create logs and errors tables
    """
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT,
            timestamp TEXT,
            method TEXT,
            path TEXT,
            protocol TEXT,
            status INTEGER,
            bytes INTEGER,
            referrer TEXT,
            agent TEXT,
            hash TEXT UNIQUE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_line TEXT,
            error_reason TEXT
        )
    """)

    conn.commit()


def insert_log(conn, log_data):
    """
    Insert a parsed log into the logs table
    """
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO logs (
            ip, timestamp, method, path, protocol,
            status, bytes, referrer, agent, hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        log_data["ip"],
        log_data["timestamp"],
        log_data["method"],
        log_data["path"],
        log_data["protocol"],
        log_data["status"],
        log_data["bytes"],
        log_data["referrer"],
        log_data["agent"],
        log_data["hash"]
    ))

    conn.commit()


def insert_error(conn, raw_line, reason):
    """
    Insert malformed log lines
    """
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO errors (raw_line, error_reason)
        VALUES (?, ?)
    """, (raw_line, reason))

    conn.commit()
