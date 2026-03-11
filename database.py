import sqlite3
import os

DB_FILE = os.path.join(os.path.dirname(__file__), 'data', 'proverbs.db')

def get_connection():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    return sqlite3.connect(DB_FILE)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proverbs (
            id INTEGER PRIMARY KEY,
            date TEXT,
            text TEXT,
            views INTEGER,
            forwards INTEGER
        )
    ''')
    conn.commit()
    conn.close()

# Initialize the database when the module is imported
init_db()

def save_proverbs(proverbs, append=True):
    """
    Saves a list of proverb dictionaries to the SQLite database.
    :param proverbs: List of dicts with message data.
    :param append: Ignored for SQLite, kept for signature compatibility.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Using context manager for automatic commit/rollback
        with conn:
            for entry in proverbs:
                cursor.execute('''
                    INSERT OR IGNORE INTO proverbs (id, date, text, views, forwards)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    entry.get('id'),
                    entry.get('date'),
                    entry.get('text'),
                    entry.get('views', 0),
                    entry.get('forwards', 0)
                ))
    finally:
        conn.close()

def get_last_message_id():
    """
    Reads the last message ID from the database.
    Returns 0 if the table is empty.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(id) FROM proverbs')
        result = cursor.fetchone()[0]
        return result if result is not None else 0
    finally:
        conn.close()

def get_all_proverbs():
    """
    Yields all proverbs from the database as dictionaries.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, date, text, views, forwards FROM proverbs ORDER BY id ASC')
        
        columns = [col[0] for col in cursor.description]
        for row in cursor.fetchall():
            yield dict(zip(columns, row))
    finally:
        conn.close()
