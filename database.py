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
            forwards INTEGER,
            english_translation TEXT DEFAULT '',
            amharic_meaning TEXT DEFAULT '',
            english_meaning TEXT DEFAULT '',
            translation_source TEXT DEFAULT '',
            meaning_source TEXT DEFAULT '',
            confidence REAL DEFAULT 0.0,
            needs_review REAL DEFAULT 0.0,
            updated_at TEXT DEFAULT ''
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
                msg_id = entry.get('id')
                # Fail fast to prevent auto-increment issues with NULL primary keys
                if msg_id is None or not isinstance(msg_id, int):
                    raise ValueError(f"Invalid or missing 'id' in entry: {entry}")
                    
                cursor.execute('''
                    INSERT OR IGNORE INTO proverbs (id, date, text, views, forwards)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    msg_id,
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
        cursor.execute('SELECT * FROM proverbs ORDER BY id ASC')
        
        columns = [col[0] for col in cursor.description]
        for row in cursor.fetchall():
            yield dict(zip(columns, row))
    finally:
        conn.close()

def get_unprocessed_proverbs(limit=75):
    """
    Fetches proverbs that do not have an english_translation yet.
    Returns them as a list of dictionaries.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM proverbs WHERE english_translation = '' ORDER BY id ASC LIMIT ?", (limit,))
        
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()

def update_proverbs_ai_data(ai_results):
    """
    Updates the database with the AI-generated translations and meanings.
    :param ai_results: List of dictionaries containing the parsed AI data and original message ID.
    Example: [{"id": 1, "english_translation": "...", "confidence": 0.95...}]
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        with conn:
            for result in ai_results:
                msg_id = result.get('id')
                if msg_id is None:
                    continue
                    
                cursor.execute('''
                    UPDATE proverbs
                    SET english_translation = ?,
                        amharic_meaning = ?,
                        english_meaning = ?,
                        translation_source = ?,
                        meaning_source = ?,
                        confidence = ?,
                        needs_review = ?,
                        updated_at = datetime('now')
                    WHERE id = ?
                ''', (
                    result.get('english_translation', ''),
                    result.get('amharic_meaning', ''),
                    result.get('english_meaning', ''),
                    result.get('translation_source', ''),
                    result.get('meaning_source', ''),
                    result.get('confidence', 0.0),
                    result.get('needs_review', 0.0),
                    msg_id
                ))
    finally:
        conn.close()
