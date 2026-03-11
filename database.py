import json
import os

DATA_FILE = os.path.join(os.path.dirname(__file__), 'data', 'proverbs.jsonl')

def save_proverbs(proverbs, append=True):
    """
    Saves a list of proverb dictionaries to the jsonl file.
    :param proverbs: List of dicts with message data.
    :param append: If True, appends to the file. If False, overwrites.
    """
    mode = 'a' if append else 'w'
    
    # Ensure directory exists (just in case)
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    
    with open(DATA_FILE, mode, encoding='utf-8') as f:
        for entry in proverbs:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')

def get_last_message_id():
    """
    Reads the last message ID from the jsonl file.
    Returns 0 if the file doesn't exist or is empty.
    """
    if not os.path.exists(DATA_FILE):
        return 0
    
    last_id = 0
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        entry = json.loads(line)
                        last_id = max(last_id, entry.get('id', 0))
                    except json.JSONDecodeError:
                        continue
    except Exception as e:
        print(f"Error reading last message ID: {e}")
        
    return last_id

def get_all_proverbs():
    """
    Yields all proverbs from the file as dictionaries.
    """
    if not os.path.exists(DATA_FILE):
        return
    
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)
