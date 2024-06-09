import json
import sqlite3
from tqdm import tqdm

def create_level1_table(cursor):
    """Create the level1 table in the SQLite database."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS level1alt (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keys TEXT,
            name TEXT,
            names TEXT
        )
    ''')

def insert_record(cursor, name, value, names):
    """Insert a record into the SQLite database."""
    cursor.execute('''
        INSERT INTO level1alt (name, keys, names)
        VALUES (?, ?, ?)
    ''', (name, value, names))

def get_value_from_db(cursor, key, column="key"):
    """Retrieve the JSON-loaded value associated with a single key from the database."""
    cursor.execute(f'SELECT emb_idx, value FROM records WHERE {column}=?', (key,))
    row = cursor.fetchone()
    return row[0], json.loads(row[1]) if row else None

def get_values_from_db(cursor, keys, column="key"):
    """Retrieve the JSON-loaded values associated with a list of keys from the database."""
    placeholders = ','.join(['?'] * len(keys))  # Create placeholders for the keys
    cursor.execute(f'SELECT key, emb_idx, value FROM records WHERE {column} IN ({placeholders})', keys)
    rows = cursor.fetchall()
    return [[row[0], {row[1]: json.loads(row[2])}] for row in rows]

def find_coreferential_groups(original, name, keys, cursor):
    """Find the largest group of keys that are co-referential."""
    key_to_links = get_values_from_db(cursor, keys, "emb_idx")
    emb_idcs = {original}
    names = {name}
    candidates = set(keys)

    for elem in key_to_links:
        for k,v in elem[1].items():
            intersection = candidates.intersection(set(v))
            if len(intersection) >= 0.8 * len(v):
                emb_idcs.add(k)
                names.add(elem[0])
            else:
                pass
    return emb_idcs, names

def process_key(db_filename, key):
    """Process a given key to find and store co-referential groups."""
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    
    create_level1_table(cursor)
    used = set()
    with open(key, 'r') as inj:
        mykeys = json.load(inj)
    for k in tqdm(mykeys, desc="Processing keys"):
        if k not in used:
            original, links = get_value_from_db(cursor, k, "key")
            if not links: continue
            coreferential_groups, used_names = find_coreferential_groups(original,k, links, cursor)
            used = used | used_names
            insert_record(cursor, k, json.dumps(list(coreferential_groups)), json.dumps(list(used_names)))
            conn.commit()
        else:
            continue
    conn.close()


# Example usage
db_filename = 'wikilinksdata.db'
allkeys = 'link_impact.json'
process_key(db_filename, allkeys)
