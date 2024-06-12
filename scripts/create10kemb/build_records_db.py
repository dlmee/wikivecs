import json
import sqlite3
from tqdm import tqdm

def create_table(cursor):
    """Create the table in the SQLite database."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT,
            emb_idx INTEGER,
            value TEXT
        )
    ''')

def insert_record(cursor, key, myidx, value):
    """Insert a record into the SQLite database."""
    cursor.execute('''
        INSERT INTO records (key, emb_idx, value)
        VALUES (?, ?, ?)
    ''', (key, myidx, value))

def process_jsonl_to_sqlite(jsonl_filename, sqlite_db_filename, link_indices):
    """Process JSONL file and insert data into SQLite database."""
    with open(link_indices, "r") as inj:
        myindices = json.load(inj)
    check = set()
    conn = sqlite3.connect(sqlite_db_filename)
    cursor = conn.cursor()
    
    create_table(cursor)
    
    with open(jsonl_filename, 'r') as f:
        for line in tqdm(f, desc="Processing JSONL file"):
            record = json.loads(line)
            if record[0] in check:
                continue
            else:
                check.add(record[0])
            if record[0] in myindices:
                myidx = myindices[record[0]]
            else:
                myidx = "UNKNOWN"
            insert_record(cursor, record[0], myidx, json.dumps(record[2]))
                
    conn.commit()
    conn.close()
    print(f"Data inserted into {sqlite_db_filename}")


jsonl_filename = 'page_embeddings.jsonl'
link_indices = 'data/link_embeddings.json'
sqlite_db_filename = 'wikilinksdata.db'
process_jsonl_to_sqlite(jsonl_filename, sqlite_db_filename, link_indices)
