import sqlite3
import json
import signal
import sys
from tqdm import tqdm

def load_reverse_mapping(filename):
    """Load the reverse mapping JSON file."""
    with open(filename, 'r') as f:
        return json.load(f)

def create_output_table(cursor):
    """Create the output table in the SQLite database."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS embeddings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emb_idx INTEGER,
            "values" TEXT
        )
    ''')

def process_batch(batch, reverse_mapping, key_list):
    """Process a batch of records."""
    processed_records = []
    for record in tqdm(batch, desc="Processing batch"):
        emb_idx = record[2]
        values = json.loads(record[3])

        if not values:
            continue

        # Initialize the result vector
        result_vector = {key: 0 for key in key_list}
        
        # Process the emb_idx
        if str(emb_idx) in reverse_mapping:
            for idx, value in reverse_mapping[str(emb_idx)].items():
                result_vector[int(idx)] += value * 2
        
        # Process the values
        for val in values:
            if str(val) in reverse_mapping:
                for idx, value in reverse_mapping[str(val)].items():
                    result_vector[int(idx)] += value
        
        # Convert result vector to sparse format
        sparse_result = [[k, v] for k, v in result_vector.items() if v != 0]
        
        processed_records.append((emb_idx, json.dumps(sparse_result)))
    
    return processed_records

def write_batch_to_db(batch, db_filename):
    """Write a batch of processed records to the output table."""
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO embeddings (emb_idx, "values") VALUES (?, ?)', batch)
    conn.commit()
    conn.close()

def process_records(db_filename, reverse_mapping, key_list, batch_size=32000):
    """Process all records in the database in batches."""
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    create_output_table(cursor)

    offset = 6000000
    total_records = cursor.execute('SELECT COUNT(*) FROM records').fetchone()[0]

    with tqdm(total=total_records, desc="Processing records") as pbar:
        while offset < total_records:
            cursor.execute('SELECT * FROM records LIMIT ? OFFSET ?', (batch_size, offset))
            batch = cursor.fetchall()

            if not batch:
                break

            processed_batch = process_batch(batch, reverse_mapping, key_list)

            # Write the processed batch to the database
            write_batch_to_db(processed_batch, db_filename)
            
            pbar.update(batch_size)
            offset += batch_size

    cursor.close()
    conn.close()

def cleanup(conn, cursor):
    """Close the database connection and cursor."""
    if conn:
        conn.close()
    if cursor:
        cursor.close()

def signal_handler(sig, frame):
    """Handle SIGINT (Ctrl+C) to clean up resources."""
    print("Interrupt received, cleaning up...")
    cleanup(conn, cursor)
    sys.exit(0)

def main():
    global conn, cursor
    conn = None
    cursor = None

    reverse_mapping_filename = 'reverse_mapping.json'
    db_filename = 'database/wikilinksdata_1.db'
    
    reverse_mapping = load_reverse_mapping(reverse_mapping_filename)
    key_list = list(range(10000))  # Assuming 10k embeddings
    
    # Register the signal handler
    signal.signal(signal.SIGINT, signal_handler)

    process_records(db_filename, reverse_mapping, key_list)

if __name__ == '__main__':
    main()
