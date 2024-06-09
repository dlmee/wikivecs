import sqlite3
import os
import signal
import sys
from tqdm import tqdm

def create_level1_table(cursor):
    """Create the level1unified table in the SQLite database."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS level1unified (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keys TEXT,
            name TEXT,
            names TEXT
        )
    ''')

def merge_databases(input_folder, merged_db):
    global conn, cursor
    conn = sqlite3.connect(merged_db)
    cursor = conn.cursor()
    create_level1_table(cursor)

    # List all database files in the input folder
    db_files = [os.path.join(input_folder, file) for file in os.listdir(input_folder) if file.endswith('.db')]

    for db in tqdm(db_files, desc="Merging databases"):
        print(f"Merging {db} into {merged_db}")
        temp_conn = sqlite3.connect(db)
        temp_cursor = temp_conn.cursor()
        
        # Copy data from the temporary database to the merged database
        temp_cursor.execute('SELECT keys, name, names FROM level1alt')
        rows = temp_cursor.fetchall()
        cursor.executemany('INSERT INTO level1unified (keys, name, names) VALUES (?, ?, ?)', rows)
        conn.commit()
        
        temp_conn.close()

    conn.close()

def cleanup():
    """Close the database connections and clean up resources."""
    if conn:
        conn.close()
        print("Closed the merged database connection.")
    if cursor:
        cursor.close()
        print("Closed the merged database cursor.")

def signal_handler(sig, frame):
    """Handle SIGINT (Ctrl+C) to clean up resources."""
    print("Interrupt received, cleaning up...")
    cleanup()
    sys.exit(0)

def main():
    global conn, cursor
    conn = None
    cursor = None
    input_folder = 'database/level1'
    merged_db = 'database/wikilinks_main.db'
    
    # Remove the merged database if it already exists
    if os.path.exists(merged_db):
        os.remove(merged_db)
    
    # Register the signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    merge_databases(input_folder, merged_db)

if __name__ == '__main__':
    main()
