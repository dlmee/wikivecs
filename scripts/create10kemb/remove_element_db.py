import sqlite3

def delete_keys_containing(db_filename, substring):
    """Delete keys containing the specified substring from the database."""
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    
    # Find and delete keys containing the substring
    cursor.execute('''
        DELETE FROM records WHERE key LIKE ?
    ''', ('%' + substring + '%',))
    
    # Commit the changes
    conn.commit()
    conn.close()
    print(f"Deleted records with keys containing '{substring}'")

# Example usage
db_filename = 'wikilinksdata.db'
substring = 'Wikipedia'
delete_keys_containing(db_filename, substring)
