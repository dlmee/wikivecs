import sqlite3
import json
from collections import defaultdict
from tqdm import tqdm

def count_references(db_filename):
    """Count the references of names in the level1unified table."""
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    # Query to select all names from the level1unified table
    cursor.execute('SELECT names FROM level1unified')
    rows = cursor.fetchall()

    # Dictionary to store the counts
    reference_counts = defaultdict(int)

    for row in tqdm(rows):
        names = json.loads(row[0])
        for name in names:
            reference_counts[name] += 1

    conn.close()
    return reference_counts

def main():
    db_filename = 'database/wikilinks_main.db'
    
    # Get the reference counts
    reference_counts = count_references(db_filename)
    
    # Print the most referenced names
    sorted_counts = sorted(reference_counts.items(), key=lambda item: item[1], reverse=True)
    
    with open("sorted_l1_counts.json", "w") as outj:
        json.dump(sorted_counts, outj, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    main()
