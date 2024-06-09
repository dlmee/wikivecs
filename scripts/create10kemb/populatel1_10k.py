import sqlite3
import json
from tqdm import tqdm

def load_key_list(json_filename):
    """Load the list of keys from the JSON file and return as a dictionary."""
    with open(json_filename, 'r') as f:
        keys_list = json.load(f)
    return {key: {} for key in keys_list}

def build_cooccurrence_dict(db_filename, keys_dict):
    """Build a co-occurrence dictionary from the level1unified table."""
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    # Query to select all names from the level1unified table
    cursor.execute('SELECT name, names FROM level1unified')
    rows = cursor.fetchall()

    for row in tqdm(rows, desc="Processing rows"):
        cooccurring_names = json.loads(row[1])
        for name in cooccurring_names + [row[0]]:
            if name in keys_dict:
                for co_name in cooccurring_names:
                    if co_name != name and co_name not in keys_dict:
                        if co_name not in keys_dict[name]:
                            keys_dict[name][co_name] = 1
                        else:
                            keys_dict[name][co_name] += 1

    conn.close()

    # Sort the subkeys by their count
    sorted_dict = {}
    for key, subkeys in keys_dict.items():
        sorted_dict[key] = {k: v for k, v in sorted(subkeys.items(), key=lambda item: item[1], reverse=True)}

    return sorted_dict

def main():
    json_filename = '10kl1counts.json'
    db_filename = 'database/wikilinks_main.db'

    # Load the key list and initialize the dictionary
    keys_dict = load_key_list(json_filename)
    
    # Build the co-occurrence dictionary
    cooccurrence_dict = build_cooccurrence_dict(db_filename, keys_dict)
    
    # Save the result to a new JSON file
    with open('cooccurrence_dict.json', 'w') as outj:
        json.dump(cooccurrence_dict, outj, indent=4, ensure_ascii=False)

    print("Co-occurrence dictionary has been created and saved to cooccurrence_dict.json.")

if __name__ == '__main__':
    main()
