import json
from tqdm import tqdm

def load_jsonl(file_path):
    """Load the JSONL file into a dictionary, skipping entries with values of length 1."""
    data = {}
    with open(file_path, 'r') as f:
        for line in tqdm(f, desc="Loading JSON"):
            entry = json.loads(line.strip())
            for key, value in entry.items():
                if isinstance(value, list) and len(value) > 1:  # Only include if the value list has more than 1 item
                    data[key] = value
    return data

def process_links_iteratively(master_dict, key, max_depth):
    """Iteratively process links up to a specified depth and count the traversals."""
    connections = {}
    queue = [(key, 0)]  # Initialize the queue with the starting key and depth

    while queue:
        current_key, depth = queue.pop(0)  # Dequeue the next key-depth pair
        if depth < max_depth and current_key in master_dict and isinstance(master_dict[current_key ], list):
            for sub_key in master_dict[current_key]:
                if sub_key not in connections:
                    connections[sub_key] = 0
                connections[sub_key] += 1  # Count the traversal
                queue.append((sub_key, depth + 1))  # Enqueue the sub_key with incremented depth

    connections = dict(sorted(connections.items(), key=lambda item: item[1], reverse=True))  # Sort connections by value
    return connections

def process_and_write_jsonl(input_file, output_file, max_depth=3):
    """Process each entry in the JSONL file and write results incrementally."""
    master_dict = load_jsonl(input_file)  # Load the JSONL file into memory
    presidents = [
    "Ronald Reagan",
    "James K. Polk",
    "Chester A. Arthur",
    "Woodrow Wilson",
    "George H. W. Bush",
    "Grover Cleveland",
    "James Madison",
    "James Buchanan",
    "Presidential Issue",
    "Andrew Jackson",
    "Warren G. Harding",
    "James A. Garfield",
    "Harry S. Truman",
    "Herbert Hoover",
    "George Washington",
    "William Henry Harrison",
    "Jimmy Carter",
    "Millard Fillmore",
    "John Tyler",
    "Theodore Roosevelt",
    "Lyndon B. Johnson",
    "Richard Nixon",
    "Zachary Taylor",
    "Bill Clinton",
    "George W. Bush",
    "Calvin Coolidge",
    "Ulysses S. Grant",
    "John Adams",
    "Martin Van Buren",
    "Benjamin Harrison",
    "Timeline of Presidents of the United States",
    "Gerald Ford",
    "Andrew Johnson",
    "John Quincy Adams",
    "Thomas Jefferson",
    "Franklin Pierce",
    "James Monroe",
    "Abraham Lincoln",
    "William McKinley",
    "John F. Kennedy",
    "William Howard Taft",
    "Dwight D. Eisenhower",
    "Franklin D. Roosevelt"
]

    with open(output_file, 'w') as f:
        #for key, value in tqdm(master_dict.items(),desc="Processing Master dict"):  # Iterate through each key-value pair
        for pres in presidents:
            if pres not in master_dict:
                print(f"This president {pres} is not in the master dict, skipping")
                continue
            key = pres
            value = master_dict[pres]
            if isinstance(value, list) and len(value) > 1:  # Process only if the value is a non-trivial list
                processed_data = process_links_iteratively(master_dict, key, max_depth)
                json.dump({key: processed_data}, f, ensure_ascii=False)  # Write the processed data to the output file
                f.write('\n')  # Ensure each JSON object is on a new line

if __name__ == "__main__":
    input_file = 'wiki_knowledge/links.jsonl'
    output_file = 'wiki_knowledge/presidents.jsonl'
    process_and_write_jsonl(input_file, output_file, max_depth=2)
