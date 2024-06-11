import json

def load_json(filename):
    """Load a JSON file and return the data."""
    with open(filename, 'r') as f:
        return json.load(f)

def save_json(data, filename):
    """Save data to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def remove_keys_containing(data, substring):
    """Remove keys containing the specified substring from the dictionary."""
    keys_to_remove = [key for key in data if substring in key]
    for key in keys_to_remove:
        del data[key]

def sort_dict_by_values(data, reverse=False):
    """Sort the dictionary by its values."""
    return dict(sorted(data.items(), key=lambda item: item[1], reverse=reverse))

def process_link_impact_json(filename, substring, reverse=True):
    """Load the JSON file, process it, and save the updated data back to the file."""
    # Load the JSON file
    data = load_json(filename)
    
    # Remove keys containing the specified substring
    remove_keys_containing(data, substring)
    
    # Sort the dictionary by its values
    sorted_data = sort_dict_by_values(data, reverse=reverse)
    
    # Save the updated data back to the file
    save_json(sorted_data, filename)
    print(f"Processed and updated {filename}")

# Example usage
filename = 'link_impact.json'
substring = 'Wikipedia'
process_link_impact_json(filename, substring)
