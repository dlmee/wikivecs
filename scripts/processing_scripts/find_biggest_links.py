import json
from tqdm import tqdm

def process_jsonl_to_dict(jsonl_filename):
    """Process JSONL file and create a dictionary with each first value as the key and the length of the third value as the value."""
    result_dict = {}
    
    with open(jsonl_filename, 'r') as f:
        for line in tqdm(f, desc="Processing JSONL file"):
            record = json.loads(line)
            
            # Assuming the JSON object is a list and we need the first and third values
            first_value = record[0]
            third_value = record[2]
            
            # Calculate the length of the third value
            length_of_third_value = len(third_value)
            
            # Add to the result dictionary
            result_dict[first_value] = length_of_third_value
            
    return result_dict

# Example usage
jsonl_filename = 'page_embeddings.jsonl'
result_dict = process_jsonl_to_dict(jsonl_filename)

# Print the resulting dictionary
with open("link_impact.json", "w") as outj:
    json.dump(result_dict, outj, indent=4, ensure_ascii=False)
