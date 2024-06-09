import json

def process_verbs(input_path, output_path, threshold=135000):
    # Read the input JSON file
    with open(input_path, 'r') as file:
        data = json.load(file)
    
    # Filter out entries with counts over the threshold
    filtered_verbs = {verb: count for verb, count in data.items() if count <= threshold}
    
    # Get the sorted list of verbs
    sorted_verbs = sorted(filtered_verbs.keys())
    
    # Build the indexing dictionary
    index_dict = {verb: idx + 1 for idx, verb in enumerate(sorted_verbs)}
    
    # Save the indexing dictionary to the output JSON file
    with open(output_path, 'w') as out_file:
        json.dump(index_dict, out_file, indent=4, ensure_ascii=False)
    
    print(f"Processed verbs saved to {output_path}")

# Example usage
if __name__ == "__main__":
    input_path = 'wiki_knowledge/verb_counts_all.json'  # Replace with your input JSON file path
    output_path = 'wiki_knowledge/processed_verbs.json'  # Replace with your output JSON file path
    process_verbs(input_path, output_path)
