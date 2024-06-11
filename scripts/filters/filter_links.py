import re
import json

def filter_non_latinate_keys(data):
    def is_majority_non_alphanumeric(key):
        non_alnum_count = sum(1 for char in key if not re.match(r'[a-zA-Z0-9]', char))
        return non_alnum_count > (len(key) / 2)

    filtered_data = {key: value for key, value in data.items() if not is_majority_non_alphanumeric(key)}
    
    return filtered_data

def reindex(data):
    return {key: idx + 1 for idx, key in enumerate(sorted(data.keys()))}

def clean_and_reindex_links(links_path, cleaned_links_path):
    # Load the links JSON data
    with open(links_path, 'r') as file:
        links_data = json.load(file)
    
    # Filter out non-latinate keys
    filtered_links = filter_non_latinate_keys(links_data)
    
    # Reindex the filtered links
    reindexed_links = reindex(filtered_links)
    
    # Save the cleaned and reindexed links
    with open(cleaned_links_path, 'w') as file:
        json.dump(reindexed_links, file, indent=4, ensure_ascii=False)
    
    print(f"Cleaned and reindexed links saved to {cleaned_links_path}")

# Example usage
if __name__ == "__main__":
    links_path = 'wiki_knowledge/embeddings/link_embeddings.json'  # Replace with your input JSON file path
    cleaned_links_path = 'wiki_knowledge/embeddings/link_embeddings2.json'  # Replace with your output JSON file path
    
    clean_and_reindex_links(links_path, cleaned_links_path)
