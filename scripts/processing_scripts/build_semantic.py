import json

def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def save_json(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

def filter_links(link_counts):
    filtered_links = {link: count for link, count in link_counts.items() 
                      if 1 < count <= 28000 and ':' not in link and '#' not in link}
    return filtered_links

def build_index(data):
    return {key: idx + 1 for idx, key in enumerate(sorted(data.keys()))}  # Start index at 1

def build_semantic_embeddings(verbs, links):
    embeddings = {}
    last_verb_idx = max(verbs.values())
    
    for verb, verb_idx in verbs.items():
        embeddings[verb] = verb_idx
    
    for link, link_idx in links.items():
        embeddings[link] = link_idx + last_verb_idx
    
    return embeddings

def main(link_counts_path, processed_verbs_path, semantic_embeddings_path, processed_links_path):
    # Load JSON data
    link_counts = load_json(link_counts_path)
    processed_verbs = load_json(processed_verbs_path)
    
    # Filter links based on given conditions
    filtered_links = filter_links(link_counts)
    
    # Build indices for verbs and links
    verb_index = build_index(processed_verbs)
    link_index = build_index(filtered_links)
    
    # Build semantic embeddings
    semantic_embeddings = build_semantic_embeddings(verb_index, link_index)
    
    # Save the outputs
    save_json(semantic_embeddings, semantic_embeddings_path)
    save_json(link_index, processed_links_path)
    
    print(f"Semantic embeddings saved to {semantic_embeddings_path}")
    print(f"Processed links saved to {processed_links_path}")

# Example usage
if __name__ == "__main__":
    link_counts_path = 'wiki_knowledge/link_counts.json'  # Replace with your input JSON file path
    processed_verbs_path = 'wiki_knowledge/processed_verbs.json'  # Replace with your input JSON file path
    semantic_embeddings_path = 'wiki_knowledge/semantic_embeddings.json'  # Replace with your output JSON file path
    processed_links_path = 'wiki_knowledge/processed_links.json'  # Replace with your output JSON file path
    
    main(link_counts_path, processed_verbs_path, semantic_embeddings_path, processed_links_path)
