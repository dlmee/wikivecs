import json
from tqdm import tqdm

def load_json(filename):
    """Load a JSON file and return the data."""
    with open(filename, 'r') as f:
        return json.load(f)

def create_reverse_mapping(cooccurrence_dict, links_embeddings):
    """Create a reverse mapping with normalized values."""
    reverse_mapping = {}
    key_to_index = {key: idx for idx, key in enumerate(cooccurrence_dict.keys())}

    for key, cooccurrences in tqdm(cooccurrence_dict.items()):
        key_index = key_to_index[key]
        key_max_value = max(cooccurrences.values())

        # Add the key itself with a value of 1
        if key in links_embeddings:
            key_embedding_index = links_embeddings[key]
            if key_embedding_index not in reverse_mapping:
                reverse_mapping[key_embedding_index] = {}
            reverse_mapping[key_embedding_index][key_index] = 1.0

        for word, value in cooccurrences.items():
            if word in links_embeddings:
                word_index = links_embeddings[word]
                normalized_value = value / key_max_value

                if word_index not in reverse_mapping:
                    reverse_mapping[word_index] = {}

                reverse_mapping[word_index][key_index] = normalized_value

    return reverse_mapping

def main():
    cooccurrence_filename = 'cooccurrence_dict.json'
    embeddings_filename = 'wiki_knowledge/embeddings/link_embeddings.json'

    # Load the JSON files
    cooccurrence_dict = load_json(cooccurrence_filename)
    links_embeddings = load_json(embeddings_filename)

    # Create the reverse mapping
    reverse_mapping = create_reverse_mapping(cooccurrence_dict, links_embeddings)
    
    # Save the result to a new JSON file
    with open('reverse_mapping.json', 'w') as outj:
        json.dump(reverse_mapping, outj, indent=4, ensure_ascii=False)

    print("Reverse mapping has been created and saved to reverse_mapping.json.")

if __name__ == '__main__':
    main()
