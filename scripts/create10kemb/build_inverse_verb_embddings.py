import json
from collections import defaultdict
from tqdm import tqdm

def load_json(filename):
    """Load a JSON file and return its contents."""
    with open(filename, 'r') as file:
        return json.load(file)

def process_verbs(verbout, verbembeddings, pageembeddings):
    """Process the verbs and create the desired dictionary structure."""
    result = {k:{'embedding': v, 'pages': []}for k,v in verbembeddings.items()}
    inverse_embeddings = {v:k for k,v in verbembeddings.items()}

    for page, verbs in tqdm(verbout.items(), desc='Processing page'):
        if page not in pageembeddings: continue
        for verb in verbs:
            if inverse_embeddings[verb] in result:
                result[inverse_embeddings[verb]]['pages'].append(pageembeddings[page])
    return result

def save_json(data, filename):
    """Save a dictionary to a JSON file."""
    with open(filename, 'w') as file:
        json.dump(data, file, indent=2)

# Example usage
verbout_file = 'wiki_knowledge/embeddings/verbout.json'
verbembeddings_file = 'wiki_knowledge/embeddings/verb_embeddings.json'
pageembeddings_file = 'wiki_knowledge/embeddings/link_embeddings.json'
output_file = 'processed_verbs.json'

# Load JSON data
verbout = load_json(verbout_file)
verbembeddings = load_json(verbembeddings_file)
pageembeddings = load_json(pageembeddings_file)

# Process verbs
processed_verbs = process_verbs(verbout, verbembeddings, pageembeddings)

# Save the result to a JSON file
save_json(processed_verbs, output_file)

print(f"Processed verbs saved to {output_file}")
