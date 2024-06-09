import json
from tqdm import tqdm

def process_jsonl(input_file, verbout_file, linkout_file):
    verbout = {}
    linkout = {}
    counter = 0
    with open(input_file, 'r') as f:
        for i, line in enumerate(tqdm(f, desc="Processing JSONL file")):
            record = json.loads(line)

            # Assuming the JSONL file has 'verbs' and 'links' keys
            page, verbs, links = record
            if 'Wikipedia' in page: continue

            """if page not in verbout:
                verbout[page] = list(set(verbs))  # You can store additional data if needed"""

            if page not in linkout:
                linkout[page] = list(set(links))

    
    with open(linkout_file, 'w') as vf:
        json.dump(linkout, vf, indent=4, ensure_ascii=False)

    

# Example usage
input_file = 'page_embeddings.jsonl'
verbout_file = 'verbout.json'
linkout_file = 'linkout.json'
process_jsonl(input_file, verbout_file, linkout_file)
