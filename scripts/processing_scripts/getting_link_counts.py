import json
from collections import defaultdict
from tqdm import tqdm

def count_links(input_path, output_path):
    link_counts = defaultdict(int)

    # Open the input JSONL file and process each line
    with open(input_path, 'r') as file:
        for line in tqdm(file, desc="Processing links"):
            data = json.loads(line.strip())
            for page, links in data.items():
                for link in links:
                    link_counts[link] += 1

    # Save the link counts to the output JSON file
    with open(output_path, 'w') as out_file:
        json.dump(link_counts, out_file, indent=4, ensure_ascii=False)

    print(f"Link counts saved to {output_path}")

# Example usage
if __name__ == "__main__":
    input_path = 'wiki_knowledge/links.jsonl'  # Replace with your input JSONL file path
    output_path = 'wiki_knowledge/link_counts.json'  # Replace with your output JSON file path
    count_links(input_path, output_path)
