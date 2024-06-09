import json
from collections import defaultdict

def build_superdict_from_jsonl(input_path, output_path):
    superdict = defaultdict(int)

    with open(input_path, "r") as f:
        for line in f:
            data = json.loads(line)
            for president, elements in data.items():
                for key, count in elements.items():
                    superdict[key] += count
    
    sorted_superdict = dict(sorted(superdict.items(), key=lambda item: item[1], reverse=True))

    with open(output_path, "w") as outj:
        json.dump(sorted_superdict, outj, indent=4, ensure_ascii=False)

    print(f"Superdict saved to {output_path}")

if __name__ == "__main__":
    input_path = 'wiki_knowledge/presidents.jsonl'
    output_path = 'all_presidents_links.json'
    build_superdict_from_jsonl(input_path, output_path)
