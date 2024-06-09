import json
from collections import defaultdict

def build_superdict(input_path, output_path):
    with open(input_path, "r") as f:
        data = json.load(f)
    
    superdict = defaultdict(int)

    for president, stretches in data.items():
        for stretch in stretches.keys():
            superdict[stretch] += 1

    sorted_superdict = dict(sorted(superdict.items(), key=lambda item: item[1], reverse=True))

    with open(output_path, "w") as outj:
        json.dump(sorted_superdict, outj, indent=4, ensure_ascii=False)

    print(f"Superdict saved to {output_path}")

if __name__ == "__main__":
    input_path = 'wiki_knowledge/applied/knowledge_test_verbs_43.json'
    output_path = 'wiki_knowledge/presidents_verb_counts.json'
    build_superdict(input_path, output_path)
