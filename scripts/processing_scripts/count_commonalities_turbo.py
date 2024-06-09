import json
from collections import defaultdict, Counter

def build_superdict(input_path):
    with open(input_path, "r") as f:
        data = json.load(f)
    
    superdict = defaultdict(int)
    context_dict = defaultdict(list)

    for president, stretches in data.items():
        for stretch, context in stretches.items():
            superdict[stretch] += 1
            context_dict[stretch].append(context)
    
    sorted_superdict = dict(sorted(superdict.items(), key=lambda item: item[1], reverse=True))
    
    return sorted_superdict, context_dict

def find_common_phrases(context_dict, top_n=10):
    common_phrases = {}
    
    for key, contexts in context_dict.items():
        phrase_counter = Counter()
        for context in contexts:
            words = context.split()
            for i in range(len(words)):
                for j in range(i+1, min(i+6, len(words)+1)):  # Consider up to 5-word phrases
                    phrase = " ".join(words[i:j])
                    phrase_counter[phrase] += 1
        
        common_phrases[key] = phrase_counter.most_common(top_n)
    
    return common_phrases

def main(input_path, output_superdict_path, output_common_phrases_path):
    superdict, context_dict = build_superdict(input_path)
    
    with open(output_superdict_path, "w") as outj:
        json.dump(superdict, outj, indent=4, ensure_ascii=False)
    print(f"Superdict saved to {output_superdict_path}")
    
    common_phrases = find_common_phrases(context_dict)
    
    with open(output_common_phrases_path, "w") as outj:
        json.dump(common_phrases, outj, indent=4, ensure_ascii=False)
    print(f"Common phrases saved to {output_common_phrases_path}")

if __name__ == "__main__":
    input_path = 'wiki_knowledge/applied/knowledge_test_verbs_43.json'
    output_superdict_path = 'wiki_knowledge/presidents_verb_counts.json'
    output_common_phrases_path = 'wiki_knowledge/presidents_common_phrases.json'
    main(input_path, output_superdict_path, output_common_phrases_path)
