import json

def extract_progeny(input_path, output_path):
    with open(input_path, "r") as f:
        data = json.load(f)
    
    progeny_list = []

    for key, value in data.items():
        if 'PROGENY' in value:
            progeny_list.extend(value['PROGENY'])

    with open(output_path, "w") as outj:
        json.dump(progeny_list, outj, indent=4, ensure_ascii=False)

    print(f"Progeny list saved to {output_path}")

if __name__ == "__main__":
    input_path = '/home/ocelot/Code/odinson-llm/taxonomies/hyp_verbs_w_progeny.json'
    output_path = 'verb_list.json'
    extract_progeny(input_path, output_path)
