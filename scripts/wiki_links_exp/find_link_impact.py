import json
from collections import defaultdict

# Read the input JSON file
with open('data/link_mapping_example.json', 'r') as f:
    data = json.load(f)

# Create a defaultdict to store the counts
counts = defaultdict(int)

# Iterate over the data and count the occurrences of each value
for key, values in data.items():
    for value in values:
        counts[value] += 1

# Convert the defaultdict to a regular dictionary
result = dict(counts)

# Write the result to the output JSON file
with open('link_impact.json', 'w') as f:
    json.dump(result, f, indent=4, ensure_ascii=False)

print("The file 'link_impact.json' has been created with the counts of each value.")
