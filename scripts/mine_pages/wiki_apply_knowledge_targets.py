import json
import os
import re
import signal
import sys
from tqdm import tqdm

class WikipediaProcessor:
    def __init__(self, knowledge_path, truncated_knowledge_path, output_dir='wiki_knowledge/applied'):
        with open(knowledge_path, "r") as f:
            self.myknowledge = json.load(f)
        print("Finished loading presidents knowledge!")

        with open(truncated_knowledge_path, "r") as f:
            self.truncated_knowledge = json.load(f)
            self.truncated_knowledge = set(self.truncated_knowledge)
        print("Finished loading truncated knowledge!")

        self.output_dir = output_dir
        self.knowledge = {}
        self.counter = 0
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set up the signal handler
        signal.signal(signal.SIGINT, self.create_sigint_handler())
    
    def find_word_stretches_and_context(self, text, min_words=1, stretch_max=8):
        words = text.split()
        stretches = {}
        potential_stretches = []

        def find_context(start, end):
            # Find the nearest punctuation mark before the stretch
            before_context_idx = max(text[:start].rfind(p) for p in '.?![{')
            if before_context_idx == -1:
                before_context_idx = 0
            else:
                if text[before_context_idx] in '[{':
                    before_context_idx += 0  # Keep the opening bracket
                else:
                    before_context_idx += 1

            # Find the nearest punctuation mark after the stretch
            after_context_idx = min((text[end:].find(p) for p in '.?!]}' if text[end:].find(p) != -1), default=len(text))
            if after_context_idx == -1:
                after_context_idx = len(text)
            else:
                after_context_idx += end  # Include the punctuation or closing bracket

            return text[before_context_idx:after_context_idx].strip()

        # Collect all possible stretches
        for start in range(len(words)):
            for end in range(start + min_words, min(start + stretch_max + 1, len(words) + 1)):
                stretch = " ".join(words[start:end])
                if stretch.lower() in self.truncated_knowledge:
                    potential_stretches.append((stretch, start, end))

        # Sort stretches by length (longest first)
        potential_stretches.sort(key=lambda x: len(x[0]), reverse=True)

        used_ranges = []

        for stretch, start, end in potential_stretches:
            stretch_start = text.find(stretch)
            stretch_end = stretch_start + len(stretch)
            # Check for overlaps with already used ranges
            if not any(stretch_start < used_end and stretch_end > used_start for used_start, used_end in used_ranges):
                context = find_context(stretch_start, stretch_end)
                stretches[stretch.lower()] = context
                used_ranges.append((stretch_start, stretch_end))

        return stretches
    
    def create_sigint_handler(self):
        def handle_sigint(signal, frame):
            print("\nSIGINT received. Writing knowledge to file...")
            self.save_knowledge()
            print("Exiting gracefully.")
            sys.exit(0)
        return handle_sigint

    def save_knowledge(self):
        with open(f'{self.output_dir}/knowledge_test_verbs_{self.counter}.json', 'w') as outj:
            json.dump(self.knowledge, outj, indent=4, ensure_ascii=False)
        print(f"Knowledge written to {self.output_dir}/knowledge_test_{self.counter}.json")

    def process(self):
        for president, text in tqdm(self.myknowledge.items(), desc="Processing presidents"):
            self.counter += 1
            if self.counter % 100 == 0:
                self.save_knowledge()

            if text:
                stretches = self.find_word_stretches_and_context(text)
                self.knowledge[president] = stretches

        self.save_knowledge()


if __name__ == "__main__":
    knowledge_path = 'wiki_knowledge/presidents_knowledge.json'
    truncated_knowledge_path = 'verb_list.json'
    processor = WikipediaProcessor(knowledge_path, truncated_knowledge_path)
    processor.process()
