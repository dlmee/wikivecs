from lxml import etree
from tqdm import tqdm
import signal
import sys
import json
import os
import re

class WikipediaProcessor:
    def __init__(self, dump_path, knowledge_path, output_dir='wiki_knowledge/applied'):
        with open(knowledge_path, "r") as foo:
            self.myknowledge = json.load(foo)
        print("Finished loading that beast!")
        self.dump_path = dump_path
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
                if stretch.lower() in self.knowledge:
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
        #self.forget()
        #sorted_knowledge = dict(sorted(self.knowledge.items(), key=lambda item: item[1], reverse=True))
        with open(f'{self.output_dir}/knowledge_test_{self.counter}.json', 'w') as outj:
            json.dump(self.knowledge, outj, indent=4, ensure_ascii=False)
        print(f"Knowledge written to {self.output_dir}/knowledge_test_{self.counter}.json")

    def process(self):
        # Compile a regex pattern for extracting links
        link_pattern = re.compile(r"\[\[(.+?)\]\]")
        
        # Start parsing
        context = etree.iterparse(self.dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
        for event, elem in tqdm(context, desc="finding knowledge"):
            self.counter += 1
            if self.counter % 100 == 0:
                self.save_knowledge()
            title = elem.findtext('{http://www.mediawiki.org/xml/export-0.10/}title')
            text = elem.findtext('.//{http://www.mediawiki.org/xml/export-0.10/}text')

            if text:
                if '#REDIRECT' in text: continue
                # Find all matches in the text
                stretches = self.find_word_stretches_and_context(text)
                self.knowledge[title] = stretches

            # Clear the element to free up memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        self.save_knowledge()


if __name__ == "__main__":
    dump_path = 'enwiki-latest-pages-articles.xml'
    knowledge_path = 'wiki_knowledge/truncatedknowledge.json'
    processor = WikipediaProcessor(dump_path, knowledge_path)
    processor.process()
