import json
import os
import re
import signal
import sys
from lxml import etree
from tqdm import tqdm
from collections import defaultdict

class WikipediaProcessor:
    def __init__(self, dump_path, verb_list_path, output_dir='wiki_knowledge'):
        self.dump_path = dump_path
        self.output_dir = output_dir
        self.knowledge = defaultdict(int)
        self.counter = 0
        self.load_verb_list(verb_list_path)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set up the signal handler
        signal.signal(signal.SIGINT, self.create_sigint_handler())
    
    def load_verb_list(self, verb_list_path):
        with open(verb_list_path, "r") as f:
            self.verb_list = json.load(f)
        print("Loaded verb list.")
    
    def clean_and_split_text(self, text):
        # Remove any non-alphabetic characters except spaces and lowercase all words
        text = re.sub(r'[^a-zA-Z ]', '', text).lower()
        # Split text into words
        words = text.split()
        return set(words)

    def create_sigint_handler(self):
        def handle_sigint(signal, frame):
            print("\nSIGINT received. Writing knowledge to file...")
            self.save_knowledge()
            print("Exiting gracefully.")
            sys.exit(0)
        return handle_sigint

    def save_knowledge(self):
        sorted_knowledge = dict(sorted(self.knowledge.items(), key=lambda item: item[1], reverse=True))
        with open(f'{self.output_dir}/verb_counts_{self.counter}.json', 'w') as outj:
            json.dump(sorted_knowledge, outj, indent=4, ensure_ascii=False)
        print(f"Knowledge written to {self.output_dir}/verb_counts_{self.counter}.json")

    def process(self):
        # Start parsing
        context = etree.iterparse(self.dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
        
        for event, elem in tqdm(context, desc="Processing Wikipedia dump"):
            
            title = elem.findtext('{http://www.mediawiki.org/xml/export-0.10/}title')
            text = elem.findtext('.//{http://www.mediawiki.org/xml/export-0.10/}text')
            
            if text:
                words = self.clean_and_split_text(text)
                for key in self.verb_list:
                    if key in words:
                        self.knowledge[key] += 1

            # Clear the element to free up memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        self.save_knowledge()

if __name__ == "__main__":
    dump_path = 'enwiki-latest-pages-articles.xml'
    verb_list_path = 'verb_list.json'
    processor = WikipediaProcessor(dump_path, verb_list_path)
    processor.process()
