from lxml import etree
from tqdm import tqdm
import signal
import sys
import json
import os
import re

class WikipediaProcessor:
    def __init__(self, dump_path, output_dir='wiki_knowledge'):
        self.dump_path = dump_path
        self.output_dir = output_dir
        self.knowledge = {}
        self.counter = 0
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set up the signal handler
        signal.signal(signal.SIGINT, self.create_sigint_handler())
    
    def find_word_stretches(self, text, min_words=1, max_words=8):
        words = text.split()
        stretches = set()
        pattern = r'[^a-zA-Z.,? ]'
        for start in range(len(words)):
            for end in range(start + min_words, min(start + max_words + 1, len(words) + 1)):
                stretch = " ".join(words[start:end])
                if not re.findall(pattern, stretch):
                    stretches.add(stretch.lower())
                else:
                    pass
        return stretches

    def forget(self):
        self.knowledge = {k:v for k,v in self.knowledge.items() if v > 1}

    def create_sigint_handler(self):
        def handle_sigint(signal, frame):
            print("\nSIGINT received. Writing knowledge to file...")
            self.save_knowledge()
            print("Exiting gracefully.")
            sys.exit(0)
        return handle_sigint

    def save_knowledge(self):
        self.forget()
        sorted_knowledge = dict(sorted(self.knowledge.items(), key=lambda item: item[1], reverse=True))
        with open(f'{self.output_dir}/knowledge_test_{self.counter}.json', 'w') as outj:
            json.dump(sorted_knowledge, outj, indent=4, ensure_ascii=False)
        print(f"Knowledge written to {self.output_dir}/knowledge_test_{self.counter}.json")

    def process(self):
        # Compile a regex pattern for extracting links
        link_pattern = re.compile(r"\[\[(.+?)\]\]")
        
        # Start parsing
        context = etree.iterparse(self.dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
        
        total = 23528001
        for event, elem in tqdm(context, desc="finding knowledge"):
            self.counter += 1
            if self.counter % 10000 == 0:
                self.save_knowledge()
            title = elem.findtext('{http://www.mediawiki.org/xml/export-0.10/}title')
            text = elem.findtext('.//{http://www.mediawiki.org/xml/export-0.10/}text')
            
            if text:
                # Find all matches in the text
                stretches = self.find_word_stretches(text)
                for s in stretches:
                    if s not in self.knowledge:
                        self.knowledge[s] = 1
                    else:
                        self.knowledge[s] += 1

            # Clear the element to free up memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        self.save_knowledge()


if __name__ == "__main__":
    dump_path = 'enwiki-latest-pages-articles.xml'
    processor = WikipediaProcessor(dump_path)
    processor.process()
