from lxml import etree
from tqdm import tqdm
import signal
import sys
import json
import os
import re

class WikipediaProcessor:
    def __init__(self, dump_path, output_dir='wiki_phrases'):
        self.dump_path = dump_path
        self.output_dir = output_dir
        self.phrases = {}
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
        self.phrases = {k:v for k,v in self.phrases.items() if v > 1}

    def create_sigint_handler(self):
        def handle_sigint(signal, frame):
            print("\nSIGINT received. Writing phrases to file...")
            self.save_phrases()
            print("Exiting gracefully.")
            sys.exit(0)
        return handle_sigint

    def save_phrases(self):
        self.forget()
        sorted_phrases = dict(sorted(self.phrases.items(), key=lambda item: item[1], reverse=True))
        with open(f'{self.output_dir}/phrases_test_{self.counter}.json', 'w') as outj:
            json.dump(sorted_phrases, outj, indent=4, ensure_ascii=False)
        print(f"phrases written to {self.output_dir}/phrases_test_{self.counter}.json")

    def process(self):
        # Compile a regex pattern for extracting links
        link_pattern = re.compile(r"\[\[(.+?)\]\]")
        
        # Start parsing
        context = etree.iterparse(self.dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
        
        total = 23528001
        for event, elem in tqdm(context, desc="finding phrases"):
            self.counter += 1
            if self.counter % 10000 == 0:
                self.save_phrases()
            title = elem.findtext('{http://www.mediawiki.org/xml/export-0.10/}title')
            text = elem.findtext('.//{http://www.mediawiki.org/xml/export-0.10/}text')
            
            if text:
                # Find all matches in the text
                stretches = self.find_word_stretches(text)
                for s in stretches:
                    if s not in self.phrases:
                        self.phrases[s] = 1
                    else:
                        self.phrases[s] += 1

            # Clear the element to free up memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        self.save_phrases()


if __name__ == "__main__":
    dump_path = 'enwiki-latest-pages-articles.xml'
    dump_path = 'enwiki-20240501-pages-articles15.xml-p17324603p17460152'
    processor = WikipediaProcessor(dump_path)
    processor.process()
