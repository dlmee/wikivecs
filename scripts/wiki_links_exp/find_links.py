from lxml import etree
import json
import os
import re
from tqdm import tqdm

def process_wikipedia_dump(dump_path):

    my_links = {}
    output_dir = 'data'
    os.makedirs(output_dir, exist_ok=True)

    link_pattern = re.compile(r"\[\[(.+?)\]\]")

    context = etree.iterparse(dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
    for event, elem in tqdm(context):

        title = elem.findtext('{http://www.mediawiki.org/xml/export-0.10/}title').lower()
        text = elem.findtext('.//{http://www.mediawiki.org/xml/export-0.10/}text')

        if text:
            links = re.findall(link_pattern, text)
            my_links[title] = links

        # Clear the element to free up memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    with open(f"{output_dir}/links.json", "w") as outj:
        json.dump(my_links, outj, indent=4, ensure_ascii=False)


dump_path = 'enwiki-latest-pages-articles.xml'
dump_path = 'enwiki-20240501-pages-articles15.xml-p17324603p17460152'
process_wikipedia_dump(dump_path)
