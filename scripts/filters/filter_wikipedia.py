from lxml import etree
import os
import re
from tqdm import tqdm

def filter_wikipedia_dump(dump_path, output_path):
    # Compile a regex pattern for extracting links
    link_pattern = re.compile(r"\[\[(.+?)\]\]")

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    context = etree.iterparse(dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
    root = None
    output_tree = None
    counter = 0

    with open(output_path, 'wb') as output_file:
        for event, elem in tqdm(context, desc="Processing pages"):
            if root is None:
                root = elem.getroottree()

            title = elem.findtext('{http://www.mediawiki.org/xml/export-0.10/}title')
            text = elem.findtext('.//{http://www.mediawiki.org/xml/export-0.10/}text')

            if text:
                links = link_pattern.findall(text)
                links = [link.split("|")[0] for link in links]  # Split and take the first part of the link
                links = [link for link in links if not link.lower().startswith('file:') and not link.lower().endswith('.png')]  # Filter out 'File:' links and images
                
                if len(links) > 1:
                    if output_tree is None:
                        output_tree = etree.ElementTree(elem)
                        output_tree.write(output_file, encoding='utf-8', xml_declaration=True)
                    else:
                        output_file.write(etree.tostring(elem, encoding='utf-8'))
            
            # Clear the element to free up memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    print(f"Filtered dump saved to {output_path}")

if __name__ == "__main__":
    dump_path = 'enwiki-latest-pages-articles.xml'  # Replace with your input XML file path
    output_path = 'wiki_knowledge/filtered_wikipedia_dump.xml'  # Replace with your output XML file path
    
    filter_wikipedia_dump(dump_path, output_path)
