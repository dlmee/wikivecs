import json
import re
import multiprocessing as mp
from lxml import etree
from tqdm import tqdm

# Load JSON data
def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Clean and split text by punctuation
def clean_and_split_text(text):
    sentences = re.split(r'[.!?]', text)
    clean_sentences = [re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9.,?]', ' ', sentence.lower())).strip() for sentence in sentences]
    return clean_sentences

# Process a batch of pages
def process_batch(serialized_pages, verb_index, link_index):
    verb_link_vectors = []
    intersected_sentences = []

    link_pattern = re.compile(r"\[\[(.+?)\]\]")

    for serialized_page in serialized_pages:
        page = etree.fromstring(serialized_page)
        title = page.findtext('{http://www.mediawiki.org/xml/export-0.10/}title')
        text = page.findtext('.//{http://www.mediawiki.org/xml/export-0.10/}text')

        if not text:
            continue

        # Create verb and link vectors
        verb_vector = []
        link_vector = []

        words = text.split()
        for word in words:
            if word in verb_index:
                verb_vector.append(verb_index[word])
            if word in link_index:
                link_vector.append(link_index[word])

        if verb_vector or link_vector:
            verb_link_vectors.append([title, verb_vector, link_vector])

        # Extract links using the pattern
        links = link_pattern.findall(text)
        links = [link.split("|")[0] for link in links if not link.lower().startswith('file:') and not link.lower().endswith('.png')]

        # Split text into sentences and find intersections
        sentences = clean_and_split_text(text)
        for sentence in sentences:
            found_verb = None
            found_link = None
            for word in sentence.split():
                if not found_verb and word in verb_index:
                    found_verb = word
                if not found_link and word in links:
                    found_link = word
                if found_verb and found_link:
                    intersected_sentences.append([title, found_link, found_verb, sentence])
                    break

    return verb_link_vectors, intersected_sentences

# Worker function
def worker(task_queue, result_queue, verb_index, link_index):
    while True:
        try:
            serialized_pages = task_queue.get(timeout=1)
            verb_link_vectors, intersected_sentences = process_batch(serialized_pages, verb_index, link_index)
            result_queue.put((verb_link_vectors, intersected_sentences))
        except mp.queues.Empty:
            break
        except Exception as e:
            print(f"Error: {e}")
            break
    print("Worker exiting...")

# Writer function
def writer(result_queue, verb_link_output_path, sentences_output_path):
    with open(verb_link_output_path, 'a') as verb_link_file, open(sentences_output_path, 'a') as sentences_file:
        while True:
            try:
                verb_link_vectors, intersected_sentences = result_queue.get(timeout=1)
                if not verb_link_vectors and not intersected_sentences:
                    break
                for vectors in verb_link_vectors:
                    verb_link_file.write(json.dumps(vectors) + '\n')
                for sentence in intersected_sentences:
                    sentences_file.write(json.dumps(sentence) + '\n')
            except mp.queues.Empty:
                continue
            except Exception as e:
                print(f"Error in writer: {e}")
    print("Writer exiting...")

# Main function
def main(dump_path, verb_index_path, link_index_path, verb_link_output_path, sentences_output_path, num_workers=6, batch_size=1000, resume=0):
    # Load verb and link indices
    verb_index = load_json(verb_index_path)
    link_index = load_json(link_index_path)

    # Setup multiprocessing queues
    task_queue = mp.Queue()
    result_queue = mp.Queue()

    # Start worker processes
    processes = []
    for _ in range(num_workers):
        p = mp.Process(target=worker, args=(task_queue, result_queue, verb_index, link_index))
        p.start()
        processes.append(p)

    # Start writer process
    writer_process = mp.Process(target=writer, args=(result_queue, verb_link_output_path, sentences_output_path))
    writer_process.start()

    # Parse the XML and split into batches dynamically
    context = etree.iterparse(dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
    pages = []
    page_counter = 0
    for event, elem in tqdm(context, desc="Reading pages"):
        page_counter += 1
        if page_counter <= resume:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue

        serialized_page = etree.tostring(elem, encoding='utf-8')
        pages.append(serialized_page)
        if len(pages) >= batch_size:
            task_queue.put(pages)
            pages = []
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    if pages:
        task_queue.put(pages)

    # Ensure all worker processes have finished
    for p in processes:
        p.join()

    # Signal the writer process to stop
    result_queue.put(([], []))  # Signal the writer to exit
    writer_process.join()

    print(f"Results saved to {verb_link_output_path} and {sentences_output_path}")

# Real file paths
if __name__ == "__main__":
    dump_path = 'enwiki-latest-pages-articles.xml' 
    dump_path = 'enwiki-20240501-pages-articles15.xml-p17324603p17460152'
    verb_index_path = 'data/verb_embeddings.json'  
    link_index_path = 'data/link_embeddings.json'  
    verb_link_output_path = 'page_embeddings.jsonl'  
    sentences_output_path = 'intersected_sentences.jsonl'  
    
    main(dump_path, verb_index_path, link_index_path, verb_link_output_path, sentences_output_path, num_workers=5, batch_size=100)
