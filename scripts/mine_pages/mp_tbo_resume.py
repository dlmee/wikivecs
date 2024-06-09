import json
import re
import multiprocessing as mp
from lxml import etree
from tqdm import tqdm
import queue
import time

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
            while True:
                try:
                    result_queue.put((verb_link_vectors, intersected_sentences), timeout=2)
                    break
                except queue.Full:
                    print("Result queue is full, retrying...")
                    time.sleep(1)  # Wait before retrying
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
def main(dump_path, verb_index_path, link_index_path, verb_link_output_path, sentences_output_path, num_workers=6, batch_size=1000, resume=0, queue_max_size=100):
    # Load verb and link indices
    verb_index = load_json(verb_index_path)
    link_index = load_json(link_index_path)

    # Setup multiprocessing queues with size limits
    task_queue = mp.Queue(maxsize=queue_max_size)
    result_queue = mp.Queue(maxsize=queue_max_size)

    # Parse the XML and skip to the resume point
    context = etree.iterparse(dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
    page_counter = 0
    for event, elem in tqdm(context, desc="Skipping pages", total=resume):
        page_counter += 1
        if page_counter > resume:
            break
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context

    # Start worker processes
    worker_processes = []
    for _ in range(num_workers):
        p = mp.Process(target=worker, args=(task_queue, result_queue, verb_index, link_index))
        p.start()
        worker_processes.append(p)

    # Start writer process
    writer_process = mp.Process(target=writer, args=(result_queue, verb_link_output_path, sentences_output_path))
    writer_process.start()

    # Parse the XML and split into batches dynamically
    context = etree.iterparse(dump_path, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}page')
    pages = []
    page_counter = 0
    for event, elem in tqdm(context, desc="Reading pages"):
        page_counter += 1
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
    for p in worker_processes:
        p.join()

    # Signal the writer process to stop
    result_queue.put(([], []))  # Signal the writer to exit
    writer_process.join()

    print(f"Results saved to {verb_link_output_path} and {sentences_output_path}")

# Real file paths
if __name__ == "__main__":
    dump_path = 'enwiki-latest-pages-articles.xml'  # Replace with your filtered input XML file path
    verb_index_path = 'wiki_knowledge/embeddings/verb_embeddings.json'  # Replace with your processed verbs JSON file path
    link_index_path = 'wiki_knowledge/embeddings/link_embeddings.json'  # Replace with your processed links JSON file path
    verb_link_output_path = 'page_embeddings.jsonl'  # Replace with your output JSONL file path
    sentences_output_path = 'intersected_sentences.jsonl'  # Replace with your output JSONL file path
    
    main(dump_path, verb_index_path, link_index_path, verb_link_output_path, sentences_output_path, num_workers=10, batch_size=1000, resume=8308044)
