import sqlite3
import json
import chromadb
import logging
import time
from typing import List
import numpy as np
import numpy.typing as npt
import random
import signal
import sys
import gc

BATCH_SIZE = 10000
OFFSET = 16630000

class CustomEmbeddingFunction:
    def _normalize(self, vector: npt.NDArray) -> npt.NDArray:
        """Normalizes a vector to unit length using L2 norm."""
        norm = np.linalg.norm(vector)
        if norm == 0:
            return vector
        return vector / norm

    def create_embeddings(self, input_docs):
        """Creates normalized embeddings from input documents."""
        embeddings = []
        for doc in input_docs:
            emb_idx, sparse_embedding = doc
            embedding = np.zeros(10000, dtype='float32')
            for index, value in sparse_embedding:
                embedding[index] = value
            normalized_embedding = self._normalize(embedding)
            embeddings.append(normalized_embedding.tolist())
        return embeddings

def load_link_embeddings(filename: str):
    """Load the link_embeddings.json and create an inverse mapping."""
    try:
        with open(filename, 'r') as f:
            link_embeddings = json.load(f)
        # Create inverse mapping
        inverse_link_embeddings = {str(v): k for k, v in link_embeddings.items()}
        return inverse_link_embeddings
    except Exception as e:
        logging.error(f"Error loading link embeddings from '{filename}': {e}")
        return {}

def read_embeddings(db_filename: str, offset: int, limit: int):
    """Read embeddings from the SQLite database."""
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()
        cursor.execute('SELECT emb_idx, "values" FROM embeddings LIMIT ? OFFSET ?', (limit, offset))
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logging.error(f"Error reading embeddings from '{db_filename}': {e}")
        return []

def get_random_document(collection, total_docs: int):
    """Retrieve a random document from the collection."""
    random_offset = random.randint(0, total_docs - 1)
    result = collection.get(limit=1, offset=random_offset, include=["embeddings", "documents"])
    return result["documents"][0], result["embeddings"][0], result["ids"][0]

def query_collection(collection, query_vector):
    """Query the ChromaDB collection for the nearest neighbors."""
    results = collection.query(query_embeddings=[query_vector])
    return results

def handle_signal(sig, frame):
    global OFFSET
    print(f"Interrupt received, exiting gracefully. Current offset: {OFFSET}")
    sys.exit(0)

def main():
    global OFFSET

    logging.basicConfig(level=logging.INFO)
    
    db_filename = 'database/final_embedding.db'
    link_embeddings_filename = 'wiki_knowledge/embeddings/link_embeddings.json'
    collection_name = "wc_final_9"
    dimension = 10000

    # Load the inverse link embeddings
    inverse_link_embeddings = load_link_embeddings(link_embeddings_filename)

    # Create the custom embedding function instance
    embedding_function = CustomEmbeddingFunction()

    # Create a ChromaDB client and collection
    chroma_client = chromadb.PersistentClient(path="wikichroma")
    collection = chroma_client.get_or_create_collection(name=collection_name)
    if collection is None:
        logging.error("Failed to retrieve or create collection.")
        return

    signal.signal(signal.SIGINT, handle_signal)

    total_records = collection.count()
    print(f"The number of elements in the collection before adding: {total_records}")

    while True:
        start_time = time.time()

        # Read the embeddings from the database
        rows = read_embeddings(db_filename, OFFSET, BATCH_SIZE)
        if not rows:
            print("No more rows to process.")
            break

        # Process and prepare vectors for insertion
        documents = []
        ids = []
        embeddings = []

        for emb_idx, sparse_embedding_json in rows:
            name = inverse_link_embeddings.get(str(emb_idx), None)
            if not name:
                continue

            sparse_embedding = json.loads(sparse_embedding_json)
            if not sparse_embedding:
                continue

            documents.append(name)
            ids.append(str(emb_idx))
            embeddings.append((emb_idx, sparse_embedding))

        # Normalize embeddings using the custom embedding function
        normalized_embeddings = embedding_function.create_embeddings(embeddings)

        # Add vectors to the collection
        try:
            collection.add(ids=ids, embeddings=normalized_embeddings, documents=documents)
            batch_time = time.time() - start_time
            print(f"Added {len(normalized_embeddings)} embeddings to the ChromaDB collection in {batch_time:.2f} seconds.")
            print(f"Total elements in the collection: {collection.count()}")
        except Exception as e:
            logging.error(f"Error adding documents to the collection: {e}")

        # Perform a query to check progress
        if len(normalized_embeddings) > 0:
            random_doc, random_embedding, random_id = get_random_document(collection, collection.count())
            results = query_collection(collection, random_embedding)
            print(f"The current OFFSET is {OFFSET}")
            print(f"Querying with document ID: {random_id}, Name: {random_doc}")
            for result, dist in zip(results['documents'], results['distances']):
                print(result, dist)

        OFFSET += BATCH_SIZE
        #if OFFSET > 16000000: break
        gc.collect()

if __name__ == '__main__':
    main()
