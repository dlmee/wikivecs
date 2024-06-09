import json
import logging
import time
import random
import signal
import sys
import gc
import numpy as np
import numpy.typing as npt
import psycopg2
from psycopg2.errors import UniqueViolation
import sqlite3
from typing import List

BATCH_SIZE = 30000
OFFSET = 18480000

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

def get_random_document(conn, total_docs: int):
    """Retrieve a random document from the PostgreSQL database."""
    random_offset = random.randint(0, total_docs - 1)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, document, embedding
            FROM my_vectors
            OFFSET %s LIMIT 1;
        """, (random_offset,))
        row = cur.fetchone()
        return row[1], row[2], row[0]

def query_collection(conn, query_vector):
    """Query the PostgreSQL collection for the nearest neighbors."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT document, embedding, (embedding <-> %s) AS distance
            FROM my_vectors
            ORDER BY distance ASC
            LIMIT 5;
        """, (query_vector,))
        rows = cur.fetchall()
        return {"documents": [row[0] for row in rows], "distances": [row[2] for row in rows]}

def handle_signal(sig, frame):
    global OFFSET
    print(f"Interrupt received, exiting gracefully. Current offset: {OFFSET}")
    sys.exit(0)

def main():
    global OFFSET

    logging.basicConfig(level=logging.INFO)
    
    db_filename = 'database/final_embedding.db'
    link_embeddings_filename = 'wiki_knowledge/embeddings/link_embeddings.json'
    dimension = 10000

    # Load the inverse link embeddings
    inverse_link_embeddings = load_link_embeddings(link_embeddings_filename)

    # Create the custom embedding function instance
    embedding_function = CustomEmbeddingFunction()

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname='wikiknowledge',
        user='ocelot',
        password='odinson',
        host='localhost'
    )

    signal.signal(signal.SIGINT, handle_signal)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM my_vectors;")
        total_records = cur.fetchone()[0]
    print(f"The number of elements in the collection before adding: {total_records}")

    while True:
        start_time = time.time()

        # Read the embeddings from the SQLite database
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

        # Add vectors to the PostgreSQL database
        try:
            with conn.cursor() as cur:
                for doc_id, doc, embedding in zip(ids, documents, normalized_embeddings):
                    try:
                        cur.execute("""
                            INSERT INTO my_vectors (id, document, embedding)
                            VALUES (%s, %s, %s);
                        """, (doc_id, doc, embedding))
                    except UniqueViolation:
                        logging.warning(f"Duplicate ID found: {doc_id}. Skipping entry.")
                        conn.rollback()  # Rollback the last transaction to continue
                        continue
            conn.commit()
            batch_time = time.time() - start_time
            print(f"Added {len(normalized_embeddings)} embeddings to the PostgreSQL collection in {batch_time:.2f} seconds.")
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM my_vectors;")
                total_records = cur.fetchone()[0]
            print(f"Total elements in the collection: {total_records}")
        except Exception as e:
            logging.error(f"Error adding documents to the collection: {e}")
            conn.rollback()

        # Perform a query to check progress
        if len(normalized_embeddings) > 0:
            random_doc, random_embedding, random_id = get_random_document(conn, total_records)
            results = query_collection(conn, random_embedding)
            print(f"The current OFFSET is {OFFSET}")
            print(f"Querying with document ID: {random_id}, Name: {random_doc}")
            for result, dist in zip(results['documents'], results['distances']):
                print(result, dist)

        OFFSET += BATCH_SIZE
        gc.collect()

if __name__ == '__main__':
    main()
