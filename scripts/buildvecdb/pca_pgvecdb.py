import numpy as np
import psycopg2
from sklearn.decomposition import PCA
import signal
import sys

# Database connection settings
db_settings = {
    'dbname': 'wikiknowledge',
    'user': 'ocelot',
    'password': 'odinson',
    'host': 'localhost'
}

BATCH_SIZE = 10000
pca = PCA(n_components=1000)
conn = None

def fetch_vectors_batch(conn, offset, limit):
    """Fetch a batch of vectors from PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, document, embedding::text FROM my_vectors OFFSET %s LIMIT %s;", (offset, limit))
        rows = cur.fetchall()
        ids, documents, embeddings = [], [], []
        for row in rows:
            ids.append(row[0])
            documents.append(row[1])
            embedding_str = row[2][1:-1]  # Remove curly braces
            embedding = [float(x) for x in embedding_str.split(',')]
            embeddings.append(embedding)
        embeddings = np.array(embeddings, dtype=np.float32)
    return ids, documents, embeddings


def store_reduced_vectors(conn, ids, documents, reduced_embeddings):
    """Store reduced vectors in PostgreSQL."""
    with conn.cursor() as cur:
        for doc_id, doc, reduced_embedding in zip(ids, documents, reduced_embeddings):
            cur.execute("""
                INSERT INTO my_vectors_reduced (id, document, reduced_embedding)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET reduced_embedding = EXCLUDED.reduced_embedding, document = EXCLUDED.document;
            """, (doc_id, doc, reduced_embedding.tolist()))
        conn.commit()

def create_index_on_reduced_table(conn):
    """Create an ivfflat index on the reduced vectors."""
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reduced_embedding ON my_vectors_reduced USING ivfflat (reduced_embedding vector_cosine_ops) WITH (lists = 100);")
        conn.commit()

def handle_signal(sig, frame):
    """Handle SIGINT signal for graceful shutdown."""
    global conn
    print("\nSIGINT received. Exiting gracefully...")
    if conn:
        conn.close()
    sys.exit(0)

def main():
    global conn
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_settings)

    # Determine the total number of vectors
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM my_vectors;")
        total_vectors = cur.fetchone()[0]

    # Process vectors in batches
    offset = 0
    while offset < total_vectors:
        try:
            # Fetch the next batch of vectors
            ids, documents, embeddings = fetch_vectors_batch(conn, offset, BATCH_SIZE)
            
            # Check if embeddings list is empty
            if len(embeddings) == 0:
                print(f"No valid embeddings found in batch at offset {offset}. Skipping batch.")
                offset += BATCH_SIZE
                continue

            # Apply PCA to reduce dimensions to 2000
            reduced_embeddings = pca.fit_transform(embeddings)

            # Store reduced vectors
            store_reduced_vectors(conn, ids, documents, reduced_embeddings)

            offset += BATCH_SIZE
            print(f"Processed batch up to offset {offset} of {total_vectors}")

        except Exception as e:
            print(f"Error processing batch at offset {offset}: {e}")
            break

    # Close the connection
    conn.close()
    print("Process completed successfully. You can now create the index on the reduced table.")

if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_signal)
    main()
