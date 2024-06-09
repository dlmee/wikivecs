from flask import Flask, request, jsonify
import chromadb
import json
import sqlite3
import pandas as pd
import numpy as np
import numpy.typing as npt

app = Flask(__name__)

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

# Initialize the ChromaDB client
client = chromadb.PersistentClient(path="wikichroma")
collection = client.get_collection("wc_final_6")
with open("wiki_knowledge/embeddings/link_embeddings.json", "r") as inj:
    link_idx = json.load(inj)
#link_idx = {v: k for k, v in link_idx.items()}
processor = CustomEmbeddingFunction()

def query_embedding_table(db_filename, embidx):
    """Query the embeddings table and return its contents as a pandas DataFrame."""
    conn = sqlite3.connect(db_filename)
    
    # Execute the query with the embidx parameter
    df = pd.read_sql_query('SELECT * FROM embeddings WHERE emb_idx = ?', conn, params=(embidx,))
    
    # Close the connection
    conn.close()
    
    return df

@app.route('/collections', methods=['GET'])
def list_collections():
    collections = client.list_collections()
    collections = [c.name for c in collections]
    return jsonify(collections)

@app.route('/collections/query', methods=['POST'])
def query_collection():
    data = request.json
    query_embedding = data.get('emb_idx')
    
    if not query_embedding:
        return jsonify({'error': 'Query embedding is required'}), 400
    try:
        query_embedding = link_idx[query_embedding]
    except:
        return jsonify({'error': 'Not a viable key'}), 404
    db_filename = "database/final_embedding.db"  # Replace with your actual database filename
    df = query_embedding_table(db_filename, query_embedding)
    
    if df.empty:
        return jsonify({'error': 'No data found for the given embedding index'}), 404
    
    raw = json.loads(df.loc[0, 'values'])
    emb = processor.create_embeddings([[query_embedding, raw]])
    print("ready to query for results!")
    results = collection.query(emb)
    
    return jsonify(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
