import random
import numpy as np
import chromadb

def create_chromadb_client():
    """Create and configure a ChromaDB client."""
    client = chromadb.Client()
    mycollection = client.list_collections()
    print(mycollection)
    return client

def query_collection(collection, query_vector, top_k=5):
    """Query the ChromaDB collection for the nearest neighbors."""
    results = collection.query(query_vector, top_k=top_k)
    return results

def main():
    collection_name = "wikilinks"
    dimension = 10000  # Assuming the embeddings have a dimension of 10,000

    # Create a ChromaDB client and get the collection
    client = create_chromadb_client()
    collection = client.get_collection(collection_name)

    # Load all documents in the collection
    all_documents = collection.get_all_documents()

    # Select a random document for querying
    random_doc = random.choice(all_documents)
    print(f"The document to be queried is: {random_doc}")
    query_vector = random_doc['embedding']

    # Query the collection based on the selected random document
    results = query_collection(collection, query_vector, top_k=5)
    print("Query Results:")
    for result in results:
        print(result)

if __name__ == '__main__':
    main()
