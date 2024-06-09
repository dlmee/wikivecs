import sqlite3
import json
import chromadb
import logging
from typing import List
import numpy as np
import numpy.typing as npt
import random

# Initialize the ChromaDB client
client = chromadb.PersistentClient(path="wikichroma")
collection = client.get_collection("wc_final_6")
collections = client.list_collections()
collections = [c.name for c in collections]
print("And done!")
