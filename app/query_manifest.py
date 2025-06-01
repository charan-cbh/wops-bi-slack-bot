import pickle
import faiss
import numpy as np
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
EMBED_MODEL = "text-embedding-ada-002"

FAISS_INDEX_PATH = "manifest_index.faiss"
METADATA_PATH = "manifest_metadata.pkl"

def embed_query(query: str) -> np.ndarray:
    response = client.embeddings.create(
        input=[query],
        model=EMBED_MODEL
    )
    return np.array(response.data[0].embedding, dtype="float32").reshape(1, -1)

def load_index_and_metadata():
    index = faiss.read_index(FAISS_INDEX_PATH)
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)
    return index, metadata["ids"], metadata["texts"]

def search_manifest(query: str, k: int = 5) -> list:
    index, ids, texts = load_index_and_metadata()
    query_vector = embed_query(query)
    distances, indices = index.search(query_vector, k)

    results = []
    for i in indices[0]:
        if i < len(ids):
            results.append({
                "model": ids[i],
                "context": texts[i]
            })
    return results