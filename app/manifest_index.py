import os
import pickle
import json
import faiss
import numpy as np
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

USE_VECTOR_STORE = os.getenv("USE_VECTOR_STORE", "false").lower() == "true"
VECTOR_STORE_ID = os.getenv("OPENAI_VECTOR_STORE_ID")
INDEX_PATH = "manifest_index.faiss"
CHUNKS_PATH = "manifest_metadata.pkl"
EMBED_MODEL = "text-embedding-3-small"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

if not USE_VECTOR_STORE:
    # Load FAISS index and chunks
    index = faiss.read_index(INDEX_PATH)
    with open(CHUNKS_PATH, "rb") as f:
        chunks = pickle.load(f)

def search_relevant_models(query: str, top_k: int = 3) -> list[dict]:
    print(f"🔍 Searching manifest index for query: {query}")

    if USE_VECTOR_STORE:
        try:
            response = client.vector_stores.query(
                vector_store_id=VECTOR_STORE_ID,
                query=query,
                top_k=top_k,
            )
            matches = []
            for doc in response.data:
                metadata = doc.metadata or {}
                matches.append({
                    "model": metadata.get("model", "unknown_model"),
                    "context": doc.text
                })
                print(f"✅ Match found: {metadata.get('model', 'unknown_model')}")
            return matches
        except Exception as e:
            print(f"❌ Error during vector store search: {e}")
            return []

    else:
        try:
            embedding_response = client.embeddings.create(
                input=[query],
                model=EMBED_MODEL
            )
            embedding = np.array(embedding_response.data[0].embedding, dtype=np.float32).reshape(1, -1)

            distances, indices = index.search(embedding, top_k)
            results = []
            for idx in indices[0]:
                if idx < len(chunks):
                    chunk = chunks[idx]
                    if isinstance(chunk, dict) and "model" in chunk and "context" in chunk:
                        results.append(chunk)
                        print(f"✅ Match found: {chunk['model']}")
                    else:
                        print(f"⚠️ Skipping invalid chunk at index {idx}")
            return results
        except Exception as e:
            print(f"❌ Error during FAISS search: {e}")
            return []
