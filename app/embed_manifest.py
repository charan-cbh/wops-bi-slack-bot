import json
import os
import pickle
from typing import List, Dict

import faiss
import numpy as np
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
EMBED_MODEL = "text-embedding-3-small"

def load_manifest(filepath: str = "manifest.json") -> Dict:
    with open(filepath, "r") as f:
        return json.load(f)

def prepare_model_chunks(manifest, team_model_list=None):
    chunks = []
    for unique_id, node in manifest["nodes"].items():
        if node["resource_type"] == "model":
            model_name = node["name"]

            if team_model_list and model_name not in team_model_list:
                continue

            model_doc = f"""
Model Name: {model_name}
Purpose: {node.get('description', '')}
Fully qualified table name: ANALYTICS.DBT_PRODUCTION.{model_name}
This model is used in Snowflake and may be referred by its name or parts of it.

Columns:
"""
            for col, meta in node.get("columns", {}).items():
                col_desc = meta.get("description", "")
                model_doc += f"- {col}: {col_desc}\n"

            chunks.append({
                "model": model_name,
                "text": model_doc.strip(),
                "context": model_doc.strip(),
            })
    return chunks


def embed_texts(texts: List[str], batch_size: int = 100) -> List[List[float]]:
    print("🔁 Generating embeddings in batches...")

    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        try:
            response = client.embeddings.create(
                input=batch,
                model=EMBED_MODEL
            )
            batch_embeddings = [r.embedding for r in response.data]
            all_embeddings.extend(batch_embeddings)
            print(f"✅ Embedded batch {i // batch_size + 1}: {len(all_embeddings)} / {len(texts)}")
        except Exception as e:
            print(f"❌ Failed to embed batch {i // batch_size + 1}: {e}")
            raise

    return all_embeddings

def build_faiss_index(chunks: List[Dict], faiss_file="manifest_index.faiss", metadata_file="manifest_metadata.pkl"):
    texts = [chunk["text"] for chunk in chunks]
    ids = [chunk["model"] for chunk in chunks]

    vectors = embed_texts(texts)
    dim = len(vectors[0])

    index = faiss.IndexFlatL2(dim)
    index.add(np.array(vectors).astype("float32"))

    print("💾 Saving index and metadata...")
    faiss.write_index(index, faiss_file)
    with open(metadata_file, "wb") as f:
        pickle.dump([{"model": model, "context": text} for model, text in zip(ids, texts)], f)

    print(f"✅ Indexed {len(chunks)} models.")
