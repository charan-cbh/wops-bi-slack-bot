import os
import json
from dotenv import load_dotenv
import openai

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")

VECTOR_STORE_NAME = os.getenv("VECTOR_STORE_NAME", "wops_bi_insights_bot_store")
MANIFEST_PATH = "manifest.json"

# Load manifest JSON
def load_manifest():
    with open(MANIFEST_PATH, "r") as f:
        return json.load(f)

# Format each model as a retrievable text chunk
def format_model_chunks(manifest):
    chunks = []
    for unique_id, node in manifest["nodes"].items():
        if node["resource_type"] == "model":
            model_name = node.get("name", "unknown_model")
            description = node.get("description", "")
            columns = node.get("columns", {})

            col_docs = "\n".join(
                f"- {col}: {meta.get('description', '')}" for col, meta in columns.items()
            )

            context = f"Model: {model_name}\nDescription: {description}\nColumns:\n{col_docs}"
            chunks.append({
                "model": model_name,
                "context": context
            })
    return chunks

# Upload to OpenAI Vector Store
def upload_to_vector_store(chunks):
    print("🔁 Creating vector store...")

    try:
        store = openai.vector_stores.create(name=VECTOR_STORE_NAME)
    except Exception as e:
        print(f"❌ Failed to create vector store: {e}")
        return

    # Write .jsonl file
    os.makedirs("vector_temp", exist_ok=True)
    file_path = "vector_temp/manifest_chunks.json"  # ✅ Must be .jsonl

    with open(file_path, "w") as f:
        for chunk in chunks:
            f.write(json.dumps({
                "text": chunk["context"],
                "metadata": {
                    "model": chunk["model"]
                }
            }) + "\n")

    print("📤 Uploading chunks to vector store...")
    try:
        file = openai.files.create(file=open(file_path, "rb"), purpose="assistants")
        openai.vector_stores.file_batches.create(
            vector_store_id=store.id,
            file_ids=[file.id]
        )
        print(f"✅ Uploaded {len(chunks)} models to vector store '{VECTOR_STORE_NAME}'")
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    manifest = load_manifest()
    chunks = format_model_chunks(manifest)
    upload_to_vector_store(chunks)