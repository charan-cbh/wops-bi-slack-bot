from app.embed_manifest import load_manifest, prepare_model_chunks, build_faiss_index
from app.team_models import team_model_list  # put your full list in a separate module

print("🔍 Loading manifest...")
manifest = load_manifest("manifest.json")

print("🔧 Preparing model chunks...")
chunks = prepare_model_chunks(manifest)

print(f"📦 Total models to embed: {len(chunks)}")

print("🔁 Generating embeddings and saving index...")
build_faiss_index(chunks)