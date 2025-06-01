from app.query_manifest import search_manifest

query = "How many tickets were created in Voice channel for Worker Ops?"

results = search_manifest(query, k=3)

for i, r in enumerate(results, 1):
    print(f"\n🔎 Result {i}: {r['model']}")
    print(r["context"])