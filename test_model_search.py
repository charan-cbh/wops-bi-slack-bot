from app.manifest_index import search_relevant_models

question = "how many tickets were created in the Voice channel in the last 7 days? Use wfm zendesk tickets staging table to get this data"
print("🔍 Query:", question)

results = search_relevant_models(question)
print("\n📄 Matched context:\n")
for r in results:
    print(f"Model: {r['model']}\nContext: {r['context'][:300]}...\n")