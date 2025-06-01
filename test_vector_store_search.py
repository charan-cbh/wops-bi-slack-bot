import os
import httpx
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VECTOR_STORE_ID = os.getenv("OPENAI_VECTOR_STORE_ID")

def search_vector_store(query: str, max_results: int = 5):
    url = f"https://api.openai.com/v1/vector_stores/{VECTOR_STORE_ID}/search"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "query": query,
        "max_num_results": max_results,
        "rewrite_query": True
    }

    print(f"🔍 Searching vector store for: {query}")
    response = httpx.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        results = response.json()
        for match in results.get("data", []):
            metadata = match.get("metadata", {})
            text = match.get("text", "")[:300]
            print(f"\n✅ Match: {metadata.get('model')}")
            print(f"{text}\n---")
    else:
        print(f"❌ Search failed: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    test_question = "how many tickets were created in the Voice channel in the last 7 days?"
    search_vector_store(test_question)