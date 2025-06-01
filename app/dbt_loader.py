import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

DBT_API_TOKEN = os.getenv("DBT_API_TOKEN")
DBT_ACCOUNT_ID = os.getenv("DBT_ACCOUNT_ID")
DBT_JOB_ID = os.getenv("DBT_JOB_ID")

HEADERS = {
    "Authorization": f"Token {DBT_API_TOKEN}",
    "Content-Type": "application/json"
}

def get_latest_successful_run_id():
    print(f"🔍 Checking latest runs for Job ID: {DBT_JOB_ID}")

    # Use /runs endpoint with job_id filter
    url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/"
    params = {
        "job_definition_id": DBT_JOB_ID,
        "order_by": "-id",
        "limit": 10
    }

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    runs = response.json().get("data", [])

    for run in runs:
        if run.get("status") == 10:  # 10 = success
            print(f"✅ Found successful run: {run['id']}")
            return run["id"]

    raise Exception("❌ No successful runs found for the job.")

def fetch_manifest_json():
    run_id = get_latest_successful_run_id()
    manifest_url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/artifacts/manifest.json"
    response = requests.get(manifest_url, headers=HEADERS)
    response.raise_for_status()
    manifest = response.json()

    # ✅ Save to local file
    with open("manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
        print("📁 Manifest saved to manifest.json")

    return manifest

def extract_models_from_manifest(manifest):
    models = {}
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model":
            models[node["name"]] = {
                "description": node.get("description", ""),
                "columns": {
                    col_name: col_data.get("description", "")
                    for col_name, col_data in node.get("columns", {}).items()
                }
            }
    return models