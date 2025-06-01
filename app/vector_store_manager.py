# vector_store_manager.py
import os
import json
from openai import OpenAI
from dotenv import load_dotenv
from team_models import team_model_list

load_dotenv()

# Check OpenAI version and available APIs
import openai

print(f"📦 OpenAI library version: {openai.__version__}")

print(f"✅ Successfully imported team_model_list with {len(team_model_list)} models")
print(f"📋 Sample models: {team_model_list[:3]}")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Debug: Check what's available in the client
print(f"🔧 Checking OpenAI client structure...")
client_attrs = [attr for attr in dir(client) if not attr.startswith('_')]
relevant_attrs = [attr for attr in client_attrs if 'vector' in attr.lower() or 'assistant' in attr.lower()]
print(f"🔧 Vector/Assistant related attributes: {relevant_attrs}")
if hasattr(client, 'beta'):
    beta_attrs = [attr for attr in dir(client.beta) if not attr.startswith('_')]
    print(f"🔧 Beta attributes: {beta_attrs}")

VECTOR_STORE_NAME = "wops_bi_insights_bot_store"
MANIFEST_PATH = "/Users/charantej/cbh_bi_bot/bi-slack-bot/manifest.json"


# Create vector store
def create_vector_store():
    print("✨ Creating vector store...")
    try:
        response = client.vector_stores.create(name=VECTOR_STORE_NAME)
        store_id = response.id
        print(f"✅ Vector Store created: {store_id}")
        return store_id
    except Exception as e:
        print(f"❌ Error creating vector store: {e}")
        return None


# Convert manifest nodes to documents (filtered by team models)
def prepare_documents_from_manifest():
    print(f"🔍 Starting document preparation...")
    print(f"📋 Team model list has {len(team_model_list)} models")
    print(f"📋 Sample team models: {team_model_list[:5]}")

    with open(MANIFEST_PATH, "r") as f:
        manifest = json.load(f)

    documents = []
    models_found = []
    total_models_in_manifest = 0

    for unique_id, node in manifest["nodes"].items():
        if node["resource_type"] != "model":
            continue

        total_models_in_manifest += 1
        name = node.get("name")

        # CRITICAL FILTERING LOGIC - Only process team models
        if name not in team_model_list:
            continue

        print(f"✅ FOUND TEAM MODEL: {name}")
        models_found.append(name)

        desc = node.get("description", "No description available")
        database = node.get("database", "")
        schema = node.get("schema", "")
        alias = node.get("alias", name)
        columns = node.get("columns", {})

        # Create detailed column descriptions
        column_descriptions = []
        for col_name, col_info in columns.items():
            col_desc = col_info.get("description", "No description")
            col_type = col_info.get("data_type", "Unknown type")
            column_descriptions.append(f"  - {col_name} ({col_type}): {col_desc}")

        column_text = "\n".join(column_descriptions) if column_descriptions else "  No column information available"

        # Create comprehensive searchable content
        file_content = f"""TABLE INFORMATION:
Name: {name}
Database: {database}
Schema: {schema}
Alias: {alias}
Full Table Name: {database}.{schema}.{alias}
Fully Qualified Reference: {database}.{schema}.{alias}

DESCRIPTION:
{desc}

COLUMNS:
{column_text}

SEARCH KEYWORDS:
{name} {alias} {database} {schema} {name.replace('_', ' ')} 
{' '.join(name.split('_'))} table model

SQL REFERENCE:
Use this table as: {database}.{schema}.{alias}
"""

        documents.append({
            "name": f"{name}.txt",
            "content": file_content,
            "metadata": {
                "model": name,
                "db": database,
                "schema": schema,
                "alias": alias,
                "full_table_name": f"{database}.{schema}.{alias}"
            }
        })

    # Summary
    models_not_found = [model for model in team_model_list if model not in models_found]

    print(f"\n📊 FILTERING RESULTS:")
    print(f"📊 Total models in manifest: {total_models_in_manifest}")
    print(f"📊 Team models expected: {len(team_model_list)}")
    print(f"📊 Team models found: {len(models_found)}")
    print(f"📄 Documents prepared: {len(documents)}")

    if models_found:
        print(f"✅ Team models found: {models_found}")

    if models_not_found:
        print(f"⚠️ Missing models: {models_not_found}")

    print(f"📄 Final document count: {len(documents)} (should be ~{len(team_model_list)})")
    return documents


# Upload documents as files to vector store
def upload_files_to_vector_store(vector_store_id, documents):
    print("🚀 Uploading files to vector store...")

    file_ids = []
    temp_files = []

    try:
        for i, doc in enumerate(documents):
            # Create temporary file
            temp_filename = f"temp_{doc['name']}"
            temp_files.append(temp_filename)

            with open(temp_filename, "w", encoding="utf-8") as f:
                f.write(doc["content"])

            # Upload file
            with open(temp_filename, "rb") as f:
                file_response = client.files.create(
                    file=f,
                    purpose="assistants"
                )
                file_ids.append(file_response.id)

            print(f"📄 Uploaded file {i + 1}/{len(documents)}: {doc['name']}")

        # Add files to vector store
        client.vector_stores.file_batches.create_and_poll(
            vector_store_id=vector_store_id,
            file_ids=file_ids
        )

        print(f"✅ Added {len(file_ids)} files to vector store")

    except Exception as e:
        print(f"❌ Error uploading files: {e}")
        return None
    finally:
        # Clean up temporary files
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
            except:
                pass

    return file_ids


def setup_complete_assistant(vector_store_id: str):
    """Set up assistant with proper vector store configuration"""
    try:
        # Try beta first (assistants might still be in beta)
        if hasattr(client, 'beta') and hasattr(client.beta, 'assistants'):
            assistant = client.beta.assistants.create(
                name="WOPS BI Assistant",
                instructions="""
You are a Snowflake SQL expert with access to Worker Operations team's DBT manifest documentation through file search.

CRITICAL: You MUST use the file_search tool to find relevant tables before generating any SQL.

Process:
1. When asked for SQL, first use file_search to find relevant tables and models
2. Look for database, schema, and table name information in the search results
3. Use the exact fully qualified table names (database.schema.table) from the manifest
4. Generate accurate SQL queries with proper Snowflake syntax

Always return SQL in this format:
```sql
SELECT columns
FROM database.schema.table_name
WHERE conditions;
```

If file_search doesn't return relevant information, reply:
"I don't have enough model information to answer that."
""",
                model="gpt-4-turbo",
                tools=[{"type": "file_search"}],
                tool_resources={
                    "file_search": {
                        "vector_store_ids": [vector_store_id]
                    }
                },
                temperature=0.1
            )
        else:
            # Try without beta
            assistant = client.assistants.create(
                name="WOPS BI Assistant",
                instructions="""
You are a Snowflake SQL expert with access to Worker Operations team's DBT manifest documentation through file search.

CRITICAL: You MUST use the file_search tool to find relevant tables before generating any SQL.

Process:
1. When asked for SQL, first use file_search to find relevant tables and models
2. Look for database, schema, and table name information in the search results
3. Use the exact fully qualified table names (database.schema.table) from the manifest
4. Generate accurate SQL queries with proper Snowflake syntax

Always return SQL in this format:
```sql
SELECT columns
FROM database.schema.table_name
WHERE conditions;
```

If file_search doesn't return relevant information, reply:
"I don't have enough model information to answer that."
""",
                model="gpt-4-turbo",
                tools=[{"type": "file_search"}],
                tool_resources={
                    "file_search": {
                        "vector_store_ids": [vector_store_id]
                    }
                },
                temperature=0.1
            )

        print(f"🤖 Assistant created: {assistant.id}")
        print(f"📋 Copy this ASSISTANT_ID to your .env file: {assistant.id}")
        return assistant.id

    except Exception as e:
        print(f"❌ Error creating assistant: {e}")
        print("🔧 Let's check what OpenAI client attributes are available:")
        print(f"🔧 Available attributes: {[attr for attr in dir(client) if not attr.startswith('_')]}")
        return None


def recreate_complete_setup():
    """Recreate vector store and assistant with proper configuration"""
    print("🔄 Setting up WOPS BI assistant...")
    print(f"📊 Will process {len(team_model_list)} team models")

    # Create new vector store
    vector_store_id = create_vector_store()
    if not vector_store_id:
        return None, None

    # Prepare documents with filtering
    docs = prepare_documents_from_manifest()

    if not docs:
        print("❌ No documents prepared - check team_models.py and manifest.json")
        return vector_store_id, None

    if len(docs) > 50:
        print(f"⚠️ WARNING: {len(docs)} documents - seems like filtering didn't work!")
        return vector_store_id, None

    # Upload files to vector store
    file_ids = upload_files_to_vector_store(vector_store_id, docs)
    if not file_ids:
        return vector_store_id, None

    print(f"🎉 Vector store ready: {vector_store_id}")

    # Set up assistant
    assistant_id = setup_complete_assistant(vector_store_id)

    if assistant_id:
        print("\n" + "=" * 50)
        print("✅ SETUP COMPLETE!")
        print(f"📊 Vector Store ID: {vector_store_id}")
        print(f"🤖 Assistant ID: {assistant_id}")
        print(f"📋 Models included: {len(docs)}")
        print("\n🔧 Add to your .env file:")
        print(f"ASSISTANT_ID={assistant_id}")
        print("=" * 50)

    return vector_store_id, assistant_id


if __name__ == "__main__":
    recreate_complete_setup()