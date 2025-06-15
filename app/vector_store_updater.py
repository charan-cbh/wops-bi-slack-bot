import os
import json
import requests
import schedule
import time
import argparse
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from team_models import team_model_list

load_dotenv()

# OpenAI Configuration
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
VECTOR_STORE_ID = os.getenv("OPENAI_VECTOR_STORE_ID")  # Your existing vector store ID
ASSISTANT_ID = os.getenv("ASSISTANT_ID")  # Your existing assistant ID

# DBT Configuration
DBT_API_TOKEN = os.getenv("DBT_API_TOKEN")
DBT_ACCOUNT_ID = os.getenv("DBT_ACCOUNT_ID")
DBT_JOB_ID = os.getenv("DBT_JOB_ID")

# Schedule Configuration
UPDATE_SCHEDULE_TIME = os.getenv("VECTOR_STORE_UPDATE_TIME", "02:00")  # Default 2 AM
UPDATE_SCHEDULE_DAYS = os.getenv("VECTOR_STORE_UPDATE_DAYS", "daily")  # daily, weekdays, or specific days

# Local manifest path (for backup/cache)
LOCAL_MANIFEST_PATH = "manifest.json"
MANIFEST_BACKUP_PATH = "manifest_backup.json"

HEADERS = {
    "Authorization": f"Token {DBT_API_TOKEN}",
    "Content-Type": "application/json"
}


@dataclass
class TableColumn:
    name: str
    data_type: str
    description: str
    is_metric: bool = False
    is_dimension: bool = False
    is_date: bool = False


@dataclass
class TableSchema:
    full_name: str  # e.g., "ANALYTICS.dbt_production.fct_zendesk__mqr_tickets"
    table_name: str  # e.g., "fct_zendesk__mqr_tickets"
    description: str
    columns: List[TableColumn]
    common_queries: List[str] = None
    keywords: List[str] = None

    def to_searchable_text(self) -> str:
        """Convert to searchable text for vector store"""
        text_parts = [
            f"TABLE: {self.full_name}",
            f"NAME: {self.table_name}",
            f"DESCRIPTION: {self.description}",
            "",
            "USE THIS TABLE FOR:",
        ]

        # Add keywords
        if self.keywords:
            text_parts.append(f"- Questions about: {', '.join(self.keywords)}")

        # Add common queries
        if self.common_queries:
            text_parts.append("\nCOMMON QUERIES:")
            for query in self.common_queries:
                text_parts.append(f"- {query}")

        # Add columns section
        text_parts.extend([
            "",
            "COLUMNS:",
        ])

        # Group columns by type
        metrics = [col for col in self.columns if col.is_metric]
        dimensions = [col for col in self.columns if col.is_dimension]
        dates = [col for col in self.columns if col.is_date]
        others = [col for col in self.columns if not (col.is_metric or col.is_dimension or col.is_date)]

        if metrics:
            text_parts.append("\nMETRIC COLUMNS (for calculations, KPIs):")
            for col in metrics:
                text_parts.append(f"- {col.name} ({col.data_type}): {col.description}")

        if dimensions:
            text_parts.append("\nDIMENSION COLUMNS (for grouping, filtering):")
            for col in dimensions:
                text_parts.append(f"- {col.name} ({col.data_type}): {col.description}")

        if dates:
            text_parts.append("\nDATE COLUMNS (for time filters):")
            for col in dates:
                text_parts.append(f"- {col.name} ({col.data_type}): {col.description}")

        if others:
            text_parts.append("\nOTHER COLUMNS:")
            for col in others[:10]:  # Limit to avoid too much text
                text_parts.append(f"- {col.name} ({col.data_type}): {col.description}")

        # Add SQL examples
        text_parts.extend([
            "",
            "SQL REFERENCE:",
            f"SELECT * FROM {self.full_name} LIMIT 10;",
        ])

        if metrics and dates:
            metric_col = metrics[0].name
            date_col = dates[0].name
            text_parts.append(
                f"SELECT COUNT(*), AVG({metric_col}) FROM {self.full_name} WHERE {date_col} = CURRENT_DATE();")

        # Add JSON output instructions
        text_parts.extend([
            "",
            "WHEN SEARCHING FOR TABLES:",
            f"Return this table as: [\"{self.full_name}\"]",
        ])

        return "\n".join(text_parts)


def get_latest_successful_run_id() -> Optional[str]:
    """Fetch the latest successful dbt run ID"""
    print(f"🔍 Checking latest runs for Job ID: {DBT_JOB_ID}")

    url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/"
    params = {
        "job_definition_id": DBT_JOB_ID,
        "order_by": "-id",
        "limit": 10
    }

    try:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        runs = response.json().get("data", [])

        for run in runs:
            if run.get("status") == 10:  # 10 = success
                print(f"✅ Found successful run: {run['id']}")
                return run["id"]

        print("❌ No successful runs found for the job")
        return None

    except Exception as e:
        print(f"❌ Error fetching run ID: {e}")
        return None


def fetch_manifest_from_dbt() -> Optional[Dict]:
    """Fetch the latest manifest from dbt Cloud"""
    run_id = get_latest_successful_run_id()
    if not run_id:
        return None

    manifest_url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/artifacts/manifest.json"

    try:
        print("📥 Downloading manifest from dbt Cloud...")
        response = requests.get(manifest_url, headers=HEADERS)
        response.raise_for_status()
        manifest = response.json()

        # Backup existing manifest
        if os.path.exists(LOCAL_MANIFEST_PATH):
            os.rename(LOCAL_MANIFEST_PATH, MANIFEST_BACKUP_PATH)
            print("📁 Backed up existing manifest")

        # Save new manifest
        with open(LOCAL_MANIFEST_PATH, "w") as f:
            json.dump(manifest, f, indent=2)
            print("📁 New manifest saved locally")

        return manifest

    except Exception as e:
        print(f"❌ Error fetching manifest: {e}")
        # Try to use local manifest as fallback
        if os.path.exists(LOCAL_MANIFEST_PATH):
            print("📁 Using local manifest as fallback")
            with open(LOCAL_MANIFEST_PATH, "r") as f:
                return json.load(f)
        return None


def classify_column_type(col_name: str, col_description: str, data_type: str) -> Dict[str, bool]:
    """Classify column as metric, dimension, or date based on name and description"""
    col_lower = col_name.lower()
    desc_lower = col_description.lower() if col_description else ""

    # Date columns
    date_indicators = ['date', 'created', 'updated', 'time', 'timestamp', 'at_', '_at', 'when']
    is_date = any(indicator in col_lower for indicator in date_indicators) or 'date' in data_type.lower()

    # Metric columns
    metric_indicators = [
        'count', 'sum', 'avg', 'average', 'total', 'amount',
        'score', 'rate', 'percentage', 'percent', 'pct',
        'minutes', 'seconds', 'hours', 'duration',
        'aht', 'csat', 'fcr', 'qa_', 'kpi', 'metric',
        'productivity', 'efficiency', 'performance'
    ]
    is_metric = (any(indicator in col_lower for indicator in metric_indicators) or
                 any(indicator in desc_lower for indicator in metric_indicators) or
                 data_type.upper() in ['NUMBER', 'FLOAT', 'DECIMAL', 'INTEGER'])

    # Dimension columns
    dimension_indicators = [
        'id', 'name', 'type', 'status', 'category', 'group',
        'channel', 'source', 'agent', 'team', 'customer',
        'priority', 'tier', 'segment', 'region', 'country'
    ]
    is_dimension = (any(indicator in col_lower for indicator in dimension_indicators) or
                    data_type.upper() in ['VARCHAR', 'STRING', 'TEXT'])

    # If it's a date, it's not a metric or dimension
    if is_date:
        is_metric = False
        is_dimension = False

    return {
        'is_metric': is_metric and not is_date,
        'is_dimension': is_dimension and not is_date and not is_metric,
        'is_date': is_date
    }


def extract_table_schemas_from_manifest(manifest: Dict) -> Dict[str, TableSchema]:
    """Extract structured table schemas from dbt manifest"""
    print(f"🔍 Extracting schemas for team models...")

    table_schemas = {}

    for unique_id, node in manifest.get("nodes", {}).items():
        if node["resource_type"] != "model":
            continue

        name = node.get("name")

        # Only process team models
        if name not in team_model_list:
            continue

        print(f"✅ Processing: {name}")

        # Extract basic info
        database = node.get("database", "")
        schema = node.get("schema", "")
        alias = node.get("alias", name)
        full_name = f"{database}.{schema}.{alias}"
        description = node.get("description", "No description available")

        # Extract columns
        columns_data = node.get("columns", {})
        columns = []

        for col_name, col_info in columns_data.items():
            col_desc = col_info.get("description", "No description")
            col_type = col_info.get("data_type", "Unknown type")

            # Classify column type
            col_classification = classify_column_type(col_name, col_desc, col_type)

            columns.append(TableColumn(
                name=col_name,
                data_type=col_type,
                description=col_desc,
                **col_classification
            ))

        # Generate keywords based on table name and description
        keywords = []

        # Add words from table name
        table_words = name.replace('_', ' ').split()
        keywords.extend(table_words)

        # Add specific keywords based on table patterns
        if 'ticket' in name:
            keywords.extend(['tickets', 'ticket', 'volume', 'count', 'created', 'zendesk'])
        if 'agent' in name:
            keywords.extend(['agent', 'agents', 'performance', 'kpi', 'kpis'])
        if 'csat' in name:
            keywords.extend(['csat', 'satisfaction', 'survey', 'feedback'])
        if 'performance' in name:
            keywords.extend(['performance', 'kpi', 'metrics', 'productivity'])

        # Generate common queries based on table type
        common_queries = []

        if 'ticket' in name:
            common_queries.extend([
                "How many tickets were created today?",
                "What is the ticket volume?",
                "Show ticket count by date",
                "Ticket creation trends"
            ])

        if 'agent' in name and 'performance' in name:
            common_queries.extend([
                "What are the KPIs that determine agent performance?",
                "Show agent performance metrics",
                "Agent KPI report",
                "Which agents have the best performance?"
            ])

        if 'csat' in name:
            common_queries.extend([
                "What is the CSAT score?",
                "Show customer satisfaction trends",
                "CSAT by agent or team"
            ])

        # Create TableSchema
        table_schema = TableSchema(
            full_name=full_name,
            table_name=name,
            description=description,
            columns=columns,
            common_queries=common_queries if common_queries else None,
            keywords=list(set(keywords)) if keywords else None
        )

        table_schemas[name] = table_schema

    print(f"📊 Extracted {len(table_schemas)} table schemas")
    return table_schemas


def create_enhanced_documents(table_schemas: Dict[str, TableSchema]) -> List[Dict]:
    """Create enhanced documents for vector store"""
    documents = []

    for table_name, schema in table_schemas.items():
        # Create main table document with structured format
        main_doc = {
            "name": f"{table_name}_enhanced.txt",
            "content": schema.to_searchable_text(),
            "metadata": {
                "type": "table_schema",
                "table_name": schema.full_name,
                "has_metrics": any(col.is_metric for col in schema.columns),
                "has_dates": any(col.is_date for col in schema.columns),
                "metric_count": sum(1 for col in schema.columns if col.is_metric),
                "dimension_count": sum(1 for col in schema.columns if col.is_dimension)
            }
        }
        documents.append(main_doc)

        # Create a columns-focused document
        columns_content = f"TABLE: {schema.full_name}\n\n"
        columns_content += f"This table contains columns for: {schema.description}\n\n"
        columns_content += "ALL COLUMNS:\n"

        for col in schema.columns:
            col_type_str = ""
            if col.is_metric:
                col_type_str = " [METRIC]"
            elif col.is_dimension:
                col_type_str = " [DIMENSION]"
            elif col.is_date:
                col_type_str = " [DATE]"

            columns_content += f"- {col.name}{col_type_str}: {col.description}\n"

        columns_content += f"\n\nWhen asked about columns in {table_name}, return: [\"{schema.full_name}\"]"

        columns_doc = {
            "name": f"{table_name}_columns.txt",
            "content": columns_content,
            "metadata": {
                "type": "table_columns",
                "table_name": schema.full_name
            }
        }
        documents.append(columns_doc)

        # Create a query patterns document if common queries exist
        if schema.common_queries:
            queries_content = f"QUERY PATTERNS FOR TABLE: {schema.full_name}\n\n"
            queries_content += "This table is commonly used for:\n"
            for query in schema.common_queries:
                queries_content += f"- {query}\n"

            queries_content += f"\n\nFor any of these questions, use table: [\"{schema.full_name}\"]"

            queries_doc = {
                "name": f"{table_name}_queries.txt",
                "content": queries_content,
                "metadata": {
                    "type": "query_patterns",
                    "table_name": schema.full_name
                }
            }
            documents.append(queries_doc)

    # Create a master index document
    index_content = "MASTER TABLE INDEX\n\n"
    index_content += "Available tables for SQL queries:\n\n"

    # Group tables by category
    ticket_tables = []
    agent_tables = []
    other_tables = []

    for table_name, schema in table_schemas.items():
        table_entry = f"- {schema.full_name}: {schema.description[:100]}..."

        if 'ticket' in table_name:
            ticket_tables.append(table_entry)
        elif 'agent' in table_name:
            agent_tables.append(table_entry)
        else:
            other_tables.append(table_entry)

    if ticket_tables:
        index_content += "TICKET TABLES:\n"
        index_content += "\n".join(ticket_tables) + "\n\n"

    if agent_tables:
        index_content += "AGENT/PERFORMANCE TABLES:\n"
        index_content += "\n".join(agent_tables) + "\n\n"

    if other_tables:
        index_content += "OTHER TABLES:\n"
        index_content += "\n".join(other_tables) + "\n\n"

    index_doc = {
        "name": "table_index.txt",
        "content": index_content,
        "metadata": {
            "type": "table_index"
        }
    }
    documents.append(index_doc)

    return documents


def get_existing_vector_store_files(vector_store_id: str) -> List[str]:
    """Get list of existing file IDs in the vector store"""
    print("📋 Fetching existing files from vector store...")

    try:
        files = client.beta.vector_stores.files.list(
            vector_store_id=vector_store_id,
            limit=100
        )

        file_ids = [file.id for file in files.data]
        print(f"📋 Found {len(file_ids)} existing files")
        return file_ids

    except Exception as e:
        print(f"❌ Error fetching existing files: {e}")
        return []


def delete_files_from_vector_store(vector_store_id: str, file_ids: List[str]):
    """Delete files from vector store"""
    if not file_ids:
        return

    print(f"🗑️ Deleting {len(file_ids)} old files from vector store...")

    for file_id in file_ids:
        try:
            # Delete from vector store
            client.beta.vector_stores.files.delete(
                vector_store_id=vector_store_id,
                file_id=file_id
            )
            # Delete the file itself
            client.files.delete(file_id)
        except Exception as e:
            print(f"⚠️ Error deleting file {file_id}: {e}")

    print("✅ Old files deleted")


def upload_new_files_to_vector_store(vector_store_id: str, documents: List[Dict]) -> List[str]:
    """Upload new documents as files to vector store"""
    print(f"🚀 Uploading {len(documents)} new files to vector store...")

    file_ids = []
    temp_files = []

    try:
        # Upload each document as a file
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

            if (i + 1) % 10 == 0:
                print(f"📄 Uploaded {i + 1}/{len(documents)} files...")

        # Add files to vector store in batch
        if file_ids:
            client.beta.vector_stores.file_batches.create_and_poll(
                vector_store_id=vector_store_id,
                file_ids=file_ids
            )
            print(f"✅ Added {len(file_ids)} files to vector store")

        return file_ids

    except Exception as e:
        print(f"❌ Error uploading files: {e}")
        return []
    finally:
        # Clean up temporary files
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
            except:
                pass


def update_vector_store():
    """Main function to update the vector store with latest manifest"""
    print(f"\n{'=' * 60}")
    print(f"🔄 VECTOR STORE UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    if not VECTOR_STORE_ID:
        print("❌ VECTOR_STORE_ID not found in environment variables")
        return False

    # Step 1: Fetch latest manifest
    manifest = fetch_manifest_from_dbt()
    if not manifest:
        print("❌ Failed to fetch manifest")
        return False

    # Step 2: Extract table schemas
    table_schemas = extract_table_schemas_from_manifest(manifest)
    if not table_schemas:
        print("❌ No table schemas extracted")
        return False

    # Step 3: Create enhanced documents
    documents = create_enhanced_documents(table_schemas)
    if not documents:
        print("❌ No documents created")
        return False

    print(f"📄 Created {len(documents)} documents from {len(table_schemas)} tables")

    # Step 4: Get existing files
    existing_file_ids = get_existing_vector_store_files(VECTOR_STORE_ID)

    # Step 5: Delete old files
    if existing_file_ids:
        delete_files_from_vector_store(VECTOR_STORE_ID, existing_file_ids)

    # Step 6: Upload new files
    new_file_ids = upload_new_files_to_vector_store(VECTOR_STORE_ID, documents)

    if new_file_ids:
        print(f"\n✅ VECTOR STORE UPDATE COMPLETE!")
        print(f"📊 Updated with {len(new_file_ids)} files")
        print(f"📊 Covering {len(table_schemas)} team model tables")
        print(f"🤖 Assistant ID: {ASSISTANT_ID}")
        print(f"📚 Vector Store ID: {VECTOR_STORE_ID}")

        # Print summary of tables
        print(f"\n📋 Tables included:")
        for table_name in sorted(table_schemas.keys()):
            schema = table_schemas[table_name]
            metrics = sum(1 for col in schema.columns if col.is_metric)
            dimensions = sum(1 for col in schema.columns if col.is_dimension)
            print(f"   - {table_name}: {len(schema.columns)} cols ({metrics} metrics, {dimensions} dimensions)")

        return True
    else:
        print("❌ Failed to upload new files")
        return False


def setup_schedule():
    """Setup the update schedule based on configuration"""
    schedule_time = UPDATE_SCHEDULE_TIME
    schedule_days = UPDATE_SCHEDULE_DAYS.lower()

    print(f"⏰ Setting up schedule: {schedule_days} at {schedule_time}")

    if schedule_days == "daily":
        schedule.every().day.at(schedule_time).do(update_vector_store)
    elif schedule_days == "weekdays":
        schedule.every().monday.at(schedule_time).do(update_vector_store)
        schedule.every().tuesday.at(schedule_time).do(update_vector_store)
        schedule.every().wednesday.at(schedule_time).do(update_vector_store)
        schedule.every().thursday.at(schedule_time).do(update_vector_store)
        schedule.every().friday.at(schedule_time).do(update_vector_store)
    elif schedule_days == "weekly":
        schedule.every().monday.at(schedule_time).do(update_vector_store)
    else:
        # Default to daily
        schedule.every().day.at(schedule_time).do(update_vector_store)

    print(f"✅ Schedule configured: {schedule_days} at {schedule_time}")


def run_scheduled_updates():
    """Run the scheduled update service"""
    print(f"\n🚀 Starting Vector Store Update Service")
    print(f"⏰ Schedule: {UPDATE_SCHEDULE_DAYS} at {UPDATE_SCHEDULE_TIME}")
    print(f"📚 Vector Store ID: {VECTOR_STORE_ID}")
    print(f"🤖 Assistant ID: {ASSISTANT_ID}")
    print(f"👥 Team models: {len(team_model_list)} models")
    print(f"\nPress Ctrl+C to stop the service\n")

    setup_schedule()

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n🛑 Stopping Vector Store Update Service")


def main():
    """Main entry point with CLI options"""
    parser = argparse.ArgumentParser(description="Enhanced Vector Store Updater for BI Bot")
    parser.add_argument("--update-now", action="store_true", help="Run update immediately")
    parser.add_argument("--schedule", action="store_true", help="Run scheduled updates")
    parser.add_argument("--check-status", action="store_true", help="Check vector store status")
    parser.add_argument("--test-schemas", action="store_true", help="Test schema extraction without updating")

    args = parser.parse_args()

    if args.check_status:
        print(f"📚 Vector Store ID: {VECTOR_STORE_ID}")
        print(f"🤖 Assistant ID: {ASSISTANT_ID}")
        print(f"👥 Team models configured: {len(team_model_list)}")

        # Check existing files
        if VECTOR_STORE_ID:
            files = get_existing_vector_store_files(VECTOR_STORE_ID)
            print(f"📄 Current files in vector store: {len(files)}")

    elif args.test_schemas:
        # Test schema extraction
        print("🧪 Testing schema extraction...")
        manifest = fetch_manifest_from_dbt()
        if manifest:
            table_schemas = extract_table_schemas_from_manifest(manifest)
            print(f"\n📊 Extracted {len(table_schemas)} table schemas")

            for table_name, schema in list(table_schemas.items())[:3]:  # Show first 3
                print(f"\n{'=' * 60}")
                print(f"Table: {schema.full_name}")
                print(f"Metrics: {sum(1 for col in schema.columns if col.is_metric)}")
                print(f"Dimensions: {sum(1 for col in schema.columns if col.is_dimension)}")
                print(f"Date columns: {sum(1 for col in schema.columns if col.is_date)}")
                if schema.keywords:
                    print(f"Keywords: {', '.join(schema.keywords[:5])}")

    elif args.update_now:
        # Run immediate update
        success = update_vector_store()
        if success:
            print("\n✅ Update completed successfully!")
        else:
            print("\n❌ Update failed!")

    elif args.schedule:
        # Run scheduled updates
        run_scheduled_updates()

    else:
        # Default: show help
        parser.print_help()
        print("\n💡 Examples:")
        print("  python vector_store_updater.py --update-now     # Update immediately")
        print("  python vector_store_updater.py --schedule       # Run scheduled updates")
        print("  python vector_store_updater.py --check-status   # Check current status")
        print("  python vector_store_updater.py --test-schemas   # Test schema extraction")


if __name__ == "__main__":
    main()