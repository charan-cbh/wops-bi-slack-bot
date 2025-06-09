import os
import asyncio
import time
import json
import traceback
from typing import Optional, Dict, Any, Tuple, List
from dotenv import load_dotenv
from openai import OpenAI
from glide import GlideClient, GlideClientConfiguration, NodeAddress, GlideClusterClient, \
    GlideClusterClientConfiguration

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("ASSISTANT_ID")
VECTOR_STORE_ID = os.getenv("OPENAI_VECTOR_STORE_ID")

# Valkey configuration
VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_USE_TLS = os.getenv("VALKEY_USE_TLS", "true").lower() == "true"
IS_LOCAL_DEV = os.getenv("IS_LOCAL_DEV", "false").lower() == "true"

# Import Snowflake runner for schema discovery
try:
    from app.snowflake_runner import run_query

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("⚠️ Snowflake runner not available for schema discovery")

# Cache TTL settings (in seconds)
THREAD_CACHE_TTL = 3600  # 1 hour
SQL_CACHE_TTL = 86400  # 24 hours
SCHEMA_CACHE_TTL = 604800  # 7 days for table schema cache
CONVERSATION_CACHE_TTL = 600  # 10 minutes

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)

# Global Valkey client
valkey_client = None

# Fallback to local memory if Valkey is not available
_local_cache = {
    'thread': {},
    'sql': {},
    'schema': {},  # Cache for table schemas
    'conversation': {}
}

# Cache key prefixes
CACHE_PREFIX = "bi_slack_bot"
THREAD_CACHE_PREFIX = f"{CACHE_PREFIX}:thread"
SQL_CACHE_PREFIX = f"{CACHE_PREFIX}:sql"
SCHEMA_CACHE_PREFIX = f"{CACHE_PREFIX}:schema"
CONVERSATION_CACHE_PREFIX = f"{CACHE_PREFIX}:conversation"

# Known tables mapping
KNOWN_TABLES = {
    'tickets': 'ANALYTICS.dbt_production.fct_zendesk_tickets',
    'messaging': 'ANALYTICS.dbt_production.fct_zendesk_tickets',
    'agents': 'ANALYTICS.dbt_production.fct_amazon_connect__agent_metrics',
    'reviews': 'ANALYTICS.dbt_production.fct_klaus__reviews'
}

# Common column name variations (to help the bot find the right column)
COLUMN_VARIATIONS = {
    'reply_time': ['reply_time_in_minutes', 'first_reply_time', 'first_reply_time_minutes',
                   'reply_time_minutes', 'time_to_first_reply', 'first_response_time'],
    'group': ['group_name', 'channel', 'via_channel', 'ticket_channel', 'group'],
    'created': ['created_at', 'created_date', 'creation_date', 'ticket_created_at'],
    'resolved': ['resolved_at', 'resolution_date', 'closed_at', 'solved_at']
}

# Minimal pattern recognition for intent
INTENT_PATTERNS = {
    'count': ['how many', 'count', 'total number', 'number of'],
    'time_filter': ['today', 'yesterday', 'last week', 'this week', 'last month'],
    'group_filter': ['messaging', 'voice', 'email'],
    'metrics': ['reply time', 'response time', 'resolution time', 'aht', 'average handling', 'first reply'],
    'ranking': ['highest', 'lowest', 'top', 'bottom', 'most', 'least']
}


async def init_valkey_client():
    """Initialize Valkey client - must be called in async context"""
    global valkey_client

    if IS_LOCAL_DEV:
        print("🏠 Local development mode - skipping Valkey connection")
        valkey_client = None
        return

    try:
        addresses = [NodeAddress(VALKEY_HOST, VALKEY_PORT)]
        config = GlideClusterClientConfiguration(
            addresses=addresses,
            use_tls=VALKEY_USE_TLS,
            request_timeout=10000,
        )

        from glide import GlideClusterClient
        valkey_client = await GlideClusterClient.create(config)

        pong = await valkey_client.ping()
        print(f"✅ Valkey connection established: {pong}")

    except Exception as e:
        print(f"❌ Valkey connection failed: {e}")
        valkey_client = None


async def ensure_valkey_connection():
    """Ensure Valkey client is initialized"""
    global valkey_client
    if valkey_client is None:
        await init_valkey_client()


async def safe_valkey_get(key: str, default=None):
    """Safely get value from Valkey with fallback"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            value = await valkey_client.get(key)
            if value:
                return json.loads(value)
            return default
        except Exception as e:
            print(f"⚠️ Valkey GET error for {key}: {e}")
            return default
    else:
        cache_type = key.split(':')[1] if ':' in key else 'thread'
        return _local_cache.get(cache_type, {}).get(key, default)


async def safe_valkey_set(key: str, value: Any, ex: int = None):
    """Safely set value in Valkey with fallback"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            json_value = json.dumps(value)
            if ex:
                await valkey_client.set(key, json_value)
                await valkey_client.expire(key, ex)
            else:
                await valkey_client.set(key, json_value)
            return True
        except Exception as e:
            print(f"⚠️ Valkey SET error for {key}: {e}")
            return False
    else:
        cache_type = key.split(':')[1] if ':' in key else 'thread'
        if cache_type not in _local_cache:
            _local_cache[cache_type] = {}
        _local_cache[cache_type][key] = value
        return True


async def safe_valkey_delete(key: str):
    """Safely delete key from Valkey with fallback"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            await valkey_client.delete([key])
            return True
        except Exception as e:
            print(f"⚠️ Valkey DELETE error for {key}: {e}")
            return False
    else:
        cache_type = key.split(':')[1] if ':' in key else 'thread'
        if cache_type in _local_cache and key in _local_cache[cache_type]:
            del _local_cache[cache_type][key]
        return True


async def safe_valkey_exists(key: str) -> bool:
    """Check if key exists in Valkey with fallback"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            result = await valkey_client.exists([key])
            return result > 0
        except Exception as e:
            print(f"⚠️ Valkey EXISTS error for {key}: {e}")
            return False
    else:
        cache_type = key.split(':')[1] if ':' in key else 'thread'
        return key in _local_cache.get(cache_type, {})


async def close_valkey_connection():
    """Close Valkey connection gracefully"""
    global valkey_client
    if valkey_client:
        try:
            await valkey_client.close()
            print("🔌 Valkey connection closed")
        except Exception as e:
            print(f"❌ Error closing Valkey connection: {e}")
        valkey_client = None


def get_question_hash(question: str) -> str:
    """Generate hash for question to use as cache key"""
    import hashlib
    normalized = ' '.join(question.lower().strip().split())
    return hashlib.md5(normalized.encode()).hexdigest()[:12]


def classify_question_type(question: str) -> str:
    """Simple classification - is this a data query or conversation?"""
    question_lower = question.lower()

    # Check for data query indicators
    data_indicators = [
        'how many', 'count', 'show me', 'list', 'find',
        'highest', 'lowest', 'average', 'total',
        'tickets', 'agents', 'reviews', 'performance',
        'reply time', 'response time', 'resolution'
    ]

    # Check for meta/help indicators
    meta_indicators = [
        'what can you', 'help', 'capabilities', 'questions can',
        'how do you work', 'what data', 'explain'
    ]

    # Check for follow-up indicators
    followup_indicators = ['why', 'what does that mean', 'explain that']

    if any(indicator in question_lower for indicator in data_indicators):
        return 'sql_required'
    elif any(indicator in question_lower for indicator in meta_indicators):
        return 'conversational'
    elif any(indicator in question_lower for indicator in followup_indicators):
        return 'followup'
    else:
        return 'sql_required'  # Default to trying SQL


def extract_intent(question: str) -> dict:
    """Extract basic intent from question"""
    question_lower = question.lower()
    intent = {
        'needs_count': any(p in question_lower for p in INTENT_PATTERNS['count']),
        'time_filter': next((t for t in INTENT_PATTERNS['time_filter'] if t in question_lower), None),
        'group_filter': next((g for g in INTENT_PATTERNS['group_filter'] if g in question_lower), None),
        'metric_type': next((m for m in INTENT_PATTERNS['metrics'] if m in question_lower), None),
        'needs_ranking': any(r in question_lower for r in INTENT_PATTERNS['ranking'])
    }
    return intent


def identify_table_from_question(question: str) -> str:
    """Identify which table to use based on question keywords"""
    question_lower = question.lower()

    # Check for specific table indicators
    if any(word in question_lower for word in ['messaging', 'tickets', 'reply time', 'response time']):
        return KNOWN_TABLES.get('tickets', 'ANALYTICS.dbt_production.fct_zendesk_tickets')
    elif any(word in question_lower for word in ['agent', 'aht', 'handling time']):
        return KNOWN_TABLES.get('agents', 'ANALYTICS.dbt_production.fct_amazon_connect__agent_metrics')
    elif any(word in question_lower for word in ['review', 'klaus', 'qa']):
        return KNOWN_TABLES.get('reviews', 'ANALYTICS.dbt_production.fct_klaus__reviews')
    else:
        # Default to tickets table
        return KNOWN_TABLES.get('tickets', 'ANALYTICS.dbt_production.fct_zendesk_tickets')


async def discover_table_schema(table_name: str) -> dict:
    """Discover table schema by running SELECT * LIMIT 5"""
    print(f"\n{'=' * 60}")
    print(f"🔍 SCHEMA DISCOVERY for table: {table_name}")
    print(f"{'=' * 60}")

    # Check cache first
    cache_key = f"{SCHEMA_CACHE_PREFIX}:{table_name}"
    cached_schema = await safe_valkey_get(cache_key)

    if cached_schema:
        print(f"📋 Using cached schema (cached at: {cached_schema.get('discovered_at', 'unknown')})")
        print(
            f"📋 Columns ({len(cached_schema.get('columns', []))}): {', '.join(cached_schema.get('columns', [])[:10])}")
        if len(cached_schema.get('columns', [])) > 10:
            print(f"    ... and {len(cached_schema.get('columns', [])) - 10} more columns")
        return cached_schema

    # If Snowflake is available, run actual discovery
    if SNOWFLAKE_AVAILABLE:
        try:
            # Run discovery query
            discovery_sql = f"SELECT * FROM {table_name} LIMIT 5"
            print(f"🔍 Running schema discovery query:")
            print(f"   {discovery_sql}")

            df = run_query(discovery_sql)

            if isinstance(df, str):
                print(f"❌ Schema discovery failed: {df}")
                # Try alternative table names if the exact name failed
                if "does not exist" in df.lower() or "invalid identifier" in df.lower():
                    print("🔄 Trying alternative table paths...")
                    alternatives = [
                        table_name.split('.')[-1],  # Just table name
                        f"dbt_production.{table_name.split('.')[-1]}",  # With dbt_production
                        f"ANALYTICS.{table_name.split('.')[-1]}",  # With ANALYTICS
                    ]

                    for alt_table in alternatives:
                        if alt_table != table_name:
                            print(f"🔄 Trying: {alt_table}")
                            alt_sql = f"SELECT * FROM {alt_table} LIMIT 5"
                            df_alt = run_query(alt_sql)
                            if not isinstance(df_alt, str):
                                print(f"✅ Found table at: {alt_table}")
                                table_name = alt_table
                                df = df_alt
                                break

                # If still a string, it's an error
                if isinstance(df, str):
                    return {
                        'table': table_name,
                        'error': df,
                        'columns': []
                    }

            # Extract column names and sample data
            columns = list(df.columns)

            print(f"🔍 Processing {len(columns)} columns for serialization...")

            # Convert timestamps and other non-serializable types to strings for caching
            df_serializable = df.copy()
            for col in df_serializable.columns:
                col_dtype = str(df_serializable[col].dtype)
                if df_serializable[
                    col].dtype == 'datetime64[ns]' or 'timestamp' in col_dtype.lower() or 'datetime' in col_dtype.lower():
                    print(f"   Converting timestamp column '{col}' (dtype: {col_dtype}) to string")
                    df_serializable[col] = df_serializable[col].astype(str)
                elif df_serializable[col].dtype == 'object':
                    # Check if any values are timestamps
                    try:
                        if len(df_serializable) > 0 and hasattr(df_serializable[col].iloc[0], 'isoformat'):
                            print(f"   Converting object column '{col}' with timestamp values to string")
                            df_serializable[col] = df_serializable[col].astype(str)
                    except:
                        pass

            sample_data = df_serializable.head(3).to_dict('records') if len(df_serializable) > 0 else []

            # Verify serialization works
            try:
                test_json = json.dumps(sample_data)
                print(f"✅ Sample data is JSON serializable")
            except TypeError as e:
                print(f"⚠️ Sample data still has non-serializable types: {e}")
                # Fallback: convert everything to strings
                sample_data = []
                for row in df_serializable.head(3).itertuples(index=False):
                    sample_data.append({col: str(getattr(row, col)) for col in columns})

            schema_info = {
                'table': table_name,
                'columns': columns,
                'sample_data': sample_data,
                'row_count': len(df),
                'discovered_at': time.time()
            }

            print(f"✅ Schema discovered successfully!")
            print(f"📊 Columns ({len(columns)}):")
            for i, col in enumerate(columns):
                if i < 15:  # Show first 15 columns
                    print(f"   - {col}")
                elif i == 15:
                    print(f"   ... and {len(columns) - 15} more columns")

            # Show sample data for key columns
            if sample_data:
                print(f"\n📊 Sample data:")
                relevant_cols = [col for col in columns if any(
                    kw in col.lower() for kw in ['group', 'reply', 'created', 'channel', 'via']
                )]
                for col in relevant_cols[:5]:
                    if col in sample_data[0]:
                        print(f"   {col}: {sample_data[0][col]}")

            # Cache the schema
            await safe_valkey_set(cache_key, schema_info, ex=SCHEMA_CACHE_TTL)
            print(f"💾 Schema cached for future use")

            return schema_info

        except Exception as e:
            print(f"❌ Error discovering schema: {str(e)}")
            return {
                'table': table_name,
                'error': str(e),
                'columns': []
            }
    else:
        print("⚠️ Snowflake not available for schema discovery")
        # Return mock schema for development
        return {
            'table': table_name,
            'columns': ['created_at', 'group_name', 'ticket_id'],
            'sample_data': [],
            'note': 'Mock schema - Snowflake not available'
        }


async def rediscover_table_schema(table_name: str) -> dict:
    """Force rediscovery of a table schema (bypassing cache)"""
    print(f"🔄 Force rediscovering schema for: {table_name}")

    # Clear existing cache
    cache_key = f"{SCHEMA_CACHE_PREFIX}:{table_name}"
    await safe_valkey_delete(cache_key)
    print(f"🗑️ Cleared cached schema for {table_name}")

    # Rediscover
    return await discover_table_schema(table_name)


def find_matching_columns(schema: dict, keywords: List[str]) -> List[str]:
    """Find columns that match given keywords"""
    columns = schema.get('columns', [])
    matching = []

    if not columns:
        return matching

    # First check for exact variations
    for keyword in keywords:
        # Check if we have known variations for this keyword
        variations = COLUMN_VARIATIONS.get(keyword, [keyword])

        for variation in variations:
            for col in columns:
                if col.lower() == variation.lower():
                    if col not in matching:
                        matching.append(col)
                        print(f"✅ Found exact match: {col} for keyword: {keyword}")

    # Then do fuzzy matching
    for keyword in keywords:
        keyword_lower = keyword.lower()
        for col in columns:
            col_lower = col.lower()
            if keyword_lower in col_lower or col_lower in keyword_lower:
                if col not in matching:
                    matching.append(col)
                    print(f"✅ Found fuzzy match: {col} for keyword: {keyword}")

    return matching


async def get_or_create_thread(user_id: str, channel_id: str) -> str:
    """Get existing thread for user+channel or create new one"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = f"{THREAD_CACHE_PREFIX}:{cache_key}"

    existing_thread = await safe_valkey_get(redis_key)
    if existing_thread:
        print(f"♻️ Using existing thread: {existing_thread}")
        return existing_thread

    try:
        vector_store_id = VECTOR_STORE_ID
        if not vector_store_id:
            print("❌ No VECTOR_STORE_ID found")
            return None

        thread_params = {
            "tool_resources": {
                "file_search": {
                    "vector_store_ids": [vector_store_id]
                }
            }
        }

        use_beta = hasattr(client, 'beta') and hasattr(client.beta, 'threads')
        if use_beta:
            thread = client.beta.threads.create(**thread_params)
        else:
            thread = client.threads.create(**thread_params)

        await safe_valkey_set(redis_key, thread.id, ex=THREAD_CACHE_TTL)
        print(f"🆕 Created new thread: {thread.id}")
        return thread.id

    except Exception as e:
        print(f"❌ Error creating thread: {e}")
        return None


async def wait_for_active_runs(thread_id: str, max_wait_seconds: int = 30) -> bool:
    """Wait for any active runs on a thread to complete"""
    use_beta = hasattr(client, 'beta') and hasattr(client.beta, 'threads')

    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        try:
            if use_beta:
                runs = client.beta.threads.runs.list(thread_id=thread_id, limit=5)
            else:
                runs = client.threads.runs.list(thread_id=thread_id, limit=5)

            active_runs = [run for run in runs.data if run.status in ["queued", "in_progress", "requires_action"]]
            if not active_runs:
                return True

            print(f"⏳ Waiting for {len(active_runs)} active run(s)...")
            await asyncio.sleep(2)

        except Exception as e:
            print(f"❌ Error checking active runs: {e}")
            return False

    return False


async def send_message_and_run(thread_id: str, message: str, instructions: str = None) -> str:
    """Send message to assistant and get response"""
    try:
        # Wait for any active runs
        await wait_for_active_runs(thread_id, max_wait_seconds=15)

        use_beta = hasattr(client, 'beta') and hasattr(client.beta, 'threads')

        # Add message
        if use_beta:
            client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=message
            )
        else:
            client.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=message
            )

        # Create run
        run_params = {
            "thread_id": thread_id,
            "assistant_id": ASSISTANT_ID,
        }
        if instructions:
            run_params["instructions"] = instructions

        if use_beta:
            run = client.beta.threads.runs.create(**run_params)
        else:
            run = client.threads.runs.create(**run_params)

        # Poll for completion
        max_attempts = 45
        attempt = 0

        while attempt < max_attempts:
            if use_beta:
                run_status = client.beta.threads.runs.retrieve(
                    thread_id=thread_id,
                    run_id=run.id
                )
            else:
                run_status = client.threads.runs.retrieve(
                    thread_id=thread_id,
                    run_id=run.id
                )

            if run_status.status == "completed":
                break
            elif run_status.status in ["failed", "cancelled", "expired"]:
                return f"⚠️ Run failed: {run_status.status}"

            await asyncio.sleep(1)
            attempt += 1

        if attempt >= max_attempts:
            return "⚠️ Response timeout"

        # Get response
        if use_beta:
            messages = client.beta.threads.messages.list(thread_id=thread_id, limit=1)
        else:
            messages = client.threads.messages.list(thread_id=thread_id, limit=1)

        if messages.data:
            return messages.data[0].content[0].text.value.strip()
        else:
            return "⚠️ No response"

    except Exception as e:
        print(f"❌ Error in send_message_and_run: {e}")
        return f"⚠️ Error: {str(e)}"


async def handle_conversational_question(user_question: str, user_id: str, channel_id: str) -> str:
    """Handle conversational questions"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    instructions = """You are a BI assistant. Be helpful and concise.
Focus on available metrics: tickets, agents, performance, reviews."""

    message = f"User question: {user_question}"

    return await send_message_and_run(thread_id, message, instructions)


async def generate_sql_intelligently(user_question: str, user_id: str, channel_id: str) -> str:
    """Generate SQL by discovering table structure dynamically"""
    print(f"\n🤖 Starting SQL generation for: {user_question}")

    try:
        # Step 1: Identify likely table
        table_name = identify_table_from_question(user_question)
        print(f"📊 Identified table: {table_name}")

        # Step 2: Discover schema
        schema = await discover_table_schema(table_name)

        if schema.get('error'):
            print(f"⚠️ Schema discovery failed, will rely on assistant's file_search")
            # Don't return error, continue with limited info
            schema = {
                'table': table_name,
                'columns': [],
                'error': schema.get('error')
            }
    except Exception as e:
        print(f"⚠️ Error during schema discovery: {e}")
        print(f"⚠️ Continuing with limited schema information")
        schema = {
            'table': table_name if 'table_name' in locals() else 'unknown',
            'columns': [],
            'error': str(e)
        }

    # Step 3: Find relevant columns based on question
    intent = extract_intent(user_question)
    print(f"🎯 Extracted intent: {intent}")

    # Look for columns related to the metric
    relevant_keywords = []
    if intent['metric_type']:
        if 'reply' in intent['metric_type'] or 'response' in intent['metric_type']:
            relevant_keywords.extend(['reply_time', 'reply', 'response', 'first'])
        elif 'resolution' in intent['metric_type']:
            relevant_keywords.extend(['resolution', 'resolve', 'resolved'])

    if intent['group_filter']:
        relevant_keywords.extend(['group', 'channel', 'type'])

    relevant_keywords.extend(['created', 'date', 'time'])

    print(f"🔎 Searching for columns with keywords: {relevant_keywords}")
    matching_columns = find_matching_columns(schema, relevant_keywords) if schema.get('columns') else []
    print(f"🔍 Found relevant columns: {matching_columns}")

    # Special handling for common queries
    if 'messaging' in user_question.lower() and 'reply time' in user_question.lower():
        print("📌 Special case: Messaging reply time query")
        # Look specifically for reply time columns
        reply_columns = find_matching_columns(schema, ['reply_time']) if schema.get('columns') else []
        if reply_columns:
            print(f"📌 Found reply time columns: {reply_columns}")

    # Step 4: Generate SQL with assistant
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "-- Error: Could not create conversation thread"

    # Build context with actual schema
    if schema.get('columns'):
        all_columns = schema['columns'][:30]  # Show first 30 columns
        instructions = f"""You are a SQL expert. Generate SQL based on this ACTUAL table schema:

Table: {table_name}
ALL Available columns (use ONLY these): 
{', '.join(all_columns)}

CRITICAL: The column names above are the ACTUAL column names from the table. 
- Do NOT make up column names
- Do NOT use generic names like 'reply_time_in_minutes' unless it's in the list above
- Use the EXACT column names as shown

Common patterns:
- For messaging filter: WHERE LOWER(group_name) = 'messaging' (only if 'group_name' exists above)
- Last week: WHERE created_at >= DATEADD('week', -1, CURRENT_DATE) AND created_at < CURRENT_DATE
- This week: WHERE created_at >= DATE_TRUNC('week', CURRENT_DATE)

Return ONLY the SQL query, no explanations."""
    else:
        # No schema discovered, rely on file_search
        instructions = f"""You are a SQL expert. Generate SQL for table: {table_name}

IMPORTANT: I could not discover the table schema. You must:
1. Use file_search to find the correct table and column names
2. Look for tables related to: {table_name.split('.')[-1]}
3. Find the exact column names before writing SQL

Common patterns:
- For messaging filter: Use appropriate group/channel column with LOWER() function
- Last week: WHERE [date_column] >= DATEADD('week', -1, CURRENT_DATE) AND [date_column] < CURRENT_DATE
- Reply time: Look for columns with 'reply', 'response', or 'first' in the name

Return ONLY the SQL query after finding the correct columns."""

    # Build message with discovered columns hint
    message_parts = [f"Generate SQL for: {user_question}"]
    message_parts.append(f"\nTable to query: {table_name}")

    if schema.get('columns'):
        all_columns = schema['columns']

        if matching_columns:
            message_parts.append(f"\nRelevant columns found in table:")
            for col in matching_columns[:10]:  # Show up to 10 matching columns
                message_parts.append(f"  - {col}")

        # Provide specific column hints based on query
        if 'reply time' in user_question.lower():
            reply_cols = [col for col in all_columns if any(kw in col.lower() for kw in ['reply', 'response', 'first'])]
            if reply_cols:
                message_parts.append(f"\nFor reply time, use one of these columns: {', '.join(reply_cols[:5])}")

        if intent['group_filter']:
            group_cols = [col for col in all_columns if any(kw in col.lower() for kw in ['group', 'channel', 'via'])]
            if group_cols:
                message_parts.append(f"\nFor {intent['group_filter']} filter, use: {group_cols[0]}")

        if intent['time_filter']:
            time_cols = [col for col in all_columns if any(kw in col.lower() for kw in ['created', 'date', 'time'])]
            if time_cols:
                message_parts.append(f"\nFor date filter, use: {time_cols[0]}")
                message_parts.append(f"Time period requested: {intent['time_filter']}")

        # Add sample data if available
        if schema.get('sample_data') and len(schema['sample_data']) > 0:
            message_parts.append(f"\nSample data from table:")
            # Show relevant fields from sample
            sample = schema['sample_data'][0]
            relevant_fields = {}
            for key, value in sample.items():
                if any(kw in key.lower() for kw in ['group', 'reply', 'created', 'channel']):
                    relevant_fields[key] = value
            if relevant_fields:
                message_parts.append(json.dumps(relevant_fields, indent=2))
    else:
        # No schema discovered
        message_parts.append("\nNOTE: Could not discover table schema. Use file_search to find correct column names.")
        message_parts.append(f"\nLooking for columns related to:")
        if 'reply time' in user_question.lower():
            message_parts.append("  - Reply time or first response time")
        if intent['group_filter']:
            message_parts.append(f"  - Group/channel filter for '{intent['group_filter']}'")
        if intent['time_filter']:
            message_parts.append(f"  - Date/time filter for '{intent['time_filter']}'")

    message = "\n".join(message_parts)

    print(f"📝 Sending to assistant with {len(schema['columns'])} discovered columns")

    response = await send_message_and_run(thread_id, message, instructions)

    # Extract SQL from response
    sql = extract_sql_from_response(response)

    print(f"\n🧠 Generated SQL:")
    print(f"{sql}")
    print(f"{'=' * 60}\n")

    return sql


def extract_sql_from_response(response: str) -> str:
    """Extract SQL from assistant response"""
    if "```sql" in response:
        try:
            return response.split("```sql")[1].split("```")[0].strip()
        except IndexError:
            pass

    lines = response.split('\n')
    sql_lines = []
    in_sql = False

    for line in lines:
        if line.strip().upper().startswith('SELECT'):
            in_sql = True
        if in_sql:
            sql_lines.append(line)
            if line.strip().endswith(';'):
                break

    if sql_lines:
        return '\n'.join(sql_lines).strip()

    if "don't have enough" in response or "cannot find" in response.lower():
        return response

    return "-- Error: Could not extract SQL from response"


async def handle_question(user_question: str, user_id: str, channel_id: str, assistant_id: str = None) -> Tuple[
    str, str]:
    """Main question handler - routes to appropriate handler"""
    global ASSISTANT_ID
    if assistant_id:
        ASSISTANT_ID = assistant_id

    try:
        question_type = classify_question_type(user_question)
        print(f"\n{'=' * 60}")
        print(f"🔍 Processing question: {user_question}")
        print(f"📊 Question type: {question_type}")

        if question_type == 'sql_required':
            # Check cache first
            question_hash = get_question_hash(user_question)
            cache_key = f"{SQL_CACHE_PREFIX}:{question_hash}"

            try:
                cached_sql = await safe_valkey_get(cache_key)

                if cached_sql and cached_sql.get('success_count', 0) > 0:
                    print(f"💰 Using cached SQL (success_count: {cached_sql['success_count']})")
                    sql = cached_sql['sql']
                    print(f"📝 Cached SQL:\n{sql}")
                    return sql, 'sql'
                else:
                    print(f"🔄 Generating new SQL (no successful cache found)")
            except Exception as cache_error:
                print(f"⚠️ Error checking cache: {cache_error}")
                print(f"🔄 Proceeding to generate new SQL")

            # Generate new SQL
            sql_query = await generate_sql_intelligently(user_question, user_id, channel_id)
            return sql_query, 'sql'

        else:
            # Handle conversational
            response = await handle_conversational_question(user_question, user_id, channel_id)
            return response, 'conversational'

    except Exception as e:
        print(f"❌ Error in handle_question: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()

        # Return error as SQL comment to be handled by the caller
        return f"-- Error: {str(e)}", 'error'


async def update_sql_cache_with_results(user_question: str, sql_query: str, result_count: int):
    """Update cache after execution"""
    try:
        if not sql_query or sql_query.startswith("--") or sql_query.startswith("⚠️"):
            print(f"🚫 Not caching failed query")
            return

        question_hash = get_question_hash(user_question)
        cache_key = f"{SQL_CACHE_PREFIX}:{question_hash}"

        # Get existing cache entry to update success count
        existing = await safe_valkey_get(cache_key, {})
        success_count = existing.get('success_count', 0)

        if result_count > 0:
            success_count += 1

        cache_entry = {
            'sql': sql_query,
            'success_count': success_count,
            'last_result_count': result_count,
            'last_used': time.time()
        }

        success = await safe_valkey_set(cache_key, cache_entry, ex=SQL_CACHE_TTL)
        if success:
            print(f"💾 Updated SQL cache (success_count: {success_count}, results: {result_count})")
        else:
            print(f"⚠️ Failed to update SQL cache (non-critical)")

    except Exception as e:
        print(f"⚠️ Error updating SQL cache: {e}")
        # Don't raise - this is non-critical


async def summarize_with_assistant(user_question: str, result_table: str, user_id: str, channel_id: str,
                                   assistant_id: str = None) -> str:
    """Summarize results - keep it simple"""
    if assistant_id:
        global ASSISTANT_ID
        ASSISTANT_ID = assistant_id

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    instructions = "Provide a clear business summary. Focus on insights, not technical details."

    message = f"""Question: "{user_question}"
Data:
{result_table}

Summarize the key findings."""

    return await send_message_and_run(thread_id, message, instructions)


async def debug_assistant_search(user_question: str, user_id: str, channel_id: str) -> str:
    """Debug vector store search"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    message = f"Debug: Search for tables and columns related to: {user_question}"
    response = await send_message_and_run(thread_id, message)
    return response


async def get_cache_stats():
    """Get cache statistics"""
    await ensure_valkey_connection()

    stats = {
        "cache_backend": "Valkey" if valkey_client else "Local Memory",
        "status": "connected" if valkey_client else "local",
        "caches": {
            "sql": len(_local_cache.get('sql', {})),
            "schema": len(_local_cache.get('schema', {})),
            "thread": len(_local_cache.get('thread', {})),
            "conversation": len(_local_cache.get('conversation', {}))
        }
    }

    # Add schema cache details
    schema_cache = _local_cache.get('schema', {})
    if schema_cache:
        stats['cached_schemas'] = list(schema_cache.keys())

    return stats


async def get_learning_insights():
    """Get learning insights"""
    stats = await get_cache_stats()

    insights = [f"Cache Backend: {stats['cache_backend']}"]
    insights.append(f"Status: {stats['status']}")
    insights.append(f"\nCached Items:")
    insights.append(f"  - SQL Queries: {stats['caches']['sql']}")
    insights.append(f"  - Table Schemas: {stats['caches']['schema']}")
    insights.append(f"  - Threads: {stats['caches']['thread']}")
    insights.append(f"  - Conversations: {stats['caches']['conversation']}")

    # Show cached schemas with details
    schema_cache = _local_cache.get('schema', {})
    if schema_cache:
        insights.append(f"\nCached Table Schemas:")
        for table_key, schema_info in schema_cache.items():
            table_name = schema_info.get('table', table_key)
            col_count = len(schema_info.get('columns', []))
            discovered_at = schema_info.get('discovered_at', 0)
            if discovered_at:
                age_hours = (time.time() - discovered_at) / 3600
                insights.append(f"  - {table_name}: {col_count} columns (cached {age_hours:.1f} hours ago)")
            else:
                insights.append(f"  - {table_name}: {col_count} columns")

    return "\n".join(insights)


def test_question_classification():
    """Test question classification"""
    test_cases = [
        ("How many messaging tickets had a reply time over 15 minutes?", "sql_required"),
        ("What can you help me with?", "conversational"),
        ("Show me ticket volume by group", "sql_required"),
        ("Why is that?", "followup"),
    ]

    print("🧪 Testing Classification:")
    for question, expected in test_cases:
        actual = classify_question_type(question)
        status = "✅" if actual == expected else "❌"
        print(f"{status} '{question}' → {actual}")


# Legacy fallback functions
def ask_llm_for_sql(user_question: str, model_context: str) -> str:
    """Fallback SQL generation"""
    print(f"⚠️ Using legacy SQL generation")
    prompt = f"""Generate SQL for: {user_question}

Context: {model_context}

Return only SQL:
```sql
SELECT ...
```"""

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        content = response.choices[0].message.content.strip()

        if "```sql" in content:
            sql = content.split("```sql")[1].split("```")[0].strip()
            print(f"📝 Legacy generated SQL:\n{sql}")
            return sql
        return content
    except Exception as e:
        return f"⚠️ Error: {e}"


def summarize_results_with_llm(user_question: str, result_table: str) -> str:
    """Fallback summarization"""
    prompt = f"""Question: "{user_question}"
Data: {result_table}

Provide a business summary:"""

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"⚠️ Error: {e}"


async def clear_thread_cache():
    """Clear thread cache"""
    _local_cache['thread'].clear()
    print("🧹 Thread cache cleared")


async def clear_sql_cache():
    """Clear SQL cache"""
    _local_cache['sql'].clear()
    print("🧹 SQL cache cleared")


async def clear_schema_cache():
    """Clear schema cache"""
    _local_cache['schema'].clear()
    print("🧹 Schema cache cleared")


async def check_valkey_health():
    """Check Valkey health"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            await valkey_client.ping()
            return {"status": "healthy", "backend": "Valkey"}
        except:
            return {"status": "unhealthy", "backend": "Valkey"}
    else:
        return {"status": "fallback", "backend": "Local Memory"}


# Add this function to manually prime the schema cache
async def prime_schema_cache():
    """Prime the schema cache with known tables"""
    print("\n" + "=" * 60)
    print("🚀 PRIMING SCHEMA CACHE")
    print("=" * 60)

    results = {
        'success': 0,
        'failed': 0,
        'errors': []
    }

    for table_alias, table_name in KNOWN_TABLES.items():
        try:
            print(f"\n📊 Discovering schema for {table_alias}: {table_name}")
            schema = await discover_table_schema(table_name)

            if not schema.get('error'):
                results['success'] += 1
                print(f"✅ Successfully cached schema for {table_alias}")
                print(f"   Columns: {len(schema.get('columns', []))}")

                # Show columns that are important for common queries
                important_cols = []
                for col in schema.get('columns', []):
                    col_lower = col.lower()
                    if any(kw in col_lower for kw in ['reply', 'group', 'created', 'channel', 'via']):
                        important_cols.append(col)

                if important_cols:
                    print(f"   Key columns: {', '.join(important_cols[:10])}")
            else:
                results['failed'] += 1
                error_msg = f"Failed to cache schema for {table_alias}: {schema.get('error')}"
                print(f"❌ {error_msg}")
                results['errors'].append(error_msg)

        except Exception as e:
            results['failed'] += 1
            error_msg = f"Exception caching {table_alias}: {str(e)}"
            print(f"❌ {error_msg}")
            results['errors'].append(error_msg)
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"✅ Schema cache priming complete: {results['success']} succeeded, {results['failed']} failed")
    if results['errors']:
        print(f"❌ Errors encountered:")
        for error in results['errors']:
            print(f"   - {error}")
    print("=" * 60 + "\n")

    return results

async def rediscover_table_schema(table_name: str) -> dict:
    """Force rediscovery of a table schema (bypassing cache)"""
    print(f"🔄 Force rediscovering schema for: {table_name}")

    # Clear existing cache
    cache_key = f"{SCHEMA_CACHE_PREFIX}:{table_name}"
    await safe_valkey_delete(cache_key)
    print(f"🗑️ Cleared cached schema for {table_name}")

    # Rediscover
    return await discover_table_schema(table_name)