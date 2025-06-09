import os
import asyncio
import time
import json
from typing import Optional, Dict, Any, Tuple
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

# Minimal pattern recognition for intent
INTENT_PATTERNS = {
    'count': ['how many', 'count', 'total number', 'number of'],
    'time_filter': ['today', 'yesterday', 'last week', 'this week', 'last month'],
    'group_filter': ['messaging', 'voice', 'email'],
    'metrics': ['reply time', 'resolution time', 'aht', 'average handling'],
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
        'tickets', 'agents', 'reviews', 'performance'
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


async def get_table_schema_sample(table_name: str) -> dict:
    """Get cached table schema or fetch sample data"""
    cache_key = f"{SCHEMA_CACHE_PREFIX}:{table_name}"
    cached_schema = await safe_valkey_get(cache_key)

    if cached_schema:
        print(f"📋 Using cached schema for {table_name}")
        return cached_schema

    # In production, this would run a query to get sample data
    # For now, return basic structure
    schema_info = {
        'table': table_name,
        'sample_query': f"SELECT * FROM {table_name} LIMIT 5",
        'common_filters': {
            'messaging': "LOWER(group_name) = 'messaging'",
            'last_week': "created_at >= DATEADD('week', -1, CURRENT_DATE) AND created_at < CURRENT_DATE",
            'this_week': "created_at >= DATE_TRUNC('week', CURRENT_DATE)"
        }
    }

    await safe_valkey_set(cache_key, schema_info, ex=SCHEMA_CACHE_TTL)
    return schema_info


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
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    # Extract basic intent
    intent = extract_intent(user_question)

    # Smart, minimal instructions
    instructions = """You are a SQL expert. Follow this process:

1. Identify the table from file_search based on the question
2. Look up the table schema to find exact column names
3. Generate clean SQL with proper filters

Common patterns:
- Messaging filter: LOWER(group_name) = 'messaging'
- Last week: created_at >= DATEADD('week', -1, CURRENT_DATE) AND created_at < CURRENT_DATE
- This week: created_at >= DATE_TRUNC('week', CURRENT_DATE)

Return ONLY the SQL query."""

    # Build focused message
    message_parts = [f"Generate SQL for: {user_question}"]

    if intent['group_filter']:
        message_parts.append(f"Filter for: {intent['group_filter']} group")
    if intent['time_filter']:
        message_parts.append(f"Time period: {intent['time_filter']}")
    if intent['metric_type']:
        message_parts.append(f"Metric: {intent['metric_type']}")

    message = "\n".join(message_parts)

    response = await send_message_and_run(thread_id, message, instructions)

    # Extract SQL from response
    return extract_sql_from_response(response)


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

    question_type = classify_question_type(user_question)
    print(f"🔍 Question type: {question_type}")

    if question_type == 'sql_required':
        # Check cache first
        question_hash = get_question_hash(user_question)
        cache_key = f"{SQL_CACHE_PREFIX}:{question_hash}"
        cached_sql = await safe_valkey_get(cache_key)

        if cached_sql and cached_sql.get('success_count', 0) > 0:
            print(f"💰 Using cached SQL")
            return cached_sql['sql'], 'sql'

        # Generate new SQL
        sql_query = await generate_sql_intelligently(user_question, user_id, channel_id)
        return sql_query, 'sql'

    else:
        # Handle conversational
        response = await handle_conversational_question(user_question, user_id, channel_id)
        return response, 'conversational'


async def update_sql_cache_with_results(user_question: str, sql_query: str, result_count: int):
    """Update cache after execution"""
    if not sql_query or sql_query.startswith("--") or result_count == 0:
        return

    question_hash = get_question_hash(user_question)
    cache_key = f"{SQL_CACHE_PREFIX}:{question_hash}"

    cache_entry = {
        'sql': sql_query,
        'success_count': 1 if result_count > 0 else 0,
        'last_used': time.time()
    }

    await safe_valkey_set(cache_key, cache_entry, ex=SQL_CACHE_TTL)
    print(f"💾 Cached SQL (results: {result_count})")


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

    if valkey_client:
        return {
            "cache_backend": "Valkey",
            "status": "connected"
        }
    else:
        return {
            "cache_backend": "Local Memory",
            "sql_cache_size": len(_local_cache.get('sql', {})),
            "schema_cache_size": len(_local_cache.get('schema', {})),
            "thread_cache_size": len(_local_cache.get('thread', {}))
        }


async def get_learning_insights():
    """Get learning insights"""
    stats = await get_cache_stats()
    return f"Cache stats: {json.dumps(stats, indent=2)}"


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
            return content.split("```sql")[1].split("```")[0].strip()
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