import os
import asyncio
import time
import json
from typing import Optional, Dict, Any, Tuple
from dotenv import load_dotenv
from openai import OpenAI
from glide import GlideClient, GlideClientConfiguration, NodeAddress, GlideClusterClient, GlideClusterClientConfiguration

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("ASSISTANT_ID")
VECTOR_STORE_ID = os.getenv("OPENAI_VECTOR_STORE_ID")

# Valkey configuration
VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_USE_TLS = os.getenv("VALKEY_USE_TLS", "true").lower() == "true"
# Add environment check
IS_LOCAL_DEV = os.getenv("IS_LOCAL_DEV", "false").lower() == "true"

# Cache TTL settings (in seconds)
THREAD_CACHE_TTL = 3600  # 1 hour
SQL_CACHE_TTL = 86400  # 24 hours
LEARNING_CACHE_TTL = 604800  # 7 days
CONVERSATION_CACHE_TTL = 600  # 10 minutes

# Debug: Print configuration
print(f"🔧 Assistant ID: {ASSISTANT_ID}")
print(f"🔧 Vector Store ID: {VECTOR_STORE_ID}")
print(f"🔧 Valkey Host: {VALKEY_HOST}")
print(f"🔧 Valkey Port: {VALKEY_PORT}")
print(f"🔧 Valkey TLS: {VALKEY_USE_TLS}")
print(f"🏠 Local Development Mode: {IS_LOCAL_DEV}")

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)

# Global Valkey client (will be initialized in async context)
valkey_client = None

# Fallback to local memory if Valkey is not available
_local_cache = {
    'thread': {},
    'sql': {},
    'learning': {},
    'conversation': {}
}

# Cache key prefixes
CACHE_PREFIX = "bi_slack_bot"
THREAD_CACHE_PREFIX = f"{CACHE_PREFIX}:thread"
SQL_CACHE_PREFIX = f"{CACHE_PREFIX}:sql"
LEARNING_CACHE_PREFIX = f"{CACHE_PREFIX}:learning"
CONVERSATION_CACHE_PREFIX = f"{CACHE_PREFIX}:conversation"


async def init_valkey_client():
    """Initialize Valkey client - must be called in async context"""
    global valkey_client

    # Skip Valkey connection for local development
    if IS_LOCAL_DEV:
        print("🏠 Local development mode - skipping Valkey connection")
        print("💾 Using in-memory cache for local testing")
        valkey_client = None
        return

    try:
        print(f"🔄 Attempting to connect to Valkey at {VALKEY_HOST}:{VALKEY_PORT}")
        print(f"🔐 TLS Enabled: {VALKEY_USE_TLS}")

        # Configure Valkey connection for CLUSTER mode (not single node)
        addresses = [
            NodeAddress(VALKEY_HOST, VALKEY_PORT)
        ]

        # Create configuration for cluster mode
        config = GlideClusterClientConfiguration(
            addresses=addresses,
            use_tls=VALKEY_USE_TLS,
            request_timeout=10000,  # 10 seconds timeout
        )

        print("📡 Creating Valkey cluster client...")

        # Create CLUSTER client (not single node)
        from glide import GlideClusterClient
        valkey_client = await GlideClusterClient.create(config)

        print("🏓 Testing connection with PING...")

        # Test connection
        pong = await valkey_client.ping()
        print(f"✅ Valkey connection established: {pong}")

    except Exception as e:
        print(f"❌ Valkey connection failed: {e}")
        print(f"📍 Connection details: {VALKEY_HOST}:{VALKEY_PORT}, TLS={VALKEY_USE_TLS}")
        print("⚠️ Falling back to local memory cache")

        # Check if this is likely a VPC connectivity issue
        if "timed out" in str(e).lower():
            print("\n🚨 Connection Timeout - Common Causes:")
            print("1. **Local Development**: ElastiCache is only accessible from within AWS VPC")
            print("   - Set IS_LOCAL_DEV=true in your .env for local testing")
            print("   - Or use SSH tunnel: ssh -L 6379:valkey-endpoint:6379 ec2-instance")
            print("2. **AWS Lambda**: Ensure Lambda is in the same VPC as ElastiCache")
            print("3. **Security Groups**: Allow inbound traffic on port 6379")
            print("4. **Network ACLs**: Check VPC network ACLs allow traffic")

        valkey_client = None


async def ensure_valkey_connection():
    """Ensure Valkey client is initialized"""
    global valkey_client
    if valkey_client is None:
        await init_valkey_client()


def get_redis_key(prefix: str, key: str) -> str:
    """Generate Redis key with prefix"""
    return f"{prefix}:{key}"


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
        # Fallback to local cache
        cache_type = key.split(':')[1] if ':' in key else 'thread'
        return _local_cache.get(cache_type, {}).get(key, default)


async def safe_valkey_set(key: str, value: Any, ex: int = None):
    """Safely set value in Valkey with fallback"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            json_value = json.dumps(value)
            if ex:
                # Set with expiration
                await valkey_client.set(key, json_value)
                await valkey_client.expire(key, ex)
            else:
                await valkey_client.set(key, json_value)
            return True
        except Exception as e:
            print(f"⚠️ Valkey SET error for {key}: {e}")
            return False
    else:
        # Fallback to local cache
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
        # Fallback to local cache
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
        # Fallback to local cache
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
    # Normalize question (lowercase, strip, remove extra spaces, remove table hints)
    normalized = ' '.join(question.lower().strip().split())
    # Remove table-specific hints for better pattern matching
    normalized = normalized.replace('use stg_worker_ops', '').replace('use fct_zendesk', '').replace(
        'table to get this data', '')
    normalized = ' '.join(normalized.split())  # Clean up extra spaces
    return hashlib.md5(normalized.encode()).hexdigest()[:12]


def classify_question_type(question: str) -> str:
    """Classify what type of response the question needs"""
    question_lower = question.lower()

    # FIRST: Check for meta/capability questions (highest priority)
    meta_indicators = [
        'what can you', 'what do you', 'what are you', 'how do you',
        'what questions can', 'what kind of questions', 'what type of questions',
        'what data do you', 'what information do you', 'what help can you',
        'what are the top', 'what are some', 'give me examples',
        'list questions', 'example questions', 'sample questions',
        'what tables', 'what sources', 'what capabilities'
    ]

    # If asking about the bot's capabilities, always conversational
    if any(indicator in question_lower for indicator in meta_indicators):
        return 'conversational'

    # SECOND: Check for SQL-requiring questions (moved higher priority)
    sql_indicators = [
        'how many', 'count of', 'total number', 'show me all', 'list all',
        'find tickets', 'find reviews', 'get data', 'query',
        'highest', 'lowest', 'most', 'least', 'average', 'sum',
        'today', 'yesterday', 'last week', 'this month', 'recent',
        'group by', 'where', 'filter'
    ]

    # Check for SQL patterns first (before followup)
    if any(indicator in question_lower for indicator in sql_indicators):
        # Double-check it's not asking about capabilities
        if not any(meta in question_lower for meta in ['what questions', 'what can', 'examples', 'sample']):
            return 'sql_required'

    # THIRD: Check for SQL follow-ups (follow-ups that still need data)
    sql_followup_indicators = [
        'agent names', 'agent name', 'names instead', 'with names',
        'same info', 'same data', 'same but', 'instead of',
        'last week', 'this week', 'last month', 'this month',
        'only voice', 'just voice', 'voice group only',
        'break down by', 'group by', 'split by',
        'can i get', 'can you show', 'show me the same',
        'what about', 'how about'
    ]

    if any(indicator in question_lower for indicator in sql_followup_indicators):
        return 'sql_required'

    # FOURTH: Check for specific data requests (very specific)
    data_request_patterns = [
        'tickets in', 'reviews for', 'agents with', 'performance of',
        'data for', 'results for', 'stats for', 'metrics for',
        'resolve', 'resolved', 'per week', 'per day', 'per month',
        'each agent', 'by agent', 'agent performance'
    ]

    if any(pattern in question_lower for pattern in data_request_patterns):
        return 'sql_required'

    # FIFTH: Check for explicit help requests
    help_indicators = [
        'help', 'how to', 'explain', 'what is', 'tell me about',
        'describe', 'definition of', 'who are you'
    ]

    if any(indicator in question_lower for indicator in help_indicators):
        return 'conversational'

    # SIXTH: Check for conversational follow-up questions (moved lower priority)
    conversational_followup_indicators = [
        'why', 'what does that mean', 'explain that', 'more details',
        'break that down', 'can you elaborate', 'tell me more'
    ]

    # Only classify as conversational follow-up if it's clearly explanatory
    if any(indicator in question_lower for indicator in conversational_followup_indicators):
        return 'followup'

    # DEFAULT: If unclear, go conversational (safer)
    return 'conversational'


def get_question_pattern(question: str) -> str:
    """Extract question pattern for learning"""
    pattern_elements = []

    if 'ticket' in question.lower():
        pattern_elements.append('tickets')
    if 'voice' in question.lower():
        pattern_elements.append('voice_group')
    if 'agent' in question.lower():
        pattern_elements.append('agent')
    if 'highest' in question.lower() or 'most' in question.lower():
        pattern_elements.append('top_analysis')
    if 'today' in question.lower():
        pattern_elements.append('today')
    if 'last' in question.lower() and 'day' in question.lower():
        pattern_elements.append('recent_days')
    if any(word in question.lower() for word in ['when', 'what time', 'which hour']):
        pattern_elements.append('time_analysis')

    return '_'.join(pattern_elements)


def analyze_question_intent(question: str) -> dict:
    """Analyze question to determine SQL pattern needed"""
    question_lower = question.lower()

    intent = {
        'type': 'standard',
        'time_analysis': False,
        'peak_analysis': False,
        'time_granularity': None,
        'filters': [],
        'keywords': [],
        'question_type': classify_question_type(question)
    }

    # Time-based questions
    if any(word in question_lower for word in ['when', 'what time', 'which hour']):
        intent['type'] = 'time_analysis'
        intent['time_analysis'] = True
        intent['time_granularity'] = 'hour'
        intent['keywords'].append('time_analysis')

    # Peak analysis
    if any(word in question_lower for word in ['highest', 'most', 'maximum', 'peak']):
        intent['peak_analysis'] = True
        intent['keywords'].append('peak_analysis')

    # Filters
    if 'today' in question_lower:
        intent['filters'].append('today')
        intent['keywords'].append('today')
    if 'voice' in question_lower:
        intent['filters'].append('voice_group')
        intent['keywords'].append('voice')
    if 'ticket' in question_lower:
        intent['filters'].append('tickets')
        intent['keywords'].append('tickets')

    return intent


def extract_table_hint(question: str) -> str:
    """Extract table hint from user question"""
    import re
    # Look for table hints in the question
    table_patterns = [
        r'use\s+([a-zA-Z_]+)',
        r'from\s+([a-zA-Z_]+)',
        r'table\s+([a-zA-Z_]+)',
        r'([a-zA-Z_]+)\s+table'
    ]

    for pattern in table_patterns:
        match = re.search(pattern, question.lower())
        if match:
            return match.group(1)
    return None


def should_cache_sql(sql: str, result_count: int) -> bool:
    """Determine if SQL should be cached based on quality"""
    if not sql or sql.startswith("I don't have enough"):
        return False
    if result_count == 0:  # Empty results might indicate wrong table
        return False
    return True


async def update_learning_cache(question: str, sql: str, result_count: int):
    """Update learning cache when user provides corrections"""
    table_hint = extract_table_hint(question)
    if table_hint and result_count > 0:  # User provided table hint and got results
        pattern = get_question_pattern(question)
        if pattern:
            redis_key = get_redis_key(LEARNING_CACHE_PREFIX, pattern)
            await safe_valkey_set(redis_key, table_hint, ex=LEARNING_CACHE_TTL)
            print(f"🧠 Learning: Pattern '{pattern}' → Table '{table_hint}'")


async def get_learned_table_preference(question: str) -> str:
    """Get learned table preference for similar questions"""
    pattern = get_question_pattern(question)
    redis_key = get_redis_key(LEARNING_CACHE_PREFIX, pattern)
    return await safe_valkey_get(redis_key)


async def update_conversation_context(user_id: str, channel_id: str, question: str, response: str):
    """Update conversation context for follow-up questions"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = get_redis_key(CONVERSATION_CACHE_PREFIX, cache_key)

    context = {
        'last_question': question,
        'last_response': response,
        'timestamp': time.time()
    }

    await safe_valkey_set(redis_key, context, ex=CONVERSATION_CACHE_TTL)


async def get_conversation_context(user_id: str, channel_id: str) -> dict:
    """Get recent conversation context"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = get_redis_key(CONVERSATION_CACHE_PREFIX, cache_key)

    context = await safe_valkey_get(redis_key, {})

    # Check if context is still valid
    if context and context.get('timestamp', 0) < time.time() - 600:
        await safe_valkey_delete(redis_key)
        return {}

    return context


async def get_or_create_thread(user_id: str, channel_id: str) -> str:
    """Get existing thread for user+channel or create new one with file_search enabled"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = get_redis_key(THREAD_CACHE_PREFIX, cache_key)

    # Check Valkey for existing thread
    existing_thread = await safe_valkey_get(redis_key)

    if existing_thread:
        print(f"♻️ Using existing thread: {existing_thread} for {cache_key}")

        # Verify the thread still has file_search enabled
        try:
            use_beta = hasattr(client, 'beta') and hasattr(client.beta, 'threads')
            if use_beta:
                thread = client.beta.threads.retrieve(existing_thread)
            else:
                thread = client.threads.retrieve(existing_thread)

            # Check if file_search is properly attached
            if hasattr(thread, 'tool_resources') and thread.tool_resources:
                if hasattr(thread.tool_resources, 'file_search') and thread.tool_resources.file_search:
                    print(f"✅ Thread {existing_thread} has file_search enabled")
                    return existing_thread
                else:
                    print(f"⚠️ Thread {existing_thread} missing file_search, creating new thread")
                    await safe_valkey_delete(redis_key)
            else:
                print(f"⚠️ Thread {existing_thread} has no tool_resources, creating new thread")
                await safe_valkey_delete(redis_key)
        except Exception as e:
            print(f"❌ Error checking thread {existing_thread}: {e}, creating new thread")
            await safe_valkey_delete(redis_key)

    # Create new thread with vector store explicitly attached
    try:
        vector_store_id = VECTOR_STORE_ID
        if not vector_store_id:
            print("❌ No VECTOR_STORE_ID found in environment")
            return None

        thread_params = {
            "tool_resources": {
                "file_search": {
                    "vector_store_ids": [vector_store_id]
                }
            }
        }

        print(f"🔗 Creating thread with vector store {vector_store_id}")

        use_beta = hasattr(client, 'beta') and hasattr(client.beta, 'threads')
        if use_beta:
            thread = client.beta.threads.create(**thread_params)
        else:
            thread = client.threads.create(**thread_params)

        # Store in Valkey
        await safe_valkey_set(redis_key, thread.id, ex=THREAD_CACHE_TTL)

        print(f"🆕 Created NEW thread: {thread.id} for {cache_key}")

        # Get cache stats
        thread_count = await get_thread_cache_size()
        print(f"📊 Thread cache now has {thread_count} threads")

        # Verify file_search was properly attached
        if hasattr(thread, 'tool_resources') and thread.tool_resources:
            if hasattr(thread.tool_resources, 'file_search') and thread.tool_resources.file_search:
                vector_stores = thread.tool_resources.file_search.vector_store_ids
                print(f"✅ File search enabled with vector stores: {vector_stores}")
            else:
                print("⚠️ File search not properly attached to thread")
        else:
            print("⚠️ No tool resources found on created thread")

        return thread.id

    except Exception as e:
        print(f"❌ Error creating thread: {e}")
        return None


async def get_thread_cache_size() -> int:
    """Get number of cached threads"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            # Valkey doesn't support pattern matching in keys command
            # We'll need to track this differently or iterate through known keys
            return 0  # Placeholder - implement proper counting if needed
        except:
            return 0
    else:
        return len(_local_cache.get('thread', {}))


async def wait_for_active_runs(thread_id: str, max_wait_seconds: int = 30) -> bool:
    """Wait for any active runs on a thread to complete"""
    use_beta = hasattr(client, 'beta') and hasattr(client.beta, 'threads')

    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        try:
            # Get all runs for the thread
            if use_beta:
                runs = client.beta.threads.runs.list(thread_id=thread_id, limit=5)
            else:
                runs = client.threads.runs.list(thread_id=thread_id, limit=5)

            # Check if any runs are still active
            active_runs = [run for run in runs.data if run.status in ["queued", "in_progress", "requires_action"]]

            if not active_runs:
                return True

            print(f"⏳ Waiting for {len(active_runs)} active run(s) to complete...")
            await asyncio.sleep(2)

        except Exception as e:
            print(f"❌ Error checking active runs: {e}")
            return False

    print(f"⚠️ Timeout waiting for active runs on thread {thread_id}")
    return False


async def cancel_stuck_runs(thread_id: str) -> bool:
    """Cancel any stuck runs on a thread"""
    use_beta = hasattr(client, 'beta') and hasattr(client.beta, 'threads')

    try:
        # Get all runs for the thread
        if use_beta:
            runs = client.beta.threads.runs.list(thread_id=thread_id, limit=5)
        else:
            runs = client.threads.runs.list(thread_id=thread_id, limit=5)

        # Find stuck runs (older than 3 minutes and still active)
        current_time = time.time()
        stuck_runs = []

        for run in runs.data:
            if run.status in ["queued", "in_progress", "requires_action"]:
                # Check if run is older than 3 minutes (180 seconds)
                run_age = current_time - run.created_at
                if run_age > 180:  # 3 minutes
                    stuck_runs.append(run)

        if stuck_runs:
            print(f"🚫 Cancelling {len(stuck_runs)} stuck run(s)...")
            for run in stuck_runs:
                try:
                    if use_beta:
                        client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run.id)
                    else:
                        client.threads.runs.cancel(thread_id=thread_id, run_id=run.id)
                    print(f"✅ Cancelled stuck run {run.id}")
                except Exception as cancel_error:
                    print(f"❌ Failed to cancel run {run.id}: {cancel_error}")

            # Wait a moment for cancellations to process
            await asyncio.sleep(2)

        return True

    except Exception as e:
        print(f"❌ Error cancelling stuck runs: {e}")
        return False


async def send_message_and_run(thread_id: str, message: str, instructions: str = None) -> str:
    """Enhanced send_message_and_run with concurrency handling"""
    try:
        # Step 1: Handle active runs
        runs_clear = await wait_for_active_runs(thread_id, max_wait_seconds=15)

        if not runs_clear:
            print("⚠️ Active runs detected, attempting to cancel stuck runs...")
            await cancel_stuck_runs(thread_id)

            # Try waiting again briefly
            runs_clear = await wait_for_active_runs(thread_id, max_wait_seconds=5)

            if not runs_clear:
                print("❌ Could not clear active runs, creating new thread...")
                # Force create a new thread by clearing cache

                # Find the cache key for this thread
                # Since we can't iterate keys in Valkey easily, we'll need to track this differently
                # For now, we'll just fail gracefully
                return "⚠️ Could not clear active runs, please try again"

        # Step 2: Send message and run
        use_beta = hasattr(client, 'beta') and hasattr(client.beta, 'threads')

        # Add message to thread
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

        # Create and run
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
        max_attempts = 45  # 45 seconds timeout
        attempt = 0

        while attempt < max_attempts:
            try:
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
                    error_msg = f"⚠️ Assistant run failed with status: {run_status.status}"
                    if hasattr(run_status, 'last_error') and run_status.last_error:
                        error_msg += f" - Error: {run_status.last_error}"
                    return error_msg
                elif run_status.status == "requires_action":
                    return "⚠️ Assistant requires action - please check configuration"

                await asyncio.sleep(1)
                attempt += 1

            except Exception as poll_error:
                print(f"❌ Error polling run status: {poll_error}")
                attempt += 1
                await asyncio.sleep(1)

        if attempt >= max_attempts:
            return f"⚠️ Assistant response timeout after {max_attempts} seconds"

        # Get the latest message
        try:
            if use_beta:
                messages = client.beta.threads.messages.list(thread_id=thread_id, limit=1)
            else:
                messages = client.threads.messages.list(thread_id=thread_id, limit=1)

            if messages.data:
                response_content = messages.data[0].content[0].text.value
                return response_content.strip()
            else:
                return "⚠️ No response from assistant"

        except Exception as message_error:
            print(f"❌ Error retrieving messages: {message_error}")
            return f"⚠️ Error retrieving assistant response: {str(message_error)}"

    except Exception as e:
        print(f"❌ Error in send_message_and_run: {e}")
        return f"⚠️ Assistant error: {str(e)}"


async def handle_conversational_question(user_question: str, user_id: str, channel_id: str) -> str:
    """Handle general conversational questions that don't require SQL"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    context = await get_conversation_context(user_id, channel_id)

    instructions = """You are a helpful data assistant with access to business data through file_search.

Your capabilities:
- Generate SQL queries for business questions about tickets, reviews, agents, and performance
- Answer questions about Klaus reviews, Zendesk tickets, and WOPs data
- Provide insights and summaries from query results
- Help users understand what data is available

Key data sources you have access to:
- Klaus reviews (fct_klaus__reviews)
- Zendesk tickets (stg_worker_ops__wfm_zendesk_tickets_data)
- Agent performance data (fct_amazon_connect__agent_metrics)
- Voice group data

Be helpful, conversational, and direct. Use file_search to provide accurate information about available data and capabilities."""

    context_message = ""
    if context:
        context_message = f"\n\nRecent context: User previously asked '{context.get('last_question', '')}'"

    message = f"""User question: {user_question}{context_message}

Provide a helpful response about your capabilities or the available data. If the user is asking about specific data, use file_search to find relevant information."""

    response = await send_message_and_run(thread_id, message, instructions)

    # Update conversation context
    await update_conversation_context(user_id, channel_id, user_question, response)

    return response


async def ask_assistant_generate_sql(user_question: str, user_id: str, channel_id: str, assistant_id: str = None,
                                     result_count: int = None) -> str:
    """Generate SQL using assistant with vector store context and smart caching"""
    if assistant_id:
        global ASSISTANT_ID
        ASSISTANT_ID = assistant_id

    # Analyze question intent
    question_intent = analyze_question_intent(user_question)
    print(f"🔍 Question intent: {question_intent}")

    # Check for table hints and learning opportunities
    table_hint = extract_table_hint(user_question)
    if table_hint:
        print(f"🎯 Table hint detected: {table_hint}")

    # Check learned preferences
    learned_table = await get_learned_table_preference(user_question)
    if learned_table:
        print(f"🧠 Using learned table preference: {learned_table}")

    # Check SQL cache (but be smarter about it)
    question_hash = get_question_hash(user_question)
    redis_key = get_redis_key(SQL_CACHE_PREFIX, question_hash)
    cached_entry = await safe_valkey_get(redis_key)

    # Use cache only if it's high quality or user hasn't provided corrections
    if cached_entry and not table_hint:
        cached_sql = cached_entry.get('sql')
        success_count = cached_entry.get('success_count', 0)
        if success_count > 0:  # Only use cache if it has been successful before
            print(f"💰 Using cached SQL (success_count: {success_count})")
            return cached_sql
        else:
            print(f"⚠️ Cached SQL has low success rate, generating new...")

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    print(f"🤖 Generating SQL for: {user_question[:50]}...")

    # Build intent-specific guidance
    intent_guidance = ""
    if question_intent['time_analysis']:
        intent_guidance = "Use EXTRACT(HOUR FROM timestamp) for hourly analysis and GROUP BY the time component."

    if question_intent['peak_analysis']:
        intent_guidance += " Use ORDER BY count DESC LIMIT 1 for highest/most questions."

    # SIMPLIFIED INSTRUCTIONS - much shorter and clearer
    instructions = f"""You are a Snowflake SQL expert with access to DBT documentation via file_search.

PROCESS:
1. Use file_search to find the correct table
2. Use file_search to verify column names exist  
3. Generate clean SQL using only verified columns

KEY TABLES:
- Klaus reviews → fct_klaus__reviews (use reviewer_name, not assignee_name)
- Tickets → stg_worker_ops__wfm_zendesk_tickets_data
- Voice group questions → stg_worker_ops__wfm_zendesk_tickets_data
- Agent metrics → fct_amazon_connect__agent_metrics

{intent_guidance}

CRITICAL: Always verify column names through file_search before using them.

Return only SQL in this format:
```sql
SELECT columns FROM table WHERE conditions;
```"""

    # SIMPLIFIED MESSAGE - much shorter
    message = f"""Generate SQL for: {user_question}

Requirements:
- Use file_search to find the right table
- Verify column names exist in the schema
- Return clean SQL only

Question type: {question_intent['type']}
Keywords: {question_intent['keywords']}"""

    response = await send_message_and_run(thread_id, message, instructions)

    # IMPROVED SQL extraction with better error handling
    sql_query = None

    # First try to extract SQL from code blocks
    if "```sql" in response:
        try:
            sql_query = response.split("```sql")[1].split("```")[0].strip()
        except IndexError:
            print("❌ Failed to extract SQL from code block")
            sql_query = None

    # If no SQL found, look for SELECT statements
    if not sql_query:
        lines = response.split('\n')
        for line in lines:
            if line.strip().upper().startswith('SELECT'):
                # Found a SELECT statement, try to extract it
                sql_lines = []
                for i, l in enumerate(lines[lines.index(line):]):
                    sql_lines.append(l)
                    if l.strip().endswith(';'):
                        break
                sql_query = '\n'.join(sql_lines).strip()
                break

    # If still no SQL, check if it's an error message
    if not sql_query:
        if "I don't have enough" in response or "cannot find" in response.lower():
            sql_query = response
        else:
            # Something went wrong - return error
            print(f"❌ No SQL found in response. Response was: {response[:200]}...")
            sql_query = "-- Error: Could not extract SQL from assistant response"

    # Validate the SQL doesn't contain instruction text
    if sql_query and ("MANDATORY" in sql_query or "Generate SQL for:" in sql_query):
        print("❌ Assistant returned instruction text instead of SQL")
        sql_query = "-- Error: Assistant returned instructions instead of SQL"

    print(f"🧠 Generated SQL: {sql_query}")
    return sql_query


async def handle_question(user_question: str, user_id: str, channel_id: str, assistant_id: str = None) -> Tuple[
    str, str]:
    """Main question handler that determines response type and routes accordingly"""

    # Get conversation context to help with classification
    context = await get_conversation_context(user_id, channel_id)

    question_intent = analyze_question_intent(user_question)
    question_type = question_intent['question_type']

    # ENHANCED: Check if this might be a SQL follow-up based on context
    if question_type == 'conversational' and context:
        last_question = context.get('last_question', '').lower()
        current_question = user_question.lower()

        # If the last question was SQL-related and current question seems like a modification
        sql_context_indicators = [
            'tickets', 'reviews', 'agents', 'performance', 'data', 'query'
        ]

        modification_indicators = [
            'same', 'instead', 'but', 'with names', 'names instead',
            'agent names', 'can i get', 'can you show', 'what about'
        ]

        # If previous question had SQL context and current has modification language
        has_sql_context = any(indicator in last_question for indicator in sql_context_indicators)
        has_modification = any(indicator in current_question for indicator in modification_indicators)

        if has_sql_context and has_modification:
            print("🔄 Context suggests this is a SQL follow-up, reclassifying...")
            question_type = 'sql_required'

    print(f"🔍 Question classified as: {question_type}")

    if question_type == 'sql_required':
        # Generate SQL and return it for execution
        sql_query = await ask_assistant_generate_sql(user_question, user_id, channel_id, assistant_id)
        return sql_query, 'sql'

    elif question_type in ['conversational', 'followup']:
        # Handle conversational questions directly
        response = await handle_conversational_question(user_question, user_id, channel_id)
        return response, 'conversational'

    else:
        # Default to conversational for ambiguous questions
        response = await handle_conversational_question(user_question, user_id, channel_id)
        return response, 'conversational'


# Separate function to update cache after we know the results
async def update_sql_cache_with_results(user_question: str, sql_query: str, result_count: int):
    """Update cache after we know query results - NOW ASYNC for Valkey"""
    question_hash = get_question_hash(user_question)
    redis_key = get_redis_key(SQL_CACHE_PREFIX, question_hash)

    # Update learning cache if user provided hints
    await update_learning_cache(user_question, sql_query, result_count)

    # Update SQL cache with quality metrics
    if should_cache_sql(sql_query, result_count):
        existing_entry = await safe_valkey_get(redis_key, {})

        if existing_entry:
            # Update existing entry
            existing_entry['success_count'] = existing_entry.get('success_count', 0) + (1 if result_count > 0 else 0)
            existing_entry['last_used'] = time.time()
        else:
            # Create new entry
            existing_entry = {
                'sql': sql_query,
                'success_count': 1 if result_count > 0 else 0,
                'last_used': time.time(),
                'result_quality': 'good' if result_count > 0 else 'poor'
            }

        await safe_valkey_set(redis_key, existing_entry, ex=SQL_CACHE_TTL)
        print(f"💾 Updated cache with quality metrics (results: {result_count})")
    else:
        print(f"🚫 Not caching poor quality SQL (results: {result_count})")


async def summarize_with_assistant(user_question: str, result_table: str, user_id: str, channel_id: str,
                                   assistant_id: str = None) -> str:
    """Summarize query results using assistant"""
    if assistant_id:
        global ASSISTANT_ID
        ASSISTANT_ID = assistant_id

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    instructions = """You are a business intelligence assistant. Provide clear, concise business summaries.

IMPORTANT RULES:
- Focus ONLY on business insights and answers
- DO NOT mention table names, databases, or technical details  
- DO NOT mention "based on the data" or "using data from..."
- Give direct, conversational business answers
- Include relevant numbers, trends, and insights
- Sound like a business analyst, not a database query tool"""

    message = f"""User question: "{user_question}"

Query results:
{result_table}

Provide a direct business answer without any technical references."""

    response = await send_message_and_run(thread_id, message, instructions)

    # Update conversation context with the result
    await update_conversation_context(user_id, channel_id, user_question, response)

    return response


async def debug_assistant_search(user_question: str, user_id: str, channel_id: str) -> str:
    """Debug function to see what the assistant finds in vector store"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    instructions = """You have access to a DBT manifest via vector store. Help debug by:
1. Search the vector store for any tables related to the user's question
2. List what tables, models, or information you can find
3. Show the database, schema, and table names you discover
4. If you find relevant information, show a sample of the metadata
5. Be verbose about what you're finding or not finding
6. Pay special attention to timestamp fields and their data types"""

    message = f"Debug search for: {user_question}\n\nWhat tables and information can you find related to this question? Focus on timestamp fields and data types."

    response = await send_message_and_run(thread_id, message, instructions)
    return response


async def test_file_search_connection(user_id: str, channel_id: str) -> str:
    """Test if file_search is properly connected to the thread"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    instructions = """You have access to a vector store with DBT documentation. Test your file_search capability:

1. Use file_search to look for any DBT models or tables
2. Report what you can find - be specific about table names
3. If you find anything, show a sample of the content
4. If you can't find anything, say "FILE_SEARCH_NOT_WORKING"

This is a test to verify file_search is properly connected."""

    message = "Test file_search: What DBT models and tables can you find in the vector store?"

    response = await send_message_and_run(thread_id, message, instructions)

    if "FILE_SEARCH_NOT_WORKING" in response:
        print("❌ File search is not working properly")
    elif "fct_" in response or "stg_" in response or "dim_" in response:
        print("✅ File search appears to be working - found DBT models")
    else:
        print("⚠️ File search connection unclear - response doesn't show clear DBT content")

    return response


async def clear_problematic_thread(user_id: str, channel_id: str):
    """Clear a specific thread from cache if it's having issues"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = get_redis_key(THREAD_CACHE_PREFIX, cache_key)

    if await safe_valkey_exists(redis_key):
        old_thread = await safe_valkey_get(redis_key)
        await safe_valkey_delete(redis_key)
        print(f"🗑️ Cleared problematic thread {old_thread} for {cache_key}")
        return True
    return False


async def clear_thread_cache():
    """Clear thread cache - useful for testing or memory management - NOW ASYNC"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            # Since we can't pattern match, we'd need to track keys separately
            # For now, just clear local cache
            print("🧹 Thread cache clear requested - implement key tracking for full clear")
        except Exception as e:
            print(f"❌ Error clearing thread cache: {e}")
    else:
        _local_cache['thread'].clear()
        print("🧹 Local thread cache cleared")


async def clear_sql_cache():
    """Clear SQL query cache - NOW ASYNC"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            # Since we can't pattern match, we'd need to track keys separately
            print("🧹 SQL cache clear requested - implement key tracking for full clear")
        except Exception as e:
            print(f"❌ Error clearing SQL cache: {e}")
    else:
        _local_cache['sql'].clear()
        print("🧹 Local SQL cache cleared")


async def clear_learning_cache():
    """Clear learning cache - NOW ASYNC"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            # Since we can't pattern match, we'd need to track keys separately
            print("🧹 Learning cache clear requested - implement key tracking for full clear")
        except Exception as e:
            print(f"❌ Error clearing learning cache: {e}")
    else:
        _local_cache['learning'].clear()
        print("🧹 Local learning cache cleared")


async def clear_conversation_cache():
    """Clear conversation context cache - NOW ASYNC"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            # Since we can't pattern match, we'd need to track keys separately
            print("🧹 Conversation cache clear requested - implement key tracking for full clear")
        except Exception as e:
            print(f"❌ Error clearing conversation cache: {e}")
    else:
        _local_cache['conversation'].clear()
        print("🧹 Local conversation cache cleared")


def test_question_classification():
    """Test the question classification to ensure it works correctly"""
    test_cases = [
        # Should be CONVERSATIONAL
        ("What are the top 5 questions you can answer about agent productivity?", "conversational"),
        ("What can you help me with?", "conversational"),
        ("What data do you have access to?", "conversational"),
        ("Give me examples of questions I can ask", "conversational"),
        ("What questions can you answer?", "conversational"),
        ("Tell me about Klaus reviews", "conversational"),
        ("What is agent productivity?", "conversational"),
        ("Help me understand what you can do", "conversational"),

        # Should be SQL_REQUIRED (including SQL follow-ups)
        ("How many tickets are open today?", "sql_required"),
        ("How many tickets does each agent resolve on average per week?", "sql_required"),
        ("Can i get the same info with agent names instead of id's", "sql_required"),  # KEY TEST CASE
        ("Show me all reviews for agent John", "sql_required"),
        ("What's the highest number of tickets this week?", "sql_required"),
        ("Count tickets in Voice group", "sql_required"),
        ("Find tickets created yesterday", "sql_required"),
        ("Total reviews today", "sql_required"),
        ("Agent performance data", "sql_required"),
        ("Tickets resolved per agent", "sql_required"),
        ("Average tickets per week", "sql_required"),
        ("Same data but for last week", "sql_required"),
        ("Can you show me with names instead", "sql_required"),
        ("What about Voice group only", "sql_required"),
        ("Same info but group by team", "sql_required"),

        # Should be FOLLOWUP (conversational follow-ups)
        ("Why is that?", "followup"),
        ("Can you explain that more?", "followup"),
        ("What does that mean?", "followup"),
        ("Tell me more about this", "followup"),
        ("Break that down for me", "followup"),
    ]

    print("🧪 Testing Question Classification:")
    print("=" * 50)

    for question, expected in test_cases:
        actual = classify_question_type(question)
        status = "✅" if actual == expected else "❌"
        print(f"{status} '{question}' → {actual} (expected: {expected})")

    print("=" * 50)


async def get_cache_stats():
    """Get cache statistics for debugging - NOW ASYNC"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            # Basic stats - would need key tracking for accurate counts
            return {
                "cache_backend": "Valkey",
                "status": "connected",
                "note": "Full stats require key tracking implementation"
            }
        except Exception as e:
            print(f"❌ Error getting cache stats: {e}")
            return {"error": str(e)}
    else:
        return {
            "cache_backend": "Local Memory",
            "thread_cache_size": len(_local_cache.get('thread', {})),
            "sql_cache_size": len(_local_cache.get('sql', {})),
            "learning_cache_size": len(_local_cache.get('learning', {})),
            "conversation_cache_size": len(_local_cache.get('conversation', {})),
            "cached_questions": len(_local_cache.get('sql', {})),
            "learned_patterns": dict(_local_cache.get('learning', {}))
        }


async def get_learning_insights():
    """Get insights about what the bot has learned - NOW ASYNC"""
    await ensure_valkey_connection()

    if valkey_client:
        # Would need to implement key tracking to get all learning patterns
        return "Learning insights require key tracking implementation for Valkey"
    else:
        learning_cache = _local_cache.get('learning', {})
        if not learning_cache:
            return "No learning patterns discovered yet."

        insights = []
        for pattern, table in learning_cache.items():
            insights.append(f"Pattern '{pattern}' → Prefers table '{table}'")

        return "\n".join(insights)


async def force_new_thread(user_id: str, channel_id: str):
    """Force creation of new thread for user+channel (useful for testing)"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = get_redis_key(THREAD_CACHE_PREFIX, cache_key)

    if await safe_valkey_exists(redis_key):
        await safe_valkey_delete(redis_key)
        print(f"🔄 Cleared thread for {cache_key} - next request will create new thread")


# Legacy functions for fallback (keeping for backward compatibility)
def ask_llm_for_sql(user_question: str, model_context: str) -> str:
    """Fallback SQL generation using direct completion"""
    OPENAI_MODEL = "gpt-4"
    prompt = f"""You are a Snowflake SQL expert. Based on the model documentation below, generate an accurate SQL query to answer the user's question.

Model documentation:
{model_context}

User question:
{user_question}

Only return the SQL query between triple backticks like this:
```sql
SELECT ...
```

If the question cannot be answered with the context, reply exactly:
"I don't have enough model information to answer that."
"""
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        content = response.choices[0].message.content.strip()

        if "```sql" in content:
            return content.split("```sql")[1].split("```")[0].strip()
        elif content.startswith("I don't have enough"):
            return content
        else:
            return content
    except Exception as e:
        return f"⚠️ LLM error during SQL generation: {e}"


def summarize_results_with_llm(user_question: str, result_table: str) -> str:
    """Fallback summarization using direct completion"""
    OPENAI_MODEL = "gpt-4"
    prompt = f"""You are a business intelligence assistant. Provide a clear, direct business answer.

User question: "{user_question}"

Query results:
{result_table}

IMPORTANT RULES:
- Give a direct business answer without technical details
- DO NOT mention table names, databases, or "based on data"  
- Focus on business insights and numbers
- Sound conversational and professional
- Answer the question directly

Provide a clean business summary:
"""
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"⚠️ LLM error during result summarization: {e}"


# Health check function for Valkey
async def check_valkey_health():
    """Check Valkey connection health - NOW ASYNC"""
    await ensure_valkey_connection()

    if valkey_client:
        try:
            await valkey_client.ping()
            return {"status": "healthy", "backend": "Valkey"}
        except Exception as e:
            return {"status": "unhealthy", "backend": "Valkey", "error": str(e)}
    else:
        return {"status": "fallback", "backend": "Local Memory"}
# - test git deployment