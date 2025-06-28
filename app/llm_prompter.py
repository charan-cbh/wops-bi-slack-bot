import os
import asyncio
import time
import json
import traceback
import re
from typing import Optional, Dict, Any, Tuple, List
from dotenv import load_dotenv
from openai import OpenAI
import pandas as pd
from glide import GlideClient, GlideClientConfiguration, NodeAddress, GlideClusterClient, \
    GlideClusterClientConfiguration
from app.pattern_matcher import PatternMatcher, PatternBasedQueryHelper
import tiktoken
import hashlib

# Import the pattern matcher
try:
    pattern_matcher = PatternMatcher()
    pattern_query_builder = PatternBasedQueryHelper()
    PATTERNS_AVAILABLE = True
except ImportError:
    PATTERNS_AVAILABLE = False
    print("⚠️ Pattern matcher not available - using standard flow")

load_dotenv()

# Configuration
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
TABLE_SELECTION_CACHE_TTL = 2592000  # 30 days for table selection patterns
FEEDBACK_CACHE_TTL = 7776000  # 90 days for feedback data

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)

# Global Valkey client
valkey_client = None

# Fallback to local memory if Valkey is not available
_local_cache = {
    'thread': {},
    'sql': {},
    'schema': {},  # Cache for table schemas
    'conversation': {},
    'table_selection': {},  # Cache for question->table mappings
    'table_samples': {},  # Cache for table sample data
    'feedback': {}  # Cache for user feedback
}

# Cache key prefixes
CACHE_PREFIX = "bi_slack_bot"
THREAD_CACHE_PREFIX = f"{CACHE_PREFIX}:thread"
SQL_CACHE_PREFIX = f"{CACHE_PREFIX}:sql"
SCHEMA_CACHE_PREFIX = f"{CACHE_PREFIX}:schema"
CONVERSATION_CACHE_PREFIX = f"{CACHE_PREFIX}:conversation"
TABLE_SELECTION_PREFIX = f"{CACHE_PREFIX}:table_selection"
TABLE_SAMPLES_PREFIX = f"{CACHE_PREFIX}:table_samples"
FEEDBACK_PREFIX = f"{CACHE_PREFIX}:feedback"
# Rate limiting configuration
MAX_TOKENS_PER_USER_PER_DAY = int(os.getenv("MAX_TOKENS_PER_USER_PER_DAY", 1000000))
MAX_TOKENS_PER_USER_PER_HOUR = int(os.getenv("MAX_TOKENS_PER_USER_PER_HOUR", 200000))
MAX_TOKENS_PER_THREAD = int(os.getenv("MAX_TOKENS_PER_THREAD", 1000000))
ENABLE_RATE_LIMITING = os.getenv("ENABLE_RATE_LIMITING", "true").lower() == "true"
ADMIN_USERS = [u.strip() for u in os.getenv("ADMIN_USERS", "").split(",") if u.strip()]
# Cache prefixes for token tracking
TOKEN_USAGE_PREFIX = f"{CACHE_PREFIX}:token_usage"
DAILY_USAGE_PREFIX = f"{TOKEN_USAGE_PREFIX}:daily"
HOURLY_USAGE_PREFIX = f"{TOKEN_USAGE_PREFIX}:hourly"
THREAD_USAGE_PREFIX = f"{TOKEN_USAGE_PREFIX}:thread"
TOKEN_USAGE_CACHE_TTL = 86400  # 24 hours for daily limits

# Stop words for phrase extraction
STOP_WORDS = {'the', 'is', 'at', 'which', 'on', 'and', 'a', 'an', 'as', 'are',
              'was', 'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does',
              'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must',
              'shall', 'to', 'of', 'in', 'for', 'with', 'by', 'from', 'about'}


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


def convert_to_serializable(obj):
    """Convert non-serializable objects to serializable format"""
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif hasattr(obj, 'isoformat'):  # datetime, Timestamp
        return obj.isoformat()
    elif hasattr(obj, 'to_dict'):  # DataFrame
        return obj.to_dict()
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        return str(obj)


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
        except TypeError as te:
            print(f"⚠️ JSON serialization error for {key}: {te}")
            print(f"⚠️ Problematic value type: {type(value)}")
            # Try to convert to a serializable format
            try:
                if isinstance(value, dict):
                    value = convert_to_serializable(value)
                    json_value = json.dumps(value)
                    if ex:
                        await valkey_client.set(key, json_value)
                        await valkey_client.expire(key, ex)
                    else:
                        await valkey_client.set(key, json_value)
                    return True
            except Exception as e2:
                print(f"⚠️ Failed to convert to serializable: {e2}")
                return False
        except Exception as e:
            print(f"⚠️ Valkey SET error for {key}: {e}")
            return False
    else:
        # Fallback to local cache
        cache_type = key.split(':')[1] if ':' in key else 'thread'
        if cache_type not in _local_cache:
            _local_cache[cache_type] = {}
        try:
            # Ensure value is serializable for consistency
            _ = json.dumps(value)
            _local_cache[cache_type][key] = value
        except TypeError:
            # Convert to serializable format
            if isinstance(value, dict):
                value = convert_to_serializable(value)
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


def extract_key_phrases(question: str) -> List[str]:
    """Extract key phrases from question for learning"""
    words = question.lower().split()
    phrases = []

    # Single words (excluding stop words)
    important_words = [w for w in words if len(w) > 3 and w not in STOP_WORDS]
    phrases.extend(important_words)

    # Bigrams for important patterns
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i + 1]}"
        # Add all bigrams that don't contain only stop words
        if not all(word in STOP_WORDS for word in [words[i], words[i + 1]]):
            phrases.append(bigram)

    # Trigrams for complex patterns
    for i in range(len(words) - 2):
        trigram = f"{words[i]} {words[i + 1]} {words[i + 2]}"
        # Add trigrams that contain at least one important word
        if any(word not in STOP_WORDS and len(word) > 3 for word in [words[i], words[i + 1], words[i + 2]]):
            phrases.append(trigram)

    return list(set(phrases))  # Remove duplicates


async def classify_question_with_openai(question: str, user_id: str, channel_id: str, context: dict = None) -> str:
    """Use OpenAI to classify question type with context awareness"""

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return 'sql_required'  # Default fallback

    # Build classification instructions
    instructions = """You are a BI expert who needs to classify user questions into two categories:

**sql_required**: Questions that need data analysis, queries, calculations, rankings, comparisons, or retrieving specific information from databases
**conversational**: Questions about definitions, explanations, capabilities, or clarifications about previous results

**SQL_REQUIRED Examples:**
- "How many tickets were created today?"
- "Who has the highest QA scores?"  
- "Can you tell me who has made the most improvement in QA out of these agents in the last 2 weeks?"
- "Show me agent performance for these specific agents"
- "What's the average handle time by team?"
- "Compare performance between agents X, Y, Z"
- "Show trends for the last month"
- "Which agent performed best?"
- "Top 10 agents by resolution time"
- Any question asking for WHO, WHAT (metrics), HOW MANY, SHOW ME, COMPARE

**CONVERSATIONAL Examples:**
- "What does AHT mean?" (definition)
- "What is the source of this data?" (about previous results)
- "How do you calculate QA scores?" (methodology)
- "What tables do you have access to?" (capabilities)
- "Explain these results" (clarification about previous results)
- "What does that number mean?" (about previous results)

**CRITICAL CLASSIFICATION RULES:**
1. Questions asking for specific data, metrics, comparisons, rankings = sql_required
2. Questions with agent names + performance/improvement = sql_required  
3. Questions asking WHO, WHAT metrics, HOW MANY, SHOW ME = sql_required
4. Questions about definitions, explanations, or data sources = conversational
5. If unsure between data vs explanation, lean toward sql_required

Return ONLY one word: "sql_required" or "conversational" (no explanation, no other text)"""

    # Build message with context
    message_parts = [f"Question to classify: {question}"]

    # Add context if it might affect classification
    if context and context.get('last_response_type') == 'sql_results':
        # Check if this looks like a follow-up about previous results
        followup_indicators = ['this data', 'these results', 'that number', 'the source', 'why is it',
                               'what does this mean', 'explain this']
        if any(indicator in question.lower() for indicator in followup_indicators):
            message_parts.append("Context: This appears to be a follow-up question about previous SQL results.")

    message = "\n".join(message_parts)

    try:
        response = await send_message_and_run(thread_id, message, instructions)

        # Extract classification
        response_clean = response.strip().lower()
        if 'sql_required' in response_clean:
            return 'sql_required'
        elif 'conversational' in response_clean:
            return 'conversational'
        else:
            print(f"⚠️ Unclear OpenAI classification response: {response}")
            # Fallback to simple heuristic
            return classify_question_type_fallback(question)

    except Exception as e:
        print(f"❌ Error in OpenAI classification: {e}")
        # Fallback to simple heuristic
        return classify_question_type_fallback(question)


def classify_question_type_fallback(question: str) -> str:
    """Simple fallback classification if OpenAI fails"""
    question_lower = question.lower()

    # Strong indicators for SQL queries
    strong_sql_indicators = [
        'how many', 'count', 'show me', 'list', 'who has', 'who is',
        'what is the', 'compare', 'vs', 'versus', 'ranking', 'rank',
        'improvement', 'performance', 'metrics', 'agents', 'tickets',
        'highest', 'lowest', 'best', 'worst', 'most', 'least',
        'average', 'total', 'sum', 'breakdown', 'analysis'
    ]

    # Strong indicators for conversational
    strong_conversational_indicators = [
        'what does', 'definition of', 'meaning of', 'explain',
        'what is the source', 'how do you calculate', 'methodology',
        'what tables', 'what data do you have', 'capabilities'
    ]

    # Check for strong SQL indicators first
    if any(indicator in question_lower for indicator in strong_sql_indicators):
        return 'sql_required'

    # Check for strong conversational indicators
    if any(indicator in question_lower for indicator in strong_conversational_indicators):
        return 'conversational'

    # For ambiguous cases, default to sql_required
    # Better to try SQL and fail than miss a data request
    return 'sql_required'

def classify_question_type(question: str) -> str:
    """Enhanced classification for data queries vs conversational questions"""
    question_lower = question.lower()

    # FIRST: Check for follow-up indicators about previous results
    followup_about_results = [
        'what is the source', 'where does this data', 'where is this from',
        'how did you get', 'what table', 'which database',
        'explain this', 'what does this mean', 'why is it',
        'can you clarify', 'tell me more about this',
        'break this down', 'what are these', 'who are these'
    ]

    if any(indicator in question_lower for indicator in followup_about_results):
        return 'conversational'

    # NEW: Check for metadata/definition questions (EXPANDED)
    metadata_questions = [
        'what metric is used', 'which metric', 'what is the metric',
        'is the metric', 'metric being used', 'what does aht mean',
        'definition of', 'what is aht', 'what does', 'meaning of',
        'switched to', 'changed to', 'still using', 'still the metric',
        'clarify', 'clarification', 'qq for clarity', 'quick question',
        'what column', 'which column', 'column used', 'field used',
        'handle time vs', 'resolution time vs', 'difference between',
        'currently being used', 'currently using', 'what represents',
        'how is calculated', 'calculation method', 'methodology',
        'what is fcr', 'what does fcr mean', 'fcr definition',
        'what kpis', 'available kpis', 'kpi definition',
        'business logic', 'business rules', 'how do you calculate',
        'what makes', 'how is determined', 'logic behind', 'why do we'
    ]

    if any(indicator in question_lower for indicator in metadata_questions):
        return 'conversational'

    # NEW: Check for data discovery questions
    discovery_questions = [
        'what data', 'what tables', 'what columns', 'available data',
        'what information', 'what metrics', 'data sources',
        'what can you tell me', 'what do you know', 'data available'
    ]

    if any(indicator in question_lower for indicator in discovery_questions):
        return 'conversational'

    # NEW: Check for capability questions
    capability_questions = [
        'what can you', 'help', 'capabilities', 'questions can',
        'how do you work', 'how to use', 'what commands',
        'what types of questions', 'how to ask'
    ]

    if any(indicator in question_lower for indicator in capability_questions):
        return 'conversational'

    # Check for data query indicators - EXPANDED LIST
    data_indicators = [
        'how many', 'count', 'show me', 'list', 'find',
        'highest', 'lowest', 'average', 'total',
        'tickets', 'agents', 'reviews', 'performance',
        'reply time', 'response time', 'resolution',
        'which ticket type', 'what ticket type', 'driving',
        'volume', 'trend', 'compare', 'by group', 'by channel',
        'contact driver', 'handling time', 'aht', 'kpi', 'kpis',
        'determine', 'metrics', 'performance metrics',
        'created', 'today', 'yesterday', 'this week', 'last week',
        'this month', 'last month', 'zendesk', 'chat', 'email',
        'what are the', 'give me', 'provide', 'fetch',
        'calculate', 'sum', 'aggregate', 'breakdown'
    ]

    # Check for general follow-up indicators
    general_followup_indicators = [
        'why', 'what does that mean', 'explain that',
        'can you elaborate', 'tell me more',
        'what about', 'how about'
    ]

    # Check context - short questions after data results are often follow-ups
    word_count = len(question.split())

    # Special handling for "volume" questions - these are ALWAYS data queries
    if 'volume' in question_lower and any(
            word in question_lower for word in ['ticket', 'tickets', 'agent', 'chat', 'email']):
        return 'sql_required'

    # Check if it's asking for specific data (but not about definitions/metadata)
    if any(indicator in question_lower for indicator in data_indicators):
        # Double check it's not asking about the data source or definitions
        if any(meta in question_lower for meta in ['source', 'definition', 'meaning', 'what does', 'clarify', 'switched', 'changed']):
            return 'conversational'
        return 'sql_required'

    if any(indicator in question_lower for indicator in general_followup_indicators):
        # But if it also contains data indicators, it might be a data query
        if any(indicator in question_lower for indicator in data_indicators):
            return 'sql_required'
        return 'conversational'
    else:
        # For ambiguous cases, check if it contains entities or time references
        entities = ['ticket', 'agent', 'customer', 'zendesk', 'chat', 'email', 'messaging']
        time_refs = ['today', 'yesterday', 'week', 'month', 'year', 'date']

        # If asking about definitions/clarifications of entities, it's conversational
        if any(meta in question_lower for meta in ['what is', 'definition', 'meaning', 'clarify', 'switched', 'changed', 'still using']):
            return 'conversational'

        if any(entity in question_lower for entity in entities) or any(time in question_lower for time in time_refs):
            return 'sql_required'

        # Very short questions are likely follow-ups
        if word_count <= 4:
            return 'conversational'

        return 'sql_required'  # Default to trying SQL

async def find_relevant_tables_from_vector_store(question: str, user_id: str, channel_id: str, top_k: int = 8) -> List[str]:
    """Use assistant's file_search to find relevant tables from dbt manifest"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        print("❌ Could not create thread for vector search")
        return []

    # More thorough instructions for finding tables
    instructions = """You are a BI expert searching the dbt manifest for tables.

CRITICAL: Find ALL tables that could potentially answer the question. Do not filter or shortlist prematurely.

Search comprehensively through:
1. Table names and their full descriptions
2. ALL column names and their descriptions
3. Business logic and definitions in the manifest
4. Table relationships and data lineage

IMPORTANT:
- Include tables even if they might have the data in a different format
- Include both raw and aggregated tables
- Include tables with partial matches - better to check more tables than miss the right one
- Look for synonyms and related terms (e.g., "tickets" could be "issues", "cases", "requests")

Return a JSON array of ALL potentially relevant tables.
Format: ["schema.database.table1", "schema.database.table2", ...]"""

    message = f"""Find ALL tables that might help answer: {question}

Search for tables containing:
- Direct matches to entities mentioned (tickets, agents, customers, etc.)
- Any metrics or measures that could be calculated or derived
- Time/date columns if time analysis is needed
- Any related or auxiliary data that might be required
- Both detail and summary tables

Be COMPREHENSIVE - include any table that might have relevant data."""

    try:
        response = await send_message_and_run(thread_id, message, instructions)

        # Try multiple extraction methods
        tables = []

        # Method 1: Direct JSON parse
        try:
            tables = json.loads(response.strip())
            if isinstance(tables, list):
                print(f"🔍 Vector store found {len(tables)} tables (direct parse)")
                return tables[:top_k]
        except:
            pass

        # Method 2: Extract JSON from anywhere in response
        json_match = re.search(r'\[[\s\S]*?\]', response)
        if json_match:
            try:
                tables = json.loads(json_match.group())
                print(f"🔍 Vector store found {len(tables)} tables (regex extract)")
                return tables[:top_k]
            except:
                pass

        # Method 3: If response contains table names but not in JSON format
        # Extract anything that looks like a table name (schema.database.table pattern)
        table_pattern = r'[A-Z_]+\.[a-z_]+\.[a-z_]+'
        found_tables = re.findall(table_pattern, response, re.IGNORECASE)
        if found_tables:
            tables = list(set(found_tables))  # Remove duplicates
            print(f"🔍 Vector store found {len(tables)} tables (pattern matching)")
            return tables[:top_k]

        print(f"⚠️ Could not extract tables from response: {response[:200]}...")
        return []

    except Exception as e:
        print(f"❌ Error in vector search: {e}")
        traceback.print_exc()
        return []


async def sample_table_data(table_name: str, sample_size: int = 10) -> Dict[str, Any]:
    """Sample random rows from a table to understand its structure and content"""
    print(f"📊 Sampling {sample_size} rows from {table_name}")

    # Check cache first
    cache_key = f"{TABLE_SAMPLES_PREFIX}:{table_name}:{sample_size}"
    cached_sample = await safe_valkey_get(cache_key)

    if cached_sample and cached_sample.get('cached_at', 0) > time.time() - 3600:  # 1 hour cache
        print(f"📋 Using cached sample for {table_name}")
        return cached_sample

    if not SNOWFLAKE_AVAILABLE:
        return {'error': 'Snowflake not available'}

    try:
        # First, get column information
        column_info_sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE, COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name.split('.')[-1].upper()}'
        AND TABLE_SCHEMA = '{table_name.split('.')[-2].upper()}'
        AND TABLE_CATALOG = '{table_name.split('.')[-3].upper()}'
        ORDER BY ORDINAL_POSITION
        """

        try:
            column_info = run_query(column_info_sql)
            column_descriptions = {}
            if not isinstance(column_info, str) and hasattr(column_info, 'iterrows'):
                for _, row in column_info.iterrows():
                    column_descriptions[row['COLUMN_NAME'].lower()] = {
                        'type': row['DATA_TYPE'],
                        'comment': row.get('COMMENT', '')
                    }
        except:
            column_descriptions = {}

        # Now sample the actual data
        # First, try to discover columns to find PROPER audit columns
        schema_sql = f"SELECT * FROM {table_name} LIMIT 1"
        df_schema = run_query(schema_sql)

        if isinstance(df_schema, str):
            # If error, fall back to simple sampling
            sample_sql = f"SELECT * FROM {table_name} SAMPLE ({sample_size} ROWS)"
        else:
            # Look for ACTUAL audit/timestamp columns with strict patterns
            columns = list(df_schema.columns)

            # Define strict patterns for audit columns
            audit_patterns = [
                '_updated_at', '_created_at', '_inserted_at', '_modified_at',
                'updated_at', 'created_at', 'inserted_at', 'modified_at',
                'update_date', 'create_date', 'insert_date', 'modify_date',
                'dw_updated_at', 'dw_created_at', 'dw_insert_date',
                'audit_timestamp', 'last_modified', 'last_updated'
            ]

            # Find audit columns - must match exactly or end with the pattern
            audit_cols = []
            for col in columns:
                col_lower = col.lower()
                # Check if column ends with or equals any audit pattern
                for pattern in audit_patterns:
                    if col_lower.endswith(pattern) or col_lower == pattern:
                        # Also verify it's actually a timestamp type if we have metadata
                        col_type = column_descriptions.get(col_lower, {}).get('type', '').lower()
                        if not col_type or 'timestamp' in col_type or 'date' in col_type:
                            audit_cols.append(col)
                            break

            # If no audit columns found, look for date columns with strict patterns
            if not audit_cols:
                date_patterns = ['_date', 'date_', 'transaction_date', 'event_date', 'process_date']
                for col in columns:
                    col_lower = col.lower()
                    for pattern in date_patterns:
                        if pattern in col_lower:
                            col_type = column_descriptions.get(col_lower, {}).get('type', '').lower()
                            if not col_type or 'date' in col_type or 'timestamp' in col_type:
                                audit_cols.append(col)
                                break

            if audit_cols:
                # Use the LAST audit column (usually the most recent update timestamp)
                order_col = audit_cols[-1]
                sample_sql = f"""
                SELECT * FROM (
                    SELECT * FROM {table_name} 
                    ORDER BY {order_col} DESC 
                    LIMIT 1000
                ) SAMPLE ({sample_size} ROWS)
                """
                print(f"🔍 Sampling with audit column: {order_col} DESC")
            else:
                # No audit column found, use random sampling
                sample_sql = f"SELECT * FROM {table_name} SAMPLE ({sample_size} ROWS)"
                print(f"🔍 No audit columns found, using random sampling")

        print(f"🔍 Executing: {sample_sql}")
        df = run_query(sample_sql)

        if isinstance(df, str):
            print(f"❌ Sample query failed: {df}")
            return {'error': df}

        # Handle the case where run_query returns a list (raw results)
        if isinstance(df, list):
            print(f"⚠️ Got raw list result, expected DataFrame")
            return {'error': 'Unexpected result format from query'}

        # Ensure we have a DataFrame
        if not hasattr(df, 'columns'):
            print(f"⚠️ Result doesn't have columns attribute")
            return {'error': 'Invalid result format'}

        # Get column info
        columns = list(df.columns)
        dtypes = {col: str(df[col].dtype) for col in columns}

        # Convert to serializable format
        df_serializable = df.copy()
        for col in df_serializable.columns:
            if df_serializable[col].dtype == 'datetime64[ns]' or 'timestamp' in str(df_serializable[col].dtype).lower():
                df_serializable[col] = df_serializable[col].astype(str)
            elif df_serializable[col].dtype == 'object':
                try:
                    if len(df_serializable) > 0 and hasattr(df_serializable[col].iloc[0], 'isoformat'):
                        df_serializable[col] = df_serializable[col].astype(str)
                except:
                    pass

        sample_data = df_serializable.to_dict('records')

        # Get value statistics for numeric columns
        value_stats = {}
        for col in columns:
            if df[col].dtype in ['int64', 'float64', 'Int64', 'Float64']:
                try:
                    non_null_values = df[col].dropna()
                    if len(non_null_values) > 0:
                        value_stats[col] = {
                            'min': float(non_null_values.min()),
                            'max': float(non_null_values.max()),
                            'mean': float(non_null_values.mean()),
                            'non_null_count': int(non_null_values.count()),
                            'null_count': int(df[col].isna().sum()),
                            'unique_count': int(df[col].nunique())
                        }
                except:
                    pass

        # Identify audit columns found
        audit_columns_found = []
        for col in columns:
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in ['_updated_at', '_created_at', 'audit', 'modified', 'inserted']):
                audit_columns_found.append(col)

        # Create sample info
        sample_info = {
            'table': table_name,
            'columns': columns,
            'column_types': dtypes,
            'column_descriptions': column_descriptions,
            'sample_data': sample_data,
            'sample_size': len(df),
            'value_stats': value_stats,
            'audit_columns': audit_columns_found,
            'cached_at': time.time()
        }

        # Cache the sample
        await safe_valkey_set(cache_key, sample_info, ex=3600)  # 1 hour expiry

        print(f"✅ Sampled {len(df)} rows from {table_name} ({len(columns)} columns)")
        if audit_columns_found:
            print(f"📋 Audit columns found: {', '.join(audit_columns_found)}")

        return sample_info

    except Exception as e:
        print(f"❌ Error sampling table {table_name}: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

async def select_best_table_using_samples(question: str, candidate_tables: List[str], user_id: str, channel_id: str) -> Tuple[str, str]:
    """
    Sample data from candidate tables and use assistant to select the best one
    Returns: (selected_table, reason)
    """
    print(f"\n🔍 Analyzing {len(candidate_tables)} candidate tables for question: {question[:50]}...")

    # Sample data from each candidate table
    table_samples = {}
    sample_errors = []

    for table in candidate_tables:
        sample = await sample_table_data(table, sample_size=10)
        if not sample.get('error'):
            table_samples[table] = sample
            print(f"✅ Successfully sampled {table}")
        else:
            error_msg = f"Could not sample {table}: {sample.get('error')}"
            print(f"⚠️ {error_msg}")
            sample_errors.append(error_msg)

    if not table_samples:
        print("❌ Could not sample any tables")
        # Return first candidate with explanation
        if candidate_tables:
            return candidate_tables[0], f"Could not sample tables. Errors: {'; '.join(sample_errors)}"
        return "", "No tables could be sampled"

    # Use assistant to analyze samples and select best table
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return list(table_samples.keys())[0], "Could not create analysis thread"

    instructions = """You are a BI expert representing the Business Intelligence team. Your job is to select the MOST ACCURATE table for answering the user's question.

CRITICAL REQUIREMENTS:
1. ACCURACY IS PARAMOUNT - Do not take shortcuts or choose convenience over correctness
2. The selected table must contain ALL data elements needed to answer the question COMPLETELY
3. Verify each required metric/dimension exists with the EXACT data needed
4. Check actual data values, not just column names
5. Ensure proper data granularity (don't use aggregated tables if raw data is needed)
6. Validate that the table has sufficient data quality and coverage

ANALYSIS PROCESS:
1. Break down the user's question into required data elements:
   - What metrics/measures are needed?
   - What dimensions/attributes are needed?
   - What time period/granularity is required?
   - What filters/conditions must be applied?

2. For each table, rigorously check:
   - Does it have ALL required metrics with correct calculations?
   - Does it have ALL required dimensions at the right level?
   - Does it have proper time columns for the analysis?
   - Is the data fresh and complete (check audit columns)?
   - Are the values in the expected format and range?

3. REJECT tables that:
   - Are missing ANY required data element
   - Have pre-aggregated data when raw data is needed
   - Have incorrect granularity
   - Show poor data quality (many nulls, suspicious values)

4. Select the table that:
   - Contains 100% of required data elements
   - Has the most appropriate granularity
   - Shows the best data quality
   - Has the most recent data

DO NOT select a table just because it seems easier to query. Select based on COMPLETENESS and ACCURACY.

Return ONLY:
SELECTED_TABLE: <full table name>
REASON: <detailed explanation listing EVERY required data element and exactly which columns provide them>"""

    # Build comprehensive analysis message
    message_parts = [f"User Question: {question}\n\nAnalyze these tables:"]

    for table, sample in table_samples.items():
        message_parts.append(f"\n\n{'=' * 80}")
        message_parts.append(f"TABLE: {table}")
        message_parts.append(f"COLUMNS ({len(sample.get('columns', []))}): {', '.join(sample.get('columns', []))}")

        # Show audit columns if found
        if sample.get('audit_columns'):
            message_parts.append(f"AUDIT COLUMNS: {', '.join(sample['audit_columns'])}")

        # Show column descriptions if available
        if sample.get('column_descriptions'):
            message_parts.append("\nColumn Descriptions:")
            for col, desc in list(sample.get('column_descriptions', {}).items())[:30]:
                if desc.get('comment'):
                    message_parts.append(f"  - {col} ({desc['type']}): {desc['comment']}")
                else:
                    message_parts.append(f"  - {col}: {desc['type']}")

        # Show value statistics for numeric columns
        if sample.get('value_stats'):
            message_parts.append("\nNumeric Column Statistics:")
            for col, stats in list(sample.get('value_stats', {}).items())[:20]:
                null_pct = (stats['null_count'] / (
                            stats['null_count'] + stats['non_null_count']) * 100) if 'null_count' in stats else 0
                message_parts.append(
                    f"  - {col}: min={stats['min']:.2f}, max={stats['max']:.2f}, mean={stats['mean']:.2f}, non_null={stats['non_null_count']}, nulls={null_pct:.1f}%, unique={stats.get('unique_count', 'N/A')}")

        # Show actual sample data
        if sample.get('sample_data') and len(sample['sample_data']) > 0:
            message_parts.append("\nSample Data (5 rows):")
            for i, row_data in enumerate(sample['sample_data'][:5]):
                message_parts.append(f"\nRow {i + 1}:")
                # Show ALL columns to give complete picture
                shown_count = 0
                for col, val in row_data.items():
                    if shown_count < 25:  # Limit to 25 columns per row
                        val_str = str(val)[:100] + "..." if len(str(val)) > 100 else str(val)
                        message_parts.append(f"  {col}: {val_str}")
                        shown_count += 1
                    elif shown_count == 25:
                        message_parts.append(f"  ... and {len(row_data) - 25} more columns")
                        break

    message_parts.append(f"\n\n{'=' * 80}")
    message_parts.append("\nREMEMBER: Select based on ACCURACY and COMPLETENESS, not convenience!")
    message_parts.append("The table must contain ALL required data elements to answer the question perfectly.")

    message = "\n".join(message_parts)

    print("📤 Sending table analysis request to assistant...")
    response = await send_message_and_run(thread_id, message, instructions)

    # Parse response
    selected_table = ""
    reason = ""

    for line in response.split('\n'):
        if line.strip().startswith('SELECTED_TABLE:'):
            selected_table = line.replace('SELECTED_TABLE:', '').strip()
        elif line.strip().startswith('REASON:'):
            reason = line.replace('REASON:', '').strip()

    if not selected_table and table_samples:
        # Fallback to first table if parsing fails
        selected_table = list(table_samples.keys())[0]
        reason = "Failed to parse selection, using first candidate"

    print(f"✅ Selected table: {selected_table}")
    print(f"📝 Reason: {reason}")

    return selected_table, reason


async def get_table_descriptions_from_manifest(table_names: List[str], user_id: str, channel_id: str) -> Dict[str, str]:
    """Get table descriptions from dbt manifest for given tables"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return {}

    instructions = """Search the dbt manifest and return detailed descriptions for these tables.

Include:
1. Table purpose and description
2. Business context
3. Key metrics and dimensions available
4. Data sources and update frequency

Return ONLY a JSON object with table names as keys and descriptions as values."""

    message = f"Get detailed descriptions for these tables:\n{json.dumps(table_names, indent=2)}"

    response = await send_message_and_run(thread_id, message, instructions)

    try:
        # Extract JSON from response
        if '{' in response and '}' in response:
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            json_str = response[json_start:json_end]
            descriptions = json.loads(json_str)
            return descriptions
    except Exception as e:
        print(f"⚠️ Error parsing table descriptions: {e}")

    return {}


async def cache_table_selection(question: str, selected_table: str, reason: str, success: bool = True):
    """Cache the table selection for future similar questions"""
    # Extract key phrases
    key_phrases = extract_key_phrases(question)

    # Cache the full selection
    selection_key = f"{TABLE_SELECTION_PREFIX}:{get_question_hash(question)}"
    selection_data = {
        'question': question,
        'selected_table': selected_table,
        'reason': reason,
        'success': success,
        'key_phrases': key_phrases,
        'timestamp': time.time()
    }
    await safe_valkey_set(selection_key, selection_data, ex=TABLE_SELECTION_CACHE_TTL)

    # Also cache by key phrases for pattern matching
    for phrase in key_phrases:
        phrase_key = f"{TABLE_SELECTION_PREFIX}:phrase:{phrase}"
        phrase_data = await safe_valkey_get(phrase_key, {})

        if selected_table not in phrase_data:
            phrase_data[selected_table] = {
                'count': 0,
                'success_count': 0,
                'last_used': time.time()
            }

        phrase_data[selected_table]['count'] += 1
        if success:
            phrase_data[selected_table]['success_count'] += 1
        phrase_data[selected_table]['last_used'] = time.time()

        await safe_valkey_set(phrase_key, phrase_data, ex=TABLE_SELECTION_CACHE_TTL)

    print(f"💾 Cached table selection: {selected_table} for {len(key_phrases)} key phrases")


async def get_cached_table_suggestion(question: str) -> Optional[str]:
    """Check if we have a cached table selection for this question"""
    # First check exact question match
    selection_key = f"{TABLE_SELECTION_PREFIX}:{get_question_hash(question)}"
    cached_selection = await safe_valkey_get(selection_key)

    if cached_selection and cached_selection.get('success'):
        # Check if there's negative feedback for this selection
        feedback_key = f"{FEEDBACK_PREFIX}:{get_question_hash(question)}"
        feedback = await safe_valkey_get(feedback_key, {})

        if feedback.get('negative_count', 0) > feedback.get('positive_count', 0):
            print(f"⚠️ Skipping cached suggestion due to negative feedback")
            return None

        print(f"💰 Found exact cached table selection: {cached_selection['selected_table']}")
        return cached_selection['selected_table']

    # Check phrase-based suggestions
    key_phrases = extract_key_phrases(question)
    table_scores = {}

    for phrase in key_phrases:
        phrase_key = f"{TABLE_SELECTION_PREFIX}:phrase:{phrase}"
        phrase_data = await safe_valkey_get(phrase_key, {})

        for table, stats in phrase_data.items():
            if table not in table_scores:
                table_scores[table] = 0

            # Weight by success rate and recency
            success_rate = stats['success_count'] / stats['count'] if stats['count'] > 0 else 0
            recency_weight = 1.0 if (time.time() - stats['last_used']) < 86400 else 0.5  # Less weight if >1 day old

            table_scores[table] += success_rate * recency_weight * stats['count']

    if table_scores:
        best_table = max(table_scores, key=table_scores.get)
        if table_scores[best_table] > 2.0:  # Threshold for confidence
            print(f"💰 Found pattern-based cached suggestion: {best_table} (score: {table_scores[best_table]:.2f})")
            return best_table

    return None


async def record_feedback(question: str, sql: str, table: str, feedback_type: str):
    """Record user feedback (positive or negative) for a query"""
    feedback_key = f"{FEEDBACK_PREFIX}:{get_question_hash(question)}"
    feedback_data = await safe_valkey_get(feedback_key, {
        'question': question,
        'sql': sql,
        'table': table,
        'positive_count': 0,
        'negative_count': 0,
        'last_feedback': None
    })

    if feedback_type == 'positive':
        feedback_data['positive_count'] += 1
        # Boost the table selection cache for positive feedback
        await cache_table_selection(question, table, "User confirmed this was correct", success=True)
    else:
        feedback_data['negative_count'] += 1
        # Mark the table selection as unsuccessful
        await cache_table_selection(question, table, "User indicated this was incorrect", success=False)

    feedback_data['last_feedback'] = {
        'type': feedback_type,
        'timestamp': time.time()
    }

    await safe_valkey_set(feedback_key, feedback_data, ex=FEEDBACK_CACHE_TTL)

    print(
        f"{'✅' if feedback_type == 'positive' else '❌'} Recorded {feedback_type} feedback for question: {question[:50]}...")
    print(f"   Stats: {feedback_data['positive_count']} positive, {feedback_data['negative_count']} negative")


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
            # Get detailed column information
            column_info_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name.split('.')[-1].upper()}'
            AND TABLE_SCHEMA = '{table_name.split('.')[-2].upper()}'
            AND TABLE_CATALOG = '{table_name.split('.')[-3].upper()}'
            ORDER BY ORDINAL_POSITION
            """

            column_descriptions = {}
            try:
                column_info = run_query(column_info_sql)
                if not isinstance(column_info, str) and hasattr(column_info, 'iterrows'):
                    for _, row in column_info.iterrows():
                        column_descriptions[row['COLUMN_NAME'].lower()] = {
                            'type': row['DATA_TYPE'],
                            'comment': row.get('COMMENT', '')
                        }
            except:
                pass

            # Now get sample data
            discovery_sql = f"SELECT * FROM {table_name} LIMIT 5"
            df = run_query(discovery_sql)

            if isinstance(df, str):
                print(f"❌ Schema discovery failed: {df}")
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
                    df_serializable[col] = df_serializable[col].astype(str)
                elif df_serializable[col].dtype == 'object':
                    # Check if any values are timestamps
                    try:
                        if len(df_serializable) > 0 and hasattr(df_serializable[col].iloc[0], 'isoformat'):
                            df_serializable[col] = df_serializable[col].astype(str)
                    except:
                        pass

            sample_data = df_serializable.head(3).to_dict('records') if len(df_serializable) > 0 else []

            # Identify audit columns
            audit_columns_found = []
            for col in columns:
                col_lower = col.lower()
                if any(pattern in col_lower for pattern in
                       ['updated_at', 'created_at', 'audit', 'modified', 'inserted', 'dw_']):
                    audit_columns_found.append(col)

            schema_info = {
                'table': table_name,
                'columns': columns,
                'column_descriptions': column_descriptions,
                'sample_data': sample_data,
                'row_count': len(df),
                'audit_columns': audit_columns_found,
                'discovered_at': time.time()
            }

            print(f"✅ Schema discovered successfully!")
            print(f"📊 Columns ({len(columns)}) - showing first 15:")
            for i, col in enumerate(columns):
                if i < 15:  # Show first 15 columns
                    desc = column_descriptions.get(col.lower(), {}).get('comment', '')
                    print(f"   - {col}" + (f": {desc}" if desc else ""))
                elif i == 15:
                    print(f"   ... and {len(columns) - 15} more columns")

            # Cache the schema
            cache_success = await safe_valkey_set(cache_key, schema_info, ex=SCHEMA_CACHE_TTL)
            if cache_success:
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


async def get_conversation_context(user_id: str, channel_id: str) -> dict:
    """Get recent conversation context"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = f"{CONVERSATION_CACHE_PREFIX}:{cache_key}"

    context = await safe_valkey_get(redis_key, {})

    # Check if context is still valid (10 minutes)
    if context and context.get('timestamp', 0) < time.time() - 600:
        await safe_valkey_delete(redis_key)
        return {}

    return context


async def update_conversation_context(user_id: str, channel_id: str, question: str, response: str,
                                      response_type: str = None, table_used: str = None):
    """Update conversation context for follow-up questions"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = f"{CONVERSATION_CACHE_PREFIX}:{cache_key}"

    context = {
        'last_question': question,
        'last_response': response,
        'last_response_type': response_type,
        'last_table_used': table_used,
        'timestamp': time.time()
    }

    await safe_valkey_set(redis_key, context, ex=CONVERSATION_CACHE_TTL)


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
    """Send message to assistant and get response - with better context awareness"""
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
    """Handle conversational questions - both follow-ups and standalone informational questions"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    # Get conversation context
    context = await get_conversation_context(user_id, channel_id)

    # Categorize the type of conversational question
    question_lower = user_question.lower()

    # Is this a follow-up about previous results?
    is_followup = context and context.get('last_response_type') == 'sql_results' and any(
        indicator in question_lower for indicator in [
            'this data', 'these results', 'that data', 'the source',
            'why is it', 'what does this mean', 'explain this'
        ]
    )

    # Is this a metadata/definition question?
    is_metadata_question = any(indicator in question_lower for indicator in [
        'what metric', 'which metric', 'definition of', 'what does', 'meaning of',
        'what is aht', 'what is fcr', 'how is calculated', 'methodology',
        'switched to', 'changed to', 'still using', 'currently using',
        'handle time vs', 'resolution time vs', 'difference between'
    ])

    # Is this a data discovery question?
    is_discovery_question = any(indicator in question_lower for indicator in [
        'what data', 'what tables', 'what columns', 'available data',
        'what information', 'what metrics', 'what kpis', 'data sources',
        'what can you tell me', 'what do you know'
    ])

    # Is this a capability question?
    is_capability_question = any(indicator in question_lower for indicator in [
        'what can you', 'how do you work', 'what questions', 'capabilities',
        'help me', 'what types', 'how to use', 'what commands'
    ])

    # Is this a business logic question?
    is_business_logic = any(indicator in question_lower for indicator in [
        'how do you calculate', 'what makes', 'how is determined',
        'business rules', 'logic behind', 'why do we', 'calculation method'
    ])

    # Build appropriate instructions based on question type
    if is_followup:
        print(f"💬 Handling follow-up question about previous results")
        instructions = """You are a BI assistant responding to a follow-up question about previous query results.

Use the conversation history to understand what data they're asking about.
Be helpful in explaining the data source, methodology, or clarifying the results.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names"""

    elif is_metadata_question:
        print(f"💬 Handling metadata/definition question")
        instructions = """You are a BI expert with deep knowledge of business intelligence data and metrics.

Answer this definition/metadata question using your knowledge of:
- Table schemas and column definitions from the dbt manifest
- Business metrics and KPI calculations (AHT, FCR, resolution times, etc.)
- Data warehouse structure and relationships
- Industry standard BI terminology

For AHT (Average Handling Time) questions specifically:
- Reference actual column names like HANDLE_TIME_IN_MINUTES from handle time tables
- Explain the difference between handle time vs full resolution time
- Be specific about which metric is currently used based on the data model

Be specific and definitive in your explanations. Reference actual table/column names when relevant.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic  
- Use `text` for code/table names"""

    elif is_discovery_question:
        print(f"💬 Handling data discovery question")
        instructions = """You are a BI expert helping users understand what data is available.

Use your knowledge of the dbt manifest and table schemas to explain:
- What tables and data sources are available (Zendesk tickets, Klaus QA, handle time, etc.)
- What metrics and KPIs can be calculated (ticket volume, AHT, QA scores, FCR, etc.)
- What dimensions and attributes exist (agents, teams, channels, ticket types, etc.)
- How different data elements relate to each other

Be comprehensive but organized in your response. Give practical examples.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names
- Use bullet points with •"""

    elif is_capability_question:
        print(f"💬 Handling capability question")
        instructions = """You are a BI assistant explaining your capabilities to help users get the most value.

Explain what you can help with:
- Answering questions about data definitions and metrics
- Running SQL queries to get specific data and analytics
- Explaining business logic and calculation methods
- Providing insights about ticket volume, agent performance, QA scores, etc.
- Finding relevant tables and understanding data structure

Give examples of good questions to ask. Be helpful and encouraging.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names
- Use bullet points with •"""

    elif is_business_logic:
        print(f"💬 Handling business logic question")
        instructions = """You are a BI expert explaining business logic and calculation methods.

Use your knowledge of business processes and data models to explain:
- How metrics like AHT, FCR, QA scores are calculated
- What business rules apply to ticket handling and agent performance
- Why certain logic is used in the data warehouse
- How different processes work (ticket resolution, quality assurance, etc.)

Be clear about methodology and reasoning. Reference actual calculation logic when possible.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names"""

    else:
        print(f"💬 Handling general informational question")
        instructions = """You are a knowledgeable BI assistant helping users understand data and analytics.

Use your expertise to provide helpful, accurate information about:
- Data definitions and terminology
- Available metrics and dimensions  
- How to approach data analysis
- Best practices for business intelligence

Be conversational but authoritative in your response. Act as a helpful data expert.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names"""

    # Build message with appropriate context
    message_parts = [f"User question: {user_question}"]

    # Add context only for follow-ups
    if is_followup and context:
        if context.get('last_question'):
            message_parts.append(f"\nPrevious question: {context['last_question']}")
        message_parts.append("\nNote: The user is asking a follow-up question about recent query results.")
        if context.get('last_table_used'):
            message_parts.append(f"Table used in previous query: {context['last_table_used']}")

    # Add guidance for standalone questions
    else:
        message_parts.append(f"\nThis is a standalone informational question about data/analytics.")
        if is_metadata_question:
            message_parts.append("Focus on providing clear definitions and explanations based on actual data model.")
        elif is_discovery_question:
            message_parts.append("Help them understand what data and capabilities are available.")
        elif is_capability_question:
            message_parts.append("Explain your capabilities and how to best use the BI assistant.")

    message = "\n".join(message_parts)

    response = await send_message_and_run(thread_id, message, instructions)

    # Additional safety check - convert any remaining markdown bold to Slack format
    response = response.replace("**", "*")

    return response


def analyze_question_intent(question_lower: str) -> dict:
    """Analyze the intent behind the question to generate appropriate SQL"""

    intent = {
        'type': 'unknown',
        'needs_aggregation': False,
        'time_filter': None,
        'group_by': None,
        'order_by': None,
        'limit': 100
    }

    # Pattern 1: "What are the..." - asking for list/definition
    if any(phrase in question_lower for phrase in [
        "what are the", "which are the", "list the", "show me the",
        "what kpis", "which kpis", "available kpis", "kpi list"
    ]):
        intent['type'] = 'list_or_sample'
        intent['limit'] = 10  # Show more examples

    # Pattern 2: Counting/Volume questions
    elif any(phrase in question_lower for phrase in [
        "how many", "count", "total number", "number of",
        "volume", "quantity"
    ]):
        intent['type'] = 'count'
        intent['needs_aggregation'] = True

    # Pattern 3: Average/Summary statistics
    elif any(phrase in question_lower for phrase in [
        "average", "mean", "avg", "typical", "usual",
        "median", "summary", "overview"
    ]):
        intent['type'] = 'summary_stats'
        intent['needs_aggregation'] = True

    # Pattern 4: Ranking/Top/Bottom
    elif any(phrase in question_lower for phrase in [
        "top", "bottom", "highest", "lowest", "best", "worst",
        "most", "least", "ranked"
    ]):
        intent['type'] = 'ranking'
        intent['needs_aggregation'] = True
        intent['order_by'] = 'DESC' if any(w in question_lower for w in ['top', 'highest', 'best', 'most']) else 'ASC'
        intent['limit'] = 10

    # Pattern 5: Trend/Time series
    elif any(phrase in question_lower for phrase in [
        "trend", "over time", "by date", "daily", "weekly",
        "monthly", "timeline"
    ]):
        intent['type'] = 'trend'
        intent['needs_aggregation'] = True
        intent['group_by'] = 'date'

    # Pattern 6: Comparison/Breakdown
    elif any(phrase in question_lower for phrase in [
        "by", "per", "breakdown", "compare", "versus", "vs",
        "across", "between", "group by"
    ]):
        intent['type'] = 'breakdown'
        intent['needs_aggregation'] = True
        # Try to identify what to group by
        if "agent" in question_lower:
            intent['group_by'] = 'agent'
        elif "team" in question_lower:
            intent['group_by'] = 'team'
        elif "channel" in question_lower:
            intent['group_by'] = 'channel'
        elif "group" in question_lower:
            intent['group_by'] = 'group'

    # Time filters
    if "today" in question_lower:
        intent['time_filter'] = 'today'
    elif "yesterday" in question_lower:
        intent['time_filter'] = 'yesterday'
    elif "last week" in question_lower:
        intent['time_filter'] = 'last_week'
    elif "this week" in question_lower:
        intent['time_filter'] = 'this_week'
    elif "last month" in question_lower:
        intent['time_filter'] = 'last_month'
    elif "this month" in question_lower:
        intent['time_filter'] = 'this_month'

    # Default to simple query if no pattern matched
    if intent['type'] == 'unknown':
        intent['type'] = 'simple_query'
        intent['limit'] = 100

    return intent


def build_sql_instructions(intent: dict, table: str, schema: dict, original_question: str) -> dict:
    """Build specific SQL instructions based on intent and actual data with schema validation"""

    columns = schema.get('columns', [])
    column_descriptions = schema.get('column_descriptions', {})
    audit_columns = schema.get('audit_columns', [])

    # Create column info string with descriptions (keep existing approach)
    column_info_parts = []
    for col in columns:
        desc = column_descriptions.get(col.lower(), {}).get('comment', '')
        col_type = column_descriptions.get(col.lower(), {}).get('type', '')
        if desc:
            column_info_parts.append(f"{col} ({col_type}): {desc}")
        else:
            column_info_parts.append(f"{col} ({col_type})")

    column_info = "\n".join(column_info_parts)

    # Base instructions - keep existing approach but ADD schema validation
    base_instructions = f"""You are a SQL expert representing the Business Intelligence team. Generate PERFECT Snowflake SQL.

    TABLE: {table}
    AUDIT COLUMNS: {', '.join(audit_columns) if audit_columns else 'None found'}

    🔍 COMPLETE COLUMN INVENTORY (Total: {len(columns)}):
    {column_info}

    ⚠️ SCHEMA COMPLIANCE: Use ONLY columns from the list above. If you need columns not listed, state clearly that they're missing from this table.

    COMMON MISTAKES TO AVOID:
    - Do NOT assume column names like TEAM_NAME, QA_SCORE, HANDLE_TIME unless they appear above
    - Do NOT use business logic to guess column names  
    - Do NOT use columns from other tables or systems

    CRITICAL REQUIREMENTS:
    1. Generate SQL that COMPLETELY and ACCURATELY answers the question
    2. Use ONLY columns that exist in the schema above
    3. Verify each column reference against the schema
    4. Use appropriate aggregations and calculations
    5. Apply correct filters and date ranges
    6. Handle NULLs appropriately
    7. Use the full table name: {table}

    ACCURACY RULES:
    - If calculating averages, ensure you're averaging the right values
    - If counting, ensure you're counting distinct values when appropriate  
    - If filtering by time, use the most appropriate date/timestamp column
    - If grouping, ensure all non-aggregated columns are in GROUP BY
    - Always consider data quality (NULL handling, data types)

    Return ONLY the SQL query - no explanations."""

    # Keep existing intent-specific guidance (unchanged)
    if intent['type'] == 'list_or_sample':
        specific_instructions = f"""
Generate SQL to show what data is available in the table.
- Select ALL relevant columns that answer the "what are" question
- Show actual data rows, not aggregations
- Use appropriate ORDER BY (prefer audit columns: {', '.join(audit_columns[-3:])})
- LIMIT {intent.get('limit', 10)}"""

    elif intent['type'] == 'count':
        specific_instructions = f"""
Generate SQL to count records accurately.
- Use COUNT(*) for total records or COUNT(DISTINCT column) for unique values
- Apply all necessary filters from the question
- Consider if you need total count or distinct count
- Return a single number with descriptive alias"""

    elif intent['type'] == 'summary_stats':
        specific_instructions = f"""
Generate SQL for accurate summary statistics.
- Calculate the exact aggregates requested (AVG, SUM, MIN, MAX, etc.)
- Handle NULLs appropriately in calculations
- Use meaningful aliases that describe what's being calculated
- Round numbers to appropriate precision
- Include COUNT(*) to show sample size if relevant"""

    elif intent['type'] == 'ranking':
        specific_instructions = f"""
Generate SQL to rank/order data correctly.
- Identify the correct grouping dimension
- Calculate the exact metric to rank by
- Use appropriate aggregation for the metric
- Order {intent.get('order_by', 'DESC')}
- Include all relevant columns in output
- LIMIT {intent.get('limit', 10)}"""

    elif intent['type'] == 'breakdown':
        specific_instructions = f"""
Generate SQL to break down data accurately by categories.
- GROUP BY the exact dimension(s) requested
- Calculate all requested aggregates
- Include count per group
- Order by the most relevant column
- Ensure all non-aggregated columns are in GROUP BY"""

    elif intent['type'] == 'trend':
        specific_instructions = f"""
Generate SQL for time-based trend analysis.
- Use appropriate date truncation (DATE_TRUNC)
- Group by the time period requested
- Calculate metrics for each period
- Order by date/time ascending
- Include all necessary date filters"""

    else:
        specific_instructions = f"""
Generate SQL that completely answers the question.
- Select all necessary columns
- Apply all required filters
- Use appropriate joins if needed
- LIMIT {intent.get('limit', 100)} unless aggregating"""

    # Keep existing time filter guidance (unchanged)
    if intent.get('time_filter'):
        # Find the best date/time column
        date_columns = []
        for col in columns:
            col_lower = col.lower()
            col_type = column_descriptions.get(col_lower, {}).get('type', '').lower()
            # Prefer audit columns for time filtering
            if col in audit_columns and ('date' in col_type or 'timestamp' in col_type):
                date_columns.insert(0, col)  # Add audit columns at the beginning
            elif any(pattern in col_lower for pattern in ['date', 'time', 'created', 'updated', '_at']):
                if 'date' in col_type or 'timestamp' in col_type:
                    date_columns.append(col)

        if date_columns:
            date_col = date_columns[0]  # Use first (best) date column found
            time_instructions = f"\n\nTIME FILTER: Use column '{date_col}' for '{intent['time_filter']}' filter"

            if intent['time_filter'] == 'today':
                time_instructions += f"\nUse: WHERE DATE({date_col}) = CURRENT_DATE()"
            elif intent['time_filter'] == 'yesterday':
                time_instructions += f"\nUse: WHERE DATE({date_col}) = DATEADD(day, -1, CURRENT_DATE())"
            elif intent['time_filter'] == 'last_week':
                time_instructions += f"\nUse: WHERE {date_col} >= DATEADD(week, -1, CURRENT_DATE())"
            elif intent['time_filter'] == 'this_week':
                time_instructions += f"\nUse: WHERE WEEK({date_col}) = WEEK(CURRENT_DATE()) AND YEAR({date_col}) = YEAR(CURRENT_DATE())"
            elif intent['time_filter'] == 'last_month':
                time_instructions += f"\nUse: WHERE {date_col} >= DATEADD(month, -1, CURRENT_DATE())"
            elif intent['time_filter'] == 'this_month':
                time_instructions += f"\nUse: WHERE MONTH({date_col}) = MONTH(CURRENT_DATE()) AND YEAR({date_col}) = YEAR(CURRENT_DATE())"
        else:
            time_instructions = "\n\nWARNING: No suitable date column found for time filtering"
    else:
        time_instructions = ""

    full_instructions = base_instructions + "\n\n" + specific_instructions + time_instructions

    # Build the message (keep existing approach)
    message = f"""Question: {original_question}

Table: {table}
Question Type: {intent['type']}

Generate ACCURATE SQL that completely answers this question using the schema provided."""

    return {
        'instructions': full_instructions,
        'message': message
    }


def validate_and_fix_sql(sql: str, question: str, table: str, columns: List[str]) -> str:
    """Validate generated SQL and fix common issues"""

    sql_upper = sql.upper()
    question_lower = question.lower()

    # Fix 1: Check for non-existent columns
    for col in columns:
        # This is a simple check - could be enhanced with proper SQL parsing
        pass

    # Fix 2: Ensure proper aggregation
    if "GROUP BY" in sql_upper and not any(agg in sql_upper for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]):
        print("⚠️ GROUP BY without aggregation detected - might need fixing")

    # Fix 3: Add LIMIT for non-aggregation queries
    if not any(agg in sql_upper for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN(", "GROUP BY"]):
        if "LIMIT" not in sql_upper:
            print("⚠️ Adding LIMIT to prevent large result sets")
            sql = sql.rstrip(';').rstrip() + "\nLIMIT 100;"

    # Fix 4: Ensure semicolon at end
    if not sql.rstrip().endswith(';'):
        sql = sql.rstrip() + ';'

    return sql


# PATTERN-BASED ENHANCEMENTS START HERE
async def generate_sql_with_patterns(user_question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
    """
    Enhanced SQL generation that uses patterns for context but lets OpenAI generate the SQL
    Returns: (sql_query, selected_table)
    """
    print(f"\n🤖 Starting pattern-enhanced SQL generation for: {user_question}")

    selected_table = None
    pattern_context = None

    # Step 1: Try to find a matching pattern for context
    if PATTERNS_AVAILABLE:
        matched_pattern = pattern_matcher.match_pattern(user_question)

        if matched_pattern:
            print(f"✅ Found pattern context: {matched_pattern['name']}")
            selected_table = matched_pattern['table']
            pattern_context = matched_pattern

            # Cache this pattern match for table selection
            await cache_table_selection(user_question, selected_table, f"Pattern match: {matched_pattern['name']}")
        else:
            print("🔍 No pattern match, using standard table discovery...")

    # Step 2: If no pattern match, use intelligent table discovery
    if not selected_table:
        selected_table, selection_reason = await enhanced_table_selection(user_question, user_id, channel_id)

        if not selected_table:
            return "-- Error: Could not determine appropriate table", None

        # Check if selected table has a pattern for context
        if PATTERNS_AVAILABLE and not pattern_context:
            pattern_context = pattern_matcher.get_pattern_by_table(selected_table)

    print(f"📊 Selected table: {selected_table}")

    # Step 3: Discover schema
    schema = await discover_table_schema(selected_table)

    if schema.get('error'):
        print(f"⚠️ Schema discovery failed: {schema.get('error')}")
        schema = {
            'table': selected_table,
            'columns': [],
            'error': schema.get('error')
        }

    # Step 4: Store the fact that we're generating SQL for context
    await update_conversation_context(user_id, channel_id, user_question, "Generating SQL query...", 'sql_generation',
                                      selected_table)

    # Step 5: Analyze question intent
    question_intent = analyze_question_intent(user_question.lower())
    print(f"🎯 Question intent: {question_intent['type']}")

    # Step 6: Generate SQL with OpenAI, providing pattern context
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "-- Error: Could not create conversation thread", selected_table

    # Build enhanced instructions with pattern context
    instructions = build_pattern_enhanced_instructions(
        user_question,
        selected_table,
        schema,
        pattern_context,
        question_intent
    )

    # Build message with pattern context
    message = build_pattern_enhanced_message_for_ai(
        user_question,
        selected_table,
        schema,
        pattern_context,
        question_intent
    )

    response = await send_message_and_run(thread_id, message, instructions)

    # Extract SQL from response
    sql = extract_sql_from_response(response)

    # Validate and fix common SQL issues
    # sql = validate_and_fix_sql(sql, user_question, selected_table, schema.get('columns', []))

    print(f"\n🧠 Generated SQL:")
    print(f"{sql}")
    print(f"{'=' * 60}\n")

    return sql, selected_table

async def regenerate_sql_with_error_context(question: str, failed_sql: str, error_message: str,
                                            table_name: str, problematic_column: str,
                                            user_id: str, channel_id: str) -> str:
    """Regenerate SQL using the actual error message as context"""

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return ""

    # Get fresh schema info
    schema = await discover_table_schema(table_name)
    available_columns = schema.get('columns', [])
    column_descriptions = schema.get('column_descriptions', {})

    # Build column info with descriptions
    column_info_parts = []
    for col in available_columns:
        desc = column_descriptions.get(col.lower(), {}).get('comment', '')
        if desc:
            column_info_parts.append(f"• {col}: {desc}")
        else:
            column_info_parts.append(f"• {col}")

    column_info = "\n".join(column_info_parts)

    instructions = f"""You are a SQL expert fixing a failed query based on the actual database error.

The SQL failed with this Snowflake error: {error_message}

TASK: Fix the SQL to work with the actual table schema while preserving the original intent.

TABLE: {table_name}
AVAILABLE COLUMNS:
{column_info}

FIXING STRATEGY:
1. Identify why the SQL failed (usually missing columns)
2. Find similar/equivalent columns in the available list
3. Replace problematic columns with correct ones
4. Keep the same business logic and query intent
5. Use exact column names as shown above

COMMON FIXES:
- If "TEAM_NAME" missing → try "GROUP_NAME" or similar
- If "QA_SCORE" missing → try "QA_SCORE" variations or performance metrics
- If "HANDLE_TIME" missing → try time-related columns
- Match column purpose, not just name

Return ONLY the corrected SQL query - no explanations."""

    # Build context message
    message_parts = [
        f"Original question: {question}",
        "",
        "Failed SQL:",
        f"```sql\n{failed_sql}\n```",
        "",
        f"Snowflake Error: {error_message}"
    ]

    if problematic_column:
        message_parts.extend([
            "",
            f"Specific problematic column: {problematic_column}"
        ])

    message_parts.extend([
        "",
        "Fix this SQL to use only the available columns listed above.",
        "Find the best matching columns for the intended analysis."
    ])

    message = "\n".join(message_parts)

    try:
        response = await send_message_and_run(thread_id, message, instructions)
        fixed_sql = extract_sql_from_response(response)

        if fixed_sql and not fixed_sql.startswith("--") and not fixed_sql.startswith("❌"):
            print(f"🔧 Regenerated SQL successfully")
            return fixed_sql
        else:
            print(f"⚠️ Could not extract valid fixed SQL from response: {response[:200]}...")
            return ""

    except Exception as e:
        print(f"❌ Error regenerating SQL: {e}")
        traceback.print_exc()
        return ""

def build_pattern_enhanced_instructions(question: str, table: str, schema: dict, pattern: Dict = None,
                                        intent: dict = None) -> str:
    """Build instructions for OpenAI that include pattern context"""

    columns = schema.get('columns', [])
    column_descriptions = schema.get('column_descriptions', {})
    audit_columns = schema.get('audit_columns', [])

    # Create column info string
    column_info_parts = []
    for col in columns[:50]:
        desc = column_descriptions.get(col.lower(), {}).get('comment', '')
        col_type = column_descriptions.get(col.lower(), {}).get('type', '')
        if desc:
            column_info_parts.append(f"{col} ({col_type}): {desc}")
        else:
            column_info_parts.append(f"{col} ({col_type})")

    column_info = "\n".join(column_info_parts)

    # Base instructions
    instructions = f"""You are a SQL expert representing the Business Intelligence team. Generate PERFECT Snowflake SQL.

TABLE: {table}
AUDIT COLUMNS: {', '.join(audit_columns) if audit_columns else 'None found'}

AVAILABLE COLUMNS WITH TYPES AND DESCRIPTIONS:
{column_info}

"""

    # Add pattern-specific context if available
    if pattern:
        instructions += f"""
IMPORTANT PATTERN INFORMATION:
This table follows the "{pattern['name']}" pattern with specific requirements:

STANDARD FILTERS THAT MUST BE APPLIED:
{pattern.get('standard_filters', 'None')}

KEY COLUMNS FOR THIS PATTERN:
- Identifiers: {', '.join(pattern['key_columns']['identifiers'])}
- Dimensions: {', '.join(pattern['key_columns']['dimensions'])}
- Metrics: {', '.join(pattern['key_columns']['metrics'])}

BUSINESS RULES:
{chr(10).join('- ' + rule for rule in pattern.get('business_rules', []))}

"""

        # Add pattern-specific guidance
        if pattern['id'] == 'wops_tickets':
            instructions += """
SPECIFIC REQUIREMENTS FOR WOPS TICKETS:
1. ALWAYS apply ALL the standard filters shown above - every single one is required
2. For contact channel analysis, use this EXACT derived field:
   CASE 
     WHEN GROUP_ID = '5495272772503' THEN 'Web'
     WHEN GROUP_ID = '17837476387479' THEN 'Chat'
     WHEN GROUP_ID = '28949203098007' THEN 'Voice'
     ELSE 'Other'
   END AS Contact_Channel
3. Use _PST timestamp columns when available for time-based queries
4. For ticket sub-types, consider mapping WOPS_TICKET_TYPE_A to appropriate category columns

"""
        elif pattern['id'] == 'klaus_qa':
            instructions += """
SPECIFIC REQUIREMENTS FOR KLAUS QA:
1. Use complex score calculations with these exact pass thresholds:
   - Scorecard 53921 (QA v2): Pass threshold = 85%
   - Scorecard 54262 (ATA v2): Pass threshold = 85%
   - Scorecard 59144 (ATA): Pass threshold = 90%
   - Scorecard 60047: Always results in 'Fail'
2. Overall Score calculation must consider No_Auto_Fail and rating weights
3. Clear Communication Score: Average of 6 sub-components (IDs: 319808-319813)
4. Handling Score: 75% weight on Handling_1 + 25% weight on Handling_2

"""
        elif pattern['id'] == 'handle_time':
            instructions += """
SPECIFIC REQUIREMENTS FOR HANDLE TIME:
1. Handle time is pre-calculated - use HANDLE_TIME_IN_MINUTES or HANDLE_TIME_IN_SECONDS directly
2. For voice channel analysis, use these AMAZON_CONNECT columns:
   - AMAZON_CONNECT_CALL_DURATION_IN_MINUTES
   - AMAZON_CONNECT_HOLD_TIME_IN_MINUTES
   - AMAZON_CONNECT_TALK_TIME_IN_MINUTES
3. Consider filtering outliers (e.g., WHERE HANDLE_TIME_IN_MINUTES < 120)

"""
        elif pattern['id'] == 'fcr':
            instructions += """
SPECIFIC REQUIREMENTS FOR FCR:
1. Use window functions with LEAD to analyze next ticket:
   CASE
     WHEN created_at_pst + INTERVAL '24 HOUR' >= 
          LEAD(created_at_pst) OVER (PARTITION BY requester_id ORDER BY created_at_pst) 
     THEN 0 ELSE 1
   END AS is_resolved_first_time
2. Apply the standard filters shown above
3. Include channel analysis:
   CASE
     WHEN group_id = '17837476387479' THEN 'Chat'
     WHEN group_id = '28949203098007' THEN 'Voice'
     ELSE 'Other'
   END AS contact_channel

"""

    # Add intent-specific guidance
    if intent:
        instructions += f"\nQUESTION INTENT ANALYSIS:\n"
        instructions += f"- Question Type: {intent['type']}\n"
        instructions += f"- Time Filter: {intent.get('time_filter', 'None')}\n"

        if intent.get('time_filter'):
            # Find best date column for filtering
            date_columns = []
            for col in columns:
                col_lower = col.lower()
                col_type = column_descriptions.get(col_lower, {}).get('type', '').lower()
                if col in audit_columns and ('date' in col_type or 'timestamp' in col_type):
                    date_columns.insert(0, col)
                elif any(pattern in col_lower for pattern in ['date', 'time', 'created', 'updated', '_at']):
                    if 'date' in col_type or 'timestamp' in col_type:
                        date_columns.append(col)

            if date_columns:
                date_col = date_columns[0]
                instructions += f"\nFor time filtering, use column: {date_col}\n"

    instructions += """
CRITICAL REQUIREMENTS:
1. Generate SQL that COMPLETELY and ACCURATELY answers the question
2. Use ONLY columns that exist in the schema above
3. Apply ALL required filters and business logic from the pattern
4. Handle NULLs appropriately
5. Use the full table name
6. Ensure proper GROUP BY for any aggregations
7. Apply appropriate LIMIT for non-aggregated queries

Return ONLY the SQL query - no explanations."""

    return instructions


def build_pattern_enhanced_message_for_ai(question: str, table: str, schema: dict, pattern: Dict = None,
                                          intent: dict = None) -> str:
    """Build message for OpenAI with pattern context"""

    message = f"""Question: {question}

Table: {table}
"""

    if pattern:
        message += f"""
Pattern Matched: {pattern['name']}

This pattern is specifically designed to answer questions about:
{chr(10).join('- ' + q for q in pattern['questions'][:5])}

The pattern has documented requirements including standard filters and business logic that MUST be applied to ensure accurate results.
"""

    # Add intent analysis
    if intent:
        message += f"""
Question Analysis:
- Type: {intent['type']}
- Needs Aggregation: {intent.get('needs_aggregation', False)}
- Time Filter Required: {intent.get('time_filter', 'None')}
- Suggested Grouping: {intent.get('group_by', 'None')}
"""

    message += """
Generate SQL that:
1. Completely answers the user's question
2. Applies ALL required filters and business logic from the pattern
3. Uses appropriate aggregations based on the question type
4. Returns data in the most useful format for the question asked
5. Follows all pattern-specific requirements provided in the instructions
"""

    # Add sample data note if helpful
    if schema.get('sample_data'):
        message += "\n\nNote: Table schema includes sample data to help understand data formats and values."

    return message


async def enhanced_table_selection(question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
    """
    Enhanced table selection that considers patterns first
    Returns: (selected_table, reason)
    """
    # Check if question matches a pattern
    if PATTERNS_AVAILABLE:
        matched_pattern = pattern_matcher.match_pattern(question)

        if matched_pattern:
            print(f"✅ Pattern match found: {matched_pattern['name']}")
            return matched_pattern['table'], f"Matched documented pattern: {matched_pattern['name']}"

    # Check cached suggestions
    cached_table = await get_cached_table_suggestion(question)
    if cached_table:
        # Verify if this table has a pattern
        if PATTERNS_AVAILABLE:
            pattern = pattern_matcher.get_pattern_by_table(cached_table)
            if pattern:
                return cached_table, f"Cached selection with pattern: {pattern['name']}"
            else:
                return cached_table, "Cached table selection"
        else:
            return cached_table, "Cached table selection"

    # Fall back to vector search
    print("🔍 Using vector search for table discovery...")
    candidate_tables = await find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k=8)

    if not candidate_tables:
        return "", "No relevant tables found"

    # Prioritize tables that have documented patterns
    if PATTERNS_AVAILABLE:
        pattern_tables = []
        other_tables = []

        for table in candidate_tables:
            if pattern_matcher.get_pattern_by_table(table):
                pattern_tables.append(table)
            else:
                other_tables.append(table)

        # Combine with pattern tables first
        prioritized_tables = pattern_tables + other_tables
    else:
        prioritized_tables = candidate_tables

    # Use sampling for final selection
    return await select_best_table_using_samples(question, prioritized_tables[:5], user_id, channel_id)


async def generate_pattern_aware_instructions(question: str, table: str, schema: dict) -> str:
    """Generate SQL instructions that incorporate pattern knowledge"""

    # Check if this table has a documented pattern
    if PATTERNS_AVAILABLE:
        pattern = pattern_matcher.get_pattern_by_table(table)

        if pattern:
            instructions = f"""You are a SQL expert. Generate PERFECT Snowflake SQL using the documented pattern.

PATTERN: {pattern['name']}
TABLE: {table}

DOCUMENTED COLUMNS:
{json.dumps(pattern['key_columns'], indent=2)}

STANDARD FILTERS TO APPLY:
{pattern.get('standard_filters', 'None')}

AVAILABLE DERIVED FIELDS:
{json.dumps(pattern.get('derived_fields', {}), indent=2)}

PATTERN-SPECIFIC RULES:
"""

            if pattern['id'] == 'wops_tickets':
                instructions += """
- Always apply the standard filters for ticket queries
- Use Contact_Channel derived field for channel analysis
- Group by appropriate dimensions based on the question
- Use _PST timestamp columns for time-based queries if not available convert the column to PST and provide results
"""
            elif pattern['id'] == 'klaus_qa':
                instructions += """
- Use the complex CTE structure for score calculations
- Apply correct pass/fail thresholds by scorecard
- Include component scores when relevant
- Remember different scorecards have different thresholds
"""
            elif pattern['id'] == 'handle_time':
                instructions += """
- Use pre-calculated metrics (no need to calculate handle time)
- For voice channel, use AMAZON_CONNECT_* columns
- Filter outliers if needed (e.g., HANDLE_TIME_IN_MINUTES < 120)
"""
            elif pattern['id'] == 'fcr':
                instructions += """
- Use window functions with LEAD for next ticket analysis
- Apply 24-hour window for FCR calculation
- Include channel switching analysis when relevant
"""

            instructions += f"\n\nQuestion: {question}\n\nGenerate SQL that EXACTLY answers this question."

            return instructions

    # Fall back to standard instructions
    intent = analyze_question_intent(question.lower())
    result = build_sql_instructions(intent, table, schema, question)
    return result['instructions']


async def build_pattern_enhanced_message(question: str, table: str, pattern: Dict = None) -> str:
    """Build assistant message with pattern context"""

    if pattern:
        message = f"""Question: {question}

Using documented pattern: {pattern['name']}
Table: {table}

This pattern has been proven to answer questions about:
{chr(10).join('- ' + q for q in pattern['questions'][:5])}

Key columns available:
- Identifiers: {', '.join(pattern['key_columns']['identifiers'][:3])}
- Dimensions: {', '.join(pattern['key_columns']['dimensions'][:3])}
- Metrics: {', '.join(pattern['key_columns']['metrics'][:3])}

Generate SQL that follows the documented pattern exactly."""
    else:
        message = f"""Question: {question}
Table: {table}

Generate accurate SQL to answer this question."""

    return message


async def validate_pattern_sql(sql: str, pattern: Dict) -> Tuple[bool, str]:
    """Validate that generated SQL follows pattern requirements"""

    issues = []

    # Check for required filters
    if pattern.get('standard_filters'):
        # Extract WHERE clause from SQL
        sql_upper = sql.upper()
        if 'WHERE' not in sql_upper:
            issues.append("Missing required WHERE clause with standard filters")
        else:
            # Check for key filter components
            required_filters = ['STATUS', 'CHANNEL', 'BRAND_ID']
            where_clause = sql_upper.split('WHERE')[1].split('GROUP BY')[0]

            for filter_col in required_filters:
                if filter_col not in where_clause:
                    issues.append(f"Missing required filter on {filter_col}")

    # Check for pattern-specific requirements
    if pattern['id'] == 'klaus_qa' and 'CTE' not in sql.upper() and 'WITH' not in sql.upper():
        issues.append("Klaus queries should use CTE structure for score calculations")

    if pattern['id'] == 'fcr' and 'LEAD(' not in sql.upper():
        issues.append("FCR queries require LEAD window function")

    is_valid = len(issues) == 0
    return is_valid, "; ".join(issues) if issues else "SQL follows pattern correctly"


async def debug_pattern_analysis(question: str, user_id: str, channel_id: str) -> str:
    """Debug pattern matching for a question"""

    result = f"🔍 Pattern Analysis for: '{question}'\n\n"

    if not PATTERNS_AVAILABLE:
        result += "❌ Pattern matcher not available"
        return result

    # Check pattern match
    matched_pattern = pattern_matcher.match_pattern(question)

    if matched_pattern:
        result += f"✅ **Matched Pattern**: {matched_pattern['name']}\n"
        result += f"📊 **Table**: {matched_pattern['table']}\n"
        result += f"🎯 **Pattern ID**: {matched_pattern['id']}\n\n"

        result += "**Pattern Configuration**:\n"
        result += f"- Keywords matched: {[k for k in matched_pattern['keywords'] if k in question.lower()]}\n"
        result += f"- Question patterns matched: {[q for q in matched_pattern['questions'] if q in question.lower()]}\n"

        if matched_pattern.get('standard_filters'):
            result += f"\n**Standard Filters**:\n```sql\n{matched_pattern['standard_filters']}\n```\n"

        if matched_pattern.get('business_rules'):
            result += f"\n**Business Rules**:\n"
            for rule in matched_pattern['business_rules']:
                result += f"- {rule}\n"

        result += f"\n**How this pattern will be used**:\n"
        result += f"1. Table '{matched_pattern['table']}' will be selected\n"
        result += f"2. Pattern context will be provided to OpenAI\n"
        result += f"3. OpenAI will generate SQL following the pattern requirements\n"
        result += f"4. All standard filters and business rules will be applied\n"

    else:
        result += "❌ **No pattern match found**\n\n"

        # Show pattern scores
        result += "**Pattern Scores**:\n"
        for pattern in pattern_matcher.patterns:
            score = 0
            for q in pattern["questions"]:
                if q in question.lower():
                    score += 10
            for k in pattern["keywords"]:
                if k in question.lower():
                    score += 2

            result += f"- {pattern['name']}: {score} points\n"

        result += f"\n**What happens next**:\n"
        result += f"1. Standard table discovery will be used\n"
        result += f"2. Vector search will find relevant tables\n"
        result += f"3. Table sampling will select the best option\n"
        result += f"4. SQL will be generated without pattern context\n"

    return result


def validate_sql_columns(sql: str, available_columns: list) -> tuple[bool, list]:
    """Validate that SQL only uses columns that actually exist"""

    if not sql or sql.startswith("--") or not available_columns:
        return True, []

    import re

    # Create a set of actual column names (case-insensitive)
    available_cols_lower = {col.lower() for col in available_columns}

    # Extract all potential column references from SQL
    # Remove string literals and quoted strings first
    sql_clean = re.sub(r'\'[^\']*\'', '', sql)  # Remove string literals
    sql_clean = re.sub(r'"[^"]*"', '', sql_clean)  # Remove quoted strings

    # Find words that look like column names (alphanumeric + underscore)
    potential_columns = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql_clean)

    # Filter out SQL keywords and functions
    sql_keywords = {
        'select', 'from', 'where', 'group', 'by', 'order', 'having', 'as', 'and', 'or', 'not',
        'in', 'is', 'null', 'count', 'sum', 'avg', 'max', 'min', 'distinct', 'case', 'when',
        'then', 'else', 'end', 'inner', 'left', 'right', 'outer', 'join', 'on', 'union',
        'limit', 'offset', 'desc', 'asc', 'analytics', 'dbt_production', 'current_date',
        'date_trunc', 'extract', 'year', 'month', 'week', 'day', 'hour', 'minute', 'dateadd',
        'datediff', 'to_date', 'cast', 'varchar', 'number', 'boolean', 'timestamp'
    }

    missing_columns = []

    for col in potential_columns:
        col_lower = col.lower()
        # Skip SQL keywords, table names, functions, and schema references
        if (col_lower not in sql_keywords and
                'analytics' not in col_lower and
                'dbt' not in col_lower and
                not col_lower.startswith('fct_') and
                not col_lower.startswith('dim_') and
                not col_lower.startswith('stg_') and
                len(col) > 2):  # Skip very short identifiers

            # Check if this looks like a column name and doesn't exist
            if col_lower not in available_cols_lower:
                missing_columns.append(col)

    # Remove duplicates
    missing_columns = list(set(missing_columns))

    return len(missing_columns) == 0, missing_columns

async def generate_sql_intelligently(user_question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
    """
    Generate SQL using helper SQL patterns as foundation for OpenAI
    Returns: (sql_query, selected_table)
    """
    print(f"\n🤖 Starting pattern-enhanced SQL generation for: {user_question}")

    # Store the fact that we're generating SQL for context
    await update_conversation_context(user_id, channel_id, user_question, "Generating SQL query...", 'sql_generation')

    selected_table = None
    helper_sql = None
    pattern_context = None

    try:
        # Step 1: Try pattern matching first
        pattern_matcher = PatternMatcher()
        matched_pattern = pattern_matcher.match_pattern(user_question)

        if matched_pattern:
            print(f"✅ Pattern matched: {matched_pattern['name']}")
            print(f"📊 Using table: {matched_pattern['table']}")

            # Generate helper SQL using the pattern
            query_helper = PatternBasedQueryHelper()
            question_intent = analyze_question_intent(user_question.lower())

            helper_sql = query_helper.build_helper_sql(user_question, matched_pattern, question_intent)
            selected_table = matched_pattern['table']
            pattern_context = matched_pattern

            # Cache this successful pattern match
            await cache_table_selection(user_question, selected_table, f"Pattern matched: {matched_pattern['name']}")

            print(f"🔧 Generated helper SQL for OpenAI guidance")
            print(f"Helper SQL:\n{helper_sql}")

        else:
            print(f"🔍 No pattern match, using standard table discovery...")

            # Step 2: Fall back to existing intelligent table discovery
            cached_table = await get_cached_table_suggestion(user_question)

            if cached_table:
                print(f"📋 Using cached table suggestion: {cached_table}")
                selected_table = cached_table
            else:
                # Step 3: Use vector search to find relevant tables
                print("🔍 Searching dbt manifest for relevant tables...")
                candidate_tables = await find_relevant_tables_from_vector_store(user_question, user_id, channel_id,
                                                                                top_k=8)

                if not candidate_tables:
                    print("⚠️ No candidate tables found from vector search")
                    return "-- Error: Could not find relevant tables for this question. Please ensure your question mentions specific metrics or entities.", None

                print(f"📊 Found {len(candidate_tables)} candidate tables")

                # Step 4: Sample data from candidate tables and select best one
                selected_table, selection_reason = await select_best_table_using_samples(
                    user_question, candidate_tables, user_id, channel_id
                )

                if not selected_table:
                    return "-- Error: Could not determine appropriate table", None

                # Cache this selection for future use
                await cache_table_selection(user_question, selected_table, selection_reason)

        # Step 5: Discover schema for the selected table
        schema = await discover_table_schema(selected_table)

        if schema.get('error'):
            print(f"⚠️ Schema discovery failed: {schema.get('error')}")
            schema = {
                'table': selected_table,
                'columns': [],
                'error': schema.get('error')
            }

    except Exception as e:
        print(f"⚠️ Error during intelligent table selection: {e}")
        traceback.print_exc()
        return f"-- Error: {str(e)}", None

    # Step 6: Generate final SQL with OpenAI using helper SQL as foundation
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "-- Error: Could not create conversation thread", selected_table

    if helper_sql and pattern_context:
        # Use helper SQL + pattern context for guidance
        sql_instructions = build_helper_sql_guided_instructions(
            helper_sql,
            pattern_context,
            selected_table,
            schema,
            user_question
        )
        print(f"🎯 Using helper SQL guided generation")
    else:
        # Use standard instructions with business logic
        question_intent = analyze_question_intent(user_question.lower())
        sql_instructions = build_sql_instructions_with_business_logic(
            question_intent,
            selected_table,
            schema,
            user_question
        )
        print(f"🎯 Using standard SQL generation with business logic")

    response = await send_message_and_run(thread_id, sql_instructions['message'], sql_instructions['instructions'])

    # Extract SQL from response
    sql = extract_sql_from_response(response)

    # ENHANCED schema validation
    is_valid, missing_columns = validate_sql_columns(sql, schema.get('columns', []))

    if not is_valid and missing_columns:
        print(f"❌ Schema validation failed - missing columns: {missing_columns}")

        # Create detailed error message
        error_msg = f"❌ **Schema Validation Failed**\n\n"
        error_msg += f"The generated SQL uses columns that don't exist in `{selected_table}`:\n"
        for col in missing_columns:
            error_msg += f"• `{col}` ❌\n"

        error_msg += f"\n**Available columns in {selected_table}:**\n"
        available_cols = schema.get('columns', [])
        for i, col in enumerate(available_cols[:15]):
            error_msg += f"• `{col}` ✅\n"
        if len(available_cols) > 15:
            error_msg += f"• ... and {len(available_cols) - 15} more columns\n"

        error_msg += f"\n**What this means:**\n"
        error_msg += f"• This table doesn't have the columns needed for your analysis\n"
        error_msg += f"• You might need a different table with the required metrics\n\n"
        error_msg += f"**Suggestions:**\n"
        error_msg += f"• `@bot debug find {user_question}` - Find tables with the right data\n"
        error_msg += f"• `@bot debug sample {selected_table}` - See what's available in this table\n"
        error_msg += f"• Rephrase your question using available column names"

        return error_msg, selected_table

    # NEW: Enhanced validation
    if sql.startswith("❌"):
        # This is a validation error message, return it directly
        return sql, selected_table

    # Validate and fix common SQL issues
    sql = validate_and_fix_sql(sql, user_question, selected_table, schema.get('columns', []))

    print(f"\n🧠 Generated SQL:")
    print(f"{sql}")
    print(f"{'=' * 60}\n")

    return sql, selected_table

def build_sql_instructions_with_business_logic(intent: Dict, table: str, schema: Dict, original_question: str) -> Dict:
    """Enhanced SQL instructions that include business logic for non-pattern queries"""

    # Get the base instructions first
    base_result = build_sql_instructions(intent, table, schema, original_question)

    # Enhance with business logic for performance queries
    if any(term in original_question.lower() for term in ['performance', 'best', 'top', 'ranking', 'agent']):
        base_result['instructions'] += """

🎯 ENHANCED BUSINESS LOGIC FOR PERFORMANCE QUERIES:

CRITICAL FILTERING (ALWAYS APPLY):
- ALWAYS filter out NULL/empty agent names: WHERE ASSIGNEE_NAME IS NOT NULL AND ASSIGNEE_NAME != ''
- Filter out system accounts: WHERE ASSIGNEE_NAME NOT IN ('None', 'null', 'Automated Update', 'TechOps Bot')
- Filter out empty/null strings: WHERE TRIM(ASSIGNEE_NAME) != '' AND LOWER(ASSIGNEE_NAME) NOT IN ('null', 'none')

SMART ORDERING FOR PERFORMANCE QUERIES:
- For "best" or "top" queries, order by data completeness first, then performance metrics
- Prioritize agents with complete performance data over those with partial data
- Use CASE statements to handle NULL values appropriately

EXAMPLE FILTERS TO ALWAYS INCLUDE:
```sql
WHERE ASSIGNEE_NAME IS NOT NULL 
  AND ASSIGNEE_NAME != '' 
  AND ASSIGNEE_NAME != 'None'
  AND LOWER(ASSIGNEE_NAME) NOT IN ('null', 'none', 'nan')
  AND TRIM(ASSIGNEE_NAME) != ''
```

SMART ORDERING EXAMPLE:
```sql
ORDER BY 
  CASE WHEN QA_SCORE IS NOT NULL AND FCR_PERCENTAGE IS NOT NULL THEN 0 ELSE 1 END,
  QA_SCORE DESC,
  FCR_PERCENTAGE DESC
```

PERFORMANCE BENCHMARKS FOR CONTEXT:
- Excellent QA: 90+ score
- Good FCR: 80%+ resolution rate  
- Efficient AHT: <10 minutes for most ticket types
- Active Volume: 20+ tickets per week"""

    # Add specific logic for ticket queries
    elif any(term in original_question.lower() for term in ['ticket', 'volume', 'created']):
        base_result['instructions'] += """

🎯 ENHANCED BUSINESS LOGIC FOR TICKET QUERIES:

STANDARD TICKET FILTERS (ALWAYS APPLY):
- STATUS IN ('closed', 'solved')
- Valid channels: CHANNEL IN ('api', 'email', 'native_messaging', 'web')
- Valid brands: BRAND_ID IN ('29186504989207', '360002340693')
- Exclude system accounts: ASSIGNEE_NAME NOT IN ('Automated Update', 'TechOps Bot') OR ASSIGNEE_NAME IS NULL
- Exclude test data: NOT LOWER(TICKET_TAGS) LIKE '%email_blocked%' OR TICKET_TAGS IS NULL

CONTACT CHANNEL MAPPING:
```sql
CASE 
  WHEN GROUP_ID = '5495272772503' THEN 'Web'
  WHEN GROUP_ID = '17837476387479' THEN 'Chat'
  WHEN GROUP_ID = '28949203098007' THEN 'Voice'
  ELSE 'Other'
END AS Contact_Channel
```"""

    # Add logic for time-sensitive queries
    if any(term in original_question.lower() for term in ['today', 'yesterday', 'this week', 'last week', 'current']):
        base_result['instructions'] += """

⏰ TIME-BASED QUERY ENHANCEMENTS:
- Use appropriate date functions for filtering
- Consider timezone conversions (_PST columns when available)
- For "current" periods, use dynamic date calculations
- Order by most recent data first for time-based queries"""

    return base_result

def build_helper_sql_guided_instructions(helper_sql: str, pattern_context: Dict, table: str, schema: Dict,
                                         original_question: str) -> Dict:
    """Build SQL generation instructions using helper SQL as foundation"""

    columns = schema.get('columns', [])
    column_descriptions = schema.get('column_descriptions', {})
    audit_columns = schema.get('audit_columns', [])

    # Create column info string with descriptions
    column_info_parts = []
    for col in columns[:50]:  # Limit to first 50 columns
        desc = column_descriptions.get(col.lower(), {}).get('comment', '')
        col_type = column_descriptions.get(col.lower(), {}).get('type', '')
        if desc:
            column_info_parts.append(f"{col} ({col_type}): {desc}")
        else:
            column_info_parts.append(f"{col} ({col_type})")

    column_info = "\n".join(column_info_parts)

    instructions = f"""You are a SQL expert representing the Business Intelligence team. Generate PERFECT Snowflake SQL using the helper SQL as your foundation.

🎯 PATTERN CONTEXT: {pattern_context['name']}
📊 TABLE: {table}
📋 BUSINESS CONTEXT: {pattern_context.get('business_context', 'Standard business logic applies')}

🔧 HELPER SQL PROVIDED (Use as Foundation):
```sql
{helper_sql}
```

🏗️ YOUR TASK:
1. Use the helper SQL above as your FOUNDATION/STARTING POINT
2. REFINE and ENHANCE it to perfectly answer the user's question
3. IMPROVE the query logic, add missing elements, fix any issues
4. ENSURE it follows all business rules from the pattern context
5. MAKE it more sophisticated and accurate than the helper

📊 ACTUAL TABLE SCHEMA:
Audit Columns Found: {', '.join(audit_columns) if audit_columns else 'None'}

AVAILABLE COLUMNS WITH TYPES AND DESCRIPTIONS:
{column_info}

🎯 CRITICAL REQUIREMENTS:
1. START with the helper SQL structure but IMPROVE upon it
2. Use ONLY columns that exist in the actual schema above
3. Apply ALL business rules from the pattern context
4. Handle edge cases and data quality issues
5. Make the query more robust and comprehensive
6. Ensure proper aggregations, filtering, and ordering
7. Return ONLY the final refined SQL query - no explanations

🔍 QUALITY ASSURANCE:
- Ensure results represent meaningful business data (not NULL/system accounts)
- Use appropriate LIMIT and ORDER BY for best results
- Apply proper filtering for data quality
- Handle NULL values appropriately

REMEMBER: The helper SQL is just the starting point - make it BETTER and more accurate for the specific question."""

    message = f"""Question: {original_question}

I've provided a helper SQL as your foundation. Please refine and enhance it to perfectly answer this question. 

Use the helper SQL structure but improve upon it - add missing logic, fix any issues, make it more sophisticated and accurate for the specific question asked."""

    return {
        'instructions': instructions,
        'message': message
    }


def extract_sql_from_response(response: str) -> str:
    """Extract SQL from assistant response with enhanced error detection"""

    # Check for explicit column availability errors
    column_error_indicators = [
        'column', 'not available', 'not exist', 'missing', 'doesn\'t exist',
        'not found in', 'not in the schema', 'required column'
    ]

    if any(indicator in response.lower() for indicator in column_error_indicators):
        return f"-- Schema Error: {response.strip()}"

    # Check if response contains SQL indicators
    sql_indicators = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY']
    has_sql_indicators = any(indicator in response.upper() for indicator in sql_indicators)

    # If no SQL indicators and response looks conversational, return as conversational
    conversational_indicators = [
        'based on the', 'according to', 'the metric', 'currently used',
        'definition', 'refers to', 'represents', 'calculated', 'methodology',
        'in our data warehouse', 'the column', 'table contains', 'business logic'
    ]

    if not has_sql_indicators and any(indicator in response.lower() for indicator in conversational_indicators):
        return "-- This question should be handled conversationally, not with SQL"

    if "```sql" in response:
        try:
            return response.split("```sql")[1].split("```")[0].strip()
        except IndexError:
            pass

    lines = response.split('\n')
    sql_lines = []
    in_sql = False

    for line in lines:
        line_stripped = line.strip()

        # Start capturing when we see SELECT
        if line_stripped.upper().startswith('SELECT'):
            in_sql = True
            sql_lines = [line]
        elif in_sql:
            sql_lines.append(line)
            # Stop when we see semicolon
            if line_stripped.endswith(';'):
                break

    if sql_lines:
        return '\n'.join(sql_lines).strip()

    # Check if it's an error message
    if "don't have enough" in response or "cannot find" in response.lower():
        return response

    # If no SQL found but response has content, it might be conversational
    if len(response.strip()) > 50 and not has_sql_indicators:
        return "-- This appears to be a conversational response, not SQL"

    # Return the full response if it looks like an error explanation
    if len(response.strip()) > 20:
        return f"-- Response: {response.strip()}"

    return "-- Error: Could not extract SQL from response"


async def handle_question(user_question: str, user_id: str, channel_id: str, assistant_id: str = None) -> Tuple[
    str, str]:
    """Main question handler with OpenAI-powered classification"""
    global ASSISTANT_ID
    if assistant_id:
        ASSISTANT_ID = assistant_id

    try:
        # Step 1: Check rate limits BEFORE processing
        context = await get_conversation_context(user_id, channel_id)
        estimated_tokens = await estimate_request_tokens(user_question, context)

        rate_limit_check = await check_rate_limits(user_id, channel_id, estimated_tokens)

        if not rate_limit_check["allowed"]:
            usage_info = f"""⚠️ **Usage Limit Reached**

{rate_limit_check["reason"]}

**Your Current Usage:**
- Daily: {rate_limit_check['daily_usage']:,}/{rate_limit_check['limits']['daily']:,} tokens ({rate_limit_check['daily_usage'] / rate_limit_check['limits']['daily'] * 100:.1f}%)
- Hourly: {rate_limit_check['hourly_usage']:,}/{rate_limit_check['limits']['hourly']:,} tokens ({rate_limit_check['hourly_usage'] / rate_limit_check['limits']['hourly'] * 100:.1f}%)
- This thread: {rate_limit_check['thread_usage']:,}/{rate_limit_check['limits']['thread']:,} tokens ({rate_limit_check['thread_usage'] / rate_limit_check['limits']['thread'] * 100:.1f}%)

*Limits reset hourly/daily. Try again later or start a new conversation.*"""
            return usage_info, 'rate_limited'

        # Step 2: Get conversation context
        context = await get_conversation_context(user_id, channel_id)

        # Step 3: Use OpenAI for intelligent classification
        print(f"🤖 Using OpenAI for question classification...")
        question_type = await classify_question_with_openai(user_question, user_id, channel_id, context)

        print(f"\n{'=' * 60}")
        print(f"🔍 Processing question: {user_question}")
        print(f"📊 OpenAI classified as: {question_type}")
        print(f"🔢 Estimated tokens: {estimated_tokens:,}")
        if context:
            print(f"📝 Previous response type: {context.get('last_response_type', 'none')}")

        if question_type == 'sql_required':
            # Check cache first
            question_hash = get_question_hash(user_question)
            cache_key = f"{SQL_CACHE_PREFIX}:{question_hash}"

            try:
                cached_sql = await safe_valkey_get(cache_key)

                if cached_sql and cached_sql.get('success_count', 0) > 0:
                    # Check feedback before using cache
                    feedback_key = f"{FEEDBACK_PREFIX}:{question_hash}"
                    feedback = await safe_valkey_get(feedback_key, {})

                    if feedback.get('negative_count', 0) > feedback.get('positive_count', 0):
                        print(f"⚠️ Skipping cached SQL due to negative feedback")
                    else:
                        print(f"💰 Using cached SQL (minimal tokens used)")
                        sql = cached_sql['sql']
                        print(f"📝 Cached SQL:\n{sql}")
                        # Track minimal token usage for cached response
                        await track_actual_usage(user_id, channel_id, user_question, sql)
                        return sql, 'sql'
                else:
                    print(f"🔄 Generating new SQL (no successful cache found)")
            except Exception as cache_error:
                print(f"⚠️ Error checking cache: {cache_error}")
                print(f"🔄 Proceeding to generate new SQL")

            # Generate new SQL using intelligent table discovery
            sql_query, selected_table = await generate_sql_intelligently(user_question, user_id, channel_id)

            # Track actual token usage
            await track_actual_usage(user_id, channel_id, user_question, sql_query)

            # Store the selected table in context for feedback
            if selected_table:
                await update_conversation_context(user_id, channel_id, user_question, sql_query, 'sql', selected_table)

            return sql_query, 'sql'

        else:
            # Handle conversational
            response = await handle_conversational_question(user_question, user_id, channel_id)

            # Track actual token usage
            await track_actual_usage(user_id, channel_id, user_question, response)

            return response, 'conversational'

    except Exception as e:
        print(f"❌ Error in handle_question: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        traceback.print_exc()

        # Return error as SQL comment to be handled by the caller
        return f"-- Error: {str(e)}", 'error'

async def clear_token_usage_cache():
    """Clear token usage cache"""
    _local_cache.setdefault('token_usage', {}).clear()
    print("🧹 Token usage cache cleared")

async def update_conversation_context_with_sql(user_id: str, channel_id: str, question: str, response: str,
                                               response_type: str = None, table_used: str = None, sql: str = None):
    """Update conversation context for follow-up questions with SQL"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = f"{CONVERSATION_CACHE_PREFIX}:{cache_key}"

    context = {
        'last_question': question,
        'last_response': response,
        'last_response_type': response_type,
        'last_table_used': table_used,
        'last_sql': sql,  # Store SQL for potential execution
        'timestamp': time.time()
    }

    await safe_valkey_set(redis_key, context, ex=CONVERSATION_CACHE_TTL)

async def update_sql_cache_with_results(user_question: str, sql_query: str, result_count: int,
                                        selected_table: str = None):
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

            # Update table selection cache if we know the table
            if selected_table:
                await cache_table_selection(user_question, selected_table, "Successful query execution", True)
            elif 'FROM' in sql_query.upper():
                # Try to extract table from SQL
                sql_upper = sql_query.upper()
                from_idx = sql_upper.find('FROM')
                if from_idx != -1:
                    table_part = sql_query[from_idx + 4:].strip().split()[0]
                    await cache_table_selection(user_question, table_part, "Extracted from successful query", True)

        cache_entry = {
            'sql': sql_query,
            'success_count': success_count,
            'last_result_count': result_count,
            'last_used': time.time(),
            'selected_table': selected_table
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
    """Summarize results based on question type - more concise and focused"""
    if assistant_id:
        global ASSISTANT_ID
        ASSISTANT_ID = assistant_id

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    # Analyze the question to determine response style
    question_lower = user_question.lower()
    question_intent = analyze_question_intent(question_lower)

    # Dynamic instructions based on intent
    instructions = """Analyze the query results and provide a clear, concise answer to the user's question.

Focus on:
1. Directly answering what was asked
2. Highlighting key findings or patterns
3. Formatting numbers and results clearly
4. Noting any data quality issues if relevant
5. If the query execution failed just say the user some Generic Error message dont say anything about why and how it failed

Use Slack formatting:
- *text* for bold emphasis
- _text_ for italic
- `text` for code/column names
- • for bullet points

Be concise but complete. Maximum 3-4 sentences unless listing results."""

    message = f"""Question: "{user_question}"

Data Results:
{result_table}

Provide a clear answer that directly addresses the question."""

    response = await send_message_and_run(thread_id, message, instructions)

    # Safety check - convert any remaining markdown to Slack format
    response = response.replace("**", "*")

    return response

async def debug_table_selection(question: str, user_id: str = "debug", channel_id: str = "debug") -> str:
    """Debug the table selection process for a question"""
    result = f"🔍 Table Selection Debug for: '{question}'\n\n"

    # Check cache first
    cached = await get_cached_table_suggestion(question)
    if cached:
        result += f"✅ Found cached suggestion: {cached}\n\n"
    else:
        result += "❌ No cached suggestion found\n\n"

    # Get candidate tables from vector store
    result += "📚 Vector Store Search:\n"
    candidates = await find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k=5)

    if candidates:
        # Get descriptions for these tables
        descriptions = await get_table_descriptions_from_manifest(candidates, user_id, channel_id)

        for i, table in enumerate(candidates, 1):
            result += f"\n{i}. {table}\n"
            if table in descriptions:
                result += f"   Description: {descriptions[table][:200]}...\n" if len(
                    descriptions[table]) > 200 else f"   Description: {descriptions[table]}\n"
            else:
                result += f"   Description: Not found\n"
    else:
        result += "  No tables found from vector search\n"

    result += "\n📊 Key Phrases Extracted:\n"
    phrases = extract_key_phrases(question)
    for phrase in phrases[:10]:
        result += f"  - {phrase}\n"

    # Check phrase-based cache
    result += "\n💾 Phrase-based Cache Matches:\n"
    found_any = False
    for phrase in phrases:
        phrase_key = f"{TABLE_SELECTION_PREFIX}:phrase:{phrase}"
        phrase_data = await safe_valkey_get(phrase_key, {})
        if phrase_data:
            found_any = True
            result += f"  '{phrase}':\n"
            for table, stats in list(phrase_data.items())[:3]:
                success_rate = stats['success_count'] / stats['count'] if stats['count'] > 0 else 0
                result += f"    - {table}: {stats['count']} uses, {success_rate:.0%} success\n"

    if not found_any:
        result += "  No phrase matches found in cache\n"

    # Check feedback
    feedback_key = f"{FEEDBACK_PREFIX}:{get_question_hash(question)}"
    feedback = await safe_valkey_get(feedback_key, {})
    if feedback:
        result += f"\n📝 Feedback History:\n"
        result += f"  ✅ Positive: {feedback.get('positive_count', 0)}\n"
        result += f"  ❌ Negative: {feedback.get('negative_count', 0)}\n"

    return result


async def get_cache_stats():
    """Get cache statistics"""
    await ensure_valkey_connection()

    stats = {
        "cache_backend": "Valkey" if valkey_client else "Local Memory",
        "status": "connected" if valkey_client else "local",
        "patterns_available": PATTERNS_AVAILABLE,
        "caches": {
            "sql": len(_local_cache.get('sql', {})),
            "schema": len(_local_cache.get('schema', {})),
            "thread": len(_local_cache.get('thread', {})),
            "conversation": len(_local_cache.get('conversation', {})),
            "table_selection": len(_local_cache.get('table_selection', {})),
            "table_samples": len(_local_cache.get('table_samples', {})),
            "feedback": len(_local_cache.get('feedback', {}))
        }
    }

    # Add schema cache details
    schema_cache = _local_cache.get('schema', {})
    if schema_cache:
        stats['cached_schemas'] = list(schema_cache.keys())

    return stats


async def get_learning_insights():
    """Get learning insights about table selection patterns"""
    stats = await get_cache_stats()

    insights = [f"Cache Backend: {stats['cache_backend']}"]
    insights.append(f"Status: {stats['status']}")
    insights.append(f"Pattern Matching: {'Enabled' if stats['patterns_available'] else 'Disabled'}")
    insights.append(f"\nCached Items:")
    insights.append(f"  - SQL Queries: {stats['caches']['sql']}")
    insights.append(f"  - Table Schemas: {stats['caches']['schema']}")
    insights.append(f"  - Table Selections: {stats['caches']['table_selection']}")
    insights.append(f"  - Table Samples: {stats['caches']['table_samples']}")
    insights.append(f"  - User Feedback: {stats['caches']['feedback']}")
    insights.append(f"  - Threads: {stats['caches']['thread']}")
    insights.append(f"  - Conversations: {stats['caches']['conversation']}")

    # Show table selection patterns
    insights.append(f"\n📊 Table Selection Patterns:")

    # Get all phrase keys
    phrase_stats = {}
    for key in _local_cache.get('table_selection', {}).keys():
        if ':phrase:' in key:
            phrase = key.split(':phrase:')[-1]
            phrase_data = await safe_valkey_get(key, {})
            if phrase_data:
                total_uses = sum(stats['count'] for stats in phrase_data.values())
                phrase_stats[phrase] = total_uses

    # Show top phrases
    if phrase_stats:
        top_phrases = sorted(phrase_stats.items(), key=lambda x: x[1], reverse=True)[:10]
        for phrase, count in top_phrases:
            insights.append(f"  - '{phrase}': {count} uses")
    else:
        insights.append("  No patterns learned yet")

    # Show feedback summary
    feedback_stats = {'positive': 0, 'negative': 0}
    for key in _local_cache.get('feedback', {}).keys():
        feedback_data = await safe_valkey_get(key, {})
        if feedback_data:
            feedback_stats['positive'] += feedback_data.get('positive_count', 0)
            feedback_stats['negative'] += feedback_data.get('negative_count', 0)

    if feedback_stats['positive'] or feedback_stats['negative']:
        insights.append(f"\n📝 User Feedback Summary:")
        insights.append(f"  ✅ Positive: {feedback_stats['positive']}")
        insights.append(f"  ❌ Negative: {feedback_stats['negative']}")
        if feedback_stats['positive'] + feedback_stats['negative'] > 0:
            success_rate = feedback_stats['positive'] / (feedback_stats['positive'] + feedback_stats['negative']) * 100
            insights.append(f"  📊 Success Rate: {success_rate:.1f}%")

    return "\n".join(insights)


def test_question_classification():
    """Test question classification"""
    test_cases = [
        # SQL queries
        ("How many messaging tickets had a reply time over 15 minutes?", "sql_required"),
        ("Which ticket type is driving Chat AHT for agent Jesse?", "sql_required"),
        ("What are the KPIs that determine agent performance?", "sql_required"),
        ("Show me ticket volume by group", "sql_required"),
        ("What is the contact driver with the highest Handling Time?", "sql_required"),
        ("List all agents with high resolution time", "sql_required"),
        ("Find tickets created yesterday", "sql_required"),
        ("Compare agent performance across teams", "sql_required"),
        ("What's the trend in ticket volume this month?", "sql_required"),
        ("What is the ticket volume for today?", "sql_required"),  # This should be sql_required

        # Conversational follow-ups about data
        ("What is the source for this data?", "conversational"),
        ("Where does this data come from?", "conversational"),
        ("What table did you use?", "conversational"),
        ("Explain these results", "conversational"),
        ("What are these groups?", "conversational"),
        ("Why is it showing None?", "conversational"),
        ("How did you calculate this?", "conversational"),
        ("What does WOPs mean?", "conversational"),

        # Meta questions
        ("What can you help me with?", "conversational"),
        ("What data do you have access to?", "conversational"),

        # Short follow-ups (likely conversational)
        ("Why?", "conversational"),
        ("Tell me more", "conversational"),
        ("What about last month?", "conversational"),
        ("Break this down", "conversational"),
        ("This data", "conversational"),
        ("Clarify", "conversational"),
    ]

    print("🧪 Testing Question Classification:")
    print("=" * 60)

    passed = 0
    failed = 0

    for question, expected in test_cases:
        actual = classify_question_type(question)
        if actual == expected:
            status = "✅"
            passed += 1
        else:
            status = "❌"
            failed += 1
        # Truncate long questions for display
        display_q = question if len(question) <= 50 else question[:47] + "..."
        print(f"{status} '{display_q}' → {actual} (expected: {expected})")

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")


async def test_conversation_flow():
    """Test a typical conversation flow"""
    print("\n🧪 Testing Conversation Flow:")
    print("=" * 60)

    # Simulate user and channel
    test_user = "test_user"
    test_channel = "test_channel"

    # Test 1: SQL Query
    print("\n1️⃣ Testing SQL query...")
    response1, type1 = await handle_question(
        "What are the KPIs that determine agent performance?",
        test_user, test_channel
    )
    print(f"   Response type: {type1}")
    if type1 == 'sql':
        print(f"   SQL Generated: {response1[:100]}..." if len(response1) > 100 else f"   SQL: {response1}")

    # Simulate that SQL was executed and results were shown
    await update_conversation_context(
        test_user, test_channel,
        "What are the KPIs that determine agent performance?",
        "Results showed: AHT, CSAT, FCR metrics",
        'sql_results'
    )

    # Test 2: Follow-up question
    print("\n2️⃣ Testing follow-up question...")
    response2, type2 = await handle_question(
        "What is the source for this data?",
        test_user, test_channel
    )
    print(f"   Response type: {type2}")
    print(f"   Should be conversational: {'✅' if type2 == 'conversational' else '❌'}")

    # Test 3: Another follow-up
    print("\n3️⃣ Testing another follow-up...")
    response3, type3 = await handle_question(
        "Which table contains this performance data?",
        test_user, test_channel
    )
    print(f"   Response type: {type3}")
    print(f"   Should be conversational: {'✅' if type3 == 'conversational' else '❌'}")

    # Test 4: New SQL query
    print("\n4️⃣ Testing new SQL query...")
    response4, type4 = await handle_question(
        "Show me ticket volume by group",
        test_user, test_channel
    )
    print(f"   Response type: {type4}")
    print(f"   Should be sql_required: {'✅' if type4 == 'sql' else '❌'}")

    print("\n" + "=" * 60)
    print("✅ Conversation flow test complete")


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

Provide a business summary. If asked about data source, explain it comes from our data warehouse tables.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names
- Use bullet points with •"""

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4
        )
        result = response.choices[0].message.content.strip()
        # Safety check - convert any remaining markdown bold to Slack format
        result = result.replace("**", "*")
        return result
    except Exception as e:
        return f"⚠️ Error: {e}"


async def analyze_result_quality(question: str, sql: str, df, selected_table: str) -> Dict[str, Any]:
    """
    Analyze the quality of SQL results and detect if they make business sense
    Returns suggestions for improvement if results are poor quality
    """
    analysis = {
        "quality_score": 100,  # Start with perfect score
        "issues": [],
        "suggestions": [],
        "should_retry": False,
        "retry_sql": None
    }

    # Check if we have any results
    if isinstance(df, str) or not hasattr(df, '__len__') or len(df) == 0:
        analysis["quality_score"] = 0
        analysis["issues"].append("No results returned")
        analysis["suggestions"].append(
            "Try broadening the search criteria or check if data exists for the specified time period")
        return analysis

    question_lower = question.lower()

    # Performance-related question analysis
    if any(term in question_lower for term in ["best", "top", "performing", "agent performance", "rankings"]):
        analysis.update(await _analyze_performance_results(question, sql, df, selected_table))

    # Volume/count question analysis
    elif any(term in question_lower for term in ["how many", "count", "volume"]):
        analysis.update(await _analyze_volume_results(question, sql, df, selected_table))

    # Time-based analysis
    elif any(term in question_lower for term in ["today", "yesterday", "this week", "last week"]):
        analysis.update(await _analyze_time_based_results(question, sql, df, selected_table))

    return analysis


async def _analyze_performance_results(question: str, sql: str, df, selected_table: str) -> Dict[str, Any]:
    """Analyze performance-related query results - FIXED VERSION"""
    analysis = {"quality_score": 100, "issues": [], "suggestions": [], "should_retry": False, "retry_sql": None}

    # Check for null/empty agent names in performance queries
    if hasattr(df, 'columns') and any(
            col.lower() in ['assignee_name', 'user_name', 'agent_name'] for col in df.columns):
        name_column = None
        for col in df.columns:
            if col.lower() in ['assignee_name', 'user_name', 'agent_name']:
                name_column = col
                break

        if name_column and len(df) > 0:
            # Check if first result is null/empty
            first_result = df.iloc[0][name_column]
            if pd.isna(first_result) or first_result in ['', 'None', None, 'null'] or str(first_result).lower() in [
                'none', 'null', 'nan']:
                analysis["quality_score"] -= 50
                analysis["issues"].append(f"Top result has null/empty agent name: '{first_result}'")

                # Check if there are valid agents in the data
                valid_agents = df[
                    df[name_column].notna() &
                    (df[name_column] != '') &
                    (df[name_column] != 'None') &
                    (df[name_column] != 'null') &
                    (df[name_column].str.lower() != 'none') &
                    (df[name_column].str.lower() != 'null')
                    ]

                if len(valid_agents) > 0:
                    analysis["should_retry"] = True
                    analysis["suggestions"].append("Filtering out unassigned/null records and re-running query")

                    # Generate improved SQL with comprehensive filtering
                    analysis["retry_sql"] = _improve_sql_with_comprehensive_filters(sql, name_column)
                else:
                    analysis["suggestions"].append(
                        "No valid agent data found - check if there's performance data for the specified time period")

    # Check for missing key performance metrics
    performance_columns = ['qa_score', 'fcr_percentage', 'aht_minutes', 'handle_time', 'avg_qa_score',
                           'avg_fcr_percentage', 'avg_aht_minutes']
    if hasattr(df, 'columns'):
        available_perf_cols = [col for col in df.columns if any(perf in col.lower() for perf in performance_columns)]

        if len(available_perf_cols) == 0:
            analysis["quality_score"] -= 30
            analysis["issues"].append("No performance metrics found in results")
        else:
            # Check if performance metrics are mostly null
            for col in available_perf_cols:
                if len(df) > 0:
                    non_null_count = df[col].count()
                    null_percentage = (len(df) - non_null_count) / len(df) * 100
                    if null_percentage > 80:
                        analysis["quality_score"] -= 25
                        analysis["issues"].append(f"Most values in {col} are null ({null_percentage:.0f}%)")

                        # If ALL metrics are null AND we have agent issues, definitely retry
                        if null_percentage == 100 and analysis["should_retry"]:
                            analysis["quality_score"] -= 30  # Extra penalty

    return analysis


def _improve_sql_with_comprehensive_filters(original_sql: str, name_column: str) -> str:
    """
    Comprehensively improve SQL by adding filters for valid agents and better ordering
    """
    sql = original_sql.strip()

    # Comprehensive filters to add
    filters_to_add = [
        f"{name_column} IS NOT NULL",
        f"{name_column} != ''",
        f"{name_column} != 'None'",
        f"LOWER({name_column}) NOT IN ('null', 'none', 'nan')",
        f"TRIM({name_column}) != ''"
    ]

    # Check if there's already a WHERE clause
    if "WHERE" in sql.upper():
        # Find the WHERE clause and add our filters at the beginning
        where_pos = sql.upper().find("WHERE")
        before_where = sql[:where_pos + 5]  # Include "WHERE"
        after_where = sql[where_pos + 5:].strip()

        # Add our filters at the beginning of WHERE clause
        filter_clause = " AND ".join(filters_to_add)
        improved_sql = f"{before_where} {filter_clause} AND ({after_where})"
    else:
        # No WHERE clause exists, add one before GROUP BY, ORDER BY, or LIMIT
        insertion_keywords = ["GROUP BY", "ORDER BY", "LIMIT", "HAVING"]
        insertion_pos = len(sql)

        for keyword in insertion_keywords:
            pos = sql.upper().find(keyword)
            if pos != -1 and pos < insertion_pos:
                insertion_pos = pos

        filter_clause = " AND ".join(filters_to_add)
        before_insertion = sql[:insertion_pos].rstrip()
        after_insertion = sql[insertion_pos:]

        improved_sql = f"{before_insertion}\nWHERE {filter_clause}\n{after_insertion}"

    # Also improve ordering for performance queries - prioritize complete data
    if "ORDER BY" in improved_sql.upper() and any(
            perf in improved_sql.lower() for perf in ['qa_score', 'performance', 'fcr']):
        # Add data completeness ordering
        order_pos = improved_sql.upper().find("ORDER BY")
        before_order = improved_sql[:order_pos]
        after_order = improved_sql[order_pos + 8:].strip()  # Skip "ORDER BY"

        # Add data completeness check first
        improved_sql = f"{before_order}ORDER BY CASE WHEN QA_SCORE IS NOT NULL AND FCR_PERCENTAGE IS NOT NULL THEN 0 ELSE 1 END, {after_order}"

    return improved_sql.strip()


async def _analyze_volume_results(question: str, sql: str, df, selected_table: str) -> Dict[str, Any]:
    """Analyze volume/count query results"""
    analysis = {"quality_score": 100, "issues": [], "suggestions": [], "should_retry": False, "retry_sql": None}

    if hasattr(df, 'iloc') and len(df) > 0:
        # For count queries, check if result seems reasonable
        first_row = df.iloc[0]

        # Look for count columns
        count_columns = [col for col in df.columns if 'count' in col.lower() or 'total' in col.lower()]
        if count_columns:
            for col in count_columns:
                count_value = first_row[col]
                if pd.isna(count_value) or count_value == 0:
                    analysis["quality_score"] -= 40
                    analysis["issues"].append(
                        f"Count result is {count_value} - may indicate no data for specified criteria")
                    analysis["suggestions"].append("Try expanding the time range or checking different filters")

    return analysis


async def _analyze_time_based_results(question: str, sql: str, df, selected_table: str) -> Dict[str, Any]:
    """Analyze time-based query results"""
    analysis = {"quality_score": 100, "issues": [], "suggestions": [], "should_retry": False, "retry_sql": None}

    question_lower = question.lower()

    # Check if asking for recent data but getting old data
    if any(term in question_lower for term in ["today", "this week"]) and hasattr(df, 'columns'):
        date_columns = [col for col in df.columns if
                        any(date_term in col.lower() for date_term in ['date', 'created', 'solved', 'week'])]

        if date_columns and len(df) > 0:
            for col in date_columns:
                try:
                    if df[col].dtype == 'datetime64[ns]' or 'date' in str(df[col].dtype).lower():
                        latest_date = df[col].max()
                        if pd.notna(latest_date):
                            days_old = (pd.Timestamp.now() - latest_date).days
                            if days_old > 7:  # Data is more than a week old
                                analysis["quality_score"] -= 30
                                analysis["issues"].append(f"Most recent data is {days_old} days old")
                                analysis["suggestions"].append(
                                    "Data may not be up to date - check if recent data is available")
                except:
                    pass  # Skip if date parsing fails

    return analysis


async def suggest_query_improvements(question: str, sql: str, df, selected_table: str) -> str:
    """
    Generate suggestions for improving queries based on result analysis
    """
    analysis = await analyze_result_quality(question, sql, df, selected_table)

    if analysis["quality_score"] >= 80:
        return None  # Results are good quality

    suggestions = []
    suggestions.append(f"**Result Quality Score: {analysis['quality_score']}/100**")

    if analysis["issues"]:
        suggestions.append("\n**Issues Detected:**")
        for issue in analysis["issues"]:
            suggestions.append(f"• {issue}")

    if analysis["suggestions"]:
        suggestions.append("\n**Suggestions:**")
        for suggestion in analysis["suggestions"]:
            suggestions.append(f"• {suggestion}")

    if analysis["should_retry"] and analysis["retry_sql"]:
        suggestions.append(f"\n**🔄 Let me try an improved query...**")

    return "\n".join(suggestions)


async def execute_with_quality_analysis(question: str, sql: str, selected_table: str, user_id: str, channel_id: str):
    """
    Execute SQL with error handling, auto-fixing, and quality analysis
    Returns: (result_df, result_count, analysis_info)
    """
    from app.snowflake_runner import run_query

    print(f"🔍 Executing query with error handling and quality analysis...")

    original_sql = sql
    attempts = 0
    max_attempts = 3

    while attempts < max_attempts:
        attempts += 1
        print(f"🔄 Attempt {attempts}/{max_attempts}")

        # Execute SQL query
        df = run_query(sql)

        # Check if query failed due to SQL errors
        if isinstance(df, str):
            error_message = df.lower()

            # Detect different types of SQL errors
            sql_error_type = detect_sql_error_type(df)

            if sql_error_type and attempts < max_attempts:
                print(f"🔧 SQL Error detected: {sql_error_type}")
                print(f"❌ Error: {df}")

                # Try to fix the SQL based on error type
                fixed_sql = await fix_sql_error(sql, df, sql_error_type, selected_table, question, user_id, channel_id)

                if fixed_sql and fixed_sql != sql:
                    print(f"🛠️ Attempting SQL fix for {sql_error_type}")
                    print(f"🔧 Fixed SQL:\n{fixed_sql}")
                    sql = fixed_sql
                    continue  # Try again with fixed SQL
                else:
                    print(f"⚠️ Could not generate fix for {sql_error_type}")
                    break
            else:
                # Non-SQL error or max attempts reached
                print(f"❌ Query execution failed: {df}")
                return df, 0, {"error_type": "execution_error", "original_sql": original_sql, "attempts": attempts}
        else:
            # Query succeeded, now do quality analysis
            print(f"✅ SQL execution successful on attempt {attempts}")

            # If we fixed the SQL, note it in the analysis
            sql_was_fixed = sql != original_sql
            if sql_was_fixed:
                print(f"🎉 SQL was auto-fixed and executed successfully!")

            # Analyze result quality
            analysis = await analyze_result_quality(question, sql, df, selected_table)

            print(f"📊 Result quality score: {analysis['quality_score']}/100")

            # AUTO-RETRY LOGIC for data quality (separate from SQL error fixing)
            if analysis["should_retry"] and analysis["retry_sql"] and analysis["quality_score"] < 60:
                print(f"🔄 AUTO-RETRYING: Quality too low ({analysis['quality_score']}/100)")
                print(f"Issues found: {', '.join(analysis['issues'])}")

                # Execute improved query for data quality
                improved_df = run_query(analysis["retry_sql"])

                if not isinstance(improved_df, str) and len(improved_df) > 0:
                    print(f"✅ Quality retry returned {len(improved_df)} results")

                    # Re-analyze improved results
                    improved_analysis = await analyze_result_quality(question, analysis["retry_sql"], improved_df,
                                                                     selected_table)
                    print(f"📈 Improved quality score: {improved_analysis['quality_score']}/100")

                    # If significantly better, use improved results
                    if improved_analysis["quality_score"] > analysis["quality_score"] + 20:
                        print(f"🎉 Quality retry successful! Using improved results.")

                        # Mark this as auto-retry success
                        improved_analysis["auto_retry_success"] = True
                        improved_analysis["original_issues"] = analysis["issues"]
                        improved_analysis["improvement"] = improved_analysis["quality_score"] - analysis[
                            "quality_score"]
                        improved_analysis["sql_was_fixed"] = sql_was_fixed
                        improved_analysis["original_sql"] = original_sql
                        improved_analysis["fixed_sql"] = sql if sql_was_fixed else None
                        improved_analysis["final_sql"] = analysis["retry_sql"]

                        return improved_df, len(improved_df), improved_analysis

            # Add SQL fix information to analysis
            if sql_was_fixed:
                analysis["sql_was_fixed"] = True
                analysis["original_sql"] = original_sql
                analysis["fixed_sql"] = sql
                analysis["fix_attempts"] = attempts

            return df, len(df) if hasattr(df, '__len__') else 0, analysis

    # If we get here, all attempts failed
    return f"❌ Failed to execute query after {max_attempts} attempts. Last error: {df}", 0, {
        "error_type": "max_attempts_exceeded",
        "original_sql": original_sql,
        "attempts": attempts
    }

def count_tokens(text: str, model: str = "gpt-4o-mini") -> int:
    """Count tokens in text using tiktoken"""
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except:
        # Fallback estimation: roughly 4 characters per token
        return len(text) // 4


async def get_user_token_usage(user_id: str, period: str = "daily") -> int:
    """Get current token usage for user"""
    today = time.strftime("%Y-%m-%d")
    current_hour = time.strftime("%Y-%m-%d-%H")

    if period == "daily":
        cache_key = f"{DAILY_USAGE_PREFIX}:{user_id}:{today}"
    elif period == "hourly":
        cache_key = f"{HOURLY_USAGE_PREFIX}:{user_id}:{current_hour}"
    else:
        return 0

    usage = await safe_valkey_get(cache_key, 0)
    return usage


async def get_thread_token_usage(user_id: str, channel_id: str) -> int:
    """Get token usage for specific thread"""
    cache_key = f"{user_id}_{channel_id}"
    thread_key = f"{THREAD_USAGE_PREFIX}:{cache_key}"
    usage = await safe_valkey_get(thread_key, 0)
    return usage


async def update_token_usage(user_id: str, channel_id: str, tokens_used: int):
    """Update token usage counters"""
    today = time.strftime("%Y-%m-%d")
    current_hour = time.strftime("%Y-%m-%d-%H")

    # Update daily usage
    daily_key = f"{DAILY_USAGE_PREFIX}:{user_id}:{today}"
    daily_usage = await safe_valkey_get(daily_key, 0)
    await safe_valkey_set(daily_key, daily_usage + tokens_used, ex=TOKEN_USAGE_CACHE_TTL)

    # Update hourly usage
    hourly_key = f"{HOURLY_USAGE_PREFIX}:{user_id}:{current_hour}"
    hourly_usage = await safe_valkey_get(hourly_key, 0)
    await safe_valkey_set(hourly_key, hourly_usage + tokens_used, ex=3600)  # 1 hour

    # Update thread usage
    cache_key = f"{user_id}_{channel_id}"
    thread_key = f"{THREAD_USAGE_PREFIX}:{cache_key}"
    thread_usage = await safe_valkey_get(thread_key, 0)
    await safe_valkey_set(thread_key, thread_usage + tokens_used, ex=THREAD_CACHE_TTL)


async def check_rate_limits(user_id: str, channel_id: str, estimated_tokens: int = 0) -> dict:
    """Check if user has exceeded rate limits"""

    # Skip rate limiting if disabled or user is admin
    if not ENABLE_RATE_LIMITING or user_id in ADMIN_USERS:
        return {
            "allowed": True,
            "reason": "Admin override" if user_id in ADMIN_USERS else "Rate limiting disabled",
            "daily_usage": 0,
            "hourly_usage": 0,
            "thread_usage": 0,
            "limits": {
                "daily": MAX_TOKENS_PER_USER_PER_DAY,
                "hourly": MAX_TOKENS_PER_USER_PER_HOUR,
                "thread": MAX_TOKENS_PER_THREAD
            }
        }

    daily_usage = await get_user_token_usage(user_id, "daily")
    hourly_usage = await get_user_token_usage(user_id, "hourly")
    thread_usage = await get_thread_token_usage(user_id, channel_id)

    result = {
        "allowed": True,
        "reason": None,
        "daily_usage": daily_usage,
        "hourly_usage": hourly_usage,
        "thread_usage": thread_usage,
        "limits": {
            "daily": MAX_TOKENS_PER_USER_PER_DAY,
            "hourly": MAX_TOKENS_PER_USER_PER_HOUR,
            "thread": MAX_TOKENS_PER_THREAD
        }
    }

    # Check daily limit
    if daily_usage + estimated_tokens > MAX_TOKENS_PER_USER_PER_DAY:
        result["allowed"] = False
        result["reason"] = f"Daily token limit exceeded ({daily_usage:,}/{MAX_TOKENS_PER_USER_PER_DAY:,})"
        return result

    # Check hourly limit
    if hourly_usage + estimated_tokens > MAX_TOKENS_PER_USER_PER_HOUR:
        result["allowed"] = False
        result["reason"] = f"Hourly token limit exceeded ({hourly_usage:,}/{MAX_TOKENS_PER_USER_PER_HOUR:,})"
        return result

    # Check thread limit
    if thread_usage + estimated_tokens > MAX_TOKENS_PER_THREAD:
        result["allowed"] = False
        result[
            "reason"] = f"Thread token limit exceeded ({thread_usage:,}/{MAX_TOKENS_PER_THREAD:,}). Starting fresh conversation."
        # Force new thread creation by clearing cache
        cache_key = f"{user_id}_{channel_id}"
        redis_key = f"{THREAD_CACHE_PREFIX}:{cache_key}"
        await safe_valkey_delete(redis_key)
        return result

    return result


async def estimate_request_tokens(question: str, context: dict = None) -> int:
    """Estimate tokens for a request"""
    # Count question tokens
    question_tokens = count_tokens(question)

    # Estimate system prompt and context tokens
    system_tokens = 1000  # Base system prompt
    context_tokens = 0

    if context:
        # Add context tokens if there's conversation history
        context_tokens = count_tokens(str(context)) if context else 0

    # Estimate response tokens (conservative estimate)
    estimated_response_tokens = 1500

    total_estimated = question_tokens + system_tokens + context_tokens + estimated_response_tokens
    return total_estimated


async def track_actual_usage(user_id: str, channel_id: str, request_text: str, response_text: str):
    """Track actual token usage after API call"""
    request_tokens = count_tokens(request_text)
    response_tokens = count_tokens(response_text)
    total_tokens = request_tokens + response_tokens

    await update_token_usage(user_id, channel_id, total_tokens)

    print(
        f"🔢 Token usage - User: {user_id}, Request: {request_tokens}, Response: {response_tokens}, Total: {total_tokens}")

    return total_tokens


def detect_sql_error_type(error_message: str) -> Optional[str]:
    """Detect the type of SQL error from the error message"""
    error_lower = error_message.lower()

    # Column/identifier errors
    if any(phrase in error_lower for phrase in [
        "invalid identifier", "column", "does not exist", "unknown column",
        "column not found", "ambiguous column"
    ]):
        return "invalid_column"

    # Table/schema errors
    if any(phrase in error_lower for phrase in [
        "table", "does not exist", "unknown table", "schema not found",
        "object does not exist", "relation does not exist"
    ]):
        return "invalid_table"

    # Syntax errors
    if any(phrase in error_lower for phrase in [
        "syntax error", "parsing error", "unexpected token", "invalid syntax",
        "sql parsing error", "compilation error"
    ]):
        return "syntax_error"

    # Aggregation/GROUP BY errors
    if any(phrase in error_lower for phrase in [
        "group by", "aggregate", "not in group by", "must appear in group by"
    ]):
        return "groupby_error"

    # Data type errors
    if any(phrase in error_lower for phrase in [
        "data type", "type conversion", "invalid data type", "cannot convert"
    ]):
        return "datatype_error"

    # Permission/access errors
    if any(phrase in error_lower for phrase in [
        "permission", "access denied", "unauthorized", "insufficient privileges"
    ]):
        return "permission_error"

    return None


async def fix_sql_error(sql: str, error_message: str, error_type: str, selected_table: str, question: str, user_id: str,
                        channel_id: str) -> Optional[str]:
    """Try to fix SQL based on the error type and message"""

    if error_type == "invalid_column":
        return await fix_invalid_column_error(sql, error_message, selected_table, question, user_id, channel_id)

    elif error_type == "invalid_table":
        return await fix_invalid_table_error(sql, error_message, selected_table)

    elif error_type == "syntax_error":
        return await fix_syntax_error(sql, error_message, question, user_id, channel_id)

    elif error_type == "groupby_error":
        return await fix_groupby_error(sql, error_message)

    elif error_type == "datatype_error":
        return await fix_datatype_error(sql, error_message)

    return None


async def fix_invalid_column_error(sql: str, error_message: str, selected_table: str, question: str, user_id: str,
                                   channel_id: str) -> Optional[str]:
    """Fix invalid column errors by finding correct column names"""

    # Extract the problematic column name from error message
    import re

    # Try different patterns for column name extraction
    patterns = [
        r"invalid identifier '([^']+)'",
        r'column "([^"]+)" does not exist',
        r"unknown column '([^']+)'",
        r'column ([^\s]+) does not exist'
    ]

    bad_column = None
    for pattern in patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match:
            bad_column = match.group(1)
            break

    if not bad_column:
        print(f"⚠️ Could not extract column name from error: {error_message}")
        return None

    print(f"🔍 Bad column detected: '{bad_column}'")

    # Get current schema for the table
    schema = await discover_table_schema(selected_table)
    available_columns = schema.get('columns', [])

    if not available_columns:
        print(f"⚠️ No schema available for table {selected_table}")
        return None

    # Try to find similar column names
    similar_columns = find_similar_column_names(bad_column, available_columns)

    if similar_columns:
        # Use the most similar column
        best_match = similar_columns[0]
        print(f"🔄 Replacing '{bad_column}' with '{best_match}'")

        # Replace the bad column in SQL
        fixed_sql = sql.replace(bad_column, best_match)
        return fixed_sql

    # If no similar columns found, try using OpenAI to suggest fix
    return await get_ai_column_fix(sql, bad_column, available_columns, question, user_id, channel_id)


def find_similar_column_names(bad_column: str, available_columns: List[str]) -> List[str]:
    """Find similar column names using fuzzy matching"""
    from difflib import SequenceMatcher

    bad_lower = bad_column.lower()
    similarities = []

    for col in available_columns:
        col_lower = col.lower()

        # Direct substring match
        if bad_lower in col_lower or col_lower in bad_lower:
            similarities.append((col, 0.9))

        # Fuzzy string matching
        else:
            ratio = SequenceMatcher(None, bad_lower, col_lower).ratio()
            if ratio > 0.6:  # 60% similarity threshold
                similarities.append((col, ratio))

    # Sort by similarity score
    similarities.sort(key=lambda x: x[1], reverse=True)

    return [col for col, score in similarities[:3]]  # Return top 3 matches


async def get_ai_column_fix(sql: str, bad_column: str, available_columns: List[str], question: str, user_id: str,
                            channel_id: str) -> Optional[str]:
    """Use OpenAI to suggest column name fix"""

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return None

    instructions = f"""You are a SQL expert fixing a column name error.

PROBLEM: The column '{bad_column}' does not exist in the table.

AVAILABLE COLUMNS:
{', '.join(available_columns[:30])}

YOUR TASK:
1. Find the correct column name from the available columns that matches the intent
2. Replace '{bad_column}' with the correct column name in the SQL
3. Return ONLY the corrected SQL query

Original Question: {question}

The goal is to find the column that best represents what '{bad_column}' was trying to reference."""

    message = f"""Fix this SQL by replacing the invalid column '{bad_column}' with the correct column name:

```sql
{sql}
```

Available columns: {', '.join(available_columns[:20])}"""

    try:
        response = await send_message_and_run(thread_id, message, instructions)

        # Extract SQL from response
        fixed_sql = extract_sql_from_response(response)

        if fixed_sql and fixed_sql != sql:
            return fixed_sql

    except Exception as e:
        print(f"⚠️ AI column fix failed: {e}")

    return None


async def fix_invalid_table_error(sql: str, error_message: str, selected_table: str) -> Optional[str]:
    """Fix invalid table name errors"""

    # If the error is about table name, ensure we're using the correct selected table
    if selected_table and selected_table not in sql:
        print(f"🔄 Replacing table reference with: {selected_table}")

        # Try to find and replace table names in FROM clause
        import re

        # Pattern to match FROM clause
        from_pattern = r'FROM\s+([^\s\n]+)'
        match = re.search(from_pattern, sql, re.IGNORECASE)

        if match:
            old_table = match.group(1)
            fixed_sql = sql.replace(old_table, selected_table)
            return fixed_sql

    return None


async def fix_syntax_error(sql: str, error_message: str, question: str, user_id: str, channel_id: str) -> Optional[str]:
    """Fix general syntax errors using OpenAI"""

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return None

    instructions = f"""You are a SQL expert fixing syntax errors.

PROBLEM: The SQL has a syntax error: {error_message}

YOUR TASK:
1. Identify and fix the syntax error
2. Ensure the SQL is valid Snowflake syntax
3. Return ONLY the corrected SQL query
4. Do not change the logic or intent of the query, only fix syntax issues

Original Question: {question}"""

    message = f"""Fix the syntax error in this SQL:

```sql
{sql}
```

Error: {error_message}"""

    try:
        response = await send_message_and_run(thread_id, message, instructions)
        fixed_sql = extract_sql_from_response(response)

        if fixed_sql and fixed_sql != sql:
            return fixed_sql

    except Exception as e:
        print(f"⚠️ AI syntax fix failed: {e}")

    return None


async def fix_groupby_error(sql: str, error_message: str) -> Optional[str]:
    """Fix GROUP BY related errors"""

    # Common GROUP BY fixes
    import re

    # If error mentions specific columns not in GROUP BY
    if "must appear in group by" in error_message.lower():
        # Extract column names that need to be added to GROUP BY
        column_pattern = r"column '([^']+)' must appear in group by"
        matches = re.findall(column_pattern, error_message, re.IGNORECASE)

        if matches:
            # Find existing GROUP BY clause
            groupby_pattern = r'GROUP BY\s+([^\n\r]+)'
            groupby_match = re.search(groupby_pattern, sql, re.IGNORECASE)

            if groupby_match:
                existing_groupby = groupby_match.group(1).strip()
                new_columns = ", ".join(matches)
                new_groupby = f"{existing_groupby}, {new_columns}"

                fixed_sql = re.sub(groupby_pattern, f"GROUP BY {new_groupby}", sql, flags=re.IGNORECASE)
                return fixed_sql

    return None


async def fix_datatype_error(sql: str, error_message: str) -> Optional[str]:
    """Fix data type conversion errors"""

    # Add explicit type casting for common issues
    import re

    if "cannot convert" in error_message.lower():
        # Add CAST statements for problematic fields
        # This is a basic implementation - could be enhanced

        # Look for common patterns that need casting
        patterns = [
            (r'(\w+)\s*=\s*(\d+)', r'CAST(\1 AS VARCHAR) = CAST(\2 AS VARCHAR)'),  # Number comparisons
            (r'(\w+)\s*=\s*\'([^\']+)\'', r'CAST(\1 AS VARCHAR) = \'\2\''),  # String comparisons
        ]

        fixed_sql = sql
        for pattern, replacement in patterns:
            fixed_sql = re.sub(pattern, replacement, fixed_sql)

        if fixed_sql != sql:
            return fixed_sql

    return None


# Cache clearing functions
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


async def clear_conversation_cache():
    """Clear conversation context cache"""
    _local_cache['conversation'].clear()
    print("🧹 Conversation cache cleared")


async def clear_table_selection_cache():
    """Clear table selection cache"""
    _local_cache['table_selection'].clear()
    _local_cache['table_samples'].clear()
    # Clear from Valkey too
    for key in list(_local_cache.get('table_selection', {}).keys()):
        await safe_valkey_delete(key)
    print("🧹 Table selection cache cleared")


async def clear_feedback_cache():
    """Clear feedback cache"""
    _local_cache['feedback'].clear()
    print("🧹 Feedback cache cleared")


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