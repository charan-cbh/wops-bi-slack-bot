import os
import asyncio
import time
import json
import traceback
import re
from typing import Optional, Dict, Any, Tuple, List
from dotenv import load_dotenv
from openai import OpenAI
from glide import GlideClient, GlideClientConfiguration, NodeAddress, GlideClusterClient, \
    GlideClusterClientConfiguration

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

# Stop words for phrase extraction
STOP_WORDS = {'the', 'is', 'at', 'which', 'on', 'and', 'a', 'an', 'as', 'are',
              'was', 'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does',
              'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must',
              'shall', 'to', 'of', 'in', 'for', 'with', 'by', 'from', 'about'}

# Minimal pattern recognition for intent
INTENT_PATTERNS = {
    'count': ['how many', 'count', 'total number', 'number of', 'volume', 'quantity'],
    'time_filter': ['today', 'yesterday', 'last week', 'this week', 'last month', 'this month',
                    'last year', 'this year', 'last 7 days', 'last 30 days', 'last 90 days'],
    'group_filter': ['messaging', 'voice', 'email', 'chat', 'phone', 'api'],
    'metrics': ['reply time', 'response time', 'resolution time', 'aht', 'average handling',
                'first reply', 'satisfaction', 'csat', 'occupancy', 'adherence', 'kpi',
                'performance', 'productivity', 'agent performance'],
    'ranking': ['highest', 'lowest', 'top', 'bottom', 'most', 'least', 'best', 'worst',
                'maximum', 'minimum', 'ranked', 'order by'],
    'comparison': ['compare', 'versus', 'vs', 'between', 'difference', 'correlation',
                   'relationship', 'impact', 'affect', 'influence'],
    'aggregation': ['average', 'avg', 'mean', 'sum', 'total', 'median', 'min', 'max',
                    'percentile', 'p50', 'p90', 'p95', 'p99'],
    'entities': ['agent', 'team', 'leader', 'supervisor', 'ticket', 'customer', 'contact']
}

# Simple table mapping based on keywords (fallback)
QUESTION_TO_TABLE_MAP = {
    "tickets": {
        "keywords": ["ticket", "tickets", "created", "volume", "count", "zendesk"],
        "table": "ANALYTICS.dbt_production.fct_zendesk__mqr_tickets",
        "description": "Use for ticket counts, volume, creation dates"
    },
    "agent_performance": {
        "keywords": ["agent", "performance", "kpi", "kpis", "aht", "csat", "fcr", "productivity"],
        "table": "ANALYTICS.dbt_production.wops_agent_performance",
        "description": "Use for agent KPIs and performance metrics"
    },
    "csat_survey": {
        "keywords": ["csat", "survey", "satisfaction", "feedback"],
        "table": "ANALYTICS.dbt_production.rpt_csat_survey_details",
        "description": "Use for CSAT survey details"
    },
    "ticket_handle_time": {
        "keywords": ["handle", "handling time", "aht", "duration"],
        "table": "ANALYTICS.dbt_production.zendesk_ticket_agent__handle_time",
        "description": "Use for ticket handling time analysis"
    }
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
        # Check if bigram matches any known pattern
        for pattern_list in INTENT_PATTERNS.values():
            if bigram in pattern_list:
                phrases.append(bigram)
                break

    # Trigrams for complex patterns
    for i in range(len(words) - 2):
        trigram = f"{words[i]} {words[i + 1]} {words[i + 2]}"
        if any(pattern in trigram for pattern in ['average handling time', 'first reply time', 'agent performance']):
            phrases.append(trigram)

    return list(set(phrases))  # Remove duplicates


def classify_question_type(question: str) -> str:
    """Simple classification - is this a data query or conversation?"""
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

    # Check for meta/help indicators
    meta_indicators = [
        'what can you', 'help', 'capabilities', 'questions can',
        'how do you work', 'what data', 'explain how',
        'how to use', 'what commands'
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

    # Check if it's asking for specific data
    if any(indicator in question_lower for indicator in data_indicators):
        # Double check it's not asking about the data source
        if 'source' in question_lower or ('where' in question_lower and 'data' in question_lower):
            return 'conversational'
        return 'sql_required'

    if any(indicator in question_lower for indicator in meta_indicators):
        return 'conversational'
    elif any(indicator in question_lower for indicator in general_followup_indicators):
        # But if it also contains data indicators, it might be a data query
        if any(indicator in question_lower for indicator in data_indicators):
            return 'sql_required'
        return 'conversational'
    else:
        # For ambiguous cases, check if it contains entities or time references
        entities = ['ticket', 'agent', 'customer', 'zendesk', 'chat', 'email', 'messaging']
        time_refs = ['today', 'yesterday', 'week', 'month', 'year', 'date']

        if any(entity in question_lower for entity in entities) or any(time in question_lower for time in time_refs):
            return 'sql_required'

        # Very short questions are likely follow-ups
        if word_count <= 4:
            return 'conversational'

        return 'sql_required'  # Default to trying SQL


def extract_intent(question: str) -> dict:
    """Extract comprehensive intent from question"""
    question_lower = question.lower()
    intent = {
        'needs_count': any(p in question_lower for p in INTENT_PATTERNS['count']),
        'time_filter': next((t for t in INTENT_PATTERNS['time_filter'] if t in question_lower), None),
        'group_filter': next((g for g in INTENT_PATTERNS['group_filter'] if g in question_lower), None),
        'metric_type': next((m for m in INTENT_PATTERNS['metrics'] if m in question_lower), None),
        'needs_ranking': any(r in question_lower for r in INTENT_PATTERNS['ranking']),
        'needs_comparison': any(c in question_lower for c in INTENT_PATTERNS['comparison']),
        'aggregation_type': next((a for a in INTENT_PATTERNS['aggregation'] if a in question_lower), None),
        'possible_join': any(j in question_lower for j in ['and their', 'with their', 'between', 'across']),
        'entities': [e for e in INTENT_PATTERNS['entities'] if e in question_lower]
    }
    return intent


def fallback_table_selection(question: str) -> Optional[Tuple[str, str]]:
    """Simple keyword-based table selection as fallback"""
    question_lower = question.lower()

    # Check each table mapping
    best_match = None
    best_score = 0

    for key, mapping in QUESTION_TO_TABLE_MAP.items():
        score = sum(1 for keyword in mapping["keywords"] if keyword in question_lower)
        if score > best_score:
            best_score = score
            best_match = mapping

    if best_match and best_score > 0:
        return best_match["table"], best_match["description"]

    return None, None


async def find_relevant_tables_from_vector_store(question: str, user_id: str, channel_id: str, top_k: int = 4) -> List[
    str]:
    """Use assistant's file_search to find relevant tables from dbt manifest"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        print("❌ Could not create thread for vector search")
        return []

    # More explicit instructions to ensure JSON-only response
    instructions = """You are a data expert analyzing the dbt manifest to find relevant tables.

CRITICAL: You must ONLY return a JSON array. Nothing else. No explanations.

Search through table DESCRIPTIONS and COLUMN DESCRIPTIONS for:
1. Tables whose description or columns match the concepts in the question
2. For KPIs and performance metrics, look for tables with agent performance data
3. For ticket counts/volume, look for fact tables about tickets
4. Consider column names and their descriptions carefully

Output format - ONLY this, nothing before or after:
["ANALYTICS.dbt_production.table1", "ANALYTICS.dbt_production.table2"]

Examples:
- Question about tickets created today → ["ANALYTICS.dbt_production.fct_zendesk__mqr_tickets"]
- Question about agent KPIs → ["ANALYTICS.dbt_production.wops_agent_performance"]
"""

    message = f"""Find tables for: {question}

Remember: Return ONLY the JSON array, no other text."""

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
        # Extract anything that looks like a table name (schema.table pattern)
        table_pattern = r'ANALYTICS\.dbt_production\.\w+'
        found_tables = re.findall(table_pattern, response)
        if found_tables:
            tables = list(set(found_tables))  # Remove duplicates
            print(f"🔍 Vector store found {len(tables)} tables (pattern matching)")
            return tables[:top_k]

        print(f"⚠️ Could not extract tables from response: {response[:200]}...")

        # Fallback to keyword-based selection
        print("📋 Using keyword-based fallback for table selection")
        fallback_table, reason = fallback_table_selection(question)
        if fallback_table:
            print(f"✅ Fallback found: {fallback_table} - {reason}")
            return [fallback_table]

        return []

    except Exception as e:
        print(f"❌ Error in vector search: {e}")
        traceback.print_exc()

        # Fallback to keyword-based selection
        fallback_table, reason = fallback_table_selection(question)
        if fallback_table:
            print(f"✅ Fallback found: {fallback_table} - {reason}")
            return [fallback_table]

        return []


async def sample_table_data(table_name: str, sample_size: int = 5) -> Dict[str, Any]:
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
        # Skip total row count - just sample directly
        # Using LIMIT instead of TABLESAMPLE for simplicity
        sample_sql = f"SELECT * FROM {table_name} LIMIT {sample_size}"
        print(f"🔍 Sampling with: {sample_sql}")

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

        # Create sample info (without total_rows to avoid expensive COUNT)
        sample_info = {
            'table': table_name,
            'columns': columns,
            'column_types': dtypes,
            'sample_data': sample_data,
            'sample_size': len(df),
            'cached_at': time.time()
        }

        # Cache the sample
        await safe_valkey_set(cache_key, sample_info, ex=3600)  # 1 hour expiry

        print(f"✅ Sampled {len(df)} rows from {table_name} ({len(columns)} columns)")

        return sample_info

    except Exception as e:
        print(f"❌ Error sampling table {table_name}: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}


async def select_best_table_using_samples(question: str, candidate_tables: List[str], user_id: str, channel_id: str) -> \
Tuple[str, str]:
    """
    Sample data from candidate tables and use assistant to select the best one
    Returns: (selected_table, reason)
    """
    print(f"\n🔍 Analyzing {len(candidate_tables)} candidate tables for question: {question[:50]}...")

    # Sample data from each candidate table
    table_samples = {}
    sample_errors = []

    for table in candidate_tables:
        sample = await sample_table_data(table, sample_size=5)
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

    instructions = """You are a data expert analyzing table samples to select the BEST table for answering the user's question.

CRITICAL ANALYSIS STEPS:
1. Look at the actual column names in each table
2. Check if the table contains the specific metrics mentioned in the question
3. For "KPIs that determine agent performance" - look for tables with performance metrics like AHT, CSAT, QA scores, FCR
4. Verify the table has the right granularity (agent-level for agent questions, ticket-level for ticket questions)
5. Consider the sample data values to understand what each column contains

IMPORTANT:
- "handling time" or "AHT" should have columns with time values (usually in seconds or minutes)
- "agent performance" should have columns with scores, percentages, or time metrics
- Look for columns that directly answer the question, not just related concepts

Return your response in this exact format:
SELECTED_TABLE: <full table name>
REASON: <specific explanation mentioning which columns contain the needed metrics>

Be specific about WHY this table is best - mention the actual column names that contain the data."""

    # Build message with table samples
    message_parts = [f"User Question: {question}\n\nAnalyze these table samples:"]

    for table, sample in table_samples.items():
        message_parts.append(f"\n\n{'=' * 60}")
        message_parts.append(f"TABLE: {table}")
        message_parts.append(f"COLUMNS ({len(sample.get('columns', []))}): {', '.join(sample.get('columns', [])[:30])}")

        if len(sample.get('columns', [])) > 30:
            message_parts.append(f"... and {len(sample.get('columns', [])) - 30} more columns")

        # Show column types for key columns
        if sample.get('column_types'):
            key_cols = [col for col in sample['columns'][:20] if any(
                kw in col.lower() for kw in
                ['time', 'aht', 'score', 'rate', 'percentage', 'kpi', 'performance', 'csat', 'fcr']
            )]
            if key_cols:
                message_parts.append("\nKey Column Types:")
                for col in key_cols[:10]:
                    message_parts.append(f"  - {col}: {sample['column_types'].get(col, 'unknown')}")

        if sample.get('sample_data') and len(sample['sample_data']) > 0:
            message_parts.append("\nSample Data (showing relevant columns):")
            # Show one sample row with key columns
            sample_row = sample['sample_data'][0]

            # Filter to show only relevant columns based on question
            relevant_keywords = ['kpi', 'performance', 'agent', 'aht', 'handling', 'time', 'score',
                                 'csat', 'satisfaction', 'fcr', 'resolution', 'rate', 'percentage',
                                 'productivity', 'quality', 'metric', 'volume', 'count', 'created']

            # Add keywords from the question
            question_words = question.lower().split()
            relevant_keywords.extend([w for w in question_words if len(w) > 3])

            relevant_cols = {}
            for col, val in sample_row.items():
                if val is not None and str(val).strip() != '':
                    # Include if column name contains relevant keywords
                    if any(keyword in col.lower() for keyword in relevant_keywords):
                        relevant_cols[col] = val

            # If no relevant columns found, show first 15 non-null columns
            if not relevant_cols:
                relevant_cols = {k: v for k, v in sample_row.items()
                                 if v is not None and str(v).strip() != ''}
                relevant_cols = dict(list(relevant_cols.items())[:15])

            if relevant_cols:
                message_parts.append("```")
                for col, val in list(relevant_cols.items())[:20]:
                    # Truncate long values
                    val_str = str(val)[:100] + "..." if len(str(val)) > 100 else str(val)
                    message_parts.append(f"{col}: {val_str}")
                message_parts.append("```")

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

    instructions = """Search the dbt manifest for the descriptions of these specific tables.

Return a JSON object with table names as keys and their descriptions as values.
Include information about:
1. Table purpose and description
2. What kind of data it contains
3. Key metrics available in the table

Example: {"table1": "This table contains agent performance metrics including AHT, CSAT scores...", "table2": "This table stores..."}

Return ONLY the JSON object."""

    message = f"Find descriptions for these tables from the dbt manifest:\n{json.dumps(table_names, indent=2)}"

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
            # Run discovery query
            discovery_sql = f"SELECT * FROM {table_name} LIMIT 5"
            print(f"🔍 Running schema discovery query")

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

            schema_info = {
                'table': table_name,
                'columns': columns,
                'sample_data': sample_data,
                'row_count': len(df),
                'discovered_at': time.time()
            }

            print(f"✅ Schema discovered successfully!")
            print(f"📊 Columns ({len(columns)}) - showing first 15:")
            for i, col in enumerate(columns):
                if i < 15:  # Show first 15 columns
                    print(f"   - {col}")
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
    """Handle conversational questions"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    # Get conversation context
    context = await get_conversation_context(user_id, channel_id)

    # Build context-aware instructions
    if context and context.get('last_response_type') == 'sql_results':
        print(f"💬 Using SQL follow-up context")
        print(f"   Previous question: {context.get('last_question', 'unknown')[:100]}")

        instructions = """You are a BI assistant responding to a follow-up question about previous query results.

The user just received data from a SQL query and is asking a follow-up question.
Use the conversation history in this thread to understand what data they're asking about.
Be helpful in explaining the data source, methodology, or clarifying the results.

Use your knowledge from the dbt manifest to provide accurate information about:
- Table purposes and contents
- Column meanings and relationships
- Data sources and transformations
- Business logic and definitions"""
    else:
        print(f"💬 Standard conversational response")
        instructions = """You are a BI assistant. Be helpful and concise.
Use your knowledge from the dbt manifest to answer questions about available data."""

    # Add context to message if available
    message_parts = [f"User question: {user_question}"]

    if context:
        if context.get('last_question'):
            message_parts.append(f"\nPrevious question: {context['last_question']}")
        if context.get('last_response_type') == 'sql_results':
            message_parts.append("\nNote: The user just received SQL query results and is asking a follow-up question.")
        if context.get('last_table_used'):
            message_parts.append(f"Table used in previous query: {context['last_table_used']}")

    message = "\n".join(message_parts)

    response = await send_message_and_run(thread_id, message, instructions)

    return response


async def generate_sql_intelligently(user_question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
    """
    Generate SQL using intelligent table discovery and sampling
    Returns: (sql_query, selected_table)
    """
    print(f"\n🤖 Starting intelligent SQL generation for: {user_question}")

    # Store the fact that we're generating SQL for context
    await update_conversation_context(user_id, channel_id, user_question, "Generating SQL query...", 'sql_generation')

    selected_table = None

    try:
        # Step 1: Check cache for similar questions
        cached_table = await get_cached_table_suggestion(user_question)

        if cached_table:
            print(f"📋 Using cached table suggestion: {cached_table}")
            selected_table = cached_table
            selection_reason = "Based on successful similar queries"
        else:
            # Step 2: Use vector search to find relevant tables from dbt manifest
            print("🔍 Searching dbt manifest for relevant tables...")
            candidate_tables = await find_relevant_tables_from_vector_store(user_question, user_id, channel_id, top_k=4)

            if not candidate_tables:
                print("⚠️ No candidate tables found from vector search")
                return "-- Error: Could not find relevant tables for this question. Please ensure your question mentions specific metrics or entities.", None

            print(f"📊 Found {len(candidate_tables)} candidate tables")

            # Step 3: Sample data from candidate tables and select best one
            selected_table, selection_reason = await select_best_table_using_samples(
                user_question, candidate_tables, user_id, channel_id
            )

            if not selected_table:
                return "-- Error: Could not determine appropriate table", None

            # Cache this selection for future use
            await cache_table_selection(user_question, selected_table, selection_reason)

        # Step 4: Discover schema for the selected table
        schema = await discover_table_schema(selected_table)

        if schema.get('error'):
            print(f"⚠️ Schema discovery failed: {schema.get('error')}")
            schema = {
                'table': selected_table,
                'columns': [],
                'error': schema.get('error')
            }

        # Step 5: Get table description from manifest
        table_descriptions = await get_table_descriptions_from_manifest([selected_table], user_id, channel_id)
        table_description = table_descriptions.get(selected_table, "No description available")

    except Exception as e:
        print(f"⚠️ Error during intelligent table selection: {e}")
        traceback.print_exc()
        return f"-- Error: {str(e)}", None

    # Step 6: Generate SQL with assistant
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "-- Error: Could not create conversation thread", selected_table

    # Extract intent for better SQL generation
    intent = extract_intent(user_question)
    print(f"🎯 Extracted intent: {intent}")

    # Determine query type based on question
    question_lower = user_question.lower()
    is_asking_what_kpis = any(phrase in question_lower for phrase in [
        "what are the kpis", "which kpis", "what kpis", "list kpis",
        "show me kpis", "kpi columns", "performance metrics"
    ])

    is_asking_count = any(phrase in question_lower for phrase in [
        "how many", "count", "number of", "total", "volume"
    ])

    # Build comprehensive instructions
    instructions = f"""You are a SQL expert. Generate SQL for the following question using the selected table.

SELECTED TABLE: {selected_table}
TABLE DESCRIPTION: {table_description}
SELECTION REASON: {selection_reason}

Table Schema:
- Columns: {', '.join(schema.get('columns', [])[:50]) if schema.get('columns') else 'Use file_search to find columns'}
{"- Total columns: " + str(len(schema.get('columns', []))) if schema.get('columns') else ""}

CRITICAL RULES:
1. Use ONLY the selected table and its actual columns
2. Do NOT make up column names - use exact names from the schema
3. For date filters, use appropriate Snowflake date functions
4. For text filters, use LOWER() for case-insensitive matching
5. Include meaningful column aliases
6. Sort results appropriately
7. Limit results to 100 rows unless specifically asked for more
8. For "What are the KPIs" questions, SELECT a sample row to show all KPI columns
9. For COUNT/volume questions, use COUNT(*) with appropriate filters
10. Use appropriate aggregations when needed (AVG, SUM, COUNT, etc.)

SPECIAL HANDLING:
- If asked "What are the KPIs that determine agent performance?", show a sample of data with all performance metric columns
- If asked for ticket volume/count, use COUNT(*) with date filters
- If asked for specific metrics, calculate them appropriately

Return ONLY the SQL query, no explanations."""

    # Build detailed message
    message_parts = [f"Generate SQL for: {user_question}"]
    message_parts.append(f"\nTable: {selected_table}")
    message_parts.append(f"Table Purpose: {table_description[:200]}..." if len(
        table_description) > 200 else f"Table Purpose: {table_description}")

    if schema.get('columns'):
        all_columns = schema['columns']

        # Special handling for "what are the KPIs" questions
        if is_asking_what_kpis:
            message_parts.append("\nIMPORTANT: The user is asking WHAT KPIs exist, not for calculations.")
            message_parts.append(
                "Show a sample row with all KPI/metric columns to demonstrate what metrics are available.")

            # Find KPI-related columns
            kpi_keywords = ['kpi', 'performance', 'score', 'rate', 'percentage', 'aht',
                            'csat', 'fcr', 'quality', 'productivity', 'metric', 'avg',
                            'resolution', 'satisfaction', 'efficiency']
            kpi_cols = [col for col in all_columns if any(kw in col.lower() for kw in kpi_keywords)]

            if kpi_cols:
                message_parts.append(f"\nKPI columns found: {', '.join(kpi_cols[:20])}")
                message_parts.append("Generate SQL to show these KPI columns with sample data (LIMIT 5)")

        # Special handling for count/volume questions
        elif is_asking_count:
            message_parts.append("\nIMPORTANT: Use COUNT(*) for volume/count questions")

            # Find date columns for filtering
            date_cols = [col for col in all_columns if
                         any(d in col.lower() for d in ['date', 'created', 'updated', 'time', 'timestamp'])]
            if date_cols and 'today' in question_lower:
                message_parts.append(f"\nUse date column '{date_cols[0]}' with CURRENT_DATE() for today's filter")

        # Add intent-based hints
        if intent['time_filter']:
            date_cols = [col for col in all_columns if
                         any(d in col.lower() for d in ['date', 'created', 'updated', 'time', 'timestamp'])]
            if date_cols:
                message_parts.append(f"\nFor time filter '{intent['time_filter']}', use column: {date_cols[0]}")

        if intent['needs_ranking']:
            message_parts.append("\nInclude ORDER BY clause for ranking")

        if intent['aggregation_type']:
            message_parts.append(f"\nUse {intent['aggregation_type'].upper()} aggregation function")

    message = "\n".join(message_parts)

    print(f"📝 Sending SQL generation request to assistant")
    response = await send_message_and_run(thread_id, message, instructions)

    # Extract SQL from response
    sql = extract_sql_from_response(response)

    print(f"\n🧠 Generated SQL:")
    print(f"{sql}")
    print(f"{'=' * 60}\n")

    return sql, selected_table


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

    # Last resort - look for any SQL-like content
    if 'SELECT' in response.upper():
        # Extract everything from first SELECT to semicolon
        start = response.upper().find('SELECT')
        end = response.find(';', start)
        if end != -1:
            return response[start:end + 1].strip()

    return "-- Error: Could not extract SQL from response"


async def handle_question(user_question: str, user_id: str, channel_id: str, assistant_id: str = None) -> Tuple[
    str, str]:
    """Main question handler - routes to appropriate handler"""
    global ASSISTANT_ID
    if assistant_id:
        ASSISTANT_ID = assistant_id

    try:
        # Get conversation context FIRST
        context = await get_conversation_context(user_id, channel_id)

        # Initial classification
        question_type = classify_question_type(user_question)

        # Override classification based on context
        if context and context.get('last_response_type') == 'sql_results':
            # If the last response was SQL results, be more inclined to treat follow-ups as conversational
            question_lower = user_question.lower()

            # These patterns after SQL results are almost always conversational
            if any(pattern in question_lower for pattern in [
                'source', 'where', 'why', 'what is', 'explain',
                'how', 'this data', 'that data', 'these',
                'break down', 'tell me more', 'clarify'
            ]):
                print(f"🔄 Reclassifying as conversational (follow-up after SQL results)")
                question_type = 'conversational'

            # Short questions after SQL results are usually follow-ups
            elif len(user_question.split()) <= 8:
                print(f"🔄 Reclassifying as conversational (short follow-up after SQL results)")
                question_type = 'conversational'

        print(f"\n{'=' * 60}")
        print(f"🔍 Processing question: {user_question}")
        print(f"📊 Question type: {question_type}")
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
                        print(f"💰 Using cached SQL (success_count: {cached_sql['success_count']})")
                        sql = cached_sql['sql']
                        print(f"📝 Cached SQL:\n{sql}")
                        return sql, 'sql'
                else:
                    print(f"🔄 Generating new SQL (no successful cache found)")
            except Exception as cache_error:
                print(f"⚠️ Error checking cache: {cache_error}")
                print(f"🔄 Proceeding to generate new SQL")

            # Generate new SQL using intelligent table discovery
            sql_query, selected_table = await generate_sql_intelligently(user_question, user_id, channel_id)

            # Store the selected table in context for feedback
            if selected_table:
                await update_conversation_context(user_id, channel_id, user_question, sql_query, 'sql', selected_table)

            return sql_query, 'sql'

        else:
            # Handle conversational
            response = await handle_conversational_question(user_question, user_id, channel_id)
            return response, 'conversational'

    except Exception as e:
        print(f"❌ Error in handle_question: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        traceback.print_exc()

        # Return error as SQL comment to be handled by the caller
        return f"-- Error: {str(e)}", 'error'


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
    """Summarize results - keep it simple"""
    if assistant_id:
        global ASSISTANT_ID
        ASSISTANT_ID = assistant_id

    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    instructions = """Provide a concise business summary with key insights. 

IMPORTANT RULES:
1. Do NOT repeat or rephrase the user's question at the beginning
2. Start directly with the findings or answer
3. Keep the response focused and to-the-point (2-3 paragraphs max)
4. Use bullet points for multiple items or metrics
5. Include specific numbers and percentages from the data
6. If data is incomplete or shows many null values, mention this as a key finding
7. End with actionable insights or implications when relevant

Focus on what the data shows, not technical details."""

    message = f"""Question: "{user_question}"
Data:
{result_table}

Provide a concise summary of the key findings."""

    response = await send_message_and_run(thread_id, message, instructions)

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

Provide a business summary. If asked about data source, explain it comes from our data warehouse tables."""

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"⚠️ Error: {e}"


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