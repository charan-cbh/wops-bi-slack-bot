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
    'table_samples': {}  # Cache for table sample data
}

# Cache key prefixes
CACHE_PREFIX = "bi_slack_bot"
THREAD_CACHE_PREFIX = f"{CACHE_PREFIX}:thread"
SQL_CACHE_PREFIX = f"{CACHE_PREFIX}:sql"
SCHEMA_CACHE_PREFIX = f"{CACHE_PREFIX}:schema"
CONVERSATION_CACHE_PREFIX = f"{CACHE_PREFIX}:conversation"
TABLE_SELECTION_PREFIX = f"{CACHE_PREFIX}:table_selection"
TABLE_SAMPLES_PREFIX = f"{CACHE_PREFIX}:table_samples"

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
                'first reply', 'satisfaction', 'csat', 'occupancy', 'adherence'],
    'ranking': ['highest', 'lowest', 'top', 'bottom', 'most', 'least', 'best', 'worst',
                'maximum', 'minimum', 'ranked', 'order by'],
    'comparison': ['compare', 'versus', 'vs', 'between', 'difference', 'correlation',
                   'relationship', 'impact', 'affect', 'influence'],
    'aggregation': ['average', 'avg', 'mean', 'sum', 'total', 'median', 'min', 'max',
                    'percentile', 'p50', 'p90', 'p95', 'p99']
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
        if any(pattern in trigram for pattern in ['average handling time', 'first reply time']):
            phrases.append(trigram)

    return list(set(phrases))  # Remove duplicates


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
        # Get total row count first
        count_sql = f"SELECT COUNT(*) as total_rows FROM {table_name}"
        count_result = run_query(count_sql)

        if isinstance(count_result, str):
            return {'error': count_result}

        total_rows = count_result.iloc[0]['total_rows'] if len(count_result) > 0 else 0

        # Sample random rows using TABLESAMPLE
        sample_sql = f"""
        SELECT * 
        FROM {table_name} 
        TABLESAMPLE ({sample_size} ROWS)
        """

        df = run_query(sample_sql)

        if isinstance(df, str):
            # Fallback to LIMIT if TABLESAMPLE fails
            sample_sql = f"SELECT * FROM {table_name} LIMIT {sample_size}"
            df = run_query(sample_sql)

            if isinstance(df, str):
                return {'error': df}

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

        # Create sample info
        sample_info = {
            'table': table_name,
            'total_rows': total_rows,
            'columns': columns,
            'column_types': dtypes,
            'sample_data': sample_data,
            'sample_size': len(df),
            'cached_at': time.time()
        }

        # Cache the sample
        await safe_valkey_set(cache_key, sample_info, ex=3600)  # 1 hour expiry

        print(f"✅ Sampled {len(df)} rows from {table_name} (Total: {total_rows} rows, {len(columns)} columns)")

        return sample_info

    except Exception as e:
        print(f"❌ Error sampling table {table_name}: {str(e)}")
        return {'error': str(e)}


async def find_relevant_tables_from_vector_store(question: str, user_id: str, channel_id: str, top_k: int = 4) -> List[
    str]:
    """Use assistant's file_search to find relevant tables from dbt manifest"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return []

    instructions = """You are a data expert. Based on the dbt manifest in your vector store, identify the most relevant tables for answering this question.

Return ONLY a JSON array of table names (full names including schema), no explanations.
Example: ["ANALYTICS.dbt_production.fct_tickets", "ANALYTICS.dbt_production.dim_agents"]

Focus on:
1. Tables that contain the metrics mentioned in the question
2. Tables that have the entities (agents, tickets, etc.) mentioned
3. Consider both fact and dimension tables if needed

Return ONLY the JSON array, nothing else."""

    message = f"Find the top {top_k} most relevant tables for this question: {question}"

    response = await send_message_and_run(thread_id, message, instructions)

    try:
        # Extract JSON array from response
        if '[' in response and ']' in response:
            json_start = response.find('[')
            json_end = response.rfind(']') + 1
            json_str = response[json_start:json_end]
            tables = json.loads(json_str)

            print(f"🔍 Vector store suggested {len(tables)} tables: {tables}")
            return tables[:top_k]
    except Exception as e:
        print(f"⚠️ Error parsing table suggestions: {e}")
        print(f"⚠️ Raw response: {response}")

    return []


async def select_best_table_using_samples(question: str, candidate_tables: List[str], user_id: str, channel_id: str) -> \
Tuple[str, str]:
    """
    Sample data from candidate tables and use assistant to select the best one
    Returns: (selected_table, reason)
    """
    print(f"\n🔍 Analyzing {len(candidate_tables)} candidate tables for question: {question[:50]}...")

    # Sample data from each candidate table
    table_samples = {}
    for table in candidate_tables:
        sample = await sample_table_data(table, sample_size=5)
        if not sample.get('error'):
            table_samples[table] = sample
        else:
            print(f"⚠️ Could not sample {table}: {sample.get('error')}")

    if not table_samples:
        print("❌ Could not sample any tables")
        return candidate_tables[0] if candidate_tables else "", "No tables could be sampled"

    # Use assistant to analyze samples and select best table
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return list(table_samples.keys())[0], "Could not create analysis thread"

    instructions = """You are a data expert. Analyze the sample data from each table and determine which table is BEST suited to answer the user's question.

Consider:
1. Which table actually contains the data needed to answer the question
2. Look at column names and sample values
3. Check if the metrics mentioned in the question exist in the table
4. Verify the table has the right granularity (e.g., agent-level vs ticket-level)

Return your response in this exact format:
SELECTED_TABLE: <full table name>
REASON: <brief explanation of why this table is best>

Be decisive - pick the SINGLE best table."""

    # Build message with table samples
    message_parts = [f"User Question: {question}\n\nTable Samples:"]

    for table, sample in table_samples.items():
        message_parts.append(f"\n\nTable: {table}")
        message_parts.append(f"Total Rows: {sample.get('total_rows', 'Unknown')}")
        message_parts.append(f"Columns: {', '.join(sample.get('columns', [])[:20])}")

        if sample.get('sample_data'):
            message_parts.append("Sample Row:")
            # Show one sample row with key columns
            sample_row = sample['sample_data'][0]
            relevant_cols = {k: v for k, v in sample_row.items()
                             if k and v is not None and str(v).strip() != ''}
            # Limit to 10 columns for readability
            shown_cols = dict(list(relevant_cols.items())[:10])
            message_parts.append(json.dumps(shown_cols, indent=2))

    message = "\n".join(message_parts)

    response = await send_message_and_run(thread_id, message, instructions)

    # Parse response
    selected_table = ""
    reason = ""

    for line in response.split('\n'):
        if line.startswith('SELECTED_TABLE:'):
            selected_table = line.replace('SELECTED_TABLE:', '').strip()
        elif line.startswith('REASON:'):
            reason = line.replace('REASON:', '').strip()

    if not selected_table and table_samples:
        # Fallback to first table if parsing fails
        selected_table = list(table_samples.keys())[0]
        reason = "Failed to parse selection, using first candidate"

    print(f"✅ Selected table: {selected_table}")
    print(f"📝 Reason: {reason}")

    return selected_table, reason


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

    # Check for data query indicators
    data_indicators = [
        'how many', 'count', 'show me', 'list', 'find',
        'highest', 'lowest', 'average', 'total',
        'tickets', 'agents', 'reviews', 'performance',
        'reply time', 'response time', 'resolution',
        'which ticket type', 'what ticket type', 'driving',
        'volume', 'trend', 'compare', 'by group', 'by channel',
        'contact driver', 'handling time', 'aht'
    ]

    # Check for meta/help indicators
    meta_indicators = [
        'what can you', 'help', 'capabilities', 'questions can',
        'how do you work', 'what data', 'explain how'
    ]

    # Check for general follow-up indicators
    general_followup_indicators = [
        'why', 'what does that mean', 'explain that',
        'can you elaborate', 'tell me more',
        'what about', 'how about'
    ]

    # Check context - short questions after data results are often follow-ups
    word_count = len(question.split())
    if word_count <= 8 and any(word in question_lower for word in ['this', 'that', 'these', 'it']):
        return 'conversational'

    if any(indicator in question_lower for indicator in meta_indicators):
        return 'conversational'
    elif any(indicator in question_lower for indicator in general_followup_indicators):
        return 'conversational'
    elif any(indicator in question_lower for indicator in data_indicators):
        # Double check it's not asking about the data source
        if 'source' in question_lower or 'where' in question_lower and 'data' in question_lower:
            return 'conversational'
        return 'sql_required'
    else:
        # For ambiguous cases, check if it's a short question
        if word_count <= 6:
            return 'conversational'  # Likely a follow-up
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
        'possible_join': any(j in question_lower for j in ['and their', 'with their', 'between', 'across'])
    }
    return intent


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
                                      response_type: str = None):
    """Update conversation context for follow-up questions"""
    cache_key = f"{user_id}_{channel_id}"
    redis_key = f"{CONVERSATION_CACHE_PREFIX}:{cache_key}"

    context = {
        'last_question': question,
        'last_response': response,
        'last_response_type': response_type,
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

    message = "\n".join(message_parts)

    response = await send_message_and_run(thread_id, message, instructions)

    return response


async def generate_sql_intelligently(user_question: str, user_id: str, channel_id: str) -> str:
    """Generate SQL using intelligent table discovery and sampling"""
    print(f"\n🤖 Starting intelligent SQL generation for: {user_question}")

    # Store the fact that we're generating SQL for context
    await update_conversation_context(user_id, channel_id, user_question, "Generating SQL query...", 'sql_generation')

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
                return "-- Error: Could not find relevant tables for this question"

            # Step 3: Sample data from candidate tables and select best one
            selected_table, selection_reason = await select_best_table_using_samples(
                user_question, candidate_tables, user_id, channel_id
            )

            if not selected_table:
                return "-- Error: Could not determine appropriate table"

            # Cache this selection for future use
            await cache_table_selection(user_question, selected_table, selection_reason)

        # Step 4: Discover schema for the selected table
        schema = await discover_table_schema(selected_table)

        if schema.get('error'):
            print(f"⚠️ Schema discovery failed: {schema.get('error')}")
            # Don't fail completely, let assistant try with limited info
            schema = {
                'table': selected_table,
                'columns': [],
                'error': schema.get('error')
            }
    except Exception as e:
        print(f"⚠️ Error during intelligent table selection: {e}")
        traceback.print_exc()
        return f"-- Error: {str(e)}"

    # Step 5: Generate SQL with assistant
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "-- Error: Could not create conversation thread"

    # Extract intent for better SQL generation
    intent = extract_intent(user_question)
    print(f"🎯 Extracted intent: {intent}")

    # Build comprehensive instructions
    instructions = f"""You are a SQL expert. Generate SQL for the following question using the selected table.

SELECTED TABLE: {selected_table}
SELECTION REASON: {selection_reason}

Table Schema:
- Columns: {', '.join(schema.get('columns', [])[:30]) if schema.get('columns') else 'Use file_search to find columns'}
{"- Total columns: " + str(len(schema.get('columns', []))) if schema.get('columns') else ""}

CRITICAL RULES:
1. Use ONLY the selected table and its actual columns
2. Do NOT make up column names - use exact names from the schema
3. For date filters, use appropriate Snowflake date functions
4. For text filters, use LOWER() for case-insensitive matching
5. Include meaningful column aliases
6. Sort results appropriately
7. Limit results to 100 rows unless specifically asked for more

Return ONLY the SQL query, no explanations."""

    # Build detailed message
    message_parts = [f"Generate SQL for: {user_question}"]
    message_parts.append(f"\nTable: {selected_table}")

    if schema.get('columns'):
        # Add intent-based hints
        if intent['time_filter']:
            date_cols = [col for col in schema['columns'] if
                         any(d in col.lower() for d in ['date', 'created', 'updated', 'time'])]
            if date_cols:
                message_parts.append(f"\nFor time filter '{intent['time_filter']}', use column: {date_cols[0]}")

        if intent['metric_type']:
            metric_cols = [col for col in schema['columns'] if intent['metric_type'].replace(' ', '_') in col.lower()]
            if metric_cols:
                message_parts.append(
                    f"\nFor metric '{intent['metric_type']}', found columns: {', '.join(metric_cols[:3])}")

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
            sql_query = await generate_sql_intelligently(user_question, user_id, channel_id)
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

    instructions = """Provide a clear business summary. Focus on insights, not technical details.
If you can identify which table this data came from based on the columns shown, mention it.
Use your knowledge from the dbt manifest to provide context about what the data represents."""

    message = f"""Question: "{user_question}"
Data:
{result_table}

Summarize the key findings."""

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
        for i, table in enumerate(candidates, 1):
            result += f"  {i}. {table}\n"
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
            "table_samples": len(_local_cache.get('table_samples', {}))
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

    return "\n".join(insights)


def test_question_classification():
    """Test question classification"""
    test_cases = [
        # SQL queries
        ("How many messaging tickets had a reply time over 15 minutes?", "sql_required"),
        ("Which ticket type is driving Chat AHT for agent Jesse?", "sql_required"),
        ("Show me ticket volume by group", "sql_required"),
        ("What is the contact driver with the highest Handling Time?", "sql_required"),
        ("List all agents with high resolution time", "sql_required"),
        ("Find tickets created yesterday", "sql_required"),
        ("Compare agent performance across teams", "sql_required"),
        ("What's the trend in ticket volume this month?", "sql_required"),

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
        "What is the contact driver with the highest Handling Time?",
        test_user, test_channel
    )
    print(f"   Response type: {type1}")
    if type1 == 'sql':
        print(f"   SQL Generated: {response1[:100]}..." if len(response1) > 100 else f"   SQL: {response1}")

    # Simulate that SQL was executed and results were shown
    await update_conversation_context(
        test_user, test_channel,
        "What is the contact driver with the highest Handling Time?",
        "Results showed: Contact driver X with 45 minutes average handling time",
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
        "Which table contains this handling time data?",
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