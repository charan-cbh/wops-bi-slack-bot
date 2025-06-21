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

# Import the pattern matcher
try:
    from app.pattern_matcher import PatternMatcher, PatternBasedQueryBuilder
    pattern_matcher = PatternMatcher()
    pattern_query_builder = PatternBasedQueryBuilder()
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


def classify_question_type(question: str) -> str:
    """Simple classification - is this a data query or conversation?"""
    question_lower = question.lower()

    # FIRST: Check if this is asking for SQL execution
    if any(phrase in question_lower for phrase in [
        'share the results', 'run this', 'execute this', 'run the query',
        'show the results', 'give me the results'
    ]):
        return 'sql_execution_request'

    # SECOND: Check for follow-up indicators about previous results
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
        'calculate', 'sum', 'aggregate', 'breakdown',
        # Add PST/timezone indicators
        'pst', 'pdt', 'pacific time', 'timezone', 'time zone'
    ]

    # IMPORTANT: Data indicators take priority
    if any(indicator in question_lower for indicator in data_indicators):
        # Only treat as conversational if explicitly asking about the source/method
        if 'source' in question_lower or ('where' in question_lower and 'data' in question_lower):
            return 'conversational'
        return 'sql_required'

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
                       ['_updated_at', '_created_at', 'audit', 'modified', 'inserted', 'dw_']):
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

Use the conversation history to understand what data they're asking about.
Be helpful in explaining the data source, methodology, or clarifying the results.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names
- Use bullet points with •"""
    else:
        print(f"💬 Standard conversational response")
        instructions = """You are a BI assistant. Be helpful and concise.
Use your knowledge from the dbt manifest to answer questions about available data.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names"""

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
    """Build specific SQL instructions based on question intent and actual data"""

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

    # Base instructions - emphasizing accuracy
    base_instructions = f"""You are a SQL expert representing the Business Intelligence team. Generate PERFECT Snowflake SQL.

TABLE: {table}
AUDIT COLUMNS: {', '.join(audit_columns) if audit_columns else 'None found'}

AVAILABLE COLUMNS WITH TYPES AND DESCRIPTIONS:
{column_info}

CRITICAL REQUIREMENTS:
This is a sonwflake query so the syntax and query should be return in snowflake query language only.
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

    # Intent-specific guidance (without hardcoded examples)
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

    # Add time filter guidance if needed
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

    # Build the message
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
    sql = validate_and_fix_sql(sql, user_question, selected_table, schema.get('columns', []))

    print(f"\n🧠 Generated SQL:")
    print(f"{sql}")
    print(f"{'=' * 60}\n")

    return sql, selected_table


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


async def generate_sql_intelligently(user_question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
    """
    Generate SQL using intelligent table discovery and intent analysis
    Returns: (sql_query, selected_table)
    """
    # Use the pattern-enhanced generation
    return await generate_sql_with_patterns(user_question, user_id, channel_id)


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


async def handle_question(user_question: str, user_id: str, channel_id: str, assistant_id: str = None) -> Tuple[str, str]:
    """Main question handler - routes to appropriate handler"""
    global ASSISTANT_ID
    if assistant_id:
        ASSISTANT_ID = assistant_id

    try:
        # Get conversation context FIRST
        context = await get_conversation_context(user_id, channel_id)

        # Initial classification
        question_type = classify_question_type(user_question)

        # Special handling for SQL execution requests
        if question_type == 'sql_execution_request' and context:
            # Check if we have a SQL query in context
            if context.get('last_response_type') == 'sql_shown' and context.get('last_sql'):
                # Return the SQL that was shown for execution
                sql_query = context['last_sql']
                selected_table = context.get('last_table_used')
                return sql_query, 'sql'

        # IMPORTANT: Don't override sql_required classification
        if question_type == 'sql_required':
            # This is definitely a data query - keep it!
            print(f"🎯 Keeping sql_required classification")
        elif context and context.get('last_response_type') == 'sql_results':
            # Only override for TRUE conversational follow-ups
            question_lower = user_question.lower()

            # These patterns after SQL results are conversational
            if any(pattern in question_lower for pattern in [
                'source', 'where does', 'why', 'what is', 'explain',
                'how did you', 'this data', 'that data', 'these',
                'break down', 'tell me more', 'clarify'
            ]):
                print(f"🔄 Reclassifying as conversational (follow-up after SQL results)")
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

            # Generate new SQL using intelligent table discovery with patterns
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
        return f"-- Error: {str(e)}", 'error'

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