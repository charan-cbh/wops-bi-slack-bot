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
TABLE_LEARNING_TTL = 2592000  # 30 days for learning data

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
    'table_learning': {}  # Cache for table learning
}

# Cache key prefixes
CACHE_PREFIX = "bi_slack_bot"
THREAD_CACHE_PREFIX = f"{CACHE_PREFIX}:thread"
SQL_CACHE_PREFIX = f"{CACHE_PREFIX}:sql"
SCHEMA_CACHE_PREFIX = f"{CACHE_PREFIX}:schema"
CONVERSATION_CACHE_PREFIX = f"{CACHE_PREFIX}:conversation"
TABLE_LEARNING_PREFIX = f"{CACHE_PREFIX}:table_learning"

# Enhanced table registry with metadata
TABLE_REGISTRY = {
    'tickets': {
        'table_name': 'ANALYTICS.dbt_production.fct_zendesk_tickets',
        'description': 'Zendesk support tickets data',
        'keywords': ['ticket', 'support', 'zendesk', 'reply time', 'response time',
                     'resolution', 'customer', 'issue', 'escalation', 'priority',
                     'messaging', 'chat', 'email', 'channel', 'via', 'satisfaction',
                     'solved', 'closed', 'open', 'pending'],
        'common_metrics': ['reply_time_in_minutes', 'resolution_time', 'ticket_count',
                           'satisfaction_score', 'first_reply_time'],
        'common_columns': ['ticket_id', 'created_at', 'group_name', 'channel', 'status',
                           'priority', 'assignee_id', 'requester_id'],
        'related_tables': ['agents', 'reviews']
    },
    'agents': {
        'table_name': 'ANALYTICS.dbt_production.fct_amazon_connect__agent_metrics',
        'description': 'Amazon Connect agent performance metrics',
        'keywords': ['agent', 'aht', 'handling time', 'calls', 'performance',
                     'productivity', 'amazon connect', 'voice', 'phone', 'occupancy',
                     'adherence', 'contact', 'queue', 'service level', 'abandon',
                     'transfer', 'hold', 'talk time', 'after call work', 'acw'],
        'common_metrics': ['aht', 'calls_handled', 'occupancy_rate', 'adherence_rate',
                           'average_talk_time', 'average_hold_time', 'transfer_rate'],
        'common_columns': ['agent_id', 'agent_name', 'date', 'queue_name', 'shift'],
        'related_tables': ['tickets', 'reviews']
    },
    'reviews': {
        'table_name': 'ANALYTICS.dbt_production.fct_klaus__reviews',
        'description': 'Klaus quality assurance reviews',
        'keywords': ['review', 'klaus', 'qa', 'quality', 'score', 'rating',
                     'feedback', 'evaluation', 'assessment', 'scorecard',
                     'category', 'criteria', 'reviewer', 'reviewee'],
        'common_metrics': ['review_score', 'category_scores', 'critical_count',
                           'avg_score', 'pass_rate'],
        'common_columns': ['review_id', 'reviewer_id', 'reviewee_id', 'created_at',
                           'score', 'category', 'comments'],
        'related_tables': ['agents', 'tickets']
    }
}

# Common column name variations (to help the bot find the right column)
COLUMN_VARIATIONS = {
    'reply_time': ['reply_time_in_minutes', 'first_reply_time', 'first_reply_time_minutes',
                   'reply_time_minutes', 'time_to_first_reply', 'first_response_time'],
    'group': ['group_name', 'channel', 'via_channel', 'ticket_channel', 'group'],
    'created': ['created_at', 'created_date', 'creation_date', 'ticket_created_at'],
    'resolved': ['resolved_at', 'resolution_date', 'closed_at', 'solved_at']
}

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


def identify_table_from_question(question: str) -> Tuple[str, float]:
    """
    Identify which table to use based on question keywords with confidence scoring
    Returns: (table_name, confidence_score)
    """
    question_lower = question.lower()
    question_words = set(question_lower.split())

    scores = {}

    # Check learning data first
    learning_data = _local_cache.get('table_learning', {}).get(f"{TABLE_LEARNING_PREFIX}:data", {})

    # Apply learned patterns
    key_phrases = extract_key_phrases(question)
    learned_scores = {}

    for phrase in key_phrases:
        if phrase in learning_data:
            for table, count in learning_data[phrase].items():
                if table not in learned_scores:
                    learned_scores[table] = 0
                learned_scores[table] += count * 2  # Weight learned patterns higher

    # Score each table
    for table_key, table_info in TABLE_REGISTRY.items():
        score = 0

        # Add learned score if available
        if table_info['table_name'] in learned_scores:
            score += learned_scores[table_info['table_name']]

        # Check keywords (weighted scoring)
        for keyword in table_info['keywords']:
            if keyword in question_lower:
                # Exact phrase match gets higher score
                if len(keyword.split()) > 1:  # Multi-word keyword
                    score += 4
                else:
                    score += 2
            elif any(word in keyword.split() for word in question_words):
                # Partial match
                score += 1

        # Check for metric names
        for metric in table_info.get('common_metrics', []):
            if metric.replace('_', ' ') in question_lower:
                score += 3

        # Check for column names
        for column in table_info.get('common_columns', []):
            if column.replace('_', ' ') in question_lower:
                score += 2

        # Bonus for table name mention
        if table_key in question_lower:
            score += 5

        scores[table_key] = score

    # Get best match
    if scores:
        best_table = max(scores, key=scores.get)
        max_score = max(scores.values())
        confidence = scores[best_table] / max_score if max_score > 0 else 0

        # Log scoring details
        print(f"📊 Table scoring for '{question[:50]}...':")
        for table, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
            print(f"   {table}: {score} points")
        print(f"   Selected: {best_table} (confidence: {confidence:.2f})")

        # If confidence is too low, might need multiple tables
        if confidence < 0.5 and scores[best_table] < 3:
            print(f"⚠️ Low confidence ({confidence:.2f}) for table selection")

        return TABLE_REGISTRY[best_table]['table_name'], confidence

    # Default fallback
    print("⚠️ No table matched, using default tickets table")
    return TABLE_REGISTRY['tickets']['table_name'], 0.0


def identify_tables_for_complex_query(question: str) -> List[Tuple[str, float]]:
    """
    Identify multiple tables that might be needed for a complex query
    Returns: List of (table_name, relevance_score) tuples
    """
    question_lower = question.lower()
    tables_scores = []

    # Check each table's relevance
    for table_key, table_info in TABLE_REGISTRY.items():
        score = 0

        # Count keyword matches
        for keyword in table_info['keywords']:
            if keyword in question_lower:
                score += 2 if len(keyword.split()) > 1 else 1

        # Check metrics
        for metric in table_info.get('common_metrics', []):
            if metric.replace('_', ' ') in question_lower:
                score += 2

        if score > 0:
            tables_scores.append((table_info['table_name'], score))

    # Sort by relevance score
    tables_scores.sort(key=lambda x: x[1], reverse=True)

    # Normalize scores
    if tables_scores and tables_scores[0][1] > 0:
        max_score = tables_scores[0][1]
        tables_scores = [(table, score / max_score) for table, score in tables_scores]

    return tables_scores


async def validate_table_exists(table_name: str) -> Tuple[bool, Optional[str]]:
    """
    Check if a table exists and return its correct name
    Returns: (exists, correct_table_name)
    """
    if not SNOWFLAKE_AVAILABLE:
        return True, table_name  # Assume exists in dev mode

    # Check various possible formats
    table_parts = table_name.split('.')
    table_only = table_parts[-1]

    check_sql = f"""
    SELECT 
        CONCAT(table_schema, '.', table_name) as full_name
    FROM information_schema.tables
    WHERE (
        CONCAT(table_schema, '.', table_name) = '{table_name}'
        OR table_name = '{table_only}'
    )
    AND table_type = 'BASE TABLE'
    LIMIT 1
    """

    try:
        result = run_query(check_sql)
        if not isinstance(result, str) and len(result) > 0:
            return True, result.iloc[0]['full_name']
    except Exception as e:
        print(f"⚠️ Error validating table: {e}")

    return False, None


async def track_successful_query(question: str, table_used: str, result_count: int):
    """Track successful queries to improve future table selection"""
    if result_count <= 0:
        return  # Only track successful queries

    cache_key = f"{TABLE_LEARNING_PREFIX}:data"
    learning_data = await safe_valkey_get(cache_key, {})

    # Extract key phrases from question
    key_phrases = extract_key_phrases(question)

    for phrase in key_phrases:
        if phrase not in learning_data:
            learning_data[phrase] = {}

        if table_used not in learning_data[phrase]:
            learning_data[phrase][table_used] = 0

        learning_data[phrase][table_used] += 1

    # Save to both caches
    await safe_valkey_set(cache_key, learning_data, ex=TABLE_LEARNING_TTL)
    _local_cache['table_learning'][cache_key] = learning_data

    print(f"📚 Tracked successful query pattern: {len(key_phrases)} phrases learned")


async def suggest_tables_for_question(question: str) -> List[Dict]:
    """Suggest possible tables for a question with reasoning"""
    suggestions = []

    # Get all tables with scores
    all_tables = identify_tables_for_complex_query(question)

    for table_name, score in all_tables[:3]:  # Top 3 suggestions
        # Find the table info
        table_info = None
        for key, info in TABLE_REGISTRY.items():
            if info['table_name'] == table_name:
                table_info = info
                break

        if table_info and score > 0:
            # Find matching keywords
            question_lower = question.lower()
            matched_keywords = [k for k in table_info['keywords'] if k in question_lower]

            suggestions.append({
                'table': table_name,
                'description': table_info['description'],
                'confidence': score,
                'reason': f"Keywords matched: {', '.join(matched_keywords[:5])}"
            })

    return suggestions


async def discover_all_tables(schema_name: str = 'ANALYTICS.dbt_production') -> Dict[str, Any]:
    """Discover all tables in the schema"""
    cache_key = f"{SCHEMA_CACHE_PREFIX}:all_tables_{schema_name}"

    # Check cache first
    cached_tables = await safe_valkey_get(cache_key)
    if cached_tables:
        print(f"📋 Using cached table list for {schema_name}")
        return cached_tables

    discovery_sql = f"""
    SELECT 
        table_schema,
        table_name,
        table_type,
        comment
    FROM information_schema.tables
    WHERE table_schema = '{schema_name}'
    AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """

    if SNOWFLAKE_AVAILABLE:
        try:
            df = run_query(discovery_sql)
            if not isinstance(df, str):
                tables = {}
                for _, row in df.iterrows():
                    table_full_name = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
                    tables[row['TABLE_NAME']] = {
                        'full_name': table_full_name,
                        'comment': row.get('COMMENT', ''),
                        'discovered_at': time.time()
                    }

                print(f"🔍 Discovered {len(tables)} tables in {schema_name}")

                # Cache the discovered tables
                await safe_valkey_set(cache_key, tables, ex=SCHEMA_CACHE_TTL)

                return tables
        except Exception as e:
            print(f"❌ Error discovering tables: {e}")

    return {}


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
        'volume', 'trend', 'compare', 'by group', 'by channel'
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

    # Validate table exists first
    exists, correct_name = await validate_table_exists(table_name)
    if not exists:
        print(f"❌ Table {table_name} does not exist")
        return {
            'table': table_name,
            'error': f"Table {table_name} not found",
            'columns': []
        }

    if correct_name and correct_name != table_name:
        print(f"📝 Using correct table name: {correct_name}")
        table_name = correct_name

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
            cache_success = await safe_valkey_set(cache_key, schema_info, ex=SCHEMA_CACHE_TTL)
            if cache_success:
                print(f"💾 Schema cached for future use")
            else:
                print(f"⚠️ Failed to cache schema (non-critical error)")

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

        # Get recent messages from thread for context
        try:
            if use_beta:
                recent_messages = client.beta.threads.messages.list(thread_id=thread_id, limit=5)
            else:
                recent_messages = client.threads.messages.list(thread_id=thread_id, limit=5)

            # Log recent context
            if recent_messages.data:
                print(f"📝 Thread has {len(recent_messages.data)} recent messages for context")
        except:
            print("⚠️ Could not retrieve recent messages")

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

Common follow-ups and how to respond:
- "What is the source?" → Explain that the data comes from our data warehouse tables:
  * Zendesk tickets (fct_zendesk_tickets) - Support ticket data
  * Amazon Connect (fct_amazon_connect__agent_metrics) - Agent performance
  * Klaus (fct_klaus__reviews) - Quality reviews
- "Why is it None?" → Explain that None/null values typically mean no specific value was recorded
- "What are these groups?" → Explain team names and their functions
- "What are these channels?" → Explain ticket submission channels

Be specific about which table the data came from if you can determine it from the context."""
    else:
        print(f"💬 Standard conversational response")
        instructions = """You are a BI assistant. Be helpful and concise.
Available data sources:
- Zendesk tickets data (reply times, resolution, channels)
- Agent performance metrics (AHT, calls, productivity)
- Quality review scores"""

    # Add context to message if available
    message_parts = [f"User question: {user_question}"]

    if context:
        if context.get('last_question'):
            message_parts.append(f"\nPrevious question: {context['last_question']}")
        if context.get('last_response_type') == 'sql_results':
            message_parts.append("\nNote: The user just received SQL query results and is asking a follow-up question.")

    message = "\n".join(message_parts)

    response = await send_message_and_run(thread_id, message, instructions)

    # Note: Conversation context is updated in the slack_handler

    return response


async def generate_sql_intelligently(user_question: str, user_id: str, channel_id: str) -> str:
    """Generate SQL by discovering table structure dynamically"""
    print(f"\n🤖 Starting SQL generation for: {user_question}")

    # Store the fact that we're generating SQL for context
    await update_conversation_context(user_id, channel_id, user_question, "Generating SQL query...", 'sql_generation')

    try:
        # Step 1: Identify likely table with confidence
        table_name, confidence = identify_table_from_question(user_question)
        print(f"📊 Identified table: {table_name} (confidence: {confidence:.2f})")

        # Check if we need multiple tables
        intent = extract_intent(user_question)
        possible_tables = []

        if confidence < 0.5 or intent.get('possible_join'):
            # Get other relevant tables
            all_tables = identify_tables_for_complex_query(user_question)
            possible_tables = [t for t, s in all_tables if s > 0.3 and t != table_name]
            if possible_tables:
                print(f"📊 Additional tables might be needed: {possible_tables}")

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
    print(f"🎯 Extracted intent: {intent}")

    # Look for columns related to the metric
    relevant_keywords = []
    if intent['metric_type']:
        if 'reply' in intent['metric_type'] or 'response' in intent['metric_type']:
            relevant_keywords.extend(['reply_time', 'reply', 'response', 'first'])
        elif 'resolution' in intent['metric_type']:
            relevant_keywords.extend(['resolution', 'resolve', 'resolved'])
        elif 'aht' in intent['metric_type'] or 'handling' in intent['metric_type']:
            relevant_keywords.extend(['aht', 'handling', 'average', 'time'])

    if intent['group_filter']:
        relevant_keywords.extend(['group', 'channel', 'type'])

    relevant_keywords.extend(['created', 'date', 'time'])

    print(f"🔎 Searching for columns with keywords: {relevant_keywords}")
    matching_columns = find_matching_columns(schema, relevant_keywords) if schema.get('columns') else []
    print(f"🔍 Found relevant columns: {matching_columns}")

    # Step 4: Generate SQL with assistant
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "-- Error: Could not create conversation thread"

    # Build comprehensive instructions
    instructions = f"""You are a SQL expert with deep knowledge of our data warehouse.

CRITICAL RULES FOR SQL GENERATION:
1. Primary table selected: {table_name} (confidence: {confidence:.2f})
2. Table description: {TABLE_REGISTRY.get(table_name.split('.')[-1].replace('fct_zendesk_', '').replace('fct_amazon_connect__', '').replace('fct_klaus__', ''), {}).get('description', 'Data table')}
3. {"⚠️ Low confidence - verify this is the correct table" if confidence < 0.5 else "✅ High confidence table match"}

AVAILABLE TABLES AND THEIR PURPOSE:
- ANALYTICS.dbt_production.fct_zendesk_tickets: Ticket data (reply times, resolution, channels, groups)
- ANALYTICS.dbt_production.fct_amazon_connect__agent_metrics: Agent performance (AHT, calls, productivity)
- ANALYTICS.dbt_production.fct_klaus__reviews: Quality review scores

{"CONSIDER JOINS: These tables might also be relevant: " + ", ".join(possible_tables) if possible_tables else ""}

Table: {table_name}
{"Available columns: " + ", ".join(schema['columns'][:50]) if schema.get('columns') else "Columns not discovered - use file_search to find them"}

QUERY REQUIREMENTS:
- Use ONLY columns that exist in the table
- For date filters, use appropriate date functions
- For text filters, use LOWER() for case-insensitive matching
- Include meaningful column aliases
- Sort results appropriately

Return ONLY the SQL query, no explanations."""

    # Build detailed message
    message_parts = [f"Generate SQL for: {user_question}"]
    message_parts.append(f"\nPrimary table: {table_name}")

    if schema.get('columns'):
        all_columns = schema['columns']

        if matching_columns:
            message_parts.append(f"\nRelevant columns found:")
            for col in matching_columns[:10]:
                message_parts.append(f"  - {col}")

        # Provide specific hints based on intent
        if intent['metric_type']:
            metric_cols = [col for col in all_columns if intent['metric_type'].replace(' ', '_') in col.lower()]
            if metric_cols:
                message_parts.append(f"\nFor {intent['metric_type']}, consider: {', '.join(metric_cols[:3])}")

        if intent['time_filter']:
            time_cols = [col for col in all_columns if any(kw in col.lower() for kw in ['created', 'date', 'time'])]
            if time_cols:
                message_parts.append(f"\nFor date filtering, use: {time_cols[0]}")
                message_parts.append(f"Time period: {intent['time_filter']}")

        if intent['needs_ranking']:
            message_parts.append(f"\nInclude ORDER BY clause for ranking")

        if intent['aggregation_type']:
            message_parts.append(f"\nUse {intent['aggregation_type'].upper()} aggregation")

        # Add sample data context
        if schema.get('sample_data') and len(schema['sample_data']) > 0:
            message_parts.append(f"\nSample data structure:")
            sample = schema['sample_data'][0]
            relevant_sample = {k: v for k, v in sample.items()
                               if any(kw in k.lower() for kw in relevant_keywords)}
            if relevant_sample:
                message_parts.append(json.dumps(relevant_sample, indent=2)[:500])
    else:
        message_parts.append("\n⚠️ Could not discover columns. Search for the correct column names.")

    message = "\n".join(message_parts)

    print(f"📝 Sending to assistant with {len(schema.get('columns', []))} discovered columns")

    response = await send_message_and_run(thread_id, message, instructions)

    # Extract SQL from response
    sql = extract_sql_from_response(response)

    # Track the table usage for learning
    if not sql.startswith("--") and not sql.startswith("⚠️"):
        asyncio.create_task(track_successful_query(user_question, table_name, 1))

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

            # Also track for learning
            # Extract table from SQL
            sql_upper = sql_query.upper()
            from_idx = sql_upper.find('FROM')
            if from_idx != -1:
                table_part = sql_query[from_idx + 4:].strip().split()[0]
                await track_successful_query(user_question, table_part, result_count)

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
Include information about the data source when relevant (e.g., which channels, groups, or systems the data comes from).
If you can identify which table this data came from based on the columns shown, mention it."""

    message = f"""Question: "{user_question}"
Data:
{result_table}

Summarize the key findings."""

    response = await send_message_and_run(thread_id, message, instructions)

    # Note: Don't update conversation context here - it's done in execute_sql_and_respond

    return response


async def debug_assistant_search(user_question: str, user_id: str, channel_id: str) -> str:
    """Debug vector store search"""
    thread_id = await get_or_create_thread(user_id, channel_id)
    if not thread_id:
        return "⚠️ Could not create conversation thread"

    message = f"Debug: Search for tables and columns related to: {user_question}"
    response = await send_message_and_run(thread_id, message)
    return response


async def debug_table_selection(question: str) -> str:
    """Debug why a specific table was selected"""
    all_scores = {}

    # Get scores for all tables
    for table_key, table_info in TABLE_REGISTRY.items():
        question_lower = question.lower()
        question_words = set(question_lower.split())

        score = 0
        matched_keywords = []

        # Check keywords
        for keyword in table_info['keywords']:
            if keyword in question_lower:
                score += 4 if len(keyword.split()) > 1 else 2
                matched_keywords.append(keyword)
            elif any(word in keyword.split() for word in question_words):
                score += 1
                matched_keywords.append(f"{keyword} (partial)")

        # Check metrics
        matched_metrics = []
        for metric in table_info.get('common_metrics', []):
            if metric.replace('_', ' ') in question_lower:
                score += 3
                matched_metrics.append(metric)

        all_scores[table_key] = {
            'table': table_info['table_name'],
            'score': score,
            'keywords_matched': matched_keywords,
            'metrics_matched': matched_metrics,
            'description': table_info['description']
        }

    # Sort by score
    sorted_tables = sorted(all_scores.items(), key=lambda x: x[1]['score'], reverse=True)

    result = f"Table scoring for: '{question}'\n\n"
    for table_key, info in sorted_tables:
        result += f"{table_key}: {info['score']} points\n"
        result += f"  Table: {info['table']}\n"
        result += f"  Description: {info['description']}\n"
        if info['keywords_matched']:
            result += f"  Keywords: {', '.join(info['keywords_matched'][:5])}\n"
        if info['metrics_matched']:
            result += f"  Metrics: {', '.join(info['metrics_matched'])}\n"
        result += "\n"

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
            "table_learning": len(_local_cache.get('table_learning', {}))
        }
    }

    # Add schema cache details
    schema_cache = _local_cache.get('schema', {})
    if schema_cache:
        stats['cached_schemas'] = list(schema_cache.keys())

    # Add learning stats
    learning_cache = _local_cache.get('table_learning', {})
    if learning_cache:
        stats['learned_patterns'] = len(learning_cache.get(f"{TABLE_LEARNING_PREFIX}:data", {}))

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
    insights.append(f"  - Learned Patterns: {stats['caches']['table_learning']}")

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

    # Show learning data
    learning_data = _local_cache.get('table_learning', {}).get(f"{TABLE_LEARNING_PREFIX}:data", {})
    if learning_data:
        insights.append(f"\nLearned Patterns:")
        # Show top 5 patterns
        pattern_scores = {}
        for pattern, tables in learning_data.items():
            total = sum(tables.values())
            pattern_scores[pattern] = total

        top_patterns = sorted(pattern_scores.items(), key=lambda x: x[1], reverse=True)[:5]
        for pattern, score in top_patterns:
            insights.append(f"  - '{pattern}': {score} uses")

    return "\n".join(insights)


def test_question_classification():
    """Test question classification"""
    test_cases = [
        # SQL queries
        ("How many messaging tickets had a reply time over 15 minutes?", "sql_required"),
        ("Which ticket type is driving Chat AHT for agent Jesse?", "sql_required"),
        ("Show me ticket volume by group", "sql_required"),
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

    # Test context-aware classification
    print("\n🧪 Testing Context-Aware Classification:")
    print("Simulating: User just received SQL results, then asks follow-up")
    print("-" * 60)

    # Simulate having SQL results context
    followup_tests = [
        ("What is the source for this data?", "conversational"),
        ("Why is it None?", "conversational"),
        ("Break this down by team", "sql_required"),  # This wants new SQL
        ("What are these channels?", "conversational"),
    ]

    for question, expected in followup_tests:
        # Note: In real usage, context would affect classification
        actual = classify_question_type(question)
        status = "✅" if actual == expected else "❌"
        print(f"{status} After SQL results: '{question}' → {actual}")

    print("=" * 60)


def test_table_identification():
    """Test table identification accuracy"""
    test_cases = [
        ("How many tickets did we get yesterday?", "tickets"),
        ("Show me agent AHT for this week", "agents"),
        ("What's the average Klaus score?", "reviews"),
        ("Reply time for messaging tickets", "tickets"),
        ("Agent performance in voice queue", "agents"),
        ("Quality scores by reviewer", "reviews"),
        ("Tickets by channel and group", "tickets"),
        ("Average handling time by agent", "agents"),
        ("Review categories with low scores", "reviews"),
    ]

    print("\n🧪 Testing Table Identification:")
    print("=" * 60)

    for question, expected_key in test_cases:
        table_name, confidence = identify_table_from_question(question)
        expected_table = TABLE_REGISTRY[expected_key]['table_name']

        if table_name == expected_table:
            status = "✅"
        else:
            status = "❌"

        print(f"{status} '{question[:40]}...' → {table_name.split('.')[-1]} (conf: {confidence:.2f})")
        if status == "❌":
            print(f"   Expected: {expected_table.split('.')[-1]}")

    print("=" * 60)


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
        "Which ticket type is driving Chat AHT for agent Jesse?",
        test_user, test_channel
    )
    print(f"   Response type: {type1}")
    if type1 == 'sql':
        print(f"   SQL Generated: {response1[:100]}..." if len(response1) > 100 else f"   SQL: {response1}")

    # Simulate that SQL was executed and results were shown
    await update_conversation_context(
        test_user, test_channel,
        "Which ticket type is driving Chat AHT for agent Jesse?",
        "Results showed: None for ticket type",
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
        "Why is it showing None?",
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

    # First, discover all tables in the schema
    print("\n📊 Discovering all tables in ANALYTICS.dbt_production...")
    all_tables = await discover_all_tables()
    if all_tables:
        print(f"✅ Found {len(all_tables)} tables in the schema")
        results['total_tables'] = len(all_tables)

    # Prime schemas for registered tables
    for table_alias, table_info in TABLE_REGISTRY.items():
        table_name = table_info['table_name']
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
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"✅ Schema cache priming complete: {results['success']} succeeded, {results['failed']} failed")
    if results.get('total_tables'):
        print(f"📊 Total tables in schema: {results['total_tables']}")
    if results['errors']:
        print(f"❌ Errors encountered:")
        for error in results['errors']:
            print(f"   - {error}")
    print("=" * 60 + "\n")

    return results


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


async def clear_table_learning_cache():
    """Clear table learning cache"""
    _local_cache['table_learning'].clear()
    cache_key = f"{TABLE_LEARNING_PREFIX}:data"
    await safe_valkey_delete(cache_key)
    print("🧹 Table learning cache cleared")


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