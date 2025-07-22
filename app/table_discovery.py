import os
import json
import re
import time
import traceback
from typing import List, Dict, Any, Tuple, Optional
from app.cache_manager import cache_manager, TABLE_SAMPLES_PREFIX

# Import Snowflake runner for schema discovery
try:
    from app.snowflake_runner import run_query
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("⚠️ Snowflake runner not available for schema discovery")


class TableDiscovery:
    """Handles table discovery, schema discovery, and table sampling"""
    
    def __init__(self):
        self.cache_manager = cache_manager
    
    async def find_relevant_tables_from_vector_store(self, question: str, user_id: str, channel_id: str, top_k: int = 8) -> List[str]:
        """Find relevant tables using pattern.md analysis instead of vector store"""
        print(f"🔍 Searching for tables relevant to: {question}")
        
        try:
            # Use pattern.md for table selection instead of vector store
            relevant_tables = await self._find_tables_from_pattern_file(question)
            return relevant_tables[:top_k]
            
        except Exception as e:
            print(f"❌ Pattern-based table search error: {e}")
            return []
    
    async def _find_tables_from_pattern_file(self, question: str) -> List[str]:
        """Analyze question against pattern.md to find relevant tables"""
        import os
        
        question_lower = question.lower()
        table_scores = {}
        
        # Read pattern file
        pattern_file_path = os.path.join(os.path.dirname(__file__), '..', 'resources', 'pattern_file.md')
        
        try:
            with open(pattern_file_path, 'r') as f:
                pattern_content = f.read()
        except FileNotFoundError:
            print(f"⚠️  Pattern file not found at {pattern_file_path}")
            return []
        
        # Define keyword mappings based on pattern.md content
        keyword_mappings = {
            # Response time questions - EXCLUSIVE to RPT_WOPS_TICKETS
            'response_time': {
                'keywords': ['response time', 'reply time', 'resolution time', 'sla compliance', 'sla', 'turnaround time', 'time to respond', 'time to resolve', 'average response', 'response distribution', 'response trends', 'response speed'],
                'tables': ['ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS'],
                'priority': 150  # EXCLUSIVE - highest priority
            },
            
            # FCR questions
            'fcr': {
                'keywords': ['fcr', 'first contact resolution', 'repeat contact', 'channel switching', 'callback', 'call back', 'same issue', 'resolved first time', 'multiple contacts', 'customer contacted again'],
                'tables': ['ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS'],
                'priority': 125
            },
            
            # Agent performance and counting questions
            'agent_performance': {
                'keywords': ['agent performance', 'agent metrics', 'agent productivity', 'agent efficiency', 'agent statistics', 'agent dashboard', 'agent comparison', 'agent ranking', 'which agent', 'best agent', 'top agent', 'how many agents', 'agent count', 'number of agents', 'count agents', 'agent', 'agents', 'qa score', 'qa scores', 'quality score', 'quality scores', 'quality metrics', 'quality rating', 'agent quality', 'performance score'],
                'tables': ['ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE', 'ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME'],
                'priority': 200  # Increased priority for agent questions
            },
            
            # Team lead performance and team questions
            'team_lead': {
                'keywords': ['team lead performance', 'supervisor metrics', 'manager performance', 'team leader analysis', 'supervisor analysis', 'team performance', 'team lead dashboard', 'team', 'supervisor', 'manager', "'s team", 'in team'],
                'tables': ['ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE', 'ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TL_PERFORMANCE'],
                'priority': 250  # Highest priority for team/supervisor questions
            },
            
            # Handle time questions
            'handle_time': {
                'keywords': ['handle time', 'aht', 'average handle time', 'handling time', 'efficiency', 'agent efficiency', 'time per ticket', 'call duration', 'efficiency metrics'],
                'tables': ['ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME', 'ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE'],
                'priority': 125
            },
            
            # Schedule adherence questions
            'schedule_adherence': {
                'keywords': ['schedule adherence', 'adherence rate', 'schedule compliance', 'schedule variance', 'offline time', 'break adherence', 'schedule patterns', 'adherence trends', 'schedule analysis', 'schedule performance', 'adherence metrics', 'schedule monitoring', 'schedule effectiveness', 'adherence dashboard', 'adherence comparison', 'schedule following', 'time tracking', 'work schedule', 'attendance patterns', 'adherent', 'adherence'],
                'tables': ['ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE'],
                'priority': 300  # HIGHEST priority for schedule adherence questions
            },
            
            # Ticket volume (default)
            'ticket_volume': {
                'keywords': ['ticket volume', 'ticket count', 'how many tickets', 'ticket trends', 'ticket distribution', 'tickets created', 'tickets solved', 'volume analysis'],
                'tables': ['ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS'],
                'priority': 100
            }
        }
        
        # Score each keyword category
        for category, config in keyword_mappings.items():
            score = 0
            matched_keywords = []
            
            for keyword in config['keywords']:
                if keyword in question_lower:
                    score += config['priority']
                    matched_keywords.append(keyword)
            
            # Add tables from this category if any keywords matched
            if score > 0:
                for table in config['tables']:
                    if table not in table_scores:
                        table_scores[table] = {'score': 0, 'categories': [], 'keywords': []}
                    table_scores[table]['score'] += score
                    table_scores[table]['categories'].append(category)
                    table_scores[table]['keywords'].extend(matched_keywords)
        
        # If no keywords matched, default to ticket volume table
        if not table_scores:
            table_scores['ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS'] = {
                'score': 50,
                'categories': ['default'],
                'keywords': ['fallback']
            }
        
        # Sort tables by score
        sorted_tables = sorted(table_scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        # Return just the table names
        result_tables = [table for table, data in sorted_tables]
        
        print(f"📊 Table scoring results:")
        for table, data in sorted_tables[:3]:  # Show top 3
            print(f"   {table.split('.')[-1]}: {data['score']} points ({data['categories']})")
        
        return result_tables
    
    async def sample_table_data(self, table_name: str, sample_size: int = 10) -> Dict[str, Any]:
        """Sample random rows from a table to understand its structure and content"""
        print(f"📊 Sampling {sample_size} rows from {table_name}")

        # Check cache first
        cache_key = f"{TABLE_SAMPLES_PREFIX}:{table_name}:{sample_size}"
        cached_sample = await self.cache_manager.get(cache_key)

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
            await self.cache_manager.set(cache_key, sample_info, ex=3600)  # 1 hour expiry

            print(f"✅ Sampled {len(df)} rows from {table_name} ({len(columns)} columns)")
            if audit_columns_found:
                print(f"📋 Audit columns found: {', '.join(audit_columns_found)}")

            return sample_info

        except Exception as e:
            print(f"❌ Error sampling table {table_name}: {str(e)}")
            traceback.print_exc()
            return {'error': str(e)}

    async def select_best_table_using_samples(self, question: str, candidate_tables: List[str], user_id: str, channel_id: str) -> Tuple[str, str]:
        """
        Sample data from candidate tables and use assistant to select the best one
        Returns: (selected_table, reason)
        """
        print(f"\n🔍 Analyzing {len(candidate_tables)} candidate tables for question: {question[:50]}...")

        # Sample data from each candidate table
        table_samples = {}
        sample_errors = []

        for table in candidate_tables:
            sample = await self.sample_table_data(table, sample_size=10)
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

        # For now, return the first sampled table
        # In the full implementation, this would use the assistant to analyze
        selected_table = list(table_samples.keys())[0]
        return selected_table, f"Selected based on successful sampling of {len(table_samples)} tables"

    async def discover_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Discover comprehensive schema for a table"""
        if not SNOWFLAKE_AVAILABLE:
            return {'error': 'Snowflake not available'}

        try:
            # Get column information
            column_info_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, COMMENT, IS_NULLABLE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name.split('.')[-1].upper()}'
            AND TABLE_SCHEMA = '{table_name.split('.')[-2].upper()}'
            AND TABLE_CATALOG = '{table_name.split('.')[-3].upper()}'
            ORDER BY ORDINAL_POSITION
            """

            column_info = run_query(column_info_sql)
            
            if isinstance(column_info, str):
                return {'error': column_info}

            columns = []
            column_descriptions = {}
            
            if hasattr(column_info, 'iterrows'):
                for _, row in column_info.iterrows():
                    col_name = row['COLUMN_NAME']
                    columns.append(col_name)
                    column_descriptions[col_name.lower()] = {
                        'type': row['DATA_TYPE'],
                        'comment': row.get('COMMENT', ''),
                        'nullable': row.get('IS_NULLABLE', 'YES') == 'YES',
                        'default': row.get('COLUMN_DEFAULT', '')
                    }

            schema_info = {
                'table': table_name,
                'columns': columns,
                'column_descriptions': column_descriptions,
                'discovered_at': time.time()
            }

            return schema_info

        except Exception as e:
            print(f"❌ Error discovering schema for {table_name}: {str(e)}")
            return {'error': str(e)}

    async def get_table_descriptions_from_manifest(self, tables: List[str], user_id: str, channel_id: str) -> Dict[str, str]:
        """Get table descriptions from dbt manifest"""
        # This would need the thread management functions
        # For now, returning empty dict as placeholder
        print(f"📊 Getting descriptions for {len(tables)} tables")
        return {}

    async def debug_table_selection(self, question: str, user_id: str, channel_id: str) -> str:
        """Debug the table selection process"""
        debug_info = []
        
        debug_info.append(f"Question: {question}")
        debug_info.append(f"User: {user_id}")
        debug_info.append(f"Channel: {channel_id}")
        
        # Find candidates
        candidates = await self.find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k=6)
        debug_info.append(f"Candidates found: {len(candidates)}")
        
        for table in candidates:
            debug_info.append(f"  - {table}")
        
        if candidates:
            # Try to select best
            selected, reason = await self.select_best_table_using_samples(question, candidates, user_id, channel_id)
            debug_info.append(f"Selected: {selected}")
            debug_info.append(f"Reason: {reason}")
        
        return "\n".join(debug_info)


# Global table discovery instance
table_discovery = TableDiscovery()

# Convenience functions for backward compatibility
async def find_relevant_tables_from_vector_store(question: str, user_id: str, channel_id: str, top_k: int = 8) -> List[str]:
    """Find relevant tables"""
    return await table_discovery.find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k)

async def sample_table_data(table_name: str, sample_size: int = 10) -> Dict[str, Any]:
    """Sample table data"""
    return await table_discovery.sample_table_data(table_name, sample_size)

async def select_best_table_using_samples(question: str, candidate_tables: List[str], user_id: str, channel_id: str) -> Tuple[str, str]:
    """Select best table"""
    return await table_discovery.select_best_table_using_samples(question, candidate_tables, user_id, channel_id)

async def discover_table_schema(table_name: str) -> Dict[str, Any]:
    """Discover table schema"""
    return await table_discovery.discover_table_schema(table_name)

async def get_table_descriptions_from_manifest(tables: List[str], user_id: str, channel_id: str) -> Dict[str, str]:
    """Get table descriptions"""
    return await table_discovery.get_table_descriptions_from_manifest(tables, user_id, channel_id)

async def debug_table_selection(question: str, user_id: str, channel_id: str) -> str:
    """Debug table selection"""
    return await table_discovery.debug_table_selection(question, user_id, channel_id)