#!/usr/bin/env python3
"""
Generic Query Orchestrator
Database-agnostic orchestrator that works with any Snowflake database
"""

import asyncio
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
from app.generic_intelligence_system import generic_intelligence, TableIntelligence, ColumnIntelligence


class GenericQueryOrchestrator:
    """
    Generic, database-agnostic orchestrator for SQL generation and execution
    """
    
    def __init__(self):
        self.intelligence = generic_intelligence
        self.table_cache = {}  # Cache for table intelligence
        self.schema_cache = {}  # Cache for schema information
    
    async def get_table_intelligence(self, table_name: str) -> Optional[TableIntelligence]:
        """
        Get or create table intelligence for a given table
        """
        if table_name in self.table_cache:
            return self.table_cache[table_name]
        
        try:
            # Get table schema
            schema = await self._get_table_schema(table_name)
            if not schema:
                return None
            
            # Get sample data
            sample_data = await self._get_table_sample(table_name)
            
            # Analyze table
            table_intel = self.intelligence.analyze_table(
                table_name=table_name,
                columns=schema,
                sample_data=sample_data
            )
            
            # Cache the result
            self.table_cache[table_name] = table_intel
            
            return table_intel
            
        except Exception as e:
            print(f"⚠️ Error analyzing table {table_name}: {e}")
            return None
    
    async def _get_table_schema(self, table_name: str) -> Optional[List[Dict]]:
        """
        Get table schema using existing discovery mechanism
        """
        if table_name in self.schema_cache:
            return self.schema_cache[table_name]
        
        try:
            from app.table_discovery import discover_table_schema
            
            schema_info = await discover_table_schema(table_name)
            
            if schema_info.get('error'):
                print(f"⚠️ Schema discovery error for {table_name}: {schema_info['error']}")
                return None
            
            # Convert to generic format
            columns = []
            for col_name in schema_info.get('columns', []):
                # Get column type if available
                col_type = 'TEXT'  # default
                if 'column_types' in schema_info:
                    col_type = schema_info['column_types'].get(col_name, 'TEXT')
                
                columns.append({
                    'name': col_name,
                    'type': col_type
                })
            
            self.schema_cache[table_name] = columns
            return columns
            
        except Exception as e:
            print(f"⚠️ Error getting schema for {table_name}: {e}")
            return None
    
    async def _get_table_sample(self, table_name: str, limit: int = 10) -> Optional[List[Dict]]:
        """
        Get sample data from table
        """
        try:
            from app.table_discovery import sample_table_data
            
            sample_info = await sample_table_data(table_name, sample_size=limit)
            
            if sample_info.get('error'):
                print(f"⚠️ Sample data error for {table_name}: {sample_info['error']}")
                return None
            
            # Convert to generic format
            sample_data = []
            rows = sample_info.get('sample_data', [])
            columns = sample_info.get('columns', [])
            
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    if i < len(row):
                        row_dict[col] = row[i]
                sample_data.append(row_dict)
            
            return sample_data
            
        except Exception as e:
            print(f"⚠️ Error getting sample data for {table_name}: {e}")
            return None
    
    async def find_best_table_for_question(self, question: str, user_id: str, channel_id: str) -> Optional[str]:
        """
        Find the best table for a question using existing table discovery
        """
        try:
            from app.table_discovery import find_relevant_tables_from_vector_store, select_best_table_using_samples
            
            # Find candidate tables
            candidates = await find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k=5)
            
            if not candidates:
                return None
            
            # Select best table
            selected_table, reason = await select_best_table_using_samples(question, candidates, user_id, channel_id)
            
            print(f"🎯 Generic orchestrator selected table: {selected_table}")
            print(f"📝 Reason: {reason}")
            
            return selected_table
            
        except Exception as e:
            print(f"⚠️ Error finding best table: {e}")
            return None
    
    async def generate_intelligent_sql(self, question: str, user_id: str, channel_id: str) -> Tuple[Optional[str], str]:
        """
        Generate SQL using generic intelligence
        """
        try:
            print(f"🧠 Generic Query Orchestrator: Analyzing question...")
            
            # Step 1: Find best table
            table_name = await self.find_best_table_for_question(question, user_id, channel_id)
            if not table_name:
                return None, "❌ No suitable table found for this question"
            
            # Step 2: Get table intelligence
            table_intel = await self.get_table_intelligence(table_name)
            if not table_intel:
                return None, f"❌ Could not analyze table structure for {table_name}"
            
            # Step 3: Analyze question intent
            question_intent = self.intelligence.infer_question_intent(question)
            
            # Step 4: Generate SQL
            sql, explanation = self.intelligence.generate_generic_sql(question, table_intel, question_intent)
            
            # Step 5: Validate SQL
            validation = self.intelligence.validate_generic_sql(sql, table_intel)
            
            # Step 6: Build full response
            full_explanation = f"""
🧠 GENERIC INTELLIGENCE SYSTEM

{explanation}

🔍 VALIDATION RESULTS:
✅ Valid: {validation['is_valid']}
{'⚠️ Warnings: ' + str(validation['warnings']) if validation['warnings'] else ''}
{'❌ Errors: ' + str(validation['errors']) if validation['errors'] else ''}
{'💡 Suggestions: ' + str(validation['suggestions']) if validation['suggestions'] else ''}

📊 TABLE INTELLIGENCE:
• Time Columns: {table_intel.time_columns}
• Identifier Columns: {table_intel.identifier_columns}
• Metric Columns: {table_intel.metric_columns}
• Dimension Columns: {table_intel.dimension_columns}

🎯 QUESTION INTENT:
• Type: {question_intent['type']}
• Aggregation: {question_intent['requires_aggregation']}
• Filtering: {question_intent['requires_filtering']}
• Ranking: {question_intent['requires_ranking']}
• Performance Context: {question_intent['performance_context']}
• Limit: {question_intent['limit']}

🔧 GENERATED SQL:
```sql
{sql}
```

🌐 ADAPTABILITY:
This system automatically adapts to any Snowflake database by:
• Discovering table structures dynamically
• Inferring column purposes from names and types
• Applying generic business logic patterns
• No hardcoded assumptions about schema
"""
            
            return sql, full_explanation
            
        except Exception as e:
            print(f"❌ Error in generic SQL generation: {e}")
            import traceback
            traceback.print_exc()
            return None, f"❌ Error generating SQL: {str(e)}"
    
    async def analyze_database_structure(self, user_id: str, channel_id: str) -> Dict[str, Any]:
        """
        Analyze the overall database structure
        """
        try:
            from app.table_discovery import find_relevant_tables_from_vector_store
            
            # Get all available tables
            all_tables = await find_relevant_tables_from_vector_store("", user_id, channel_id, top_k=50)
            
            if not all_tables:
                return {'error': 'No tables found in database'}
            
            # Analyze each table
            database_intelligence = {
                'total_tables': len(all_tables),
                'tables': {},
                'patterns': {
                    'fact_tables': [],
                    'dimension_tables': [],
                    'staging_tables': [],
                    'report_tables': []
                },
                'common_metrics': set(),
                'common_dimensions': set()
            }
            
            for table_name in all_tables[:10]:  # Limit to first 10 for performance
                table_intel = await self.get_table_intelligence(table_name)
                if table_intel:
                    database_intelligence['tables'][table_name] = {
                        'purpose': table_intel.likely_purpose,
                        'grain': table_intel.grain,
                        'primary_entity': table_intel.primary_entity,
                        'metric_count': len(table_intel.metric_columns),
                        'dimension_count': len(table_intel.dimension_columns)
                    }
                    
                    # Categorize by purpose
                    if table_intel.likely_purpose == 'fact':
                        database_intelligence['patterns']['fact_tables'].append(table_name)
                    elif table_intel.likely_purpose == 'dimension':
                        database_intelligence['patterns']['dimension_tables'].append(table_name)
                    elif table_intel.likely_purpose == 'staging':
                        database_intelligence['patterns']['staging_tables'].append(table_name)
                    elif table_intel.likely_purpose == 'report':
                        database_intelligence['patterns']['report_tables'].append(table_name)
                    
                    # Collect common patterns
                    database_intelligence['common_metrics'].update(table_intel.metric_columns)
                    database_intelligence['common_dimensions'].update(table_intel.dimension_columns)
            
            # Convert sets to lists for JSON serialization
            database_intelligence['common_metrics'] = list(database_intelligence['common_metrics'])[:20]
            database_intelligence['common_dimensions'] = list(database_intelligence['common_dimensions'])[:20]
            
            return database_intelligence
            
        except Exception as e:
            print(f"❌ Error analyzing database structure: {e}")
            return {'error': str(e)}
    
    def get_system_info(self) -> Dict[str, Any]:
        """
        Get information about the generic intelligence system
        """
        return {
            'system_name': 'Generic Intelligence System',
            'version': '1.0.0',
            'capabilities': [
                'Database-agnostic table analysis',
                'Generic column intelligence',
                'Pattern-based business logic',
                'Dynamic schema discovery',
                'Adaptive SQL generation'
            ],
            'supported_databases': ['Snowflake'],
            'supported_patterns': {
                'column_types': list(self.intelligence.column_patterns.keys()),
                'table_types': list(self.intelligence.table_patterns.keys()),
                'performance_patterns': list(self.intelligence.performance_patterns.keys())
            },
            'cache_status': {
                'tables_cached': len(self.table_cache),
                'schemas_cached': len(self.schema_cache)
            }
        }


# Global instance
generic_orchestrator = GenericQueryOrchestrator()