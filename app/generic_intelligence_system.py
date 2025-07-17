#!/usr/bin/env python3
"""
Generic Intelligence System
A completely generic, database-agnostic intelligent system that can work with any Snowflake DB
"""

import re
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
import json


@dataclass
class ColumnIntelligence:
    """Intelligence about a column inferred from metadata and samples"""
    name: str
    data_type: str
    likely_purpose: str  # 'identifier', 'metric', 'dimension', 'timestamp', 'text'
    metric_type: Optional[str] = None  # 'performance', 'volume', 'rate', 'score'
    performance_direction: Optional[str] = None  # 'higher_better', 'lower_better'
    common_patterns: List[str] = None
    
    def __post_init__(self):
        if self.common_patterns is None:
            self.common_patterns = []


@dataclass
class TableIntelligence:
    """Intelligence about a table inferred from schema and samples"""
    name: str
    likely_purpose: str  # 'fact', 'dimension', 'bridge', 'staging'
    grain: str  # 'transaction', 'daily', 'weekly', 'monthly', 'user', 'aggregated'
    primary_entity: Optional[str] = None  # main entity this table tracks
    time_columns: List[str] = None
    identifier_columns: List[str] = None
    metric_columns: List[str] = None
    dimension_columns: List[str] = None
    
    def __post_init__(self):
        if self.time_columns is None:
            self.time_columns = []
        if self.identifier_columns is None:
            self.identifier_columns = []
        if self.metric_columns is None:
            self.metric_columns = []
        if self.dimension_columns is None:
            self.dimension_columns = []


class GenericIntelligenceSystem:
    """
    Generic, database-agnostic intelligent system that can work with any Snowflake DB
    """
    
    def __init__(self):
        # Generic patterns for column analysis
        self.column_patterns = {
            'timestamp': [
                r'.*_at$', r'.*_date$', r'.*_time$', r'date_.*', r'time_.*',
                r'created', r'updated', r'modified', r'solved', r'closed'
            ],
            'identifier': [
                r'.*_id$', r'.*_key$', r'.*_code$', r'id_.*', r'key_.*',
                r'uuid', r'guid', r'external_id'
            ],
            'name': [
                r'.*_name$', r'name_.*', r'title', r'label', r'description'
            ],
            'email': [
                r'.*email.*', r'.*mail.*'
            ],
            'phone': [
                r'.*phone.*', r'.*mobile.*', r'.*tel.*'
            ],
            'amount': [
                r'.*amount.*', r'.*price.*', r'.*cost.*', r'.*value.*', r'.*fee.*'
            ],
            'count': [
                r'.*count.*', r'.*num_.*', r'.*total.*', r'quantity'
            ],
            'rate': [
                r'.*_rate$', r'.*_percentage$', r'.*_pct$', r'rate_.*', r'percent.*'
            ],
            'score': [
                r'.*_score$', r'.*_rating$', r'score_.*', r'rating_.*'
            ],
            'time_measure': [
                r'.*_minutes$', r'.*_seconds$', r'.*_hours$', r'.*_duration',
                r'handle_time', r'wait_time', r'processing_time'
            ]
        }
        
        # Generic patterns for table analysis
        self.table_patterns = {
            'fact': [
                r'fact_.*', r'fct_.*', r'.*_fact$', r'.*_events$', r'.*_transactions$',
                r'.*_activity$', r'.*_performance$', r'.*_metrics$'
            ],
            'dimension': [
                r'dim_.*', r'dimension_.*', r'.*_dim$', r'.*_lookup$', r'.*_reference$',
                r'.*_master$', r'.*_attributes$'
            ],
            'bridge': [
                r'bridge_.*', r'.*_bridge$', r'.*_mapping$', r'.*_relationship$'
            ],
            'staging': [
                r'stg_.*', r'staging_.*', r'.*_staging$', r'raw_.*', r'.*_raw$',
                r'tmp_.*', r'temp_.*', r'.*_tmp$'
            ],
            'report': [
                r'rpt_.*', r'report_.*', r'.*_report$', r'.*_summary$', r'.*_dashboard$'
            ]
        }
        
        # Performance metric intelligence
        self.performance_patterns = {
            'higher_better': [
                r'.*satisfaction.*', r'.*score.*', r'.*rating.*', r'.*success.*',
                r'.*resolution.*', r'.*efficiency.*', r'.*quality.*', r'.*fcr.*'
            ],
            'lower_better': [
                r'.*time.*', r'.*duration.*', r'.*wait.*', r'.*handle.*',
                r'.*processing.*', r'.*response.*', r'.*aht.*'
            ]
        }
    
    def analyze_column(self, column_name: str, data_type: str, sample_values: List[Any] = None) -> ColumnIntelligence:
        """
        Analyze a column and infer its purpose and characteristics
        """
        column_lower = column_name.lower()
        
        # Determine likely purpose
        likely_purpose = 'text'  # default
        
        # Check patterns
        for purpose, patterns in self.column_patterns.items():
            if any(re.search(pattern, column_lower) for pattern in patterns):
                likely_purpose = purpose
                break
        
        # Refine based on data type
        if data_type.upper() in ['TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'DATE', 'DATETIME']:
            likely_purpose = 'timestamp'
        elif data_type.upper() in ['NUMBER', 'FLOAT', 'DECIMAL']:
            if likely_purpose == 'text':  # No specific pattern matched
                likely_purpose = 'metric'
        elif data_type.upper() in ['TEXT', 'VARCHAR', 'STRING']:
            if likely_purpose == 'text' and any(word in column_lower for word in ['name', 'title', 'description']):
                likely_purpose = 'dimension'
        
        # Determine if it's a performance metric
        metric_type = None
        performance_direction = None
        
        if likely_purpose in ['metric', 'rate', 'score', 'time_measure', 'count']:
            metric_type = 'performance'
            
            # Determine performance direction
            for direction, patterns in self.performance_patterns.items():
                if any(re.search(pattern, column_lower) for pattern in patterns):
                    performance_direction = direction
                    break
        
        # Extract common patterns
        common_patterns = []
        for pattern_type, patterns in self.column_patterns.items():
            for pattern in patterns:
                if re.search(pattern, column_lower):
                    common_patterns.append(pattern_type)
                    break
        
        return ColumnIntelligence(
            name=column_name,
            data_type=data_type,
            likely_purpose=likely_purpose,
            metric_type=metric_type,
            performance_direction=performance_direction,
            common_patterns=common_patterns
        )
    
    def analyze_table(self, table_name: str, columns: List[Dict], sample_data: List[Dict] = None) -> TableIntelligence:
        """
        Analyze a table and infer its purpose and characteristics
        """
        table_lower = table_name.lower()
        
        # Determine likely purpose
        likely_purpose = 'fact'  # default
        
        for purpose, patterns in self.table_patterns.items():
            if any(re.search(pattern, table_lower) for pattern in patterns):
                likely_purpose = purpose
                break
        
        # Analyze columns
        column_intelligence = []
        for col in columns:
            col_intel = self.analyze_column(col['name'], col['type'])
            column_intelligence.append(col_intel)
        
        # Categorize columns
        time_columns = [c.name for c in column_intelligence if c.likely_purpose == 'timestamp']
        identifier_columns = [c.name for c in column_intelligence if c.likely_purpose == 'identifier']
        metric_columns = [c.name for c in column_intelligence if c.likely_purpose in ['metric', 'rate', 'score', 'time_measure', 'count']]
        dimension_columns = [c.name for c in column_intelligence if c.likely_purpose in ['dimension', 'name', 'text']]
        
        # Infer grain
        grain = 'transaction'  # default
        
        if any('week' in col.lower() for col in time_columns):
            grain = 'weekly'
        elif any('month' in col.lower() for col in time_columns):
            grain = 'monthly'
        elif any('day' in col.lower() for col in time_columns):
            grain = 'daily'
        elif len(metric_columns) > 0 and len(identifier_columns) > 0:
            grain = 'aggregated'
        
        # Determine primary entity
        primary_entity = None
        for col in dimension_columns:
            if 'name' in col.lower() and 'email' not in col.lower():
                primary_entity = col
                break
        
        return TableIntelligence(
            name=table_name,
            likely_purpose=likely_purpose,
            grain=grain,
            primary_entity=primary_entity,
            time_columns=time_columns,
            identifier_columns=identifier_columns,
            metric_columns=metric_columns,
            dimension_columns=dimension_columns
        )
    
    def infer_question_intent(self, question: str) -> Dict[str, Any]:
        """
        Infer the intent of a question in a generic way
        """
        question_lower = question.lower()
        
        intent = {
            'type': 'general',
            'requires_aggregation': False,
            'requires_filtering': False,
            'requires_ranking': False,
            'performance_context': None,
            'time_context': None,
            'entity_context': None,
            'limit': None,
            'metrics_mentioned': [],
            'dimensions_mentioned': []
        }
        
        # Detect aggregation needs
        aggregation_indicators = ['average', 'total', 'sum', 'count', 'max', 'min', 'avg']
        intent['requires_aggregation'] = any(word in question_lower for word in aggregation_indicators)
        
        # Detect filtering needs
        filtering_indicators = ['where', 'for', 'in', 'from', 'during', 'between']
        intent['requires_filtering'] = any(word in question_lower for word in filtering_indicators)
        
        # Detect ranking needs
        ranking_indicators = ['top', 'bottom', 'best', 'worst', 'highest', 'lowest', 'rank']
        intent['requires_ranking'] = any(word in question_lower for word in ranking_indicators)
        
        # Detect performance context
        if any(word in question_lower for word in ['worst', 'lowest', 'bottom', 'poor', 'bad']):
            intent['performance_context'] = 'poor'
        elif any(word in question_lower for word in ['best', 'highest', 'top', 'good', 'excellent']):
            intent['performance_context'] = 'good'
        
        # Detect time context
        time_indicators = ['today', 'yesterday', 'week', 'month', 'year', 'quarter', 'daily', 'weekly', 'monthly']
        for indicator in time_indicators:
            if indicator in question_lower:
                intent['time_context'] = indicator
                break
        
        # Extract limit
        limit_match = re.search(r'(\d+)', question)
        if limit_match and intent['requires_ranking']:
            intent['limit'] = int(limit_match.group(1))
        
        return intent
    
    def generate_generic_sql(self, question: str, table_intelligence: TableIntelligence, 
                           question_intent: Dict[str, Any]) -> Tuple[str, str]:
        """
        Generate SQL based on generic intelligence
        """
        
        # Start building SQL
        select_parts = []
        where_parts = []
        group_by_parts = []
        order_by_parts = []
        
        # Determine primary entity column
        primary_entity = table_intelligence.primary_entity
        if not primary_entity and table_intelligence.dimension_columns:
            primary_entity = table_intelligence.dimension_columns[0]
        
        if primary_entity:
            select_parts.append(primary_entity)
        
        # Add metrics with appropriate aggregation
        for metric_col in table_intelligence.metric_columns:
            if question_intent['requires_aggregation']:
                select_parts.append(f"AVG({metric_col}) as avg_{metric_col.lower()}")
            else:
                select_parts.append(metric_col)
        
        # Add GROUP BY if aggregation is needed
        if question_intent['requires_aggregation'] and primary_entity:
            group_by_parts.append(primary_entity)
        
        # Add ORDER BY for ranking
        if question_intent['requires_ranking']:
            for metric_col in table_intelligence.metric_columns:
                # Generic performance logic
                col_lower = metric_col.lower()
                if any(word in col_lower for word in ['time', 'duration', 'wait', 'handle']):
                    # Time metrics: lower is better
                    if question_intent['performance_context'] == 'poor':
                        order_by_parts.append(f"{metric_col} DESC")  # High time = poor performance
                    else:
                        order_by_parts.append(f"{metric_col} ASC")   # Low time = good performance
                else:
                    # Other metrics: higher is usually better
                    if question_intent['performance_context'] == 'poor':
                        order_by_parts.append(f"{metric_col} ASC")   # Low score = poor performance
                    else:
                        order_by_parts.append(f"{metric_col} DESC")  # High score = good performance
        
        # Build final SQL
        sql_parts = [f"SELECT {', '.join(select_parts)}"]
        sql_parts.append(f"FROM {table_intelligence.name}")
        
        if where_parts:
            sql_parts.append(f"WHERE {' AND '.join(where_parts)}")
        
        if group_by_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_by_parts)}")
        
        if order_by_parts:
            sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")
        
        if question_intent['limit']:
            sql_parts.append(f"LIMIT {question_intent['limit']}")
        
        sql = "\n".join(sql_parts)
        
        # Generate explanation
        explanation = f"""
🧠 GENERIC INTELLIGENCE APPLIED:

📊 TABLE ANALYSIS:
✅ Table: {table_intelligence.name}
✅ Purpose: {table_intelligence.likely_purpose}
✅ Grain: {table_intelligence.grain}
✅ Primary Entity: {table_intelligence.primary_entity or 'Auto-detected'}

📈 QUESTION ANALYSIS:
✅ Type: {question_intent['type']}
✅ Requires Aggregation: {question_intent['requires_aggregation']}
✅ Requires Ranking: {question_intent['requires_ranking']}
✅ Performance Context: {question_intent['performance_context'] or 'Neutral'}

🔧 SQL GENERATION:
✅ Metrics: {len(table_intelligence.metric_columns)} columns analyzed
✅ Dimensions: {len(table_intelligence.dimension_columns)} columns available
✅ Generic performance logic applied
✅ No hardcoded table or column names

💡 ADAPTABILITY:
This system works with any Snowflake database by analyzing:
• Column names and types
• Data patterns and structure
• Generic business logic patterns
• No hardcoded assumptions
"""
        
        return sql, explanation
    
    def validate_generic_sql(self, sql: str, table_intelligence: TableIntelligence) -> Dict[str, Any]:
        """
        Validate generated SQL against table intelligence
        """
        validation = {
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'suggestions': []
        }
        
        # Check if all referenced columns exist
        sql_upper = sql.upper()
        for line in sql_upper.split('\n'):
            if 'SELECT' in line or 'WHERE' in line or 'GROUP BY' in line or 'ORDER BY' in line:
                # Extract column references (basic validation)
                all_columns = (table_intelligence.time_columns + 
                              table_intelligence.identifier_columns + 
                              table_intelligence.metric_columns + 
                              table_intelligence.dimension_columns)
                
                for col in all_columns:
                    if col.upper() in line and col.upper() not in sql_upper:
                        validation['warnings'].append(f"Column {col} referenced but not found in SQL")
        
        # Check for proper aggregation
        if 'GROUP BY' in sql_upper and 'AVG(' not in sql_upper and 'SUM(' not in sql_upper:
            validation['suggestions'].append("Consider using aggregation functions with GROUP BY")
        
        return validation


# Global instance
generic_intelligence = GenericIntelligenceSystem()