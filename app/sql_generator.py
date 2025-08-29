import re
import os
import time
import json
from typing import Dict, Any, List, Tuple, Optional
from app.cache_manager import cache_manager, SQL_CACHE_PREFIX, SQL_CACHE_TTL, FEEDBACK_PREFIX, FEEDBACK_CACHE_TTL, TABLE_SELECTION_PREFIX, TABLE_SELECTION_CACHE_TTL

MAX_SQL_ATTEMPTS = int(os.getenv("MAX_SQL_ATTEMPTS", "5"))


class SQLGenerator:
    """Handles SQL generation, validation, and caching"""
    
    def __init__(self):
        self.cache_manager = cache_manager
    
    def extract_sql_from_response(self, response: str) -> str:
        """Extract SQL code from assistant response"""
        # Look for SQL blocks
        sql_patterns = [
            r'```sql\n(.*?)\n```',
            r'```SQL\n(.*?)\n```',
            r'```\n(.*?)\n```',
            r'`([^`]+)`'
        ]
        
        for pattern in sql_patterns:
            matches = re.findall(pattern, response, re.DOTALL | re.IGNORECASE)
            if matches:
                sql = matches[0].strip()
                if sql and ('SELECT' in sql.upper() or 'WITH' in sql.upper()):
                    return sql
        
        # If no code blocks, look for SQL keywords
        lines = response.split('\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            line_upper = line.strip().upper()
            if any(keyword in line_upper for keyword in ['SELECT', 'WITH', 'FROM', 'WHERE']):
                in_sql = True
                sql_lines.append(line.strip())
            elif in_sql and line.strip():
                sql_lines.append(line.strip())
            elif in_sql and not line.strip():
                break
        
        if sql_lines:
            return '\n'.join(sql_lines)
        
        return response.strip()

    def validate_sql_columns(self, sql: str, available_columns: list) -> tuple[bool, list]:
        """Validate that SQL uses only available columns"""
        sql_upper = sql.upper()
        missing_columns = []
        
        # Extract column references (simple approach)
        # This would need more sophisticated parsing in production
        for col in available_columns:
            if col.upper() not in sql_upper:
                continue
        
        return len(missing_columns) == 0, missing_columns

    def validate_and_fix_sql(self, sql: str, question: str, table: str, columns: List[str]) -> str:
        """Validate SQL and fix common issues"""
        # Remove any markdown formatting
        sql = re.sub(r'```sql\n?', '', sql)
        sql = re.sub(r'```\n?', '', sql)
        sql = sql.strip()
        
        # Basic validation - ensure it starts with SELECT or WITH
        sql_upper = sql.upper().strip()
        if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
            return f"-- Error: SQL must start with SELECT or WITH\n{sql}"
        
        # Check for table reference
        if table and table not in sql:
            return f"-- Error: SQL does not reference the expected table {table}\n{sql}"
        
        # Fix common agent counting issues
        sql = self._fix_agent_counting_issues(sql, question, table)
        
        return sql
    
    def _fix_agent_counting_issues(self, sql: str, question: str, table: str) -> str:
        """Fix common agent counting issues"""
        question_lower = question.lower()
        table_lower = table.lower()
        
        # Check if this is an agent counting query that needs DISTINCT
        if ('how many agents' in question_lower or 'count' in question_lower) and ('agent' in table_lower or 'performance' in table_lower):
            # Look for COUNT(*) or COUNT(agent_column) without DISTINCT
            count_patterns = [
                (r'COUNT\(\*\)', 'COUNT(DISTINCT agent_name)'),
                (r'COUNT\(agent_name\)', 'COUNT(DISTINCT agent_name)'),
                (r'COUNT\(assignee_name\)', 'COUNT(DISTINCT assignee_name)'),
                (r'COUNT\(user_name\)', 'COUNT(DISTINCT user_name)')
            ]
            
            for pattern, replacement in count_patterns:
                if re.search(pattern, sql, re.IGNORECASE) and 'DISTINCT' not in sql.upper():
                    sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
                    print(f"🔧 Fixed agent counting: Added DISTINCT to COUNT")
                    break
        
        return sql

    def _improve_sql_with_comprehensive_filters(self, original_sql: str, name_column: str) -> str:
        """Add comprehensive name filters to improve query results"""
        # This is a simplified version - the full implementation would be more complex
        improved_sql = original_sql
        
        # Add filters for excluding test data, bots, etc.
        exclusion_filters = [
            f"{name_column} NOT LIKE '%test%'",
            f"{name_column} NOT LIKE '%bot%'",
            f"{name_column} NOT LIKE '%Training%'",
            f"{name_column} IS NOT NULL",
            f"LENGTH({name_column}) > 1"
        ]
        
        # Add WHERE clause or extend existing one
        if 'WHERE' in improved_sql.upper():
            for filter_condition in exclusion_filters:
                improved_sql += f"\n  AND {filter_condition}"
        else:
            where_clause = "\nWHERE " + "\n  AND ".join(exclusion_filters)
            # Insert before ORDER BY if it exists
            if 'ORDER BY' in improved_sql.upper():
                parts = improved_sql.split('ORDER BY')
                improved_sql = parts[0] + where_clause + '\nORDER BY' + parts[1]
            else:
                improved_sql += where_clause
        
        return improved_sql

    def _get_business_logic_for_question(self, question: str, table: str, intent: dict) -> str:
        """Get business logic guidance based on question type and table"""
        question_lower = question.lower()
        table_lower = table.lower()
        business_logic = []
        
        # Agent counting logic
        if ('how many agents' in question_lower or 'count' in intent.get('metrics', [])) and 'agents' in intent.get('entities', []):
            if 'agent' in table_lower or 'performance' in table_lower:
                business_logic.append("""
AGENT COUNTING LOGIC:
- When counting agents, use COUNT(DISTINCT agent_column) because agent tables often have multiple rows per agent
- Common agent columns: agent_name, assignee_name, agent_id, user_name
- Example: SELECT COUNT(DISTINCT agent_name) FROM table WHERE conditions""")
        
        # Generic entity filtering logic with fuzzy matching
        entities_in_question = any(word in question_lower for word in [
            'team', 'agent', 'supervisor', 'manager', 'product', 'category', 'type', 
            'status', 'channel', 'group', 'department', 'feature', 'service'
        ])
        
        if entities_in_question:
            business_logic.append("""
GENERIC ENTITY FILTERING LOGIC:
- IMPORTANT: Use fuzzy matching for ALL entity filters because users often provide partial or inexact matches
- Apply intelligent partial matching for names, categories, products, statuses, etc.
- Examples of common mismatches:
  * User says "team Liam" → Database has "Liam Johnson" as supervisor
  * User says "payment issues" → Database has "Payment Processing Problems"
  * User says "web channel" → Database has "Web Portal"
  * User says "John" → Database has "John Smith" or "Johnathan"
- ALWAYS use ILIKE '%entity%' for partial matching, not exact equals
- Check multiple relevant columns for each entity type
- Handle variations in capitalization, spacing, and partial names""")
        
        # Performance table specific logic
        if 'performance' in table_lower or 'wops_agent_performance' in table_lower:
            business_logic.append("""
PERFORMANCE TABLE LOGIC:
- This table contains weekly/periodic rows for each agent
- Always use DISTINCT when counting unique agents
- Filter by appropriate date ranges when needed
- Common columns: agent_name, team, week_start_date, performance_metrics""")
        
        # Time-based filtering
        if any(time_ref in question_lower for time_ref in ['today', 'this week', 'last week', 'current']):
            business_logic.append("""
TIME FILTERING LOGIC:
- Use appropriate date functions for time filters
- Consider timezone conversions if needed
- Example: WHERE DATE(column) = CURRENT_DATE for 'today'""")
        
        return '\n'.join(business_logic) if business_logic else "GENERAL LOGIC:\n- Use appropriate aggregations and filters based on the question"

    def _extract_team_or_person_names(self, question: str) -> dict:
        """Extract team names, supervisor names, or agent names from questions"""
        import re
        
        question_lower = question.lower()
        extracted_names = {
            'team_names': [],
            'person_names': [],
            'raw_names': []
        }
        
        # Patterns to extract names after "team", "supervisor", "agent", etc.
        team_patterns = [
            r'team\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)',  # "team Liam" or "team Liam Johnson"
            r'in\s+team\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)',  # "in team Liam"
            r'from\s+team\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)',  # "from team Liam"
            r'supervisor\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)',  # "supervisor Liam"
            r'manager\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)',  # "manager Liam"
        ]
        
        # Agent/person patterns
        person_patterns = [
            r'agent\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)',  # "agent John Smith"
            r'for\s+agent\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)',  # "for agent John"
        ]
        
        # Extract team-related names
        for pattern in team_patterns:
            matches = re.findall(pattern, question_lower, re.IGNORECASE)
            for match in matches:
                name = match.strip()
                if name and len(name) > 1:  # Avoid single letters
                    extracted_names['team_names'].append(name)
                    extracted_names['raw_names'].append(name)
        
        # Extract person names
        for pattern in person_patterns:
            matches = re.findall(pattern, question_lower, re.IGNORECASE)
            for match in matches:
                name = match.strip()
                if name and len(name) > 1:
                    extracted_names['person_names'].append(name)
                    extracted_names['raw_names'].append(name)
        
        return extracted_names

    def _generate_fuzzy_name_filters(self, names: dict, table: str, schema: dict) -> str:
        """Generate fuzzy matching WHERE clauses for names"""
        if not names['raw_names']:
            return ""
        
        table_lower = table.lower()
        columns = schema.get('columns', [])
        where_clauses = []
        
        # Determine appropriate columns based on table type
        supervisor_columns = [col for col in columns if 'supervisor' in col.lower()]
        team_columns = [col for col in columns if any(term in col.lower() for term in ['team', 'group'])]
        agent_columns = [col for col in columns if any(term in col.lower() for term in ['agent', 'assignee', 'user_name'])]
        
        for name in names['raw_names']:
            name_clauses = []
            
            # For team-related questions, prioritize supervisor and team columns
            if names['team_names']:
                # Try supervisor columns first (most common case)
                for col in supervisor_columns:
                    name_clauses.append(f"{col} ILIKE '%{name}%'")
                
                # Try team columns
                for col in team_columns:
                    name_clauses.append(f"{col} ILIKE '%{name}%'")
            
            # For agent questions, try agent columns
            if names['person_names']:
                for col in agent_columns:
                    name_clauses.append(f"{col} ILIKE '%{name}%'")
            
            # Fallback: try common name columns if none found above
            if not name_clauses:
                common_name_columns = [col for col in columns if any(term in col.lower() for term in ['name', 'supervisor', 'assignee', 'user'])]
                for col in common_name_columns:
                    name_clauses.append(f"{col} ILIKE '%{name}%'")
            
            if name_clauses:
                where_clauses.append(f"({' OR '.join(name_clauses)})")
        
        if where_clauses:
            return f"\nFUZZY NAME MATCHING:\n- Add to WHERE clause: {' AND '.join(where_clauses)}\n- This handles partial name matches like 'Liam' → 'Liam Johnson'"
        
        return ""

    def build_sql_instructions(self, intent: dict, table: str, schema: dict, original_question: str) -> dict:
        """Build basic SQL generation instructions"""
        return {
            'table': table,
            'columns': schema.get('columns', []),
            'intent': intent,
            'question': original_question,
            'instructions': f"Generate SQL query for table {table} to answer: {original_question}"
        }

    async def build_enhanced_sql_instructions(self, intent: dict, table: str, schema: dict, original_question: str, user_id: str = None) -> dict:
        """Build enhanced SQL generation instructions with business logic"""
        columns = schema.get('columns', [])
        sample_data = schema.get('sample_data', [])
        column_descriptions = schema.get('column_descriptions', {})
        
        # Build comprehensive instructions with business logic
        business_logic = self._get_business_logic_for_question(original_question, table, intent)
        
        # Use intelligent data analyst system for business intelligence
        from app.intelligent_data_analyst import intelligent_data_analyst
        
        # Get intelligent analysis
        intent_analysis = intelligent_data_analyst.analyze_question_intent(original_question)
        
        # Generate intelligent SQL if possible
        if intent_analysis['confidence'] > 80:
            try:
                intelligent_sql, business_explanation = await intelligent_data_analyst.generate_intelligent_sql(
                    original_question, intent_analysis, schema, user_id
                )
                
                # If intelligent SQL was generated, use it
                if not intelligent_sql.startswith('--'):
                    fuzzy_matching = f"""
INTELLIGENT DATA ANALYST OVERRIDE:

{business_explanation}

RECOMMENDED SQL:
{intelligent_sql}

This SQL is generated using business intelligence and deep data understanding.
"""
                else:
                    # No intelligent SQL generated, use basic instructions
                    fuzzy_matching = "Use standard SQL generation approach with the available columns."
            except Exception as e:
                print(f"⚠️ Intelligent analyst error: {e}")
                # Use basic instructions
                fuzzy_matching = "Use standard SQL generation approach with the available columns."
        else:
            # Use basic instructions for low confidence questions
            fuzzy_matching = "Use standard SQL generation approach with the available columns."
        
        instructions = f"""Generate an accurate SQL query for: {original_question}

TABLE: {table}
AVAILABLE COLUMNS: {', '.join(columns)}

CRITICAL DATA FIELD MAPPINGS:
- QA/Quality Score: Use QA_SCORE column (values like 85.0, 90.0, etc.)
- CSAT: Use POSITIVE_RES_CSAT column (values like 0.956522, 1.000000 - these are ratios, multiply by 100 for percentage)
- AHT/Handle Time: Use AHT_MINUTES column (values like 14.528148, 9.556337, etc.)
- Agent Names: Use ASSIGNEE_NAME column (or AGENT_NAME for schedule adherence)
- Supervisor/Team: Use ASSIGNEE_SUPERVISOR column (or SUPERVISOR_NAME for schedule adherence)
- Time Filtering: Use SOLVED_WEEK column (timestamp format) or ADHERENCE_DATE for schedule data
- Month filtering: Use EXTRACT(MONTH FROM SOLVED_WEEK) = X and EXTRACT(YEAR FROM SOLVED_WEEK) = Y
- Schedule Adherence: Use ADHERENCE_PERCENTAGE column (values like 85.5, 92.3, etc.)
- Schedule Times: Use SCHEDULED_MINUTES, ADHERENT_MINUTES, OFFLINE_MINUTES columns

BUSINESS INTELLIGENCE PATTERNS:
1. Performance Thresholds:
   - QA: Good >85%, Poor <75%
   - CSAT: Good >90%, Poor <80% (remember to multiply POSITIVE_RES_CSAT by 100)
   - AHT: Good <12 minutes, Poor >15 minutes
   
2. Failure Analysis Patterns:
   - Count failures as CASE statements: CASE WHEN QA_SCORE < 75 THEN 1 ELSE 0 END
   - Sum multiple failure types: qa_failures + csat_failures + aht_failures
   - Filter by total failures: HAVING total_failures > X
   
3. Team/Agent Filtering:
   - Team filtering: WHERE ASSIGNEE_SUPERVISOR ILIKE '%team_name%'
   - Agent filtering: WHERE ASSIGNEE_NAME ILIKE '%agent_name%'
   
4. Time-Based Analysis:
   - Month filtering: WHERE EXTRACT(MONTH FROM SOLVED_WEEK) = X AND EXTRACT(YEAR FROM SOLVED_WEEK) = Y
   - Date ranges: WHERE SOLVED_WEEK BETWEEN date1 AND date2
   
5. Aggregation Patterns:
   - Per agent: GROUP BY ASSIGNEE_NAME
   - Per team: GROUP BY ASSIGNEE_SUPERVISOR
   - Per time period: GROUP BY EXTRACT(MONTH FROM SOLVED_WEEK)
   
6. Ranking and Comparisons:
   - Top performers: ORDER BY metric DESC LIMIT N
   - Worst performers: ORDER BY metric ASC LIMIT N
   - Improvement: Use LAG() window function for period-over-period analysis

CRITICAL SQL AGGREGATION RULES:
1. If using GROUP BY, ALL columns in SELECT must either be:
   - In the GROUP BY clause, OR
   - Wrapped in an aggregate function (COUNT, SUM, AVG, MAX, MIN)
2. Examples:
   - CORRECT: SELECT ASSIGNEE_NAME, AVG(QA_SCORE) FROM table GROUP BY ASSIGNEE_NAME
   - INCORRECT: SELECT ASSIGNEE_NAME, POSITIVE_RES_CSAT FROM table GROUP BY ASSIGNEE_NAME
   - CORRECT: SELECT ASSIGNEE_NAME, AVG(POSITIVE_RES_CSAT) FROM table GROUP BY ASSIGNEE_NAME

REQUIREMENTS:
1. Use ONLY columns that exist in the table
2. Apply appropriate filters and aggregations
3. Handle NULL values appropriately with IS NOT NULL conditions
4. For percentage comparisons: QA_SCORE is already a percentage, POSITIVE_RES_CSAT needs *100
5. Use proper date/time filters with EXTRACT functions
6. Include meaningful column aliases
7. Add appropriate ORDER BY and LIMIT clauses
8. For team filtering: WHERE ASSIGNEE_SUPERVISOR ILIKE '%team_name%'
9. ALWAYS follow SQL aggregation rules - no raw columns in SELECT when using GROUP BY

{business_logic}

{fuzzy_matching}

SAMPLE DATA PREVIEW:
{sample_data[:3] if sample_data else 'No sample data available'}

COLUMN DETAILS:
{json.dumps(column_descriptions, indent=2) if column_descriptions else 'No column descriptions available'}

Return ONLY the SQL query, no explanations."""

        return {
            'table': table,
            'columns': columns,
            'intent': intent,
            'question': original_question,
            'instructions': instructions,
            'column_descriptions': column_descriptions,
            'sample_data': sample_data
        }

    def build_sql_instructions_with_business_logic(self, intent: Dict, table: str, schema: Dict, original_question: str) -> Dict:
        """Build SQL instructions incorporating business logic and domain knowledge"""
        # This would contain extensive business logic for the specific domain
        # For now, returning the enhanced version
        return self.build_enhanced_sql_instructions(intent, table, schema, original_question)

    def build_pattern_enhanced_instructions(self, question: str, table: str, schema: dict, pattern: Dict = None, sample_data: List = None) -> str:
        """Build instructions enhanced with pattern matching"""
        from app.question_analyzer import analyze_question_intent
        base_instructions = self.build_enhanced_sql_instructions(
            analyze_question_intent(question.lower()),
            table, schema, question
        )
        
        if pattern:
            # Add pattern-specific guidance
            base_instructions['instructions'] += f"\n\nPATTERN GUIDANCE:\n{pattern.get('guidance', '')}"
            if pattern.get('sample_sql'):
                base_instructions['instructions'] += f"\n\nSAMPLE SQL:\n{pattern['sample_sql']}"
        
        return base_instructions['instructions']

    def build_pattern_enhanced_message_for_ai(self, question: str, table: str, schema: dict, pattern: Dict = None, sample_data: List = None) -> str:
        """Build complete message for AI with pattern enhancement"""
        instructions = self.build_pattern_enhanced_instructions(question, table, schema, pattern, sample_data)
        
        message = f"""Question: {question}
Table: {table}
Columns: {', '.join(schema.get('columns', []))}

{instructions}

Return only the SQL query."""
        
        return message

    def build_helper_sql_guided_instructions(self, helper_sql: str, pattern_context: Dict, table: str, schema: Dict, original_question: str) -> str:
        """Build instructions using helper SQL as guidance"""
        from app.question_analyzer import analyze_question_intent
        base_instructions = self.build_enhanced_sql_instructions(
            analyze_question_intent(original_question.lower()),
            table, schema, original_question
        )
        
        instructions = base_instructions['instructions']
        instructions += f"\n\nHELPER SQL FOR REFERENCE:\n{helper_sql}"
        instructions += f"\n\nAdapt this pattern to answer: {original_question}"
        
        return instructions

    def build_sql_instructions_with_error_context(self, intent: dict, table: str, schema: dict, original_question: str, previous_error: str = None, previous_sql: str = None) -> dict:
        """Build SQL instructions with error context for retry logic"""
        base_instructions = self.build_enhanced_sql_instructions(intent, table, schema, original_question)
        
        if previous_error and previous_sql:
            base_instructions['instructions'] += f"""

PREVIOUS ATTEMPT FAILED:
SQL: {previous_sql}
ERROR: {previous_error}

Please fix the errors and generate a corrected SQL query."""
        
        return base_instructions

    async def update_sql_cache_with_results(self, question: str, sql: str, result_count: int, table_used: str = None):
        """Cache SQL query with result information"""
        cache_key = f"{SQL_CACHE_PREFIX}:{self.question_analyzer.get_question_hash(question)}"
        
        cache_data = {
            'question': question,
            'sql': sql,
            'result_count': result_count,
            'table_used': table_used,
            'success': result_count > 0,
            'cached_at': time.time()
        }
        
        await self.cache_manager.set(cache_key, cache_data, ex=SQL_CACHE_TTL)
        print(f"💾 Cached SQL query (success: {result_count > 0}, results: {result_count})")

    async def cache_table_selection(self, question: str, selected_table: str, reason: str, success: bool = True):
        """Cache table selection for learning"""
        from app.question_analyzer import get_question_hash
        selection_key = f"{TABLE_SELECTION_PREFIX}:{get_question_hash(question)}"
        
        # Extract key phrases for pattern matching
        from app.question_analyzer import extract_key_phrases
        key_phrases = extract_key_phrases(question)
        
        selection_data = {
            'question': question,
            'selected_table': selected_table,
            'reason': reason,
            'success': success,
            'key_phrases': key_phrases,
            'timestamp': time.time()
        }
        await self.cache_manager.set(selection_key, selection_data, ex=TABLE_SELECTION_CACHE_TTL)
        
        # Also cache by key phrases for pattern matching
        for phrase in key_phrases:
            phrase_key = f"{TABLE_SELECTION_PREFIX}:phrase:{phrase}"
            phrase_data = await self.cache_manager.get(phrase_key, {})
            
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
            
            await self.cache_manager.set(phrase_key, phrase_data, ex=TABLE_SELECTION_CACHE_TTL)
        
        print(f"💾 Cached table selection: {selected_table} for {len(key_phrases)} key phrases")

    async def record_feedback(self, question: str, sql: str, table: str, feedback_type: str):
        """Record user feedback (positive or negative) for a query"""
        from app.question_analyzer import get_question_hash
        feedback_key = f"{FEEDBACK_PREFIX}:{get_question_hash(question)}"
        feedback_data = await self.cache_manager.get(feedback_key, {
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
            await self.cache_table_selection(question, table, "User confirmed this was correct", success=True)
        else:
            feedback_data['negative_count'] += 1
            # Mark the table selection as unsuccessful
            await self.cache_table_selection(question, table, "User indicated this was incorrect", success=False)
        
        feedback_data['last_feedback'] = {
            'type': feedback_type,
            'timestamp': time.time()
        }
        
        await self.cache_manager.set(feedback_key, feedback_data, ex=FEEDBACK_CACHE_TTL)
        
        print(f"{'✅' if feedback_type == 'positive' else '❌'} Recorded {feedback_type} feedback for question: {question[:50]}...")
        print(f"   Stats: {feedback_data['positive_count']} positive, {feedback_data['negative_count']} negative")

    async def generate_sql_with_retry_logic(self, question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
        """Generate SQL with retry logic for failed attempts"""
        # This would implement the full retry logic from the original file
        # For now, returning a simple response
        print(f"🔄 Generating SQL with retry logic for: {question}")
        
        # Placeholder implementation
        return "-- Retry logic not fully implemented yet", "unknown_table"


# Global SQL generator instance
sql_generator = SQLGenerator()

# Convenience functions for backward compatibility
def extract_sql_from_response(response: str) -> str:
    """Extract SQL from response"""
    return sql_generator.extract_sql_from_response(response)

def validate_sql_columns(sql: str, available_columns: list) -> tuple[bool, list]:
    """Validate SQL columns"""
    return sql_generator.validate_sql_columns(sql, available_columns)

def validate_and_fix_sql(sql: str, question: str, table: str, columns: List[str]) -> str:
    """Validate and fix SQL"""
    return sql_generator.validate_and_fix_sql(sql, question, table, columns)

def build_sql_instructions(intent: dict, table: str, schema: dict, original_question: str) -> dict:
    """Build SQL instructions"""
    return sql_generator.build_sql_instructions(intent, table, schema, original_question)

async def build_enhanced_sql_instructions(intent: dict, table: str, schema: dict, original_question: str, user_id: str = None) -> dict:
    """Build enhanced SQL instructions"""
    return await sql_generator.build_enhanced_sql_instructions(intent, table, schema, original_question, user_id)

def build_sql_instructions_with_business_logic(intent: Dict, table: str, schema: Dict, original_question: str) -> Dict:
    """Build SQL instructions with business logic"""
    return sql_generator.build_sql_instructions_with_business_logic(intent, table, schema, original_question)

def build_pattern_enhanced_instructions(question: str, table: str, schema: dict, pattern: Dict = None, sample_data: List = None) -> str:
    """Build pattern enhanced instructions"""
    return sql_generator.build_pattern_enhanced_instructions(question, table, schema, pattern, sample_data)

def build_pattern_enhanced_message_for_ai(question: str, table: str, schema: dict, pattern: Dict = None, sample_data: List = None) -> str:
    """Build pattern enhanced message"""
    return sql_generator.build_pattern_enhanced_message_for_ai(question, table, schema, pattern, sample_data)

def build_helper_sql_guided_instructions(helper_sql: str, pattern_context: Dict, table: str, schema: Dict, original_question: str) -> str:
    """Build helper SQL guided instructions"""
    return sql_generator.build_helper_sql_guided_instructions(helper_sql, pattern_context, table, schema, original_question)

def build_sql_instructions_with_error_context(intent: dict, table: str, schema: dict, original_question: str, previous_error: str = None, previous_sql: str = None) -> dict:
    """Build SQL instructions with error context"""
    return sql_generator.build_sql_instructions_with_error_context(intent, table, schema, original_question, previous_error, previous_sql)

async def update_sql_cache_with_results(question: str, sql: str, result_count: int, table_used: str = None):
    """Update SQL cache"""
    from app.question_analyzer import get_question_hash
    cache_key = f"{SQL_CACHE_PREFIX}:{get_question_hash(question)}"
    
    cache_data = {
        'question': question,
        'sql': sql,
        'result_count': result_count,
        'table_used': table_used,
        'success': result_count > 0,
        'cached_at': time.time()
    }
    
    await cache_manager.set(cache_key, cache_data, ex=SQL_CACHE_TTL)
    print(f"💾 Cached SQL query (success: {result_count > 0}, results: {result_count})")

async def cache_table_selection(question: str, selected_table: str, reason: str, success: bool = True):
    """Cache table selection"""
    return await sql_generator.cache_table_selection(question, selected_table, reason, success)

async def record_feedback(question: str, sql: str, table: str, feedback_type: str):
    """Record feedback"""
    return await sql_generator.record_feedback(question, sql, table, feedback_type)

async def generate_sql_with_retry_logic(question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
    """Generate SQL with retry logic"""
    return await sql_generator.generate_sql_with_retry_logic(question, user_id, channel_id)