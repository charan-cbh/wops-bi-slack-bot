"""
SQL Generator - Handles SQL generation and validation for the BI Slack Bot
"""
import re
from typing import Dict, List, Tuple, Optional, Any
from openai import OpenAI
from .valkey_manager import ValkeyManager
from .question_classifier import QuestionClassifier

class SQLGenerator:
    def __init__(self, openai_client: OpenAI, valkey_manager: ValkeyManager, question_classifier: QuestionClassifier):
        self.client = openai_client
        self.valkey_manager = valkey_manager
        self.question_classifier = question_classifier
    
    def build_sql_instructions(self, intent: dict, table: str, schema: dict, original_question: str) -> dict:
        """Build SQL instructions based on question intent and table schema"""
        instructions = {
            "base_query": f"SELECT * FROM {table}",
            "where_clauses": [],
            "group_by": [],
            "order_by": [],
            "limit": None,
            "aggregations": [],
            "joins": [],
            "special_instructions": []
        }
        
        # Handle aggregations
        if intent.get('is_aggregation'):
            agg_type = intent.get('aggregation_type', 'count')
            
            if agg_type == 'count':
                instructions["aggregations"].append("COUNT(*) as total_count")
            elif agg_type == 'sum':
                # Look for numeric columns that might be summed
                numeric_cols = [col['name'] for col in schema.get('columns', []) 
                              if 'number' in col.get('type', '').lower() or 'int' in col.get('type', '').lower()]
                if numeric_cols:
                    instructions["aggregations"].append(f"SUM({numeric_cols[0]}) as total_sum")
            elif agg_type == 'avg':
                numeric_cols = [col['name'] for col in schema.get('columns', []) 
                              if 'number' in col.get('type', '').lower() or 'int' in col.get('type', '').lower()]
                if numeric_cols:
                    instructions["aggregations"].append(f"AVG({numeric_cols[0]}) as average_value")
            elif agg_type == 'max':
                numeric_cols = [col['name'] for col in schema.get('columns', []) 
                              if 'number' in col.get('type', '').lower() or 'int' in col.get('type', '').lower()]
                if numeric_cols:
                    instructions["aggregations"].append(f"MAX({numeric_cols[0]}) as max_value")
            elif agg_type == 'min':
                numeric_cols = [col['name'] for col in schema.get('columns', []) 
                              if 'number' in col.get('type', '').lower() or 'int' in col.get('type', '').lower()]
                if numeric_cols:
                    instructions["aggregations"].append(f"MIN({numeric_cols[0]}) as min_value")
        
        # Handle time period filtering
        if intent.get('time_period'):
            time_period = intent['time_period']
            # Look for date columns
            date_cols = [col['name'] for col in schema.get('columns', []) 
                        if 'date' in col.get('type', '').lower() or 'timestamp' in col.get('type', '').lower()]
            
            if date_cols:
                date_col = date_cols[0]  # Use first date column
                
                if time_period == 'daily':
                    instructions["where_clauses"].append(f"{date_col} >= CURRENT_DATE - INTERVAL '1 day'")
                elif time_period == 'weekly':
                    instructions["where_clauses"].append(f"{date_col} >= CURRENT_DATE - INTERVAL '7 days'")
                elif time_period == 'monthly':
                    instructions["where_clauses"].append(f"{date_col} >= CURRENT_DATE - INTERVAL '1 month'")
                elif time_period == 'quarterly':
                    instructions["where_clauses"].append(f"{date_col} >= CURRENT_DATE - INTERVAL '3 months'")
                elif time_period == 'yearly':
                    instructions["where_clauses"].append(f"{date_col} >= CURRENT_DATE - INTERVAL '1 year'")
        
        # Handle comparison queries
        if intent.get('is_comparison'):
            comp_type = intent.get('comparison_type')
            if comp_type == 'year_over_year':
                instructions["special_instructions"].append("Include year-over-year comparison")
            elif comp_type == 'month_over_month':
                instructions["special_instructions"].append("Include month-over-month comparison")
        
        # Handle trend analysis
        if intent.get('is_trend'):
            date_cols = [col['name'] for col in schema.get('columns', []) 
                        if 'date' in col.get('type', '').lower() or 'timestamp' in col.get('type', '').lower()]
            if date_cols:
                instructions["group_by"].append(f"DATE_TRUNC('month', {date_cols[0]})")
                instructions["order_by"].append(f"DATE_TRUNC('month', {date_cols[0]})")
        
        # Handle filtering
        if intent.get('is_filtering'):
            instructions["special_instructions"].append("Apply specific filters based on question context")
        
        return instructions
    
    def validate_and_fix_sql(self, sql: str, question: str, table: str, columns: List[str]) -> str:
        """Validate and fix common SQL issues"""
        try:
            # Remove extra whitespace and normalize
            sql = re.sub(r'\s+', ' ', sql.strip())
            
            # Check for missing SELECT
            if not sql.upper().startswith('SELECT'):
                sql = f"SELECT * FROM {table}"
            
            # Check for missing FROM clause
            if 'FROM' not in sql.upper():
                sql = sql.replace('SELECT', f'SELECT * FROM {table}; SELECT')
            
            # Fix common column name issues
            for col in columns:
                # Handle column names with spaces
                if ' ' in col:
                    sql = sql.replace(col, f'"{col}"')
            
            # Remove dangerous keywords
            dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
            for keyword in dangerous_keywords:
                if keyword in sql.upper():
                    print(f"⚠️ Dangerous keyword {keyword} detected and removed")
                    sql = re.sub(rf'\b{keyword}\b', '', sql, flags=re.IGNORECASE)
            
            # Ensure proper semicolon ending
            if not sql.rstrip().endswith(';'):
                sql += ';'
            
            return sql
            
        except Exception as e:
            print(f"❌ Error validating SQL: {e}")
            return f"SELECT * FROM {table} LIMIT 10;"
    
    def validate_and_fix_sql_enhanced(self, sql: str, question: str, table: str, columns: List[str], schema: dict) -> str:
        """Enhanced SQL validation with timezone and metric validation"""
        try:
            # First run basic validation
            sql = self.validate_and_fix_sql(sql, question, table, columns)
            
            # Enhanced timezone validation and fixing
            if 'CURRENT_DATE' in sql and 'CONVERT_TIMEZONE' not in sql:
                print("⚠️ Fixing timezone handling - replacing CURRENT_DATE with PST conversion")
                # Replace CURRENT_DATE with proper PST conversion
                sql = sql.replace('CURRENT_DATE', "DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))")
            
            # Fix common timezone mistakes
            timezone_fixes = {
                'CURRENT_DATE - 1': "DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 1",
                'CURRENT_DATE - 7': "DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7",
                'CURRENT_DATE - 30': "DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 30",
                'CURRENT_TIMESTAMP()': "CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())",
                'NOW()': "CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())"
            }
            
            for old_pattern, new_pattern in timezone_fixes.items():
                if old_pattern in sql:
                    print(f"⚠️ Fixing timezone pattern: {old_pattern} -> {new_pattern}")
                    sql = sql.replace(old_pattern, new_pattern)
            
            # Validate column usage against schema
            available_columns = [col.upper() for col in columns]
            
            # Check for common metric columns and suggest pre-calculated ones
            metric_suggestions = {
                'REPLY_TIME_IN_MINUTES': 'response time',
                'FIRST_RESOLUTION_TIME_IN_MINUTES': 'resolution time',
                'FULL_RESOLUTION_TIME_IN_MINUTES': 'full resolution time',
                'HANDLE_TIME_IN_MINUTES': 'handle time',
                'ADHERENCE_PERCENTAGE': 'schedule adherence',
                'QA_SCORE': 'quality score'
            }
            
            for metric_col, description in metric_suggestions.items():
                if metric_col in available_columns:
                    # Check if the query is trying to calculate what's already available
                    if 'AVG(' in sql.upper() and description.replace(' ', '') in question.lower().replace(' ', ''):
                        print(f"💡 Suggesting pre-calculated metric: {metric_col} for {description}")
            
            # Validate all columns in the query exist
            is_valid, missing_columns = self.validate_sql_columns(sql, columns)
            
            if missing_columns:
                print(f"⚠️ Missing columns detected: {missing_columns}")
                
                # Try to fix common column name issues
                for missing_col in missing_columns:
                    # Look for similar column names
                    similar_cols = [col for col in columns if missing_col.lower() in col.lower()]
                    if similar_cols:
                        print(f"💡 Replacing {missing_col} with {similar_cols[0]}")
                        sql = sql.replace(missing_col, similar_cols[0])
                    else:
                        print(f"❌ Could not find replacement for {missing_col}")
            
            # Final validation
            if 'SELECT' not in sql.upper():
                print("⚠️ No SELECT statement found, using fallback")
                return f"SELECT * FROM {table} LIMIT 10;"
            
            return sql
            
        except Exception as e:
            print(f"❌ Error in enhanced SQL validation: {e}")
            return f"SELECT * FROM {table} LIMIT 10;"
    
    def validate_sql_columns(self, sql: str, available_columns: list) -> tuple[bool, list]:
        """Validate that SQL query only uses available columns"""
        try:
            # Extract column names from SELECT statement
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if not select_match:
                return True, []  # No specific columns to validate
            
            select_clause = select_match.group(1)
            
            # Handle SELECT *
            if '*' in select_clause:
                return True, []
            
            # Extract individual column references
            # This is a simplified approach - a full SQL parser would be more accurate
            columns_in_query = []
            
            # Split by comma and clean up
            parts = select_clause.split(',')
            for part in parts:
                part = part.strip()
                
                # Remove aggregate functions
                part = re.sub(r'\w+\s*\(([^)]+)\)', r'\1', part)
                
                # Remove aliases
                part = re.sub(r'\s+as\s+\w+', '', part, flags=re.IGNORECASE)
                
                # Extract column name
                column_match = re.search(r'\b(\w+)\b', part)
                if column_match:
                    column_name = column_match.group(1)
                    if column_name.upper() not in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DISTINCT']:
                        columns_in_query.append(column_name)
            
            # Check if all columns exist
            missing_columns = []
            available_columns_upper = [col.upper() for col in available_columns]
            
            for col in columns_in_query:
                if col.upper() not in available_columns_upper:
                    missing_columns.append(col)
            
            is_valid = len(missing_columns) == 0
            return is_valid, missing_columns
            
        except Exception as e:
            print(f"❌ Error validating SQL columns: {e}")
            return False, []
    
    async def generate_sql_from_instructions(self, question: str, table: str, schema: dict, instructions: dict) -> str:
        """Generate SQL from structured instructions"""
        try:
            # Build the SQL query from instructions
            select_parts = []
            
            # Handle aggregations
            if instructions.get("aggregations"):
                select_parts.extend(instructions["aggregations"])
            else:
                select_parts.append("*")
            
            # Build base query
            sql_parts = [f"SELECT {', '.join(select_parts)}"]
            sql_parts.append(f"FROM {table}")
            
            # Add WHERE clauses
            if instructions.get("where_clauses"):
                sql_parts.append(f"WHERE {' AND '.join(instructions['where_clauses'])}")
            
            # Add GROUP BY
            if instructions.get("group_by"):
                sql_parts.append(f"GROUP BY {', '.join(instructions['group_by'])}")
            
            # Add ORDER BY
            if instructions.get("order_by"):
                sql_parts.append(f"ORDER BY {', '.join(instructions['order_by'])}")
            
            # Add LIMIT
            if instructions.get("limit"):
                sql_parts.append(f"LIMIT {instructions['limit']}")
            
            sql = " ".join(sql_parts)
            
            # Validate and fix the SQL
            column_names = schema.get('column_names', [])
            sql = self.validate_and_fix_sql(sql, question, table, column_names)
            
            return sql
            
        except Exception as e:
            print(f"❌ Error generating SQL from instructions: {e}")
            return f"SELECT * FROM {table} LIMIT 10;"
    
    async def generate_sql_with_openai(self, question: str, table: str, schema: dict, context: dict = None) -> str:
        """Generate SQL using OpenAI with strict schema validation and business rules"""
        try:
            # Build detailed context about the table
            table_context = f"Table: {table}\n"
            table_context += f"Available Columns: {', '.join(schema.get('column_names', []))}\n"
            
            # Add detailed column information
            if schema.get('columns'):
                table_context += "\nColumn Details:\n"
                for col in schema['columns']:
                    table_context += f"- {col['name']}: {col['type']}"
                    if col.get('comment'):
                        table_context += f" ({col['comment']})"
                    table_context += "\n"
            
            # Add timezone handling rules
            timezone_rules = """
            
            CRITICAL TIMEZONE RULES:
            - For PST date filtering, ALWAYS use: DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))
            - For "today": WHERE DATE(CREATED_AT_PST) = DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))
            - For "yesterday": WHERE DATE(CREATED_AT_PST) = DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 1
            - For "last 7 days": WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7
            - For "this week": WHERE CREATED_AT_PST >= DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())))
            - For "last month": WHERE CREATED_AT_PST >= DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))) - INTERVAL '1 month'
            - NEVER use CURRENT_DATE directly for PST filtering
            """
            
            # Add pre-calculated metrics guidance
            metrics_guidance = """
            
            PRE-CALCULATED METRICS AVAILABLE:
            - If table has REPLY_TIME_IN_MINUTES, use it directly for response time (no calculations needed)
            - If table has FIRST_RESOLUTION_TIME_IN_MINUTES, use it for resolution time
            - If table has HANDLE_TIME_IN_MINUTES, use it for AHT (no calculations needed)
            - If table has ADHERENCE_PERCENTAGE, use it for schedule adherence
            - If table has QA_SCORE, use it for quality metrics
            - NEVER calculate metrics that are already pre-calculated
            """
            
            # Add conversation context if available
            context_info = ""
            if context:
                context_info = f"\nPrevious Context: {context.get('summary', '')}\n"
            
            # Enhanced system prompt with strict rules
            system_prompt = f"""You are an expert SQL query generator for business intelligence with strict adherence to schema and business rules.
            
            CRITICAL REQUIREMENTS:
            1. Use ONLY columns that exist in the provided schema - validate every column name
            2. Follow PST timezone conversion rules exactly as specified
            3. Use pre-calculated metrics when available - do not re-calculate
            4. Generate syntactically correct SQL
            5. Include appropriate WHERE, GROUP BY, ORDER BY, and LIMIT clauses
            6. Use proper aggregation functions when needed
            7. Do not use dangerous SQL keywords (DROP, DELETE, UPDATE, INSERT, ALTER, CREATE, TRUNCATE)
            8. Return only the SQL query, no explanations or markdown formatting
            
            SCHEMA VALIDATION:
            - Before using any column, verify it exists in the schema
            - If a column doesn't exist, use the closest available column or omit that part
            - Never assume column names - only use what's provided
            
            {table_context}
            {timezone_rules}
            {metrics_guidance}
            {context_info}
            """
            
            user_prompt = f"Generate SQL query for: {question}"
            
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=500,
                temperature=0.1
            )
            
            sql = response.choices[0].message.content.strip()
            
            # Clean up the response
            sql = sql.replace('```sql', '').replace('```', '').strip()
            
            # Enhanced validation and fixing
            column_names = schema.get('column_names', [])
            sql = self.validate_and_fix_sql_enhanced(sql, question, table, column_names, schema)
            
            print(f"✅ Generated SQL: {sql}")
            return sql
            
        except Exception as e:
            print(f"❌ Error generating SQL with OpenAI: {e}")
            return f"SELECT * FROM {table} LIMIT 10;"
    
    async def generate_sql_query(self, question: str, table: str, schema: dict, user_id: str, channel_id: str) -> Tuple[str, str]:
        """Main SQL generation function that combines multiple approaches"""
        try:
            print(f"🔧 Generating SQL for question: {question}")
            print(f"📋 Using table: {table}")
            
            # First, analyze the question intent
            intent = self.question_classifier.analyze_question_intent(question.lower())
            print(f"🔍 Question intent: {intent}")
            
            # Build structured instructions
            instructions = self.build_sql_instructions(intent, table, schema, question)
            print(f"📝 SQL instructions: {instructions}")
            
            # Try to generate SQL from instructions first
            sql_from_instructions = await self.generate_sql_from_instructions(question, table, schema, instructions)
            
            # Validate the generated SQL
            column_names = schema.get('column_names', [])
            is_valid, missing_columns = self.validate_sql_columns(sql_from_instructions, column_names)
            
            if is_valid:
                print(f"✅ Using instruction-based SQL: {sql_from_instructions}")
                return sql_from_instructions, "Generated using instruction-based approach"
            else:
                print(f"⚠️ Instruction-based SQL has issues: {missing_columns}")
                
                # Fall back to OpenAI generation
                sql_from_openai = await self.generate_sql_with_openai(question, table, schema)
                
                # Validate OpenAI generated SQL
                is_valid_openai, missing_columns_openai = self.validate_sql_columns(sql_from_openai, column_names)
                
                if is_valid_openai:
                    print(f"✅ Using OpenAI-generated SQL: {sql_from_openai}")
                    return sql_from_openai, "Generated using OpenAI with schema context"
                else:
                    print(f"⚠️ OpenAI SQL also has issues: {missing_columns_openai}")
                    
                    # Final fallback to simple query
                    fallback_sql = f"SELECT * FROM {table} LIMIT 10;"
                    print(f"🔄 Using fallback SQL: {fallback_sql}")
                    return fallback_sql, "Used fallback query due to validation errors"
            
        except Exception as e:
            print(f"❌ Error in SQL generation: {e}")
            fallback_sql = f"SELECT * FROM {table} LIMIT 10;"
            return fallback_sql, f"Error during generation: {str(e)}"
    
    async def record_feedback(self, question: str, sql: str, table: str, feedback_type: str):
        """Record user feedback for improving SQL generation"""
        try:
            import time
            
            feedback_data = {
                "question": question,
                "sql": sql,
                "table": table,
                "feedback_type": feedback_type,
                "timestamp": time.time()
            }
            
            # Create unique key for this feedback
            feedback_key = f"{self.valkey_manager.FEEDBACK_PREFIX}:{hash(f'{question}:{sql}')}"
            
            await self.valkey_manager.safe_valkey_set(
                feedback_key,
                feedback_data,
                ex=self.valkey_manager.FEEDBACK_CACHE_TTL
            )
            
            print(f"✅ Recorded feedback: {feedback_type} for question: {question}")
            
        except Exception as e:
            print(f"❌ Error recording feedback: {e}")