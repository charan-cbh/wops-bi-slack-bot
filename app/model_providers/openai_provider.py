#!/usr/bin/env python3
"""
OpenAI API Provider
Implements the BaseModelProvider interface for OpenAI models (including GPT-4, Assistant API)
"""

import os
import json
import asyncio
from typing import Dict, Any, Optional, List, Tuple
from .base_provider import BaseModelProvider, ModelProviderError, ModelProviderRateLimitError

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️ OpenAI package not installed. Install with: pip install openai")


class OpenAIProvider(BaseModelProvider):
    """OpenAI API provider implementation"""
    
    def __init__(self, api_key: str, model_name: str = "gpt-4", use_assistant_api: bool = True, assistant_id: str = None):
        super().__init__(api_key, model_name)
        
        if not OPENAI_AVAILABLE:
            raise ModelProviderError("OpenAI package not available. Install with: pip install openai")
        
        if not api_key:
            raise ModelProviderError("OpenAI API key not configured")
        
        self.client = openai.AsyncOpenAI(
            api_key=api_key,
            default_headers={"OpenAI-Beta": "assistants=v2"}
        )
        self.model_name = model_name
        self.use_assistant_api = use_assistant_api
        self.assistant_id = assistant_id
        self.max_tokens = 4096
        
        # Thread management for Assistant API
        self.threads = {}  # user_id -> thread_id mapping
        
        print(f"🤖 Initialized OpenAI provider with model: {model_name}")
        if use_assistant_api and assistant_id:
            print(f"   Using Assistant API with ID: {assistant_id}")
    
    async def generate_response(self, messages: List[Dict[str, str]], system_prompt: str = None, **kwargs) -> str:
        """Generate a response using OpenAI"""
        
        try:
            if self.use_assistant_api and self.assistant_id:
                return await self._generate_with_assistant(messages, system_prompt, **kwargs)
            else:
                return await self._generate_with_chat_completion(messages, system_prompt, **kwargs)
                
        except openai.RateLimitError as e:
            raise ModelProviderRateLimitError(f"OpenAI rate limit exceeded: {e}")
        except openai.AuthenticationError as e:
            raise ModelProviderError(f"OpenAI authentication error: {e}")
        except Exception as e:
            raise ModelProviderError(f"OpenAI API error: {e}")
    
    async def _generate_with_chat_completion(self, messages: List[Dict[str, str]], system_prompt: str = None, **kwargs) -> str:
        """Generate response using Chat Completion API"""
        
        # Format messages
        formatted_messages = []
        
        if system_prompt:
            formatted_messages.append({"role": "system", "content": system_prompt})
        
        formatted_messages.extend(messages)
        
        api_params = {
            'model': self.model_name,
            'messages': formatted_messages,
            'max_tokens': kwargs.get('max_tokens', self.max_tokens),
            'temperature': kwargs.get('temperature', 0.3)
        }
        
        self.log_request("chat_completion", **api_params)
        
        async def make_request():
            response = await self.client.chat.completions.create(**api_params)
            return response
        
        response = await self.retry_with_backoff(make_request)
        
        content = response.choices[0].message.content
        
        self.log_response("chat_completion", len(content), response.usage.total_tokens if response.usage else None)
        
        return content.strip()
    
    async def _generate_with_assistant(self, messages: List[Dict[str, str]], system_prompt: str = None, **kwargs) -> str:
        """Generate response using Assistant API"""
        
        user_id = kwargs.get('user_id', 'default')
        
        # Get or create thread
        thread_id = await self._get_or_create_thread(user_id)
        
        # Send message
        user_message = messages[-1]['content'] if messages else ""
        
        async def make_request():
            # Send message to thread
            await self.client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=user_message
            )
            
            # Run assistant
            run = await self.client.beta.threads.runs.create(
                thread_id=thread_id,
                assistant_id=self.assistant_id
            )
            
            # Wait for completion
            while run.status in ['queued', 'in_progress']:
                await asyncio.sleep(1)
                run = await self.client.beta.threads.runs.retrieve(
                    thread_id=thread_id,
                    run_id=run.id
                )
            
            if run.status != 'completed':
                raise ModelProviderError(f"Assistant run failed with status: {run.status}")
            
            # Get response
            messages_response = await self.client.beta.threads.messages.list(
                thread_id=thread_id,
                order="desc",
                limit=1
            )
            
            return messages_response.data[0].content[0].text.value
        
        response = await self.retry_with_backoff(make_request)
        
        self.log_response("assistant", len(response))
        
        return response.strip()
    
    async def _get_or_create_thread(self, user_id: str) -> str:
        """Get existing thread or create new one for user"""
        
        if user_id in self.threads:
            return self.threads[user_id]
        
        thread = await self.client.beta.threads.create()
        self.threads[user_id] = thread.id
        return thread.id
    
    async def generate_sql(self, question: str, instructions: str = None, context: Dict[str, Any] = None) -> str:
        """Generate SQL query using Chat Completions API with curated context"""
        
        # Build messages with system prompt and curated context
        messages = [
            {"role": "system", "content": "You are a specialized BI assistant for Worker Operations. Generate SQL queries for data analysis questions using the DBT_PRODUCTION schema, or provide conversational responses about business context. For SQL generation, return only the executable SQL query with right columns without markdown formatting."}
        ]
        
        # Add curated context from previous conversation if available
        if context and 'conversation_history' in context:
            history = context['conversation_history']
            # Only include the last question-answer pair for follow-up context
            if len(history) >= 2:
                messages.extend(history[-2:])  # Last Q&A pair
        
        # Handle retry context
        if context and 'retry_info' in context:
            retry_info = context['retry_info']
            retry_prompt = await self._build_retry_prompt(retry_info)
            messages.append({"role": "user", "content": retry_prompt})
        else:
            # Add current question
            messages.append({"role": "user", "content": question})
        
        # Use Chat Completions API directly with fine-tuned model
        response = await self._generate_with_chat_completion(messages)
        
        # Extract SQL from response (remove any markdown formatting)
        sql = response.strip()
        if sql.startswith('```sql'):
            sql = sql[6:]
        if sql.startswith('```'):
            sql = sql[3:]
        if sql.endswith('```'):
            sql = sql[:-3]
        
        return sql
    
    async def generate_sql_with_retry(self, question: str, max_retries: int = 5, context: Dict[str, Any] = None) -> Tuple[str, bool, str]:
        """
        Generate SQL with progressive retry mechanism
        Returns: (sql_query, success, error_message)
        """
        last_error = ""
        last_sql = ""
        
        for attempt in range(max_retries):
            try:
                print(f"🔄 SQL Generation Attempt {attempt + 1}/{max_retries}")
                
                # Build context for this attempt
                retry_context = self._build_retry_context(
                    question, attempt, last_sql, last_error, context
                )
                
                # Generate SQL query
                sql_query = await self.generate_sql(question, context=retry_context)
                
                # Execute the query to test it
                from app.snowflake_runner import run_query
                result = run_query(sql_query)
                
                # Check if execution was successful
                if isinstance(result, str):
                    # Query failed
                    last_error = result
                    last_sql = sql_query
                    print(f"❌ Attempt {attempt + 1} failed: {result[:100]}...")
                    continue
                else:
                    # Query succeeded
                    print(f"✅ SQL generation succeeded on attempt {attempt + 1}")
                    return sql_query, True, ""
                    
            except Exception as e:
                last_error = str(e)
                print(f"❌ Attempt {attempt + 1} error: {e}")
                continue
        
        # All attempts failed
        return last_sql, False, last_error
    
    def _build_retry_context(self, question: str, attempt: int, last_sql: str, last_error: str, original_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Build context for retry attempts"""
        retry_context = {}
        
        # Include original context if available
        if original_context:
            retry_context.update(original_context)
        
        if attempt == 0:
            # First attempt - use original context only
            return retry_context
        
        # Retry attempts - add error feedback
        retry_info = {
            "retry_attempt": attempt + 1,
            "previous_sql": last_sql,
            "error_message": last_error,
            "original_question": question
        }
        
        if attempt == 2:  # Third attempt (0-indexed)
            # Enhanced retry with schema and training examples
            retry_info["enhanced_retry"] = True
        elif attempt >= 3:  # Fourth and fifth attempts (0-indexed)
            # Data context retry with sample data
            retry_info["data_context_retry"] = True
            retry_info["sample_data_needed"] = True
            
        retry_context["retry_info"] = retry_info
        return retry_context
    
    async def _build_retry_prompt(self, retry_info: Dict[str, Any]) -> str:
        """Build retry prompt with error feedback and enhanced context"""
        attempt = retry_info["retry_attempt"]
        question = retry_info["original_question"]
        error = retry_info["error_message"]
        previous_sql = retry_info["previous_sql"]
        
        base_prompt = f"""The previous SQL query failed with an error. Please regenerate a corrected query.

Original Question: {question}

Previous Failed Query:
{previous_sql}

Error Message:
{error}"""
        
        # Check if this is a column/schema related error
        is_schema_error = self._is_schema_error(error)
        
        if is_schema_error:
            # Get table schema for any schema-related error (not just attempt 3)
            print(f"🔍 Schema error detected, getting table schema for retry attempt {attempt}")
            table_name = self._extract_table_name(previous_sql)
            if table_name:
                schema_info = await self._get_table_schema(table_name)
                if schema_info and "Error" not in schema_info:
                    base_prompt += f"""

ACTUAL TABLE SCHEMA:
{schema_info}

Please regenerate the SQL query using ONLY the columns that exist in the above schema. Map the intended columns from your original query to the correct column names that actually exist in the table."""
                else:
                    base_prompt += f"\n\nNote: Could not retrieve schema for {table_name}. Please use standard column names."
        
        # Enhanced retry (attempt 3) with additional training examples
        if retry_info.get("enhanced_retry", False):
            training_examples = await self._get_relevant_training_examples(question)
            if training_examples and "Error" not in training_examples:
                base_prompt += f"""

RELEVANT TRAINING EXAMPLES:
{training_examples}"""
        
        # Data context retry (attempts 4-5) with sample data
        if retry_info.get("data_context_retry", False):
            print(f"🔍 Data context retry - getting sample data for attempt {attempt}")
            table_names = self._extract_all_table_names(previous_sql)
            
            if table_names:
                sample_data_sections = []
                for table_name in table_names:
                    table_samples = await self._get_table_samples(table_name)
                    if table_samples and "Error" not in table_samples:
                        sample_data_sections.append(f"""
TABLE: {table_name}
{table_samples}""")
                
                if sample_data_sections:
                    base_prompt += f"""

SAMPLE DATA CONTEXT:
The following shows actual data from the table(s) to help you understand the data formats, types, and structure. Use this to generate a more accurate query.
{''.join(sample_data_sections)}

IMPORTANT: Use the exact column names and data formats shown in the sample data above. Pay attention to:
- Column names (case sensitive)
- Data types (TEXT, INTEGER, DECIMAL, TIMESTAMP, etc.)
- Date/timestamp formats
- NULL vs empty values
- Actual value patterns"""
        
        base_prompt += "\n\nPlease generate a corrected SQL query that fixes this error."
        
        return base_prompt
    
    def _is_schema_error(self, error: str) -> bool:
        """Check if the error is related to schema/column issues"""
        error_lower = error.lower()
        schema_error_keywords = [
            'invalid identifier',
            'column not found',
            'unknown column',
            'column does not exist',
            'no such column',
            'undefined column',
            'invalid column',
            'compilation error'
        ]
        
        return any(keyword in error_lower for keyword in schema_error_keywords)
    
    async def _get_enhanced_retry_context(self, failed_sql: str, question: str, error: str) -> str:
        """Get enhanced context for final retry: schema + training examples"""
        enhanced_parts = []
        
        # 1. Get table schema using DESCRIBE
        table_name = self._extract_table_name(failed_sql)
        if table_name:
            schema_info = await self._get_table_schema(table_name)
            if schema_info:
                enhanced_parts.append(f"TABLE SCHEMA:\n{schema_info}")
            
            # 2. Get recent data samples (most important for context)
            sample_data = await self._get_table_samples(table_name)
            if sample_data:
                enhanced_parts.append(f"RECENT DATA SAMPLES:\n{sample_data}")
        
        # 3. Get relevant training examples
        training_examples = await self._get_relevant_training_examples(question)
        if training_examples:
            enhanced_parts.append(f"RELEVANT TRAINING EXAMPLES:\n{training_examples}")
        
        return "\n\n".join(enhanced_parts)
    
    def _extract_table_name(self, sql: str) -> str:
        """Extract main table name from SQL query"""
        import re
        
        # Look for FROM clause patterns
        from_match = re.search(r'FROM\s+([A-Za-z0-9_.]+)', sql, re.IGNORECASE)
        if from_match:
            return from_match.group(1)
        
        return ""
    
    def _extract_all_table_names(self, sql: str) -> List[str]:
        """Extract all table names from SQL query (handles JOINs and multiple tables)"""
        import re
        
        table_names = set()
        sql_upper = sql.upper()
        
        # Common table name patterns
        patterns = [
            r'FROM\s+([A-Za-z0-9_.]+)',  # FROM clause
            r'JOIN\s+([A-Za-z0-9_.]+)',  # JOIN clauses
            r'LEFT\s+JOIN\s+([A-Za-z0-9_.]+)',  # LEFT JOIN
            r'RIGHT\s+JOIN\s+([A-Za-z0-9_.]+)',  # RIGHT JOIN  
            r'INNER\s+JOIN\s+([A-Za-z0-9_.]+)',  # INNER JOIN
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            for match in matches:
                # Clean up table name (remove aliases, schema prefixes if needed)
                table_name = match.strip()
                if table_name and not table_name.upper() in ['AS', 'ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING']:
                    table_names.add(table_name)
        
        # Remove duplicates and return as list
        return list(table_names)[:3]  # Limit to first 3 tables to avoid too much data
    
    async def _get_table_schema(self, table_name: str) -> str:
        """Get table schema using DESCRIBE query"""
        try:
            from app.snowflake_runner import run_query
            
            describe_sql = f"DESCRIBE TABLE {table_name}"
            result = run_query(describe_sql)
            
            if isinstance(result, str):
                return f"Error getting schema: {result}"
            
            # Format the schema nicely
            if hasattr(result, 'to_string'):
                return result.to_string(index=False)
            
            return str(result)
            
        except Exception as e:
            return f"Error getting schema: {str(e)}"
    
    async def _get_table_samples(self, table_name: str) -> str:
        """Get recent data samples from table ordered by audit timestamp/creation date"""
        try:
            from app.snowflake_runner import run_query
            
            # Common audit/timestamp column names to try
            timestamp_columns = [
                'AUDIT_TIMESTAMP', 'CREATED_AT_PST', 'CREATED_AT', 'UPDATED_AT', 
                'SOLVED_AT', 'SUBMITTED_AT', 'ADHERENCE_DATE', 'SOLVED_WEEK'
            ]
            
            # First, get table schema to find the right timestamp column
            describe_sql = f"DESCRIBE TABLE {table_name}"
            schema_result = run_query(describe_sql)
            
            if isinstance(schema_result, str):
                return f"Error getting schema for samples: {schema_result}"
            
            # Find the best timestamp column from the schema
            available_columns = []
            timestamp_column = None
            
            if hasattr(schema_result, 'iterrows'):
                for _, row in schema_result.iterrows():
                    col_name = row['name'] if 'name' in row else str(row.iloc[0])
                    available_columns.append(col_name)
                    
                    # Check if this is a timestamp column we can use
                    if col_name.upper() in [tc.upper() for tc in timestamp_columns]:
                        timestamp_column = col_name
                        break
            
            # Build the sample query
            if timestamp_column:
                sample_sql = f"""
                SELECT * 
                FROM {table_name} 
                WHERE {timestamp_column} IS NOT NULL 
                ORDER BY {timestamp_column} DESC 
                LIMIT 3
                """
            else:
                # Fallback: just get recent records without ordering
                sample_sql = f"SELECT * FROM {table_name} LIMIT 3"
            
            print(f"🔍 Getting samples with: {sample_sql}")
            
            # Execute sample query
            sample_result = run_query(sample_sql)
            
            if isinstance(sample_result, str):
                return f"Error getting samples: {sample_result}"
            
            # Format the samples nicely with column info
            if hasattr(sample_result, 'to_string'):
                # Limit column width for readability
                sample_str = sample_result.to_string(index=False, max_cols=10, max_colwidth=50)
                
                # Add context about the samples
                context_info = f"Showing {len(sample_result)} most recent records"
                if timestamp_column:
                    context_info += f" (ordered by {timestamp_column} DESC)"
                
                return f"{context_info}:\n\n{sample_str}"
            
            return str(sample_result)
            
        except Exception as e:
            print(f"⚠️ Error getting table samples: {e}")
            return f"Error getting samples: {str(e)}"
    
    async def _get_relevant_training_examples(self, question: str) -> str:
        """Get relevant examples from training dataset based on keywords"""
        try:
            # Extract keywords from question
            keywords = self._extract_keywords(question)
            
            # Search training dataset
            examples = await self._search_training_dataset(keywords)
            
            return examples
            
        except Exception as e:
            return f"Error getting training examples: {str(e)}"
    
    def _extract_keywords(self, question: str) -> List[str]:
        """Extract relevant keywords from question"""
        import re
        question_lower = question.lower()
        
        # Important BI keywords
        keywords = []
        keyword_patterns = [
            'average', 'avg', 'handle time', 'aht', 'agent', 'tickets',
            'chat', 'voice', 'email', 'qa score', 'adherence', 'performance',
            'team', 'supervisor', 'count', 'total', 'last week', 'this week',
            'today', 'yesterday', 'fcr', 'resolution', 'schedule', 'scheduler'
        ]
        
        for keyword in keyword_patterns:
            if keyword in question_lower:
                keywords.append(keyword)
        
        # Extract person names (capitalized words that might be names)
        person_names = re.findall(r"\b([A-Z][a-z]+(?:'s)?)\b", question)
        for name in person_names:
            if name.lower() not in ['what', 'how', 'show', 'this', 'last', 'the']:
                keywords.append(name.lower().replace("'s", ""))
        
        return keywords[:8]  # Increased limit to include more keywords
    
    async def _search_training_dataset(self, keywords: List[str]) -> str:
        """Search training dataset for relevant examples"""
        try:
            import json
            
            # Read training dataset
            training_file = "DEFINITIVE_100_PERCENT_DATASET.jsonl"
            relevant_examples = []
            
            with open(training_file, 'r') as f:
                for line in f:
                    try:
                        example = json.loads(line)
                        if 'messages' in example:
                            user_message = ""
                            assistant_message = ""
                            
                            for msg in example['messages']:
                                if msg['role'] == 'user':
                                    user_message = msg['content']
                                elif msg['role'] == 'assistant':
                                    assistant_message = msg['content']
                            
                            # Check if any keywords match
                            user_lower = user_message.lower()
                            if any(keyword in user_lower for keyword in keywords):
                                relevant_examples.append({
                                    'question': user_message,
                                    'sql': assistant_message
                                })
                                
                                if len(relevant_examples) >= 5:  # Increased to 5 examples
                                    break
                    except:
                        continue
            
            # Format examples
            if relevant_examples:
                formatted = []
                for i, ex in enumerate(relevant_examples, 1):
                    formatted.append(f"Example {i}:\nQuestion: {ex['question']}\nSQL: {ex['sql']}")
                
                return "\n\n".join(formatted)
            
            return "No relevant training examples found."
            
        except Exception as e:
            return f"Error searching training dataset: {str(e)}"
    
    async def summarize_results(self, question: str, results: str, sql_query: str = None, context: Dict[str, Any] = None) -> str:
        """Summarize query results using OpenAI"""
        
        system_prompt = """You are a business intelligence analyst who specializes in interpreting data query results and presenting them in clear, actionable insights.

Your task is to:
1. Analyze the data results and provide clear, business-friendly insights
2. Use bullet points and clear formatting for readability
3. Include specific numbers and metrics from the results
4. Highlight key findings and trends
5. Provide context about what the results mean for business operations
6. If the SQL query is provided, include it in a code block at the end

FORMATTING RULES FOR SLACK:
- Use *text* for bold (single asterisk), NOT **text**
- Use bullet points with - or •
- Keep formatting simple and Slack-compatible
- Avoid markdown that doesn't work in Slack

Be concise but informative. Focus on actionable insights rather than just restating the data."""

        user_message = f"""Please analyze and summarize these query results:

Original Question: {question}

Query Results:
{results}

{f"SQL Query Used: {sql_query}" if sql_query and self._user_requested_sql(question) else ""}

Provide a clear, business-friendly summary with key insights. Only include the SQL query in your response if it was explicitly provided in the context above."""

        messages = [{"role": "user", "content": user_message}]
        
        response = await self.generate_response(messages, system_prompt)
        
        # Only show SQL if user explicitly requested it
        if sql_query and "```sql" not in response and self._user_requested_sql(question):
            response += f"\n\n📊 *Generated SQL Query:*\n```sql\n{sql_query}\n```"
        
        return response
    
    async def classify_question(self, question: str, context: Dict[str, Any] = None) -> str:
        """Classify question type using OpenAI"""
        
        system_prompt = """You are a question classifier for a business intelligence system. Classify user questions into one of these categories:

1. "sql_required" - Questions that need data analysis, metrics, reports, or specific business information
2. "conversational" - General questions, greetings, help requests, or non-data questions

Analyze the question and return ONLY one of these classifications: "sql_required" or "conversational"

Examples:
- "How many agents are in the team?" → sql_required
- "What's the average handle time?" → sql_required  
- "Show me QA scores for last month" → sql_required
- "Hello" → conversational
- "How does this bot work?" → conversational
- "Thank you" → conversational"""

        user_message = f"""Classify this question: {question}

Return only "sql_required" or "conversational"."""

        messages = [{"role": "user", "content": user_message}]
        
        response = await self.generate_response(messages, system_prompt)
        
        classification = response.strip().lower()
        if 'sql_required' in classification:
            return 'sql_required'
        elif 'conversational' in classification:
            return 'conversational'
        else:
            # Default to sql_required for ambiguous cases
            return 'sql_required'
    
    async def handle_conversational(self, question: str, context: Dict[str, Any] = None) -> str:
        """Handle conversational questions using OpenAI"""
        
        system_prompt = """You are a specialized BI assistant for Worker Operations. Generate SQL queries for data analysis questions using the DBT_PRODUCTION schema, or provide conversational responses about business context. For SQL generation, return only the executable SQL query with right columns without markdown formatting."""

        user_message = question
        if context:
            user_message += f"\n\nContext: {json.dumps(context, indent=2)}"

        messages = [{"role": "user", "content": user_message}]
        
        return await self.generate_response(messages, system_prompt)
    
    def _user_requested_sql(self, question: str) -> bool:
        """Check if user explicitly requested to see SQL query"""
        question_lower = question.lower()
        sql_request_phrases = [
            'show sql', 'show the sql', 'show me the sql', 'show me sql',
            'what sql', 'what query', 'show query', 'show the query', 'show me the query',
            'sql query', 'generate sql', 'what is the sql', 'sql used',
            'let me see the sql', 'display sql', 'include sql',
            'with sql', 'and sql', 'also show sql', 'query used'
        ]
        return any(phrase in question_lower for phrase in sql_request_phrases)


# Factory function to create provider instances
def create_openai_provider(api_key: str = None, model_name: str = None, use_assistant_api: bool = None, assistant_id: str = None) -> OpenAIProvider:
    """Create an OpenAI provider instance"""
    
    if not api_key:
        api_key = os.getenv('OPENAI_API_KEY')
    
    if not model_name:
        # Default to latest fine-tuned model (v5) with comprehensive training data
        model_name = os.getenv('OPENAI_MODEL', 'ft:gpt-4o-mini-2024-07-18:clipboard-health:wops-bi-bot-v5:By0reSwZ')
    
    if use_assistant_api is None:
        use_assistant_api = os.getenv('USE_ASSISTANT_API', 'false').lower() == 'true'
    
    if not assistant_id:
        assistant_id = os.getenv('ASSISTANT_ID')
    
    return OpenAIProvider(api_key, model_name, use_assistant_api, assistant_id)