import os
import time
import json
from typing import Dict, Any, Tuple, Optional, List
from app.cache_manager import cache_manager
from app.question_analyzer import question_analyzer
from app.table_discovery import table_discovery
from app.sql_generator import sql_generator
from app.conversation_manager import conversation_manager
from app.result_processor import result_processor
from app.model_providers.provider_factory import get_model_provider, get_provider_info

# Configuration
USE_ASSISTANT_API = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"
ASSISTANT_ID = os.getenv("ASSISTANT_ID", "")
MAX_SQL_ATTEMPTS = int(os.getenv("MAX_SQL_ATTEMPTS", "3"))
ENABLE_CACHE = os.getenv("ENABLE_CACHE", "true").lower() == "true"
MODEL_PROVIDER = os.getenv("MODEL_PROVIDER", "openai").lower()


class LLMOrchestrator:
    """Main orchestrator that coordinates all LLM operations"""
    
    def __init__(self):
        self.cache_manager = cache_manager
        self.question_analyzer = question_analyzer
        self.table_discovery = table_discovery
        self.sql_generator = sql_generator
        self.conversation_manager = conversation_manager
        self.result_processor = result_processor
        
        # Initialize model provider
        try:
            self.model_provider = get_model_provider()
            provider_info = get_provider_info()
            print(f"🤖 LLM Orchestrator initialized with {provider_info['provider_name']} provider ({provider_info['model_name']})")
        except Exception as e:
            print(f"⚠️ Error initializing model provider: {e}")
            self.model_provider = None
    
    async def handle_question(self, question: str, user_id: str, channel_id: str, assistant_id: str = None) -> Tuple[str, str]:
        """
        Main entry point for handling questions with intelligent routing
        Returns: (response, response_type)
        """
        print(f"🎯 Handling question: {question[:100]}...")
        
        # Check rate limits first
        estimated_tokens = self.conversation_manager.estimate_request_tokens(question)
        rate_limit_info = await self.conversation_manager.check_rate_limits(user_id, channel_id, estimated_tokens)
        
        if not rate_limit_info['allowed']:
            return self._format_rate_limit_message(rate_limit_info), 'rate_limited'
        
        # Get conversation context
        context = await self.conversation_manager.get_conversation_context(user_id, channel_id)
        
        # Classify question type using model provider
        try:
            if self.model_provider:
                classification = await self.model_provider.classify_question(question, context)
            else:
                # Fallback to local analyzer
                classification = self.question_analyzer.classify_question_type(question)
        except Exception as e:
            print(f"⚠️ Error classifying question with model provider: {e}")
            classification = self.question_analyzer.classify_question_type(question)
        
        print(f"📊 Question classified as: {classification}")
        
        # Use unified Assistant API approach for ALL questions when model provider is available
        if self.model_provider and self.model_provider.use_assistant_api and self.model_provider.assistant_id:
            print("🤖 Using unified Assistant API approach with vector store")
            return await self._handle_question_with_assistant(question, user_id, channel_id, context)
        else:
            # Fallback to original routing for non-Assistant API providers
            if classification == 'conversational':
                try:
                    if self.model_provider:
                        response = await self.model_provider.handle_conversational(question, context)
                    else:
                        response = await self.result_processor.handle_conversational_question(question, user_id, channel_id)
                except Exception as e:
                    print(f"⚠️ Error handling conversational question with model provider: {e}")
                    response = await self.result_processor.handle_conversational_question(question, user_id, channel_id)
                return response, 'conversational'
            else:
                # SQL required - generate and execute SQL, return results
                sql_response = await self._handle_sql_question(question, user_id, channel_id, assistant_id)
                # Check if response contains actual data results or just raw SQL
                if sql_response.startswith(('--', '❌', 'Error:')):
                    # This is an error or raw SQL, needs further processing
                    return sql_response, 'sql'
                else:
                    # This is processed data results, ready for display
                    return sql_response, 'sql_with_data'
    
    async def _handle_question_with_assistant(self, question: str, user_id: str, channel_id: str, context: Dict[str, Any]) -> Tuple[str, str]:
        """
        Handle all questions using Assistant API with vector store
        For data questions: Assistant generates SQL -> Execute -> Assistant summarizes
        For conversational questions: Assistant provides direct response
        """
        try:
            # Build curated context from conversation history
            curated_context = await self._build_curated_context(user_id, channel_id)
            
            # Use OpenAI to classify the question type
            if await self._requires_sql_query_ai(question):
                # Data question: Use retry mechanism for SQL generation
                sql_query, success, error_message = await self.model_provider.generate_sql_with_retry(
                    question, max_retries=3, context=curated_context
                )
                
                if success:
                    # SQL generation and execution succeeded
                    from app.snowflake_runner import run_query
                    df = run_query(sql_query)
                    
                    # Summarize the results
                    result = await self._summarize_sql_results(question, df, sql_query, user_id, channel_id, context)
                    
                    # Update conversation history with Q&A pair
                    await self._update_conversation_history(user_id, channel_id, question, sql_query, 'sql')
                    
                    return result
                else:
                    # All retries failed, return error with final attempt
                    error_response = f"❌ Failed to generate working SQL query after 3 attempts.\n\nFinal error: {error_message}\n\nLast SQL attempted:\n```sql\n{sql_query}\n```"
                    
                    await self._update_conversation_history(user_id, channel_id, question, error_response, 'error')
                    return error_response, 'error'
            else:
                # Conversational question: Use fine-tuned model with Chat Completions API
                response = await self._handle_conversational_with_finetuned_model(question, curated_context)
                await self._update_conversation_history(user_id, channel_id, question, response, 'conversational')
                return response, 'conversational'
            
        except Exception as e:
            print(f"❌ Error handling question with Assistant API: {e}")
            # Fallback to original approach
            return f"❌ Error processing your question: {str(e)}", 'error'
    
    async def _handle_data_question_with_assistant(self, question: str, user_id: str, channel_id: str, context: Dict[str, Any]) -> Tuple[str, str]:
        """Handle data questions using Assistant API for SQL generation and result summarization"""
        try:
            # Step 1: Generate SQL using Assistant API with vector store context
            sql_query = await self.model_provider.generate_sql(question)
            
            print(f"🔍 Generated SQL: {sql_query[:100]}...")
            
            # Step 2: Execute SQL
            try:
                from app.snowflake_runner import run_query
                df = run_query(sql_query)
                
                # Check if query returned an error (now returned as string)
                if isinstance(df, str):
                    return f"❌ Query execution failed: {df}", 'error'
                
                # Check if query returned no data (empty results)
                if hasattr(df, 'empty') and df.empty:
                    # Provide helpful guidance for empty results
                    helpful_message = self._generate_helpful_empty_result_message(question, sql_query)
                    return helpful_message, 'sql_with_data'
                
                # Convert DataFrame to string for summarization
                result_table = df.to_string(index=False, max_rows=50)
                
                # Step 3: Summarize results using Assistant API with vector store context
                summary_prompt = f"""Provide a concise answer to this question based on the query results:

Question: {question}

Results:
{result_table}

Instructions:
- Give a direct, brief answer to the user's question
- Include the key numbers/metrics they asked for
- Keep it concise - no technical explanations about tables or procedures
- Format numbers clearly (e.g., "QA score: 85.2%", "AHT: 12.5 minutes")
- Only mention insights if they are directly relevant and brief
- Do not explain what QA scores mean or suggest follow-up actions unless asked

SLACK FORMATTING:
- Use *text* for bold (single asterisk), NOT **text**
- Use - for bullet points
- Keep formatting simple and Slack-compatible"""

                final_response = await self.model_provider.handle_conversational(summary_prompt, context)
                return final_response, 'sql_with_data'
                
            except Exception as sql_error:
                print(f"❌ SQL execution error: {sql_error}")
                
                # Check if it's a date-related SQL error and try a simpler approach
                if "001003" in str(sql_error) or "SQL compilation error" in str(sql_error):
                    print("🔄 Detected SQL compilation error, trying simplified query...")
                    
                    # Try to regenerate with simpler date logic
                    simplified_prompt = f"""Generate a simpler SQL query for this question using basic date comparisons:

{question}

CRITICAL INSTRUCTIONS:
- Use EXACT table names: ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
- Use correct column names: ASSIGNEE_NAME, QA_SCORE, SOLVED_WEEK
- For "last week" use: WHERE SOLVED_WEEK < DATE_TRUNC('week', CURRENT_DATE)
- For agent names use: WHERE ASSIGNEE_NAME LIKE '%name%'
- Keep the query as simple as possible
- Return ONLY the SQL query"""
                    
                    try:
                        simplified_sql = await self.model_provider.handle_conversational(simplified_prompt)
                        
                        print(f"🔄 Trying simplified SQL: {simplified_sql[:100]}...")
                        
                        from app.snowflake_runner import run_query
                        df_retry = run_query(simplified_sql)
                        
                        if not isinstance(df_retry, str) and hasattr(df_retry, 'empty') and not df_retry.empty:
                            result_table = df_retry.to_string(index=False, max_rows=50)
                            summary_prompt = f"""Provide a concise answer based on these results:

Question: {question}
Results: {result_table}

Give a direct, brief answer with key numbers only."""
                            
                            final_response = await self.model_provider.handle_conversational(summary_prompt, context)
                            return final_response, 'sql_with_data'
                    
                    except Exception as retry_error:
                        print(f"❌ Simplified query also failed: {retry_error}")
                
                return f"❌ Error executing query: {str(sql_error)}", 'sql'
            
        except Exception as e:
            print(f"❌ Error in data question handling: {e}")
            return f"❌ Error processing data question: {str(e)}", 'error'
    
    async def _handle_sql_question(self, question: str, user_id: str, channel_id: str, assistant_id: str = None) -> str:
        """Handle questions that require SQL generation using fine-tuned model directly"""
        try:
            # Use fine-tuned model directly with retry mechanism (same as Assistant API path)
            if self.model_provider:
                curated_context = await self._build_curated_context(user_id, channel_id)
                sql_query, success, error_message = await self.model_provider.generate_sql_with_retry(
                    question, max_retries=3, context=curated_context
                )
                
                if not success:
                    return f"-- Error: Failed to generate working SQL after 3 attempts: {error_message}"
                
                sql = sql_query
            else:
                return "-- Error: No model provider available"
            
            # Execute the SQL and get results
            try:
                from app.snowflake_runner import run_query
                df = run_query(sql)
                
                if isinstance(df, str):
                    # Query execution failed - df is the error message
                    return f"❌ Query execution failed: {df}"
                
                if df.empty:
                    return f"❌ Query execution succeeded but returned no data"
                
                # Convert DataFrame to string for summarization
                result_table = df.to_string(index=False, max_rows=50)
                
                # Process and summarize the results using model provider
                try:
                    if self.model_provider:
                        final_response = await self.model_provider.summarize_results(
                            question, result_table, sql, {'user_id': user_id, 'channel_id': channel_id}
                        )
                    else:
                        final_response = await self.result_processor.summarize_with_assistant(
                            question, result_table, user_id, channel_id, assistant_id or "", sql
                        )
                except Exception as e:
                    print(f"⚠️ Error summarizing results with model provider: {e}")
                    final_response = await self.result_processor.summarize_with_assistant(
                        question, result_table, user_id, channel_id, assistant_id or "", sql
                    )
                
                # Extract table name from SQL for conversation context
                selected_table = self._extract_table_name_from_sql(sql)
                
                # Update conversation context with successful results
                await self.conversation_manager.update_conversation_context_with_sql(
                    user_id, channel_id, question, sql, selected_table, success=True
                )
                
                return final_response
                
            except Exception as e:
                print(f"❌ Error executing SQL: {e}")
                # Return SQL with error explanation if execution fails
                return f"❌ Could not execute query: {str(e)}\n\nGenerated SQL:\n{sql}"
            
        except Exception as e:
            print(f"❌ Error handling SQL question: {e}")
            return f"-- Error: Could not generate SQL: {str(e)}"
    
    async def _generate_sql_for_table(self, question: str, table: str, schema: Dict, user_id: str, channel_id: str) -> str:
        """Generate SQL for a specific table"""
        try:
            thread_id = await self.conversation_manager.get_or_create_thread(user_id, channel_id)
            if not thread_id:
                return "-- Error: Could not create assistant thread"
            
            # Analyze question intent
            intent = self.question_analyzer.analyze_question_intent(question.lower())
            
            # Check if this is a truly personal question (not just team-specific)
            is_truly_personal = any(phrase in question.lower() for phrase in ['my team', 'my performance', 'my kpis', 'my tickets'])
            
            # Check if intelligent data analyst can handle this directly
            if is_truly_personal:
                print("🧠 Detected personal question - checking if user recognition is enabled")
                try:
                    from app.user_context_manager import ENABLE_USER_RECOGNITION
                    from app.intelligent_data_analyst import intelligent_data_analyst
                    
                    if not ENABLE_USER_RECOGNITION:
                        print("⚠️ Personal questions require user recognition feature (ENABLE_USER_RECOGNITION=true)")
                        return "❌ Personal questions are not available. Please specify names explicitly (e.g., 'how many agents in Ricardo Birck's team?')"
                    
                    # Create intent structure for intelligent analyst
                    intelligent_intent = {
                        'question_type': 'team_questions',
                        'required_table': table,
                        'confidence': 100,
                        'is_personal': intent.get('is_personal', True),
                        'personal_context': intent.get('personal_context', 'team_or_personal')
                    }
                    
                    sql, explanation = await intelligent_data_analyst.generate_intelligent_sql(
                        question, intelligent_intent, schema, user_id
                    )
                    
                    if not sql.startswith('--'):
                        print(f"✅ Generated personal SQL: {sql[:100]}...")
                        return sql
                    else:
                        print("⚠️ Intelligent analyst couldn't generate SQL, falling back to Assistant")
                        
                except Exception as e:
                    print(f"⚠️ Error with intelligent analyst: {e}")
            
            # Try intelligent data analyst for non-personal questions too
            if not is_truly_personal:
                print("🧠 Trying intelligent data analyst for non-personal question")
                try:
                    from app.intelligent_data_analyst import intelligent_data_analyst
                    
                    # Create intent structure for intelligent analyst
                    intelligent_intent = {
                        'question_type': intent.get('primary_intent', 'general'),
                        'required_table': table,
                        'confidence': 100,
                        'is_personal': False,
                        'personal_context': None
                    }
                    
                    sql, explanation = await intelligent_data_analyst.generate_intelligent_sql(
                        question, intelligent_intent, schema, user_id
                    )
                    
                    if not sql.startswith('--'):
                        print(f"✅ Generated intelligent SQL: {sql[:100]}...")
                        return sql
                    else:
                        print("⚠️ Intelligent analyst couldn't generate SQL, falling back to Assistant")
                        
                except Exception as e:
                    print(f"⚠️ Error with intelligent analyst: {e}")
            
            # Fallback to model provider for SQL generation
            try:
                if self.model_provider:
                    # Build comprehensive instructions 
                    instructions = await self.sql_generator.build_enhanced_sql_instructions(intent, table, schema, question, user_id)
                    
                    # Use model provider to generate SQL
                    sql = await self.model_provider.generate_sql(question)
                    
                    # Validate and fix SQL
                    validated_sql = self.sql_generator.validate_and_fix_sql(sql, question, table, schema.get('columns', []))
                    
                    return validated_sql
                else:
                    # Fallback to original Assistant API method
                    instructions = await self.sql_generator.build_enhanced_sql_instructions(intent, table, schema, question, user_id)
                    
                    # Create message for the assistant
                    message = f"""Generate a SQL query to answer this question:

Question: {question}
Table: {table}
Available columns: {', '.join(schema.get('columns', []))}

Requirements:
- Use only columns that exist in the table
- Include appropriate filters and aggregations
- Handle NULL values properly
- Add meaningful aliases
- Include proper ORDER BY if ranking/sorting is needed
- Add LIMIT clause if appropriate

Return ONLY the SQL query, no explanations."""
                    
                    # Send to assistant
                    response = await self.conversation_manager.send_message_and_run(
                        thread_id, message, instructions['instructions']
                    )
                    
                    # Extract and validate SQL
                    sql = self.sql_generator.extract_sql_from_response(response)
                    validated_sql = self.sql_generator.validate_and_fix_sql(sql, question, table, schema.get('columns', []))
                    
                    return validated_sql
                    
            except Exception as provider_error:
                print(f"⚠️ Error with model provider SQL generation: {provider_error}")
                # Final fallback to original method
                instructions = await self.sql_generator.build_enhanced_sql_instructions(intent, table, schema, question, user_id)
                
                response = await self.conversation_manager.send_message_and_run(
                    thread_id, message, instructions['instructions']
                )
                
                sql = self.sql_generator.extract_sql_from_response(response)
                validated_sql = self.sql_generator.validate_and_fix_sql(sql, question, table, schema.get('columns', []))
                
                return validated_sql
            
        except Exception as e:
            print(f"❌ Error generating SQL: {e}")
            return f"-- Error generating SQL: {str(e)}"
    
    def _is_sql_response(self, response: str) -> bool:
        """Check if the response from Assistant is a SQL query"""
        response_clean = response.strip().upper()
        
        # Check if it starts with SQL keywords
        sql_starters = ['SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER']
        
        return any(response_clean.startswith(starter) for starter in sql_starters)
    
    def _extract_table_name_from_sql(self, sql: str) -> str:
        """Extract table name from SQL query for context tracking"""
        import re
        
        # Look for FROM clause patterns
        from_match = re.search(r'FROM\s+([A-Za-z0-9_.]+)', sql, re.IGNORECASE)
        if from_match:
            return from_match.group(1)
        
        return "unknown_table"
    
    async def _handle_sql_response(self, question: str, sql_query: str, user_id: str, channel_id: str, context: Dict[str, Any]) -> Tuple[str, str]:
        """Execute SQL query and summarize results"""
        try:
            print(f"🔍 Generated SQL: {sql_query[:100]}...")
            
            # Execute SQL
            from app.snowflake_runner import run_query
            df = run_query(sql_query)
            
            # Check if query returned an error
            if isinstance(df, str):
                return f"❌ Query execution failed: {df}", 'error'
            
            # Check if query returned no data
            if hasattr(df, 'empty') and df.empty:
                helpful_message = self._generate_helpful_empty_result_message(question, sql_query)
                return helpful_message, 'sql_with_data'
            
            # Convert DataFrame to string for summarization
            result_table = df.to_string(index=False, max_rows=50)
            
            # Summarize results using Assistant API
            summary_prompt = f"""Provide a concise answer to this question based on the query results:

Question: {question}

Results:
{result_table}

Instructions:
- Give a direct, brief answer to the user's question
- Include the key numbers/metrics they asked for
- Keep it concise - no technical explanations about tables or procedures
- Format numbers clearly (e.g., "QA score: 85.2%", "AHT: 12.5 minutes")
- Only mention insights if they are directly relevant and brief
- Do not explain what QA scores mean or suggest follow-up actions unless asked

SLACK FORMATTING:
- Use *text* for bold (single asterisk), NOT **text**
- Use - for bullet points
- Keep formatting simple and Slack-compatible"""

            final_response = await self.model_provider.handle_conversational(summary_prompt)
            return final_response, 'sql_with_data'
            
        except Exception as e:
            print(f"❌ SQL execution error: {e}")
            return f"❌ Error executing query: {str(e)}", 'error'
    
    async def _build_curated_context(self, user_id: str, channel_id: str) -> Dict[str, Any]:
        """Build curated context from conversation history for SQL generation"""
        try:
            # Get recent conversation history (last 2-3 exchanges)
            if hasattr(self, 'conversation_manager') and self.conversation_manager:
                recent_context = await self.conversation_manager.get_conversation_context(user_id, channel_id)
                
                # Build conversation history in OpenAI format
                conversation_history = []
                
                if recent_context and 'last_question' in recent_context and 'last_response' in recent_context:
                    # Only include the last Q&A pair to avoid contamination
                    last_question = recent_context['last_question']
                    last_response = recent_context['last_response']
                    
                    # Only include if it's not too old (within last 5 minutes)
                    if 'timestamp' in recent_context:
                        import time
                        if time.time() - recent_context['timestamp'] < 300:  # 5 minutes
                            conversation_history = [
                                {"role": "user", "content": last_question},
                                {"role": "assistant", "content": last_response}
                            ]
                
                return {"conversation_history": conversation_history}
            
        except Exception as e:
            print(f"⚠️ Error building curated context: {e}")
        
        return {}
    
    async def _update_conversation_history(self, user_id: str, channel_id: str, question: str, response: str, response_type: str):
        """Update conversation history for future context"""
        try:
            if hasattr(self, 'conversation_manager') and self.conversation_manager:
                # Only store the user question, not the SQL query itself to avoid contamination
                if response_type == 'sql':
                    # Store a clean representation instead of raw SQL
                    clean_response = f"Generated SQL query for: {question}"
                else:
                    clean_response = response
                    
                await self.conversation_manager.update_conversation_context(
                    user_id, channel_id, question, clean_response, response_type
                )
        except Exception as e:
            print(f"⚠️ Error updating conversation history: {e}")
    
    async def _requires_sql_query_ai(self, question: str) -> bool:
        """Use AI to classify if question requires SQL query or is conversational"""
        try:
            classification_prompt = f"""Analyze this question and determine if it requires executing a SQL query against a database or if it's asking for definitions/explanations.

Question: {question}

Context: This is a business intelligence bot that can either:
1. Query databases for specific data/metrics (tickets, performance, schedules, etc.)
2. Provide explanations about business terms, processes, or definitions

Respond with exactly one word:
- "SQL" if this requires querying database tables for specific data/metrics/counts
- "CONVERSATIONAL" if this is asking for definitions, explanations, or general information

Examples:
- "What is HCF?" → CONVERSATIONAL (asking for definition)
- "What does FCR mean?" → CONVERSATIONAL (asking for definition)  
- "How many tickets today?" → SQL (needs database query)
- "What is John's performance this week?" → SQL (needs database query)
- "Show me handle time for agents" → SQL (needs database query)
- "Explain the escalation process" → CONVERSATIONAL (asking for explanation)"""

            classification = await self.model_provider.handle_conversational(classification_prompt)
            classification = classification.strip().upper()
            
            print(f"🤖 AI Classification for '{question}': {classification}")
            
            return "SQL" in classification
            
        except Exception as e:
            print(f"⚠️ Error in AI classification: {e}")
            # Fallback to conversational for safety
            return False
    
    async def _handle_conversational_with_finetuned_model(self, question: str, curated_context: Dict[str, Any] = None) -> str:
        """Handle conversational questions using fine-tuned model via Chat Completions API"""
        try:
            # Build messages with system prompt and curated context
            messages = [
                {"role": "system", "content": "You are a specialized BI assistant for Worker Operations. Generate SQL queries for data analysis questions using the DBT_PRODUCTION schema, or provide conversational responses about business context. For SQL generation, return only the executable SQL query with right columns without markdown formatting."}
            ]
            
            # Add curated context from previous conversation if available
            if curated_context and 'conversation_history' in curated_context:
                history = curated_context['conversation_history']
                # Only include the last question-answer pair for follow-up context
                if len(history) >= 2:
                    messages.extend(history[-2:])  # Last Q&A pair
            
            # Add current question
            messages.append({"role": "user", "content": question})
            
            # Use Chat Completions API directly with fine-tuned model
            response = await self.model_provider._generate_with_chat_completion(messages)
            
            return response.strip()
            
        except Exception as e:
            print(f"⚠️ Error in fine-tuned conversational response: {e}")
            # Fallback to Assistant API
            return await self.model_provider.handle_conversational(question)
    
    async def _summarize_sql_results(self, question: str, df, sql_query: str, user_id: str, channel_id: str, context: Dict[str, Any]) -> Tuple[str, str]:
        """Summarize SQL execution results"""
        try:
            # Check if query returned an error
            if isinstance(df, str):
                return f"❌ Query execution failed: {df}", 'error'
            
            # Check if query returned no data
            if hasattr(df, 'empty') and df.empty:
                helpful_message = self._generate_helpful_empty_result_message(question, sql_query)
                return helpful_message, 'sql_with_data'
            
            # Convert DataFrame to string for summarization
            result_table = df.to_string(index=False, max_rows=50)
            
            # Summarize results using Assistant API
            summary_prompt = f"""Provide a concise answer to this question based on the query results:

Question: {question}

Results:
{result_table}

Instructions:
- Give a direct, brief answer to the user's question
- Include the key numbers/metrics they asked for
- Keep it concise - no technical explanations about tables or procedures
- Format numbers clearly (e.g., "QA score: 85.2%", "AHT: 12.5 minutes")
- Only mention insights if they are directly relevant and brief
- Do not explain what QA scores mean or suggest follow-up actions unless asked

SLACK FORMATTING:
- Use *text* for bold (single asterisk), NOT **text**
- Use - for bullet points
- Keep formatting simple and Slack-compatible"""

            final_response = await self.model_provider.handle_conversational(summary_prompt)
            return final_response, 'sql_with_data'
            
        except Exception as e:
            print(f"❌ Error summarizing SQL results: {e}")
            return f"❌ Error processing results: {str(e)}", 'error'
    
    def _generate_helpful_empty_result_message(self, question: str, sql_query: str) -> str:
        """Generate helpful message for empty query results"""
        
        # Extract potential person/supervisor names from the question
        question_lower = question.lower()
        
        # Check if this looks like a person-specific query
        is_person_query = any(indicator in question_lower for indicator in [
            'team', 'supervisor', 'agent', 'performance', 'metrics', "'s", 'individual'
        ])
        
        # Check if this is asking about a team vs individual
        is_team_query = any(indicator in question_lower for indicator in [
            'team', 'supervisor team', "'s team", 'team performance'
        ])
        
        if is_person_query:
            if is_team_query:
                return """📊 **No team data found for this supervisor.**

**Possible reasons:**
• The supervisor name might not be exact (try checking spelling)
• This person might not currently have a team
• Data might not be available for the requested time period

**Helpful suggestions:**
• Ask "How many teams are there in WOPS?" to see all active supervisors
• Try asking "Which supervisors have teams this week?"
• Use partial names if unsure of exact spelling"""
            else:
                return """📊 **No data found for this person.**

**Possible reasons:**
• The name might not be exact (try checking spelling)
• This person might no longer be active
• Data might not be available for the requested time period

**Helpful suggestions:**
• Ask "How many agents are there?" to see all active agents
• Try using partial names if unsure of exact spelling
• Check if this person might be a supervisor instead of an agent"""
        else:
            return """📊 **No data found for your query.**

**Possible reasons:**
• The requested data might not be available for this time period
• The criteria might be too specific
• Data might be filtered out by business rules

**Helpful suggestions:**
• Try broadening the time period (e.g., "last month" instead of "this week")
• Ask for available data first (e.g., "What data is available?")
• Check if your question matches available metrics"""

    def _format_rate_limit_message(self, rate_limit_info: Dict) -> str:
        """Format rate limit exceeded message"""
        reason = rate_limit_info.get('reason', 'unknown')
        
        if reason == 'daily_limit_exceeded':
            return f"⚠️ Daily usage limit reached ({rate_limit_info['daily_usage']:,} tokens). Please try again tomorrow."
        elif reason == 'hourly_limit_exceeded':
            return f"⚠️ Hourly usage limit reached ({rate_limit_info['hourly_usage']:,} tokens). Please try again in an hour."
        elif reason == 'thread_limit_exceeded':
            return f"⚠️ Thread usage limit reached. Please start a new conversation."
        else:
            return "⚠️ Usage limit exceeded. Please try again later."
    
    # Cache management functions
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        return await self.cache_manager.get_cache_stats()
    
    async def get_learning_insights(self) -> str:
        """Get insights from cached learning data"""
        insights = []
        
        stats = await self.get_cache_stats()
        insights.append(f"Cache Status: {'Enabled' if ENABLE_CACHE else 'Disabled'}")
        insights.append(f"Connection: {'Valkey' if stats.get('valkey_connected') else 'Local fallback'}")
        
        for cache_type, size in stats.get('local_cache_sizes', {}).items():
            insights.append(f"{cache_type.title()} cache: {size} entries")
        
        return "\n".join(insights)
    
    # Clear cache functions
    async def clear_sql_cache(self):
        """Clear SQL cache"""
        await self.cache_manager.clear_cache_type('sql')
    
    async def clear_schema_cache(self):
        """Clear schema cache"""
        await self.cache_manager.clear_cache_type('schema')
    
    async def clear_thread_cache(self):
        """Clear thread cache"""
        await self.cache_manager.clear_cache_type('thread')
    
    async def clear_conversation_cache(self):
        """Clear conversation cache"""
        await self.cache_manager.clear_cache_type('conversation')
    
    async def clear_table_selection_cache(self):
        """Clear table selection cache"""
        await self.cache_manager.clear_cache_type('table_selection')
    
    async def clear_feedback_cache(self):
        """Clear feedback cache"""
        await self.cache_manager.clear_cache_type('feedback')
    
    async def clear_token_usage_cache(self):
        """Clear token usage cache"""
        await self.cache_manager.clear_cache_type('token_usage')
    
    async def rediscover_table_schema(self, table_name: str) -> dict:
        """Force rediscovery of table schema"""
        return await self.table_discovery.discover_table_schema(table_name)
    
    def get_model_provider_info(self) -> dict:
        """Get information about the current model provider"""
        try:
            return get_provider_info()
        except Exception as e:
            return {'error': str(e), 'provider_name': 'unknown'}
    
    def switch_model_provider(self, provider_name: str) -> dict:
        """Switch to a different model provider"""
        try:
            from app.model_providers.provider_factory import switch_model_provider
            self.model_provider = switch_model_provider(provider_name)
            return self.get_model_provider_info()
        except Exception as e:
            return {'error': str(e), 'provider_name': 'unknown'}


# Global orchestrator instance
llm_orchestrator = LLMOrchestrator()

# Main entry points and convenience functions
async def handle_question(question: str, user_id: str, channel_id: str, assistant_id: str = None) -> Tuple[str, str]:
    """Handle question - main entry point"""
    return await llm_orchestrator.handle_question(question, user_id, channel_id, assistant_id)

async def generate_sql_intelligently(question: str, user_id: str, channel_id: str) -> str:
    """Generate SQL intelligently"""
    response, response_type = await llm_orchestrator.handle_question(question, user_id, channel_id)
    if response_type == 'sql':
        return response
    else:
        return "-- This question doesn't require SQL generation"

# Cache and stats functions
async def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics"""
    return await llm_orchestrator.get_cache_stats()

async def get_learning_insights() -> str:
    """Get learning insights"""
    return await llm_orchestrator.get_learning_insights()

# Clear cache functions
async def clear_sql_cache():
    """Clear SQL cache"""
    return await llm_orchestrator.clear_sql_cache()

async def clear_schema_cache():
    """Clear schema cache"""
    return await llm_orchestrator.clear_schema_cache()

async def clear_thread_cache():
    """Clear thread cache"""
    return await llm_orchestrator.clear_thread_cache()

async def clear_conversation_cache():
    """Clear conversation cache"""
    return await llm_orchestrator.clear_conversation_cache()

async def clear_table_selection_cache():
    """Clear table selection cache"""
    return await llm_orchestrator.clear_table_selection_cache()

async def clear_feedback_cache():
    """Clear feedback cache"""
    return await llm_orchestrator.clear_feedback_cache()

async def clear_token_usage_cache():
    """Clear token usage cache"""
    return await llm_orchestrator.clear_token_usage_cache()

async def rediscover_table_schema(table_name: str) -> dict:
    """Rediscover table schema"""
    return await llm_orchestrator.rediscover_table_schema(table_name)

# Additional convenience functions for backward compatibility
async def ask_llm_for_sql(user_question: str, model_context: str) -> str:
    """Generate SQL - backward compatibility"""
    return await result_processor.ask_llm_for_sql(user_question, model_context)

async def summarize_results_with_llm(user_question: str, result_table: str) -> str:
    """Summarize results - backward compatibility"""
    return result_processor.summarize_results_with_llm(user_question, result_table)

async def summarize_with_assistant(user_question: str, result_table: str, user_id: str, channel_id: str, assistant_id: str, sql_query: str = None) -> str:
    """Summarize with assistant - backward compatibility"""
    return await result_processor.summarize_with_assistant(user_question, result_table, user_id, channel_id, assistant_id, sql_query)

async def handle_conversational_question(user_question: str, user_id: str, channel_id: str) -> str:
    """Handle conversational question - backward compatibility"""
    return await result_processor.handle_conversational_question(user_question, user_id, channel_id)

async def generate_sql_with_retry_logic(question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
    """Generate SQL with retry logic - backward compatibility"""
    return await sql_generator.generate_sql_with_retry_logic(question, user_id, channel_id)