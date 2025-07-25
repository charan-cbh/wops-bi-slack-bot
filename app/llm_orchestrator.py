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
            # First, ask the Assistant whether this requires SQL or is conversational
            classification_prompt = f"""Analyze this question and determine if it requires executing a SQL query against our database or if it's a conversational question about business processes/policies.

Question: {question}

Respond with either:
- "DATA" if this requires querying our database tables for specific data
- "CONVERSATIONAL" if this is about business processes, policies, or general information

Consider questions about specific metrics, counts, performance data, or "show me" requests as DATA questions."""

            classification = await self.model_provider.handle_conversational(classification_prompt, context)
            classification = classification.strip().upper()
            
            print(f"🤖 Assistant classification: {classification}")
            
            if "DATA" in classification:
                # Data question: Generate SQL using Assistant API, execute, then summarize
                return await self._handle_data_question_with_assistant(question, user_id, channel_id, context)
            else:
                # Conversational question: Direct response from Assistant API
                response = await self.model_provider.handle_conversational(question, context)
                return response, 'conversational'
            
        except Exception as e:
            print(f"❌ Error handling question with Assistant API: {e}")
            # Fallback to original approach
            return f"❌ Error processing your question: {str(e)}", 'error'
    
    async def _handle_data_question_with_assistant(self, question: str, user_id: str, channel_id: str, context: Dict[str, Any]) -> Tuple[str, str]:
        """Handle data questions using Assistant API for SQL generation and result summarization"""
        try:
            # Step 1: Generate SQL using Assistant API with vector store context
            sql_prompt = f"""Generate a SQL query to answer this question using the business intelligence knowledge base:

{question}

CRITICAL INSTRUCTIONS:
- Use EXACT table names from the knowledge base (including schema prefixes like ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE)
- Use EXACT column names as specified in the table definitions
- For QA score questions, use the RPT_WOPS_AGENT_PERFORMANCE table which has QA_SCORE column
- Generate proper Snowflake SQL syntax with correct date filters
- Include appropriate filters, joins, and aggregations
- Only add date filters when explicitly requested (e.g., "this week", "last week", "last month")
- For general performance questions without time specification, do not add date filters
- For date filtering, use these exact Snowflake patterns:
  * "this week": WHERE SOLVED_WEEK = DATE_TRUNC('week', CURRENT_DATE)
  * "last week": WHERE SOLVED_WEEK = DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'
  * "past 4 weeks": WHERE SOLVED_WEEK >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '3 week'

IMPORTANT TABLE-SPECIFIC COLUMN RULES:
- Use the CORRECT column names for each table:

RPT_WOPS_AGENT_PERFORMANCE table:
  * Person column: ASSIGNEE_NAME
  * Supervisor column: ASSIGNEE_SUPERVISOR
  * Date column: SOLVED_WEEK
  * Key columns: NUM_TICKETS, AHT_MINUTES, QA_SCORE, FCR_PERCENTAGE
  * Example: WHERE ASSIGNEE_NAME LIKE '%Sine%' AND SOLVED_WEEK >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'

RPT_AGENT_SCHEDULE_ADHERENCE table:
  * Person column: AGENT_NAME
  * Date column: ADHERENCE_DATE
  * Key columns: ADHERENT_MINUTES, SCHEDULED_MINUTES, ADHERENCE_PERCENTAGE, SCHEDULED_TASK
  * Example: WHERE AGENT_NAME LIKE '%Sine%' AND ADHERENCE_DATE >= CURRENT_DATE - INTERVAL '7 days'
  * Note: ADHERENCE_PERCENTAGE is already calculated, or use ADHERENT_MINUTES/SCHEDULED_MINUTES for custom calculations

RPT_WOPS_TL_PERFORMANCE table:
  * Person column: SUPERVISOR
  * Date column: SOLVED_WEEK
  * Example: WHERE SUPERVISOR LIKE '%John%' AND SOLVED_WEEK >= DATE_TRUNC('week', CURRENT_DATE)

- Always use LIKE with wildcards for partial name matching
- Always include the person name column in SELECT to show all matches and handle disambiguation

Return ONLY the executable SQL query, no explanations or markdown formatting.

Review the knowledge base carefully for exact table and column names before generating the query."""
            
            sql_query = await self.model_provider.handle_conversational(sql_prompt, context)
            
            # Clean up the SQL (remove markdown formatting)
            sql_query = sql_query.strip()
            if sql_query.startswith('```sql'):
                sql_query = sql_query[6:]
            if sql_query.startswith('```'):
                sql_query = sql_query[3:]
            if sql_query.endswith('```'):
                sql_query = sql_query[:-3]
            sql_query = sql_query.strip()
            
            # Fix common table name errors the Assistant makes
            # Replace incorrect table names with correct ones from knowledge base
            table_corrections = [
                ('ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE', 'ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE'),
                ('ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE', 'ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TL_PERFORMANCE'),
                ('ANALYTICS.DBT_PRODUCTION.WOPS_TICKETS', 'ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS'),
                # Handle cases without schema prefix (but be careful not to double-replace)
            ]
            
            original_sql = sql_query
            corrections_made = []
            for incorrect, correct in table_corrections:
                if incorrect in sql_query:
                    sql_query = sql_query.replace(incorrect, correct)
                    corrections_made.append((incorrect, correct))
            
            if corrections_made:
                print(f"🔧 Fixed table names in SQL query:")
                for incorrect, correct in corrections_made:
                    print(f"   {incorrect} → {correct}")
            
            print(f"🔍 Generated SQL: {sql_query[:100]}...")
            
            # Step 2: Execute SQL
            try:
                from app.snowflake_runner import run_query
                df = run_query(sql_query)
                
                # Check if query returned an error
                if isinstance(df, str):
                    return f"❌ Query execution failed: {df}", 'sql'
                
                if hasattr(df, 'columns') and 'Error' in df.columns:
                    error_msg = df.iloc[0]['Error'] if len(df) > 0 else "Unknown error"
                    return f"❌ Query execution failed: {error_msg}", 'sql'
                
                # Check if query returned no data (empty results)
                if hasattr(df, 'empty') and df.empty:
                    # Provide helpful guidance for empty results
                    helpful_message = self._generate_helpful_empty_result_message(question, sql_query)
                    return helpful_message, 'sql_with_data'
                
                # Check if this is a name-based query that returned multiple people
                name_column = None
                if 'ASSIGNEE_NAME' in df.columns:
                    name_column = 'ASSIGNEE_NAME'
                elif 'AGENT_NAME' in df.columns:
                    name_column = 'AGENT_NAME'
                elif 'ASSIGNEE_SUPERVISOR' in df.columns:
                    name_column = 'ASSIGNEE_SUPERVISOR'
                elif 'SUPERVISOR' in df.columns:
                    name_column = 'SUPERVISOR'
                
                # Only ask for clarification if this is a single-person query, not a "how many", "list", or "team" query
                is_list_query = any(phrase in question.lower() for phrase in [
                    'how many', 'list', 'show all', 'count', 'give me all', 'give me the', 'names of', 'which agents', 'what agents'
                ])
                
                # Check if this is a team query (where multiple results are expected)
                is_team_query = any(phrase in question.lower() for phrase in [
                    'team', 'adherence', 'schedule adherence', 'team adherence', 'team performance', 
                    'team metrics', 'team scores', 'team results'
                ])
                
                if name_column and len(df) > 1 and not is_list_query and not is_team_query:
                    unique_names = df[name_column].unique()
                    if len(unique_names) > 1:
                        # Multiple people found - ask for clarification
                        names_list = '\n'.join([f"• {name}" for name in unique_names])
                        person_type = "agents" if name_column in ['ASSIGNEE_NAME', 'AGENT_NAME'] else "supervisors"
                        clarification_response = f"""I found multiple {person_type} matching your search:

{names_list}

Please specify which person you're asking about by using their full name or a more specific identifier."""
                        return clarification_response, 'sql_with_data'
                
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
- Do not explain what QA scores mean or suggest follow-up actions unless asked"""

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
                        simplified_sql = await self.model_provider.handle_conversational(simplified_prompt, context)
                        simplified_sql = simplified_sql.strip()
                        if simplified_sql.startswith('```sql'):
                            simplified_sql = simplified_sql[6:]
                        if simplified_sql.startswith('```'):
                            simplified_sql = simplified_sql[3:]
                        if simplified_sql.endswith('```'):
                            simplified_sql = simplified_sql[:-3]
                        simplified_sql = simplified_sql.strip()
                        
                        print(f"🔄 Trying simplified SQL: {simplified_sql[:100]}...")
                        
                        # Apply table name corrections
                        table_corrections = [
                            ('ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE', 'ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE'),
                            ('ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE', 'ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TL_PERFORMANCE'),
                            ('ANALYTICS.DBT_PRODUCTION.WOPS_TICKETS', 'ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS'),
                        ]
                        
                        for incorrect, correct in table_corrections:
                            if incorrect in simplified_sql:
                                simplified_sql = simplified_sql.replace(incorrect, correct)
                        
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
        """Handle questions that require SQL generation"""
        try:
            # Find relevant tables
            candidate_tables = await self.table_discovery.find_relevant_tables_from_vector_store(
                question, user_id, channel_id, top_k=8
            )
            
            if not candidate_tables:
                return "-- Error: No relevant tables found for this question"
            
            # Select best table
            selected_table, reason = await self.table_discovery.select_best_table_using_samples(
                question, candidate_tables, user_id, channel_id
            )
            
            if not selected_table:
                return "-- Error: Could not select appropriate table"
            
            # Discover table schema
            schema = await self.table_discovery.discover_table_schema(selected_table)
            
            if schema.get('error'):
                return f"-- Error: Could not discover schema for {selected_table}: {schema['error']}"
            
            # Generate SQL
            sql = await self._generate_sql_for_table(question, selected_table, schema, user_id, channel_id)
            
            # Cache the table selection
            await self.sql_generator.cache_table_selection(question, selected_table, reason, success=True)
            
            # Execute the SQL and get results
            try:
                from app.snowflake_runner import run_query
                df = run_query(sql)
                
                if df.empty or 'Error' in df.columns:
                    error_msg = str(df.iloc[0]['Error']) if 'Error' in df.columns else "No data returned"
                    return f"❌ Query execution failed: {error_msg}"
                
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
                    sql = await self.model_provider.generate_sql(
                        question, 
                        instructions['instructions'],
                        {'table': table, 'schema': schema, 'user_id': user_id}
                    )
                    
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