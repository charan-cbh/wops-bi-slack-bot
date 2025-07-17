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
        
        # Route based on classification
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
            # SQL required - generate and return SQL
            sql_response = await self._handle_sql_question(question, user_id, channel_id, assistant_id)
            return sql_response, 'sql'
    
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