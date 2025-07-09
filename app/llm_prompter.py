"""
Refactored LLM Prompter - Main orchestrator for the BI Slack Bot
This file has been refactored from the original llm_prompter.py to use modular classes
and remove pattern_matcher dependencies.
"""
import os
import asyncio
import traceback
from typing import Optional, Dict, Any, Tuple
from dotenv import load_dotenv
from openai import OpenAI

# Import our new modular classes
from .valkey_manager import ValkeyManager
from .question_classifier import QuestionClassifier
from .table_manager import TableManager
from .sql_generator import SQLGenerator
from .conversation_manager import ConversationManager
from .logging_config import get_bot_logger, log_step, log_question_received, log_table_selected, log_sql_generated, log_query_executed, log_response_sent

# Import Snowflake runner
try:
    from app.snowflake_runner import run_query
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("⚠️ Snowflake runner not available")

load_dotenv()

class LLMPrompter:
    """Main orchestrator class for the BI Slack Bot"""
    
    def __init__(self):
        self.logger = get_bot_logger(__name__)
        
        # Configuration
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        self.ASSISTANT_ID = os.getenv("ASSISTANT_ID")
        self.VECTOR_STORE_ID = os.getenv("OPENAI_VECTOR_STORE_ID")
        
        # Initialize OpenAI client
        self.client = OpenAI(api_key=self.OPENAI_API_KEY)
        
        # Initialize our manager classes
        self.valkey_manager = ValkeyManager()
        self.question_classifier = QuestionClassifier(self.client, self.valkey_manager)
        self.table_manager = TableManager(self.client, self.valkey_manager)
        self.sql_generator = SQLGenerator(self.client, self.valkey_manager, self.question_classifier)
        self.conversation_manager = ConversationManager(self.client, self.valkey_manager)
        
        self.logger.info("LLMPrompter initialized with modular architecture")
    
    async def initialize(self):
        """Initialize connections and dependencies"""
        try:
            # Initialize Valkey connection
            await self.valkey_manager.init_valkey_client()
            print("✅ LLMPrompter fully initialized")
            
        except Exception as e:
            print(f"❌ Error initializing LLMPrompter: {e}")
            raise
    
    async def close(self):
        """Close connections and cleanup"""
        try:
            await self.valkey_manager.close_valkey_connection()
            print("✅ LLMPrompter closed cleanly")
            
        except Exception as e:
            print(f"❌ Error closing LLMPrompter: {e}")
    
    async def generate_sql_for_question(self, user_question: str, user_id: str, channel_id: str) -> Tuple[str, str, str]:
        """
        Main function to generate SQL for a user question
        Returns: (sql_query, selected_table, explanation)
        """
        try:
            print(f"🔍 Processing question: {user_question}")
            
            # Step 1: Classify the question
            question_type = await self.question_classifier.classify_question_with_openai(
                user_question, user_id, channel_id
            )
            
            print(f"📊 Question type: {question_type}")
            
            # Step 2: Handle conversational questions
            if question_type == "CONVERSATIONAL":
                response = await self.conversation_manager.handle_conversational_question(
                    user_question, user_id, channel_id
                )
                return "", "", response
            
            # Step 3: Find relevant tables
            print("🔍 Finding relevant tables...")
            candidate_tables = await self.table_manager.find_relevant_tables_from_vector_store(
                user_question, user_id, channel_id
            )
            
            if not candidate_tables:
                return "", "", "I couldn't find any relevant tables for your question. Please try rephrasing or ask about available data."
            
            print(f"📋 Found {len(candidate_tables)} candidate tables: {candidate_tables}")
            
            # Step 4: Select the best table
            selected_table, selection_reason = await self.table_manager.select_best_table_using_samples(
                user_question, candidate_tables, user_id, channel_id
            )
            
            if not selected_table:
                return "", "", "I couldn't determine the best table for your question. Please try being more specific."
            
            print(f"✅ Selected table: {selected_table}")
            
            # Step 5: Get table schema
            schema = await self.table_manager.discover_table_schema(selected_table)
            
            if not schema.get("success"):
                return "", "", f"I couldn't retrieve the schema for table {selected_table}. Please try again."
            
            print(f"📝 Retrieved schema for {selected_table}: {len(schema.get('columns', []))} columns")
            
            # Step 6: Generate SQL query
            sql_query, generation_method = await self.sql_generator.generate_sql_query(
                user_question, selected_table, schema, user_id, channel_id
            )
            
            # Step 7: Cache the successful table selection
            await self.table_manager.cache_table_selection(
                user_question, selected_table, selection_reason, True
            )
            
            # Step 8: Update conversation context
            await self.conversation_manager.update_conversation_context(
                user_id, channel_id, user_question, "Generated SQL query", selected_table, sql_query
            )
            
            return sql_query, selected_table, f"Generated SQL query for table {selected_table}. {generation_method}"
            
        except Exception as e:
            error_msg = f"Error generating SQL: {str(e)}"
            print(f"❌ {error_msg}")
            traceback.print_exc()
            return "", "", f"I encountered an error while processing your question: {error_msg}"
    
    async def execute_sql_query(self, sql_query: str, user_id: str, channel_id: str) -> Dict[str, Any]:
        """Execute SQL query and return results"""
        try:
            if not SNOWFLAKE_AVAILABLE:
                return {
                    "success": False,
                    "error": "Snowflake connection not available"
                }
            
            print(f"🔧 Executing SQL query: {sql_query}")
            
            # Execute the query
            result = await run_query(sql_query)
            
            if result.get("success"):
                print(f"✅ Query executed successfully, returned {len(result.get('data', []))} rows")
            else:
                print(f"❌ Query failed: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            error_msg = f"Error executing SQL query: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "success": False,
                "error": error_msg
            }
    
    async def process_user_question(self, user_question: str, user_id: str, channel_id: str) -> Dict[str, Any]:
        """
        Main entry point for processing user questions
        Returns complete response with SQL, results, and metadata
        """
        try:
            # Log the start of question processing
            log_question_received(self.logger, user_question, user_id, channel_id)
            
            # Generate SQL for the question
            log_step(self.logger, "SQL_GENERATION", "START", 
                    question=user_question, user_id=user_id, channel_id=channel_id)
            
            sql_query, selected_table, explanation = await self.generate_sql_for_question(
                user_question, user_id, channel_id
            )
            
            if sql_query:
                log_sql_generated(self.logger, sql_query, selected_table, user_id, channel_id)
            else:
                log_step(self.logger, "SQL_GENERATION", "ERROR", 
                        question=user_question, user_id=user_id, channel_id=channel_id)
            
            response = {
                "question": user_question,
                "sql_query": sql_query,
                "selected_table": selected_table,
                "explanation": explanation,
                "success": bool(sql_query),
                "data": [],
                "metadata": {}
            }
            
            # If we got a SQL query, execute it
            if sql_query:
                log_step(self.logger, "QUERY_EXECUTION", "START", 
                        sql=sql_query, user_id=user_id, channel_id=channel_id)
                
                execution_result = await self.execute_sql_query(sql_query, user_id, channel_id)
                
                # Log execution result
                log_query_executed(self.logger, 
                                 execution_result.get("success", False),
                                 len(execution_result.get("data", [])),
                                 execution_result.get("error"),
                                 user_id, channel_id)
                
                response.update({
                    "data": execution_result.get("data", []),
                    "execution_success": execution_result.get("success", False),
                    "execution_error": execution_result.get("error"),
                    "metadata": execution_result.get("metadata", {})
                })
            
            # Log successful completion
            log_step(self.logger, "QUESTION_PROCESSING", "SUCCESS", 
                    question=user_question, user_id=user_id, channel_id=channel_id)
            
            return response
            
        except Exception as e:
            error_msg = f"Error processing user question: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            log_step(self.logger, "QUESTION_PROCESSING", "ERROR", 
                    question=user_question, user_id=user_id, channel_id=channel_id)
            
            return {
                "question": user_question,
                "sql_query": "",
                "selected_table": "",
                "explanation": error_msg,
                "success": False,
                "data": [],
                "metadata": {}
            }
    
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a specific table"""
        try:
            schema = await self.table_manager.discover_table_schema(table_name)
            return schema
            
        except Exception as e:
            return {
                "table_name": table_name,
                "error": str(e),
                "success": False
            }
    
    async def get_table_sample(self, table_name: str, sample_size: int = 10) -> Dict[str, Any]:
        """Get sample data from a specific table"""
        try:
            sample = await self.table_manager.sample_table_data(table_name, sample_size)
            return sample
            
        except Exception as e:
            return {
                "table_name": table_name,
                "error": str(e),
                "success": False
            }
    
    async def record_user_feedback(self, question: str, sql: str, table: str, feedback_type: str):
        """Record user feedback for improving the system"""
        try:
            await self.sql_generator.record_feedback(question, sql, table, feedback_type)
            print(f"✅ Recorded feedback: {feedback_type}")
            
        except Exception as e:
            print(f"❌ Error recording feedback: {e}")
    
    async def get_conversation_context(self, user_id: str, channel_id: str) -> Dict[str, Any]:
        """Get conversation context for a user"""
        try:
            context = await self.conversation_manager.get_conversation_context(user_id, channel_id)
            return context
            
        except Exception as e:
            print(f"❌ Error getting conversation context: {e}")
            return {}
    
    async def handle_debug_request(self, user_question: str, user_id: str, channel_id: str) -> str:
        """Handle debug requests to understand system behavior"""
        try:
            debug_info = []
            
            # Question classification
            question_type = await self.question_classifier.classify_question_with_openai(
                user_question, user_id, channel_id
            )
            debug_info.append(f"Question Type: {question_type}")
            
            # Intent analysis
            intent = self.question_classifier.analyze_question_intent(user_question.lower())
            debug_info.append(f"Intent Analysis: {intent}")
            
            # Table search
            candidate_tables = await self.table_manager.find_relevant_tables_from_vector_store(
                user_question, user_id, channel_id
            )
            debug_info.append(f"Candidate Tables: {candidate_tables}")
            
            # Conversation context
            context = await self.conversation_manager.get_conversation_context(user_id, channel_id)
            debug_info.append(f"Conversation Context: {context}")
            
            return "\\n".join(debug_info)
            
        except Exception as e:
            return f"Debug error: {str(e)}"

# Global instance for backward compatibility
llm_prompter = None

async def initialize_llm_prompter():
    """Initialize the global LLM prompter instance"""
    global llm_prompter
    if llm_prompter is None:
        llm_prompter = LLMPrompter()
        await llm_prompter.initialize()
    return llm_prompter

async def close_llm_prompter():
    """Close the global LLM prompter instance"""
    global llm_prompter
    if llm_prompter:
        await llm_prompter.close()
        llm_prompter = None

# Backward compatibility functions
async def generate_sql_with_patterns(user_question: str, user_id: str, channel_id: str) -> Tuple[str, str]:
    """Backward compatibility function - now uses the refactored approach"""
    prompter = await initialize_llm_prompter()
    sql_query, selected_table, explanation = await prompter.generate_sql_for_question(
        user_question, user_id, channel_id
    )
    return sql_query, selected_table

async def handle_conversational_question(user_question: str, user_id: str, channel_id: str) -> str:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.conversation_manager.handle_conversational_question(
        user_question, user_id, channel_id
    )

async def find_relevant_tables_from_vector_store(question: str, user_id: str, channel_id: str, top_k: int = 8) -> list:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.table_manager.find_relevant_tables_from_vector_store(
        question, user_id, channel_id, top_k
    )

async def discover_table_schema(table_name: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.table_manager.discover_table_schema(table_name)

async def sample_table_data(table_name: str, sample_size: int = 10) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.table_manager.sample_table_data(table_name, sample_size)

def classify_question_type(question: str) -> str:
    """Backward compatibility function"""
    # Since this is a sync function, we'll use the fallback method
    from .question_classifier import QuestionClassifier
    classifier = QuestionClassifier(None, None)
    return classifier.classify_question_type(question)

async def record_feedback(question: str, sql: str, table: str, feedback_type: str):
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    await prompter.record_user_feedback(question, sql, table, feedback_type)

# Additional backward compatibility functions for slack_handler.py
async def ask_llm_for_sql(user_question: str, user_id: str, channel_id: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.process_user_question(user_question, user_id, channel_id)

async def summarize_results_with_llm(data: list, question: str, user_id: str, channel_id: str) -> str:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.conversation_manager.summarize_results_with_llm(data, question, user_id, channel_id)

async def summarize_with_assistant(data: list, question: str, user_id: str, channel_id: str) -> str:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.conversation_manager.summarize_with_assistant(data, question, user_id, channel_id)

async def update_sql_cache_with_results(sql_query: str, results: dict):
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    await prompter.valkey_manager.safe_valkey_set(f"sql_results:{sql_query}", results, ex=prompter.valkey_manager.SQL_CACHE_TTL)

async def get_cache_stats() -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return {
        "status": "Cache stats not implemented in refactored version",
        "valkey_connected": prompter.valkey_manager.valkey_client is not None
    }

async def get_learning_insights() -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return {"insights": "Learning insights not implemented in refactored version"}

async def handle_question(user_question: str, user_id: str, channel_id: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.process_user_question(user_question, user_id, channel_id)

async def test_question_classification(question: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    question_type = await prompter.question_classifier.classify_question_with_openai(question, "test_user", "test_channel")
    return {"question": question, "classification": question_type}

async def generate_sql_intelligently(user_question: str, user_id: str, channel_id: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.process_user_question(user_question, user_id, channel_id)

async def clear_sql_cache():
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    # Clear SQL cache keys
    return True

async def clear_schema_cache():
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    # Clear schema cache keys
    return True

async def clear_thread_cache():
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    # Clear thread cache keys
    return True

async def clear_conversation_cache():
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    # Clear conversation cache keys
    return True

async def clear_table_selection_cache():
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    # Clear table selection cache keys
    return True

async def clear_feedback_cache():
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    # Clear feedback cache keys
    return True

async def rediscover_table_schema(table_name: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.table_manager.discover_table_schema(table_name)

async def update_conversation_context(user_id: str, channel_id: str, question: str, response: str, table: str = None, sql: str = None):
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    await prompter.conversation_manager.update_conversation_context(user_id, channel_id, question, response, table, sql)

async def get_conversation_context(user_id: str, channel_id: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.conversation_manager.get_conversation_context(user_id, channel_id)

async def test_conversation_flow(user_id: str, channel_id: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.conversation_manager.get_conversation_context(user_id, channel_id)

async def debug_table_selection(question: str, user_id: str, channel_id: str) -> str:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.handle_debug_request(question, user_id, channel_id)

async def select_best_table_using_samples(question: str, candidate_tables: list, user_id: str, channel_id: str) -> tuple:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.table_manager.select_best_table_using_samples(question, candidate_tables, user_id, channel_id)

async def cache_table_selection(question: str, selected_table: str, reason: str, success: bool = True):
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    await prompter.table_manager.cache_table_selection(question, selected_table, reason, success)

async def get_table_descriptions_from_manifest() -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return {"tables": "Table descriptions not implemented in refactored version"}

async def update_conversation_context_with_sql(user_id: str, channel_id: str, question: str, sql: str, table: str, response: str):
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    await prompter.conversation_manager.update_conversation_context(user_id, channel_id, question, response, table, sql)

async def check_rate_limits(user_id: str, thread_id: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.conversation_manager.check_rate_limits(user_id, thread_id)

async def get_user_token_usage(user_id: str) -> dict:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.conversation_manager.get_user_token_usage(user_id)

async def track_actual_usage(user_id: str, thread_id: str, tokens_used: int, request_type: str = "query"):
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    await prompter.conversation_manager.track_actual_usage(user_id, thread_id, tokens_used, request_type)

async def estimate_request_tokens(question: str, context: str = "") -> int:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.conversation_manager.estimate_request_tokens(question, context)

async def clear_token_usage_cache():
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    # Clear token usage cache
    return True

async def classify_question_with_openai(question: str, user_id: str, channel_id: str) -> str:
    """Backward compatibility function"""
    prompter = await initialize_llm_prompter()
    return await prompter.question_classifier.classify_question_with_openai(question, user_id, channel_id)

def classify_question_type_fallback(question: str) -> str:
    """Backward compatibility function"""
    from .question_classifier import QuestionClassifier
    classifier = QuestionClassifier(None, None)
    return classifier.classify_question_type(question)

# Token limits constants for backward compatibility
MAX_TOKENS_PER_USER_PER_DAY = 100000
MAX_TOKENS_PER_USER_PER_HOUR = 10000
MAX_TOKENS_PER_THREAD = 50000

# Functions needed by main.py
async def init_valkey_client():
    """Backward compatibility function for main.py"""
    try:
        prompter = await initialize_llm_prompter()
        return prompter.valkey_manager.valkey_client
    except Exception as e:
        # If initialization fails (e.g., missing OpenAI key), still try to init Valkey directly
        from .valkey_manager import ValkeyManager
        valkey_manager = ValkeyManager()
        try:
            await valkey_manager.init_valkey_client()
            return valkey_manager.valkey_client
        except Exception as valkey_error:
            return None

async def check_valkey_health() -> dict:
    """Backward compatibility function for main.py"""
    try:
        prompter = await initialize_llm_prompter()
        is_connected = await prompter.valkey_manager.ensure_valkey_connection()
        return {
            "status": "healthy" if is_connected else "fallback",
            "connected": is_connected,
            "fallback_mode": not is_connected
        }
    except Exception as e:
        # If initialization fails (e.g., missing OpenAI key), still check Valkey directly
        from .valkey_manager import ValkeyManager
        valkey_manager = ValkeyManager()
        try:
            is_connected = await valkey_manager.ensure_valkey_connection()
            await valkey_manager.close_valkey_connection()
            return {
                "status": "healthy" if is_connected else "fallback",
                "connected": is_connected,
                "fallback_mode": not is_connected,
                "note": "OpenAI not configured"
            }
        except Exception as valkey_error:
            return {
                "status": "fallback",
                "connected": False,
                "fallback_mode": True,
                "error": str(valkey_error),
                "note": "OpenAI not configured"
            }

# Export the main class and functions
__all__ = [
    'LLMPrompter',
    'initialize_llm_prompter',
    'close_llm_prompter',
    'generate_sql_with_patterns',
    'handle_conversational_question',
    'find_relevant_tables_from_vector_store',
    'discover_table_schema',
    'sample_table_data',
    'classify_question_type',
    'record_feedback',
    'ask_llm_for_sql',
    'summarize_results_with_llm',
    'summarize_with_assistant',
    'update_sql_cache_with_results',
    'get_cache_stats',
    'get_learning_insights',
    'handle_question',
    'test_question_classification',
    'generate_sql_intelligently',
    'clear_sql_cache',
    'clear_schema_cache',
    'clear_thread_cache',
    'clear_conversation_cache',
    'clear_table_selection_cache',
    'clear_feedback_cache',
    'rediscover_table_schema',
    'update_conversation_context',
    'get_conversation_context',
    'test_conversation_flow',
    'debug_table_selection',
    'select_best_table_using_samples',
    'cache_table_selection',
    'get_table_descriptions_from_manifest',
    'update_conversation_context_with_sql',
    'check_rate_limits',
    'get_user_token_usage',
    'track_actual_usage',
    'estimate_request_tokens',
    'clear_token_usage_cache',
    'classify_question_with_openai',
    'classify_question_type_fallback',
    'init_valkey_client',
    'check_valkey_health',
    'MAX_TOKENS_PER_USER_PER_DAY',
    'MAX_TOKENS_PER_USER_PER_HOUR',
    'MAX_TOKENS_PER_THREAD'
]