import os
import time
import json
from typing import Dict, Any, Optional, Tuple
from openai import OpenAI
from app.cache_manager import cache_manager, CONVERSATION_CACHE_PREFIX, THREAD_CACHE_PREFIX, TOKEN_USAGE_PREFIX, DAILY_USAGE_PREFIX, HOURLY_USAGE_PREFIX, THREAD_USAGE_PREFIX, TOKEN_USAGE_CACHE_TTL, CONVERSATION_CACHE_TTL, THREAD_CACHE_TTL

# Configuration
ASSISTANT_ID = os.getenv("ASSISTANT_ID")
VECTOR_STORE_ID = os.getenv("OPENAI_VECTOR_STORE_ID")

# Rate limiting configuration
MAX_TOKENS_PER_USER_PER_DAY = int(os.getenv("MAX_TOKENS_PER_USER_PER_DAY", 1000000))
MAX_TOKENS_PER_USER_PER_HOUR = int(os.getenv("MAX_TOKENS_PER_USER_PER_HOUR", 200000))
MAX_TOKENS_PER_THREAD = int(os.getenv("MAX_TOKENS_PER_THREAD", 1000000))
ENABLE_RATE_LIMITING = os.getenv("ENABLE_RATE_LIMITING", "true").lower() == "true"
ADMIN_USERS = [u.strip() for u in os.getenv("ADMIN_USERS", "").split(",") if u.strip()]

# Initialize OpenAI client
client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    default_headers={"OpenAI-Beta": "assistants=v2"}
)


class ConversationManager:
    """Manages conversation context, rate limiting, and thread management"""
    
    def __init__(self):
        self.cache_manager = cache_manager
        self.client = client
    
    async def get_conversation_context(self, user_id: str, channel_id: str) -> dict:
        """Get recent conversation context"""
        cache_key = f"{user_id}_{channel_id}"
        redis_key = f"{CONVERSATION_CACHE_PREFIX}:{cache_key}"

        context = await self.cache_manager.get(redis_key, {})

        # Check if context is still valid (10 minutes)
        if context and context.get('timestamp', 0) < time.time() - 600:
            await self.cache_manager.delete(redis_key)
            return {}

        return context

    async def update_conversation_context(self, user_id: str, channel_id: str, question: str, response: str,
                                          response_type: str = None, table_used: str = None):
        """Update conversation context for follow-up questions"""
        cache_key = f"{user_id}_{channel_id}"
        redis_key = f"{CONVERSATION_CACHE_PREFIX}:{cache_key}"

        context = {
            'last_question': question,
            'last_response': response,
            'last_response_type': response_type,
            'last_table_used': table_used,
            'timestamp': time.time()
        }

        await self.cache_manager.set(redis_key, context, ex=CONVERSATION_CACHE_TTL)

    async def update_conversation_context_with_sql(self, user_id: str, channel_id: str, question: str, 
                                                   sql: str, table: str, success: bool):
        """Update conversation context after SQL execution"""
        context = {
            'last_question': question,
            'last_sql': sql,
            'last_table_used': table,
            'last_response_type': 'sql_results' if success else 'sql_error',
            'timestamp': time.time()
        }
        
        cache_key = f"{user_id}_{channel_id}"
        redis_key = f"{CONVERSATION_CACHE_PREFIX}:{cache_key}"
        await self.cache_manager.set(redis_key, context, ex=CONVERSATION_CACHE_TTL)

    async def get_or_create_thread(self, user_id: str, channel_id: str) -> str:
        """Get existing thread for user+channel or create new one"""
        cache_key = f"{user_id}_{channel_id}"
        redis_key = f"{THREAD_CACHE_PREFIX}:{cache_key}"

        existing_thread = await self.cache_manager.get(redis_key)
        if existing_thread:
            print(f"♻️ Using existing thread: {existing_thread}")
            return existing_thread

        try:
            vector_store_id = VECTOR_STORE_ID
            if not vector_store_id:
                print("❌ No VECTOR_STORE_ID found")
                return None

            # Create thread with file search enabled
            thread = self.client.beta.threads.create(
                tool_resources={
                    "file_search": {
                        "vector_store_ids": [vector_store_id]
                    }
                }
            )

            thread_id = thread.id
            print(f"🆕 Created new thread: {thread_id}")

            # Cache thread ID
            await self.cache_manager.set(redis_key, thread_id, ex=THREAD_CACHE_TTL)
            return thread_id

        except Exception as e:
            print(f"❌ Error creating thread: {e}")
            return None

    async def send_message_and_run(self, thread_id: str, message: str, instructions: str = None) -> str:
        """Send message to thread and run assistant"""
        try:
            # Add message to thread
            self.client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=message
            )

            # Create run with instructions
            run_params = {
                'thread_id': thread_id,
                'assistant_id': ASSISTANT_ID
            }
            
            if instructions:
                run_params['instructions'] = instructions

            run = self.client.beta.threads.runs.create(**run_params)

            # Wait for completion
            while run.status in ['queued', 'in_progress', 'requires_action']:
                run = self.client.beta.threads.runs.retrieve(
                    thread_id=thread_id,
                    run_id=run.id
                )
                time.sleep(1)

            if run.status == 'completed':
                # Get the latest message
                messages = self.client.beta.threads.messages.list(
                    thread_id=thread_id,
                    limit=1
                )
                
                if messages.data:
                    return messages.data[0].content[0].text.value
                else:
                    return "No response received"
            else:
                print(f"❌ Run failed with status: {run.status}")
                return f"Assistant run failed: {run.status}"

        except Exception as e:
            print(f"❌ Error in send_message_and_run: {e}")
            return f"Error communicating with assistant: {str(e)}"

    async def classify_question_with_openai(self, question: str, user_id: str, channel_id: str, context: dict = None) -> str:
        """Use OpenAI to classify question type with context awareness"""
        thread_id = await self.get_or_create_thread(user_id, channel_id)
        if not thread_id:
            return 'sql_required'  # Default fallback

        # Build classification instructions
        instructions = """You are a BI expert who needs to classify user questions into two categories:

**sql_required**: Questions that need data analysis, queries, calculations, rankings, comparisons, or retrieving specific information from databases
**conversational**: Questions about definitions, explanations, capabilities, or clarifications about previous results

**SQL_REQUIRED Examples:**
- "How many tickets were created today?"
- "Who has the highest QA scores?"  
- "Can you tell me who has made the most improvement in QA out of these agents in the last 2 weeks?"
- "Show me agent performance for these specific agents"
- "What's the average handle time by team?"
- "Compare performance between agents X, Y, Z"
- "Show trends for the last month"
- "Which agent performed best?"
- "Top 10 agents by resolution time"
- Any question asking for WHO, WHAT (metrics), HOW MANY, SHOW ME, COMPARE

**CONVERSATIONAL Examples:**
- "What does AHT mean?" (definition)
- "What is the source of this data?" (about previous results)
- "How do you calculate QA scores?" (methodology)
- "What tables do you have access to?" (capabilities)
- "Explain these results" (clarification about previous results)
- "What does that number mean?" (about previous results)

**CRITICAL CLASSIFICATION RULES:**
1. Questions asking for specific data, metrics, comparisons, rankings = sql_required
2. Questions with agent names + performance/improvement = sql_required  
3. Questions asking WHO, WHAT metrics, HOW MANY, SHOW ME = sql_required
4. Questions about definitions, explanations, or data sources = conversational
5. If unsure between data vs explanation, lean toward sql_required

Return ONLY one word: "sql_required" or "conversational" (no explanation, no other text)"""

        # Build message with context
        message_parts = [f"Question to classify: {question}"]

        # Add context if it might affect classification
        if context and context.get('last_response_type') == 'sql_results':
            # Check if this looks like a follow-up about previous results
            followup_indicators = ['this data', 'these results', 'that number', 'the source', 'why is it',
                                   'what does this mean', 'explain this']
            if any(indicator in question.lower() for indicator in followup_indicators):
                message_parts.append("Context: This appears to be a follow-up question about previous SQL results.")

        message = "\n".join(message_parts)

        try:
            response = await self.send_message_and_run(thread_id, message, instructions)

            # Extract classification
            response_clean = response.strip().lower()
            if 'sql_required' in response_clean:
                return 'sql_required'
            elif 'conversational' in response_clean:
                return 'conversational'
            else:
                print(f"⚠️ Unclear OpenAI classification response: {response}")
                # Fallback to simple heuristic
                from app.question_analyzer import classify_question_type_fallback
                return classify_question_type_fallback(question)

        except Exception as e:
            print(f"❌ Error in OpenAI classification: {e}")
            # Fallback to simple heuristic
            from app.question_analyzer import classify_question_type_fallback
            return classify_question_type_fallback(question)

    # Rate limiting methods
    async def get_user_token_usage(self, user_id: str, period: str) -> int:
        """Get user's token usage for a period (daily/hourly)"""
        now = time.time()
        
        if period == "daily":
            # Use current date as key
            date_key = time.strftime("%Y-%m-%d", time.localtime(now))
            cache_key = f"{DAILY_USAGE_PREFIX}:{user_id}:{date_key}"
        elif period == "hourly":
            # Use current hour as key
            hour_key = time.strftime("%Y-%m-%d:%H", time.localtime(now))
            cache_key = f"{HOURLY_USAGE_PREFIX}:{user_id}:{hour_key}"
        else:
            return 0

        usage = await self.cache_manager.get(cache_key, 0)
        return int(usage) if usage else 0

    async def track_actual_usage(self, user_id: str, channel_id: str, tokens_used: int):
        """Track actual token usage after API calls"""
        if not ENABLE_RATE_LIMITING:
            return

        now = time.time()
        
        # Track daily usage
        date_key = time.strftime("%Y-%m-%d", time.localtime(now))
        daily_key = f"{DAILY_USAGE_PREFIX}:{user_id}:{date_key}"
        current_daily = await self.cache_manager.get(daily_key, 0)
        await self.cache_manager.set(daily_key, current_daily + tokens_used, ex=TOKEN_USAGE_CACHE_TTL)
        
        # Track hourly usage
        hour_key = time.strftime("%Y-%m-%d:%H", time.localtime(now))
        hourly_key = f"{HOURLY_USAGE_PREFIX}:{user_id}:{hour_key}"
        current_hourly = await self.cache_manager.get(hourly_key, 0)
        await self.cache_manager.set(hourly_key, current_hourly + tokens_used, ex=3600)  # 1 hour
        
        # Track thread usage
        thread_key = f"{THREAD_USAGE_PREFIX}:{user_id}_{channel_id}"
        current_thread = await self.cache_manager.get(thread_key, 0)
        await self.cache_manager.set(thread_key, current_thread + tokens_used, ex=THREAD_CACHE_TTL)

    async def check_rate_limits(self, user_id: str, channel_id: str, estimated_tokens: int) -> Dict[str, Any]:
        """Check if user is within rate limits"""
        if not ENABLE_RATE_LIMITING or user_id in ADMIN_USERS:
            return {
                'allowed': True,
                'reason': 'unlimited' if user_id in ADMIN_USERS else 'rate_limiting_disabled',
                'limits': {
                    'daily': MAX_TOKENS_PER_USER_PER_DAY,
                    'hourly': MAX_TOKENS_PER_USER_PER_HOUR,
                    'thread': MAX_TOKENS_PER_THREAD
                },
                'daily_usage': 0,
                'hourly_usage': 0,
                'thread_usage': 0
            }

        # Get current usage
        daily_usage = await self.get_user_token_usage(user_id, "daily")
        hourly_usage = await self.get_user_token_usage(user_id, "hourly")
        
        thread_key = f"{THREAD_USAGE_PREFIX}:{user_id}_{channel_id}"
        thread_usage = await self.cache_manager.get(thread_key, 0)

        # Check limits
        if daily_usage + estimated_tokens > MAX_TOKENS_PER_USER_PER_DAY:
            return {
                'allowed': False,
                'reason': 'daily_limit_exceeded',
                'limits': {
                    'daily': MAX_TOKENS_PER_USER_PER_DAY,
                    'hourly': MAX_TOKENS_PER_USER_PER_HOUR,
                    'thread': MAX_TOKENS_PER_THREAD
                },
                'daily_usage': daily_usage,
                'hourly_usage': hourly_usage,
                'thread_usage': thread_usage
            }

        if hourly_usage + estimated_tokens > MAX_TOKENS_PER_USER_PER_HOUR:
            return {
                'allowed': False,
                'reason': 'hourly_limit_exceeded',
                'limits': {
                    'daily': MAX_TOKENS_PER_USER_PER_DAY,
                    'hourly': MAX_TOKENS_PER_USER_PER_HOUR,
                    'thread': MAX_TOKENS_PER_THREAD
                },
                'daily_usage': daily_usage,
                'hourly_usage': hourly_usage,
                'thread_usage': thread_usage
            }

        if thread_usage + estimated_tokens > MAX_TOKENS_PER_THREAD:
            return {
                'allowed': False,
                'reason': 'thread_limit_exceeded',
                'limits': {
                    'daily': MAX_TOKENS_PER_USER_PER_DAY,
                    'hourly': MAX_TOKENS_PER_USER_PER_HOUR,
                    'thread': MAX_TOKENS_PER_THREAD
                },
                'daily_usage': daily_usage,
                'hourly_usage': hourly_usage,
                'thread_usage': thread_usage
            }

        return {
            'allowed': True,
            'reason': 'within_limits',
            'limits': {
                'daily': MAX_TOKENS_PER_USER_PER_DAY,
                'hourly': MAX_TOKENS_PER_USER_PER_HOUR,
                'thread': MAX_TOKENS_PER_THREAD
            },
            'daily_usage': daily_usage,
            'hourly_usage': hourly_usage,
            'thread_usage': thread_usage
        }

    def estimate_request_tokens(self, question: str, context: str = "") -> int:
        """Estimate tokens for a request"""
        # Simple estimation: ~4 characters per token
        total_chars = len(question) + len(context) + 1000  # Add overhead
        return total_chars // 4

    async def test_conversation_flow(self):
        """Test conversation flow functionality"""
        print("🧪 Testing conversation flow:")
        
        # Test context setting and retrieval
        await self.update_conversation_context("test_user", "test_channel", 
                                               "Test question", "Test response", "sql_results", "test_table")
        
        context = await self.get_conversation_context("test_user", "test_channel")
        print(f"Context test: {context}")
        
        # Test classification
        test_question = "How many tickets were created today?"
        classification = await self.classify_question_with_openai(test_question, "test_user", "test_channel", context)
        print(f"Classification test: {classification}")


# Global conversation manager instance
conversation_manager = ConversationManager()

# Convenience functions for backward compatibility
async def get_conversation_context(user_id: str, channel_id: str) -> dict:
    """Get conversation context"""
    return await conversation_manager.get_conversation_context(user_id, channel_id)

async def update_conversation_context(user_id: str, channel_id: str, question: str, response: str,
                                      response_type: str = None, table_used: str = None):
    """Update conversation context"""
    return await conversation_manager.update_conversation_context(user_id, channel_id, question, response, response_type, table_used)

async def update_conversation_context_with_sql(user_id: str, channel_id: str, question: str, 
                                               sql: str, table: str, success: bool):
    """Update context with SQL info"""
    return await conversation_manager.update_conversation_context_with_sql(user_id, channel_id, question, sql, table, success)

async def get_or_create_thread(user_id: str, channel_id: str) -> str:
    """Get or create thread"""
    return await conversation_manager.get_or_create_thread(user_id, channel_id)

async def send_message_and_run(thread_id: str, message: str, instructions: str = None) -> str:
    """Send message and run"""
    return await conversation_manager.send_message_and_run(thread_id, message, instructions)

async def classify_question_with_openai(question: str, user_id: str, channel_id: str, context: dict = None) -> str:
    """Classify question with OpenAI"""
    return await conversation_manager.classify_question_with_openai(question, user_id, channel_id, context)

async def get_user_token_usage(user_id: str, period: str) -> int:
    """Get user token usage"""
    return await conversation_manager.get_user_token_usage(user_id, period)

async def track_actual_usage(user_id: str, channel_id: str, tokens_used: int):
    """Track token usage"""
    return await conversation_manager.track_actual_usage(user_id, channel_id, tokens_used)

async def check_rate_limits(user_id: str, channel_id: str, estimated_tokens: int) -> Dict[str, Any]:
    """Check rate limits"""
    return await conversation_manager.check_rate_limits(user_id, channel_id, estimated_tokens)

def estimate_request_tokens(question: str, context: str = "") -> int:
    """Estimate request tokens"""
    return conversation_manager.estimate_request_tokens(question, context)

async def test_conversation_flow():
    """Test conversation flow"""
    return await conversation_manager.test_conversation_flow()