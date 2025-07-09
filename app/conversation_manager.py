"""
Conversation Manager - Handles OpenAI thread management and conversational context for the BI Slack Bot
"""
import os
import time
import tiktoken
from typing import Dict, Optional, Tuple
from openai import OpenAI
from .valkey_manager import ValkeyManager

class ConversationManager:
    def __init__(self, openai_client: OpenAI, valkey_manager: ValkeyManager):
        self.client = openai_client
        self.valkey_manager = valkey_manager
        
        # Rate limiting configuration
        self.MAX_TOKENS_PER_USER_PER_DAY = int(os.getenv("MAX_TOKENS_PER_USER_PER_DAY", 1000000))
        self.MAX_TOKENS_PER_USER_PER_HOUR = int(os.getenv("MAX_TOKENS_PER_USER_PER_HOUR", 200000))
        self.MAX_TOKENS_PER_THREAD = int(os.getenv("MAX_TOKENS_PER_THREAD", 1000000))
        self.ENABLE_RATE_LIMITING = os.getenv("ENABLE_RATE_LIMITING", "true").lower() == "true"
        self.ADMIN_USERS = [u.strip() for u in os.getenv("ADMIN_USERS", "").split(",") if u.strip()]
        
        # Initialize tokenizer
        try:
            self.tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")
        except:
            self.tokenizer = tiktoken.get_encoding("cl100k_base")
    
    def count_tokens(self, text: str) -> int:
        """Count tokens in a text string"""
        try:
            return len(self.tokenizer.encode(text))
        except:
            # Fallback to approximate counting
            return len(text.split()) * 1.3
    
    async def check_rate_limits(self, user_id: str, estimated_tokens: int) -> Tuple[bool, str]:
        """Check if user has exceeded rate limits"""
        try:
            if not self.ENABLE_RATE_LIMITING or user_id in self.ADMIN_USERS:
                return True, "Rate limiting disabled or admin user"
            
            # Check daily limit
            daily_key = f"{self.valkey_manager.DAILY_USAGE_PREFIX}:{user_id}:{time.strftime('%Y-%m-%d')}"
            daily_usage = await self.valkey_manager.safe_valkey_get(daily_key, 0)
            
            if daily_usage + estimated_tokens > self.MAX_TOKENS_PER_USER_PER_DAY:
                return False, f"Daily token limit exceeded ({daily_usage}/{self.MAX_TOKENS_PER_USER_PER_DAY})"
            
            # Check hourly limit
            hourly_key = f"{self.valkey_manager.HOURLY_USAGE_PREFIX}:{user_id}:{time.strftime('%Y-%m-%d-%H')}"
            hourly_usage = await self.valkey_manager.safe_valkey_get(hourly_key, 0)
            
            if hourly_usage + estimated_tokens > self.MAX_TOKENS_PER_USER_PER_HOUR:
                return False, f"Hourly token limit exceeded ({hourly_usage}/{self.MAX_TOKENS_PER_USER_PER_HOUR})"
            
            return True, "Within rate limits"
            
        except Exception as e:
            print(f"❌ Error checking rate limits: {e}")
            return True, "Error checking limits, allowing request"
    
    async def update_token_usage(self, user_id: str, thread_id: str, tokens_used: int):
        """Update token usage counters"""
        try:
            if not self.ENABLE_RATE_LIMITING:
                return
            
            # Update daily usage
            daily_key = f"{self.valkey_manager.DAILY_USAGE_PREFIX}:{user_id}:{time.strftime('%Y-%m-%d')}"
            daily_usage = await self.valkey_manager.safe_valkey_get(daily_key, 0)
            await self.valkey_manager.safe_valkey_set(
                daily_key, 
                daily_usage + tokens_used, 
                ex=self.valkey_manager.TOKEN_USAGE_CACHE_TTL
            )
            
            # Update hourly usage
            hourly_key = f"{self.valkey_manager.HOURLY_USAGE_PREFIX}:{user_id}:{time.strftime('%Y-%m-%d-%H')}"
            hourly_usage = await self.valkey_manager.safe_valkey_get(hourly_key, 0)
            await self.valkey_manager.safe_valkey_set(
                hourly_key, 
                hourly_usage + tokens_used, 
                ex=3600  # 1 hour
            )
            
            # Update thread usage
            thread_key = f"{self.valkey_manager.THREAD_USAGE_PREFIX}:{thread_id}"
            thread_usage = await self.valkey_manager.safe_valkey_get(thread_key, 0)
            await self.valkey_manager.safe_valkey_set(
                thread_key, 
                thread_usage + tokens_used, 
                ex=self.valkey_manager.TOKEN_USAGE_CACHE_TTL
            )
            
            print(f"📊 Token usage updated: {tokens_used} tokens for user {user_id}")
            
        except Exception as e:
            print(f"❌ Error updating token usage: {e}")
    
    async def get_conversation_context(self, user_id: str, channel_id: str) -> dict:
        """Get conversation context for user in channel"""
        try:
            context_key = f"{self.valkey_manager.CONVERSATION_CACHE_PREFIX}:{user_id}:{channel_id}"
            context = await self.valkey_manager.safe_valkey_get(context_key, {})
            
            if context:
                print(f"✅ Retrieved conversation context for {user_id} in {channel_id}")
            
            return context
            
        except Exception as e:
            print(f"❌ Error getting conversation context: {e}")
            return {}
    
    async def update_conversation_context(self, user_id: str, channel_id: str, question: str, response: str, 
                                        table: str = None, sql: str = None):
        """Update conversation context with latest interaction"""
        try:
            context_key = f"{self.valkey_manager.CONVERSATION_CACHE_PREFIX}:{user_id}:{channel_id}"
            
            # Get existing context
            context = await self.valkey_manager.safe_valkey_get(context_key, {})
            
            # Update with new interaction
            context.update({
                "last_question": question,
                "last_response": response,
                "last_table": table,
                "last_sql": sql,
                "last_interaction": time.time(),
                "summary": f"User asked: {question}. Response involved table: {table}"
            })
            
            # Save updated context
            await self.valkey_manager.safe_valkey_set(
                context_key, 
                context, 
                ex=self.valkey_manager.CONVERSATION_CACHE_TTL
            )
            
            print(f"✅ Updated conversation context for {user_id} in {channel_id}")
            
        except Exception as e:
            print(f"❌ Error updating conversation context: {e}")
    
    async def get_or_create_thread(self, user_id: str, channel_id: str) -> str:
        """Get existing thread or create new one for user in channel"""
        try:
            # Check cache for existing thread
            thread_key = f"{self.valkey_manager.THREAD_CACHE_PREFIX}:{user_id}:{channel_id}"
            thread_id = await self.valkey_manager.safe_valkey_get(thread_key)
            
            if thread_id:
                print(f"✅ Using existing thread: {thread_id}")
                return thread_id
            
            # Create new thread
            thread = self.client.beta.threads.create()
            thread_id = thread.id
            
            # Cache the thread ID
            await self.valkey_manager.safe_valkey_set(
                thread_key, 
                thread_id, 
                ex=self.valkey_manager.THREAD_CACHE_TTL
            )
            
            print(f"✅ Created new thread: {thread_id}")
            return thread_id
            
        except Exception as e:
            print(f"❌ Error getting/creating thread: {e}")
            raise
    
    async def wait_for_active_runs(self, thread_id: str, max_wait_seconds: int = 30) -> bool:
        """Wait for any active runs to complete before starting new one"""
        try:
            start_time = time.time()
            
            while time.time() - start_time < max_wait_seconds:
                # Get active runs
                runs = self.client.beta.threads.runs.list(thread_id=thread_id)
                
                # Check if any runs are active
                active_runs = [run for run in runs.data if run.status in ['queued', 'in_progress']]
                
                if not active_runs:
                    return True
                
                print(f"⏳ Waiting for {len(active_runs)} active runs to complete...")
                time.sleep(1)
            
            print(f"⏰ Timeout waiting for active runs to complete")
            return False
            
        except Exception as e:
            print(f"❌ Error waiting for active runs: {e}")
            return False
    
    async def send_message_and_run(self, thread_id: str, message: str, instructions: str = None) -> str:
        """Send message to thread and run assistant"""
        try:
            # Wait for any active runs to complete
            if not await self.wait_for_active_runs(thread_id):
                return "Error: Timeout waiting for previous runs to complete"
            
            # Add message to thread
            self.client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=message
            )
            
            # Create run
            assistant_id = os.getenv("ASSISTANT_ID")
            if not assistant_id:
                return "Error: No assistant ID configured"
            
            run_kwargs = {
                "thread_id": thread_id,
                "assistant_id": assistant_id
            }
            
            if instructions:
                run_kwargs["instructions"] = instructions
            
            run = self.client.beta.threads.runs.create(**run_kwargs)
            
            # Wait for completion
            max_wait = 60  # 1 minute timeout
            start_time = time.time()
            
            while run.status in ['queued', 'in_progress']:
                if time.time() - start_time > max_wait:
                    print(f"⏰ Run timeout for thread {thread_id}")
                    return "Error: Assistant response timeout"
                
                time.sleep(1)
                run = self.client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
            
            if run.status == 'completed':
                # Get the assistant's response
                messages = self.client.beta.threads.messages.list(thread_id=thread_id)
                
                for message in messages.data:
                    if message.role == "assistant":
                        response = message.content[0].text.value
                        print(f"✅ Assistant response received")
                        return response
                
                return "Error: No assistant response found"
            
            elif run.status == 'failed':
                error_msg = f"Assistant run failed: {run.last_error}"
                print(f"❌ {error_msg}")
                return f"Error: {error_msg}"
            
            else:
                error_msg = f"Unexpected run status: {run.status}"
                print(f"❌ {error_msg}")
                return f"Error: {error_msg}"
                
        except Exception as e:
            error_msg = f"Error in send_message_and_run: {str(e)}"
            print(f"❌ {error_msg}")
            return f"Error: {error_msg}"
    
    async def handle_conversational_question(self, user_question: str, user_id: str, channel_id: str) -> str:
        """Handle conversational/general questions using OpenAI assistant"""
        try:
            print(f"💬 Handling conversational question: {user_question}")
            
            # Check rate limits
            estimated_tokens = self.count_tokens(user_question) * 2  # Estimate response tokens
            can_proceed, limit_msg = await self.check_rate_limits(user_id, estimated_tokens)
            
            if not can_proceed:
                return f"⚠️ Rate limit exceeded: {limit_msg}"
            
            # Get or create thread
            thread_id = await self.get_or_create_thread(user_id, channel_id)
            
            # Get conversation context
            context = await self.get_conversation_context(user_id, channel_id)
            
            # Build instructions for conversational response
            instructions = f"""
            You are a helpful BI assistant. The user has asked a conversational question.
            
            Provide a helpful, friendly response. If the question is about data analysis or business intelligence,
            guide them toward asking specific data questions.
            
            Keep responses concise and professional.
            
            Previous context: {context.get('summary', 'No previous context')}
            """
            
            # Send message and get response
            response = await self.send_message_and_run(thread_id, user_question, instructions)
            
            # Update token usage
            actual_tokens = self.count_tokens(user_question + response)
            await self.update_token_usage(user_id, thread_id, actual_tokens)
            
            # Update conversation context
            await self.update_conversation_context(user_id, channel_id, user_question, response)
            
            return response
            
        except Exception as e:
            error_msg = f"Error handling conversational question: {str(e)}"
            print(f"❌ {error_msg}")
            return f"I apologize, but I encountered an error while processing your question: {error_msg}"