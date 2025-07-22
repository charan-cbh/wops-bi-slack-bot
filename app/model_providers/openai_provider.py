#!/usr/bin/env python3
"""
OpenAI API Provider
Implements the BaseModelProvider interface for OpenAI models (including GPT-4, Assistant API)
"""

import os
import json
import asyncio
from typing import Dict, Any, Optional, List
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
        
        # Add system prompt as additional instructions if provided
        additional_instructions = system_prompt if system_prompt else None
        
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
                assistant_id=self.assistant_id,
                additional_instructions=additional_instructions
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
    
    async def generate_sql(self, question: str, instructions: str, context: Dict[str, Any] = None) -> str:
        """Generate SQL query using OpenAI with Assistant API and vector store when available"""
        
        user_message = f"""Generate a SQL query to answer this question:

Question: {question}

Instructions: {instructions}

{f"Additional Context: {json.dumps(context, indent=2)}" if context else ""}

Please use the business intelligence knowledge base to understand the table structures and relationships. Return only the SQL query, no explanations."""

        messages = [{"role": "user", "content": user_message}]
        
        # This will automatically use Assistant API with vector store if configured
        response = await self.generate_response(messages, temperature=0.1)
        
        # Extract SQL from response (remove any markdown formatting)
        sql = response.strip()
        if sql.startswith('```sql'):
            sql = sql[6:]
        if sql.startswith('```'):
            sql = sql[3:]
        if sql.endswith('```'):
            sql = sql[:-3]
        
        return sql.strip()
    
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

Be concise but informative. Focus on actionable insights rather than just restating the data."""

        user_message = f"""Please analyze and summarize these query results:

Original Question: {question}

Query Results:
{results}

{f"SQL Query Used: {sql_query}" if sql_query and self._user_requested_sql(question) else ""}

Provide a clear, business-friendly summary with key insights. Only include the SQL query in your response if it was explicitly provided in the context above."""

        messages = [{"role": "user", "content": user_message}]
        
        response = await self.generate_response(messages, system_prompt, temperature=0.3)
        
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
        
        response = await self.generate_response(messages, system_prompt, temperature=0.1)
        
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
        
        system_prompt = """You are a helpful assistant for a business intelligence Slack bot. You help users understand how to use the system and provide friendly, helpful responses to general questions.

Key information about the bot:
- This is a BI (Business Intelligence) bot that can answer questions about agent performance, tickets, schedules, and team metrics
- Users can ask questions like "How many agents are in team X?" or "What's the average handle time for agent Y?"
- The bot connects to Snowflake database with various performance and schedule tables
- For data questions, users should ask specific questions about agents, teams, performance metrics, etc.

Be friendly, helpful, and concise. If users ask how to use the bot, guide them on asking data-related questions."""

        user_message = question
        if context:
            user_message += f"\n\nContext: {json.dumps(context, indent=2)}"

        messages = [{"role": "user", "content": user_message}]
        
        return await self.generate_response(messages, system_prompt, temperature=0.5)
    
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
        model_name = os.getenv('OPENAI_MODEL', 'gpt-4')
    
    if use_assistant_api is None:
        use_assistant_api = os.getenv('USE_ASSISTANT_API', 'true').lower() == 'true'
    
    if not assistant_id:
        assistant_id = os.getenv('ASSISTANT_ID')
    
    return OpenAIProvider(api_key, model_name, use_assistant_api, assistant_id)