#!/usr/bin/env python3
"""
Anthropic Claude API Provider
Implements the BaseModelProvider interface for Claude models
"""

import os
import json
import asyncio
from typing import Dict, Any, Optional, List
from .base_provider import BaseModelProvider, ModelProviderError, ModelProviderRateLimitError

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("⚠️ Anthropic package not installed. Install with: pip install anthropic")


class AnthropicProvider(BaseModelProvider):
    """Anthropic Claude API provider implementation"""
    
    def __init__(self, api_key: str, model_name: str = "claude-3-5-sonnet-20241022"):
        super().__init__(api_key, model_name)
        
        if not ANTHROPIC_AVAILABLE:
            raise ModelProviderError("Anthropic package not available. Install with: pip install anthropic")
        
        if not api_key or api_key == "your_anthropic_api_key_here":
            raise ModelProviderError("Anthropic API key not configured")
        
        self.client = anthropic.AsyncAnthropic(api_key=api_key)
        self.model_name = model_name
        self.max_tokens = 4096  # Claude's typical max output tokens
        
        print(f"🤖 Initialized Anthropic provider with model: {model_name}")
    
    async def generate_response(self, messages: List[Dict[str, str]], system_prompt: str = None, **kwargs) -> str:
        """Generate a response using Claude"""
        
        try:
            # Format messages for Claude
            claude_messages = []
            system_message = system_prompt
            
            for msg in messages:
                if msg['role'] == 'system':
                    # Claude handles system messages separately
                    if not system_message:
                        system_message = msg['content']
                    else:
                        system_message += f"\n\n{msg['content']}"
                elif msg['role'] in ['user', 'assistant']:
                    claude_messages.append({
                        'role': msg['role'],
                        'content': msg['content']
                    })
            
            # Claude API parameters
            api_params = {
                'model': self.model_name,
                'max_tokens': kwargs.get('max_tokens', self.max_tokens),
                'messages': claude_messages,
                'temperature': kwargs.get('temperature', 0.3)
            }
            
            if system_message:
                api_params['system'] = system_message
            
            self.log_request("generate_response", **api_params)
            
            async def make_request():
                response = await self.client.messages.create(**api_params)
                return response
            
            response = await self.retry_with_backoff(make_request)
            
            # Extract content from response
            content = ""
            if response.content:
                for block in response.content:
                    if hasattr(block, 'text'):
                        content += block.text
            
            self.log_response("generate_response", len(content), response.usage.input_tokens + response.usage.output_tokens if response.usage else None)
            
            return content.strip()
            
        except anthropic.RateLimitError as e:
            raise ModelProviderRateLimitError(f"Claude rate limit exceeded: {e}")
        except anthropic.AuthenticationError as e:
            raise ModelProviderError(f"Claude authentication error: {e}")
        except Exception as e:
            raise ModelProviderError(f"Claude API error: {e}")
    
    async def generate_sql(self, question: str, instructions: str, context: Dict[str, Any] = None) -> str:
        """Generate SQL query using Claude"""
        
        user_message = f"""Generate a SQL query to answer this question:

Question: {question}

Instructions: {instructions}

{f"Additional Context: {json.dumps(context, indent=2)}" if context else ""}

Please use the business intelligence knowledge base to understand the table structures and relationships. Return only the SQL query, no explanations."""

        messages = [{"role": "user", "content": user_message}]
        
        # This will use the standard generate_response method
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
        """Summarize query results using Claude"""
        
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
        """Classify question type using Claude"""
        
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
        """Handle conversational questions using Claude"""
        
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
    
    def estimate_tokens(self, text: str) -> int:
        """Estimate tokens for Claude (more accurate approximation)"""
        # Claude uses a similar tokenization to GPT, roughly 3.5-4 chars per token
        return max(1, len(text) // 4)
    
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
def create_anthropic_provider(api_key: str = None, model_name: str = None) -> AnthropicProvider:
    """Create an Anthropic provider instance"""
    
    if not api_key:
        api_key = os.getenv('ANTHROPIC_API_KEY')
    
    if not model_name:
        model_name = os.getenv('CLAUDE_MODEL', 'claude-3-5-sonnet-20241022')
    
    return AnthropicProvider(api_key, model_name)