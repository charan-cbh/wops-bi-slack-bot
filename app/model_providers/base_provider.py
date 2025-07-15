#!/usr/bin/env python3
"""
Abstract Base Provider for Model APIs
Defines the interface that all model providers must implement
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
import time
import asyncio


class BaseModelProvider(ABC):
    """Abstract base class for model providers (OpenAI, Anthropic, etc.)"""
    
    def __init__(self, api_key: str, model_name: str = None):
        self.api_key = api_key
        self.model_name = model_name
        self.provider_name = self.__class__.__name__.replace('Provider', '').lower()
    
    @abstractmethod
    async def generate_response(self, messages: List[Dict[str, str]], system_prompt: str = None, **kwargs) -> str:
        """
        Generate a response from the model
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            system_prompt: Optional system prompt
            **kwargs: Provider-specific parameters
            
        Returns:
            str: Generated response
        """
        pass
    
    @abstractmethod
    async def generate_sql(self, question: str, instructions: str, context: Dict[str, Any] = None) -> str:
        """
        Generate SQL query for a given question
        
        Args:
            question: User's question
            instructions: SQL generation instructions
            context: Additional context (schema, samples, etc.)
            
        Returns:
            str: Generated SQL query
        """
        pass
    
    @abstractmethod
    async def summarize_results(self, question: str, results: str, sql_query: str = None, context: Dict[str, Any] = None) -> str:
        """
        Summarize query results into a business-friendly response
        
        Args:
            question: Original user question
            results: Query results as string
            sql_query: The SQL query that generated the results
            context: Additional context
            
        Returns:
            str: Summarized response
        """
        pass
    
    @abstractmethod
    async def classify_question(self, question: str, context: Dict[str, Any] = None) -> str:
        """
        Classify question type (sql_required, conversational, etc.)
        
        Args:
            question: User's question
            context: Additional context
            
        Returns:
            str: Question classification
        """
        pass
    
    @abstractmethod
    async def handle_conversational(self, question: str, context: Dict[str, Any] = None) -> str:
        """
        Handle conversational questions that don't require SQL
        
        Args:
            question: User's question
            context: Conversation context
            
        Returns:
            str: Conversational response
        """
        pass
    
    # Common utility methods that providers can override if needed
    
    def estimate_tokens(self, text: str) -> int:
        """Estimate token count for text (rough approximation)"""
        # Simple approximation: ~4 characters per token
        return max(1, len(text) // 4)
    
    def format_messages(self, user_message: str, system_prompt: str = None, conversation_history: List[Dict] = None) -> List[Dict[str, str]]:
        """Format messages for API call"""
        messages = []
        
        if conversation_history:
            messages.extend(conversation_history)
        
        if system_prompt and not messages:
            messages.append({"role": "system", "content": system_prompt})
        
        messages.append({"role": "user", "content": user_message})
        
        return messages
    
    async def retry_with_backoff(self, func, max_retries: int = 3, base_delay: float = 1.0):
        """Retry function with exponential backoff"""
        for attempt in range(max_retries):
            try:
                return await func()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                delay = base_delay * (2 ** attempt)
                print(f"⚠️ {self.provider_name} API error (attempt {attempt + 1}): {e}")
                print(f"🔄 Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
    
    def log_request(self, method: str, **kwargs):
        """Log API request details"""
        print(f"🔄 {self.provider_name.title()} {method} request")
        if 'model' in kwargs:
            print(f"   Model: {kwargs['model']}")
        if 'messages' in kwargs and isinstance(kwargs['messages'], list):
            print(f"   Messages: {len(kwargs['messages'])}")
    
    def log_response(self, method: str, response_length: int, tokens_used: int = None):
        """Log API response details"""
        print(f"✅ {self.provider_name.title()} {method} response: {response_length} chars")
        if tokens_used:
            print(f"   Tokens used: {tokens_used}")


class ModelProviderError(Exception):
    """Base exception for model provider errors"""
    pass


class ModelProviderRateLimitError(ModelProviderError):
    """Rate limit exceeded error"""
    pass


class ModelProviderAuthenticationError(ModelProviderError):
    """Authentication error"""
    pass


class ModelProviderQuotaError(ModelProviderError):
    """Quota exceeded error"""
    pass