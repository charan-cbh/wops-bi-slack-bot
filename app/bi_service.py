"""
BI Service - Simplified version inspired by wops-ai for Slack bot
Provides AI-only responses without SQL execution when USE_BI_SERVICE flag is enabled
"""
import os
import asyncio
import logging
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv

import openai
from app.conversation_manager import (
    get_conversation_context,
    update_conversation_context,
    check_rate_limits
)

load_dotenv()

# Configuration
USE_BI_SERVICE = os.getenv("USE_BI_SERVICE", "false").lower() == "true"
BI_SERVICE_PROVIDER = os.getenv("BI_SERVICE_PROVIDER", "openai")  # openai, anthropic
DEFAULT_MODEL = os.getenv("BI_SERVICE_MODEL", "gpt-4")

logger = logging.getLogger(__name__)

class BIService:
    """
    BI Service for generating AI-powered responses to business questions
    Simplified for Slack usage - AI responses only, no SQL execution
    """
    
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model = DEFAULT_MODEL
        self.provider_available = bool(self.api_key)
        logger.info(f"BI Service initialized with OpenAI (available: {self.provider_available})")
    
    async def process_question(
        self,
        question: str,
        user_id: str,
        channel_id: str,
        context: Optional[Dict] = None
    ) -> Tuple[str, str]:
        """
        Process a business question and return AI-generated response
        
        Args:
            question: User's natural language question
            user_id: Slack user ID
            channel_id: Slack channel ID
            context: Optional conversation context
            
        Returns:
            Tuple of (response, response_type)
        """
        if not self.provider_available:
            return "❌ BI Service is not properly configured (no OpenAI API key)", "error"
        
        try:
            # Check rate limits
            rate_limit_info = await check_rate_limits(user_id, channel_id, 1000)
            if not rate_limit_info['allowed']:
                return f"⚠️ {rate_limit_info['message']}", "rate_limited"
            
            # Get conversation context if not provided
            if not context:
                context = await get_conversation_context(user_id, channel_id)
            
            # Build enhanced prompt with business context
            enhanced_prompt = self._build_enhanced_prompt(question, context)
            
            # Generate AI response
            response = await self._generate_ai_response(enhanced_prompt, user_id, channel_id)
            
            # Update conversation context
            await update_conversation_context(
                user_id, 
                channel_id, 
                question, 
                response, 
                'bi_service'
            )
            
            return response, "ai_response"
            
        except Exception as e:
            logger.error(f"Error processing question in BI Service: {e}")
            return f"❌ Error processing your question: {str(e)}", "error"
    
    def _build_enhanced_prompt(self, question: str, context: Optional[Dict] = None) -> str:
        """
        Build enhanced prompt with business context and guidelines
        """
        base_prompt = f"""You are an expert business intelligence analyst for Clipboard Health's Worker Operations team. You help answer questions about operational metrics, agent performance, and business insights.

Context & Guidelines:
- Focus on providing clear, actionable business insights
- When discussing metrics, explain what they mean and why they matter
- If a question is about data you cannot access, explain what kind of analysis would be needed
- Be concise but comprehensive in your responses
- Always consider the business context and operational implications

Available Data Domains:
- Agent Performance (AHT, Schedule Adherence, Quality Scores)
- Ticket Operations (FCR, Resolution Times, Categories)
- Klaus QA Data (Quality Assessments, Feedback)
- Operational Metrics (Handle Time, Productivity)

"""
        
        # Add conversation context if available
        if context:
            last_question = context.get('last_question')
            last_response_type = context.get('last_response_type')
            if last_question and last_response_type:
                base_prompt += f"""
Recent Conversation Context:
- Previous question: {last_question}
- Previous response type: {last_response_type}
"""
        
        base_prompt += f"""
Current Question: {question}

Please provide a comprehensive business intelligence response. If this question would require specific data analysis, explain what metrics would be relevant and how to interpret them."""
        
        return base_prompt
    
    async def _generate_ai_response(self, prompt: str, user_id: str, channel_id: str) -> str:
        """
        Generate AI response using OpenAI Chat API directly
        """
        try:
            client = openai.AsyncOpenAI(api_key=self.api_key)
            
            response = await client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1500,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error generating AI response: {e}")
            raise
    
    def is_enabled(self) -> bool:
        """Check if BI Service is enabled"""
        return USE_BI_SERVICE and self.provider_available


# Global BI Service instance
_bi_service = None

def get_bi_service() -> BIService:
    """Get the global BI Service instance"""
    global _bi_service
    if _bi_service is None:
        _bi_service = BIService()
    return _bi_service

async def process_with_bi_service(
    question: str,
    user_id: str,
    channel_id: str,
    context: Optional[Dict] = None
) -> Tuple[str, str]:
    """
    Process question with BI Service if enabled
    
    Returns:
        Tuple of (response, response_type)
    """
    bi_service = get_bi_service()
    
    if not bi_service.is_enabled():
        return "BI Service is not enabled", "error"
    
    return await bi_service.process_question(question, user_id, channel_id, context)

def should_use_bi_service(question: str) -> bool:
    """
    Determine if a question should be routed to BI Service
    Based on question patterns and BI Service availability
    """
    if not USE_BI_SERVICE:
        return False
    
    bi_service = get_bi_service()
    if not bi_service.is_enabled():
        return False
    
    # Always use BI Service when enabled (simplified for Slack)
    # Could add more sophisticated routing logic here
    return True

# Configuration helper functions
def get_bi_service_status() -> Dict[str, Any]:
    """Get current BI Service status"""
    bi_service = get_bi_service()
    
    return {
        "enabled": USE_BI_SERVICE,
        "provider": BI_SERVICE_PROVIDER,
        "model": DEFAULT_MODEL,
        "configured": bi_service.is_enabled(),
        "provider_available": bi_service.provider_available
    }