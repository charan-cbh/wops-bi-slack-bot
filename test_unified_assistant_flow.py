#!/usr/bin/env python3
"""
Test the unified Assistant API flow for both data and conversational questions
"""
import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.llm_orchestrator import LLMOrchestrator

async def test_unified_flow():
    """Test the unified Assistant API approach"""
    print("🤖 Testing Unified Assistant API Flow")
    print("=" * 50)
    
    orchestrator = LLMOrchestrator()
    
    if not orchestrator.model_provider:
        print("❌ No model provider available")
        return
        
    print(f"Model Provider: {orchestrator.model_provider.__class__.__name__}")
    print(f"Use Assistant API: {getattr(orchestrator.model_provider, 'use_assistant_api', False)}")
    print(f"Assistant ID: {getattr(orchestrator.model_provider, 'assistant_id', 'None')}")
    print()
    
    test_questions = [
        # Data questions (should generate SQL)
        ("What's our QA score this week?", "DATA"),
        ("How many tickets were created today?", "DATA"), 
        ("Show me agent performance metrics", "DATA"),
        
        # Conversational questions (should use business context)
        ("How do I escalate a ticket?", "CONVERSATIONAL"),
        ("What is our cancellation policy?", "CONVERSATIONAL")
    ]
    
    for question, expected_type in test_questions:
        print(f"\n🧪 Testing: {question}")
        print(f"Expected type: {expected_type}")
        print("-" * 40)
        
        try:
            response, response_type = await orchestrator.handle_question(
                question=question,
                user_id="test_user",
                channel_id="test_channel"
            )
            
            print(f"Response type: {response_type}")
            print(f"Response preview: {response[:200]}...")
            
            if response_type != 'error':
                print("✅ Question processed successfully")
            else:
                print(f"❌ Error processing question")
                
        except Exception as e:
            print(f"❌ Exception: {e}")
        
        print()

if __name__ == "__main__":
    asyncio.run(test_unified_flow())