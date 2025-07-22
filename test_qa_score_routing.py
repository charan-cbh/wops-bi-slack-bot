#!/usr/bin/env python3
"""
Test QA Score question routing and processing
"""
import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.bi_service import should_use_bi_service, process_with_bi_service

async def test_qa_score_routing():
    """Test QA score question routing and processing"""
    print("🧪 Testing QA Score Question Routing")
    print("=" * 50)
    
    qa_questions = [
        "What's our QA score this week?",
        "Show me quality scores",
        "What is the quality rating?", 
        "How are our Klaus scores?",
        "What's our scorecard performance?"
    ]
    
    for question in qa_questions:
        print(f"\nQuestion: {question}")
        
        # Test routing
        should_use_bi = should_use_bi_service(question)
        print(f"Routes to BI Service: {should_use_bi}")
        
        if should_use_bi:
            # Test actual processing with BI Service
            try:
                response, response_type = await process_with_bi_service(
                    question=question,
                    user_id="test_user",
                    channel_id="test_channel"
                )
                print(f"Response type: {response_type}")
                print(f"Response preview: {response[:150]}...")
                print("✅ QA question processed successfully")
            except Exception as e:
                print(f"❌ Error processing QA question: {e}")
        else:
            print("❌ QA question incorrectly routed away from BI Service")
        
        print("-" * 40)

if __name__ == "__main__":
    asyncio.run(test_qa_score_routing())