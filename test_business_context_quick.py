#!/usr/bin/env python3
"""
Quick comprehensive test of Clipboard Health business context
"""
import asyncio
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

async def test_specific_questions():
    """Test specific Clipboard Health questions from the knowledge base"""
    from app.bi_service import process_with_bi_service
    
    # Specific questions that should be answerable from the knowledge base
    test_questions = [
        "How do Magic Shifts work?",
        "What happens if I cancel within 8 hours of my shift?",
        "How do I contact billing support?",
        "What documents do healthcare professionals need to upload?"
    ]
    
    print("🧪 Testing Specific Clipboard Health Questions")
    print("=" * 50)
    
    results = []
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n{i}. Question: {question}")
        try:
            response, response_type = await process_with_bi_service(
                question, "test_user", "test_channel"
            )
            
            if response_type == 'ai_response':
                # Check for specific expected content
                success_indicators = {
                    "magic shifts": ["$100", "guarantee", "premium", "urgent", "99%"],
                    "cancel": ["8 hours", "urgent", "premium", "fee", "policy"],
                    "billing": ["(415) 604-3272", "billing@clipboard", "support"],
                    "documents": ["license", "background", "ID", "certification"]
                }
                
                question_key = next((k for k in success_indicators.keys() if k in question.lower()), None)
                expected_terms = success_indicators.get(question_key, [])
                
                found_terms = [term for term in expected_terms if term.lower() in response.lower()]
                
                if found_terms:
                    print(f"✅ SUCCESS - Found: {found_terms}")
                    results.append(True)
                else:
                    print(f"⚠️  PARTIAL - Response: {response[:100]}...")
                    results.append(False)
            else:
                print(f"❌ ERROR: {response}")
                results.append(False)
                
        except Exception as e:
            print(f"❌ EXCEPTION: {e}")
            results.append(False)
    
    success_rate = sum(results) / len(results) * 100
    print(f"\n📊 SUCCESS RATE: {success_rate:.1f}% ({sum(results)}/{len(results)})")
    
    return success_rate >= 75  # Consider 75%+ success as passing

if __name__ == "__main__":
    success = asyncio.run(test_specific_questions())
    print(f"\n{'🎉 COMPREHENSIVE TEST PASSED!' if success else '❌ COMPREHENSIVE TEST NEEDS IMPROVEMENT'}")