#!/usr/bin/env python3
"""
Simple routing test to validate the fixes
"""
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.bi_service import should_use_bi_service
from dotenv import load_dotenv

load_dotenv()

def test_routing_logic():
    """Test the routing logic for different question types"""
    print(f"🧪 Testing Routing Logic")
    print(f"USE_BI_SERVICE: {os.getenv('USE_BI_SERVICE', 'Not set')}")
    
    # Test cases with expected routing
    test_cases = [
        # Should route to SQL_GENERATION (return False from should_use_bi_service)
        {"question": "What's our overall chat volume today?", "expected_sql": True},
        {"question": "Show me weekly ticket volume", "expected_sql": True},
        {"question": "How many tickets yesterday?", "expected_sql": True},
        {"question": "What's our QA score this week?", "expected_sql": True},
        {"question": "How is Christine Presto performing?", "expected_sql": True},
        {"question": "Show me AHT trends", "expected_sql": True},
        {"question": "CSAT metrics this month", "expected_sql": True},
        {"question": "Auditor productivity", "expected_sql": True},
        
        # Should route to BI_SERVICE (return True from should_use_bi_service)
        {"question": "How do I improve AHT?", "expected_sql": False},
        {"question": "What are best practices?", "expected_sql": False},
        {"question": "Explain quality metrics", "expected_sql": False},
        {"question": "Help me understand FCR", "expected_sql": False},
    ]
    
    print(f"\n📊 Testing {len(test_cases)} questions...")
    
    correct = 0
    total = len(test_cases)
    
    for i, test_case in enumerate(test_cases, 1):
        question = test_case["question"]
        expected_sql = test_case["expected_sql"]
        
        # Test routing
        should_use_bi = should_use_bi_service(question)
        routes_to_sql = not should_use_bi  # If should_use_bi is False, it routes to SQL
        
        is_correct = routes_to_sql == expected_sql
        if is_correct:
            correct += 1
        
        status = "✅" if is_correct else "❌"
        expected_route = "SQL" if expected_sql else "BI"
        actual_route = "SQL" if routes_to_sql else "BI"
        
        print(f"{status} {i:2d}. '{question}'")
        print(f"      Expected: {expected_route}, Got: {actual_route}")
        
        if not is_correct:
            print(f"      should_use_bi_service returned: {should_use_bi}")
    
    print(f"\n📈 Results:")
    print(f"   Correct: {correct}/{total} ({correct/total*100:.1f}%)")
    print(f"   Incorrect: {total-correct}/{total}")
    
    if correct == total:
        print(f"🎉 All routing tests passed! The fix is working correctly.")
    else:
        print(f"⚠️ Some routing tests failed. Need to adjust the logic.")
    
    return correct == total

if __name__ == "__main__":
    success = test_routing_logic()
    if success:
        print(f"\n✅ Routing logic is working correctly!")
        print(f"🚀 Ready for E2E testing with real SQL generation.")
    else:
        print(f"\n❌ Routing logic needs adjustment.")
        print(f"🔧 Review the should_use_bi_service function patterns.")