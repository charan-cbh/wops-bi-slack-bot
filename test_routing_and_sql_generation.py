#!/usr/bin/env python3
"""
Focused test for routing and SQL generation
Tests the key improvements: Question -> Routing -> SQL Generation
"""
import os
import sys
import asyncio
import time
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.bi_service import should_use_bi_service
from app.llm_orchestrator import handle_question, generate_sql_intelligently
from dotenv import load_dotenv

load_dotenv()

class RoutingSQLTest:
    """Test routing and SQL generation specifically"""
    
    def __init__(self):
        self.test_user_id = "U12345TEST"
        self.test_channel_id = "C12345TEST"
        self.assistant_id = os.getenv("ASSISTANT_ID", "")
        self.use_assistant_api = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"
        
    async def test_question(self, question: str, expected_routing: str = None):
        """Test a single question through routing and SQL generation"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: '{question}'")
        print(f"{'='*60}")
        
        start_time = time.time()
        
        # Step 1: Test Routing
        print(f"\n📍 Step 1: Routing Decision")
        should_use_bi = should_use_bi_service(question)
        routing_decision = "BI_SERVICE" if should_use_bi else "SQL_GENERATION"
        print(f"   Decision: {routing_decision}")
        
        if expected_routing:
            if routing_decision == expected_routing:
                print(f"   ✅ Routing matches expected: {expected_routing}")
            else:
                print(f"   ❌ Routing mismatch! Expected: {expected_routing}, Got: {routing_decision}")
        
        # Step 2: Test SQL Generation (if routed to SQL)
        if routing_decision == "SQL_GENERATION":
            print(f"\n📍 Step 2: SQL Generation Test")
            
            if self.use_assistant_api and self.assistant_id:
                print(f"   Using Assistant API...")
                try:
                    response, response_type = await handle_question(
                        question, self.test_user_id, self.test_channel_id, self.assistant_id
                    )
                    
                    print(f"   Response Type: {response_type}")
                    
                    if response_type == "sql_with_data":
                        print(f"   ✅ Assistant API generated SQL with data")
                        # Extract SQL if present in response
                        if "```sql" in response:
                            sql_start = response.find("```sql") + 6
                            sql_end = response.find("```", sql_start)
                            if sql_end > sql_start:
                                sql = response[sql_start:sql_end].strip()
                                print(f"   SQL Preview: {sql[:200]}...")
                                return {"success": True, "routing": routing_decision, "sql": sql, "response": response}
                        
                        print(f"   Response Preview: {response[:300]}...")
                        return {"success": True, "routing": routing_decision, "response": response}
                        
                    elif response_type == "conversational":
                        print(f"   💬 Assistant provided conversational response")
                        print(f"   Response Preview: {response[:200]}...")
                        return {"success": True, "routing": routing_decision, "response": response}
                        
                    else:
                        print(f"   ⚠️ Unexpected response type: {response_type}")
                        return {"success": False, "error": f"Unexpected response type: {response_type}"}
                        
                except Exception as e:
                    print(f"   ❌ Assistant API Error: {e}")
                    return {"success": False, "error": str(e)}
            
            else:
                print(f"   Using direct SQL generation...")
                try:
                    sql_result = await generate_sql_intelligently(
                        question, self.test_user_id, self.test_channel_id
                    )
                    
                    if sql_result.get("success") and sql_result.get("sql"):
                        print(f"   ✅ SQL Generated Successfully")
                        print(f"   SQL Preview: {sql_result['sql'][:200]}...")
                        return {"success": True, "routing": routing_decision, "sql": sql_result["sql"]}
                        
                    else:
                        print(f"   ❌ SQL Generation Failed: {sql_result.get('error', 'Unknown error')}")
                        return {"success": False, "error": sql_result.get('error', 'SQL generation failed')}
                        
                except Exception as e:
                    print(f"   ❌ Direct SQL Error: {e}")
                    return {"success": False, "error": str(e)}
        
        else:
            print(f"\n📍 Step 2: BI Service Response (Simulated)")
            print(f"   Would be routed to BI Service for conversational response")
            return {"success": True, "routing": routing_decision, "response": "BI Service would handle this"}
    
    async def run_focused_tests(self):
        """Run focused tests on key BI questions"""
        print(f"🚀 Running Focused Routing & SQL Generation Tests")
        print(f"Assistant API: {'Enabled' if self.use_assistant_api else 'Disabled'}")
        print(f"Assistant ID: {self.assistant_id or 'Not configured'}")
        
        # Key test cases with expected routing
        test_cases = [
            # These should route to SQL_GENERATION
            {"question": "What's our overall chat volume today?", "expected": "SQL_GENERATION"},
            {"question": "Show me weekly ticket volume", "expected": "SQL_GENERATION"},
            {"question": "What's our QA score this week?", "expected": "SQL_GENERATION"},
            {"question": "How is Christine Presto performing?", "expected": "SQL_GENERATION"},
            {"question": "Show me John Smith's AHT performance", "expected": "SQL_GENERATION"},
            {"question": "How many audits did Sarah complete?", "expected": "SQL_GENERATION"},
            {"question": "What's our CSAT this month?", "expected": "SQL_GENERATION"},
            {"question": "Total voice tickets yesterday", "expected": "SQL_GENERATION"},
            
            # These should route to BI_SERVICE
            {"question": "How do I improve AHT?", "expected": "BI_SERVICE"},
            {"question": "What are best practices for CSAT?", "expected": "BI_SERVICE"},
            {"question": "Explain quality metrics to me", "expected": "BI_SERVICE"},
            {"question": "Help me understand FCR calculation", "expected": "BI_SERVICE"},
        ]
        
        results = []
        successful_sql = 0
        correct_routing = 0
        
        for test_case in test_cases:
            question = test_case["question"]
            expected = test_case["expected"]
            
            result = await self.test_question(question, expected)
            result["question"] = question
            result["expected_routing"] = expected
            results.append(result)
            
            # Count successes
            if result.get("routing") == expected:
                correct_routing += 1
            
            if result.get("success") and expected == "SQL_GENERATION" and result.get("sql"):
                successful_sql += 1
            
            # Brief pause between tests
            await asyncio.sleep(1)
        
        # Summary Report
        print(f"\n{'='*80}")
        print(f"📋 FOCUSED TEST RESULTS")
        print(f"{'='*80}")
        
        total_tests = len(test_cases)
        sql_tests = sum(1 for tc in test_cases if tc["expected"] == "SQL_GENERATION")
        bi_tests = sum(1 for tc in test_cases if tc["expected"] == "BI_SERVICE")
        
        print(f"📊 Overall Statistics:")
        print(f"   Total Tests: {total_tests}")
        print(f"   SQL Expected: {sql_tests}")
        print(f"   BI Expected: {bi_tests}")
        print(f"   Correct Routing: {correct_routing}/{total_tests} ({correct_routing/total_tests*100:.1f}%)")
        print(f"   Successful SQL Generation: {successful_sql}/{sql_tests} ({successful_sql/sql_tests*100:.1f}%)" if sql_tests > 0 else "   No SQL tests")
        
        # Routing Analysis
        print(f"\n🚦 Routing Analysis:")
        routing_correct = {"SQL_GENERATION": 0, "BI_SERVICE": 0}
        routing_total = {"SQL_GENERATION": 0, "BI_SERVICE": 0}
        
        for test_case, result in zip(test_cases, results):
            expected = test_case["expected"]
            routing_total[expected] += 1
            if result.get("routing") == expected:
                routing_correct[expected] += 1
        
        for routing_type in ["SQL_GENERATION", "BI_SERVICE"]:
            if routing_total[routing_type] > 0:
                accuracy = routing_correct[routing_type] / routing_total[routing_type] * 100
                print(f"   {routing_type}: {routing_correct[routing_type]}/{routing_total[routing_type]} ({accuracy:.1f}%)")
        
        # Error Analysis
        errors = [r for r in results if not r.get("success")]
        if errors:
            print(f"\n❌ Error Analysis ({len(errors)} errors):")
            for i, error in enumerate(errors[:5]):  # Show first 5 errors
                print(f"   {i+1}. {error['question']}")
                print(f"      Error: {error.get('error', 'Unknown error')}")
        
        # Success Examples
        successes = [r for r in results if r.get("success") and r.get("sql")]
        if successes:
            print(f"\n✅ SQL Generation Examples:")
            for i, success in enumerate(successes[:3]):  # Show first 3 successes
                print(f"   {i+1}. {success['question']}")
                sql_preview = success['sql'][:100].replace('\n', ' ')
                print(f"      SQL: {sql_preview}...")
        
        return results

async def main():
    """Run the focused routing and SQL generation test"""
    tester = RoutingSQLTest()
    
    print(f"🧪 Routing & SQL Generation Test Suite")
    print(f"Time: {datetime.now().isoformat()}")
    print(f"Testing routing decisions and SQL generation...")
    
    try:
        results = await tester.run_focused_tests()
        
        print(f"\n✅ Focused Test Suite Completed!")
        print(f"📊 Total tests run: {len(results)}")
        
    except Exception as e:
        print(f"\n❌ Test Suite Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())