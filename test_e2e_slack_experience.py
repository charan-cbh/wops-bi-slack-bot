#!/usr/bin/env python3
"""
E2E Test for Complete Slack Bot Experience
Tests the full pipeline: Question -> Routing -> SQL Generation -> Data Execution -> Formatted Response
"""
import os
import sys
import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.bi_service import should_use_bi_service, process_with_bi_service
from app.llm_orchestrator import handle_question, generate_sql_intelligently
from app.snowflake_runner import run_query, format_result_for_slack
from app.slack_handler import handle_app_mention
from app.conversation_manager import get_conversation_context, update_conversation_context
from dotenv import load_dotenv

load_dotenv()

class SlackE2ESimulator:
    """Simulates complete Slack bot experience end-to-end"""
    
    def __init__(self):
        self.test_user_id = "U12345TEST"
        self.test_channel_id = "C12345TEST"
        self.assistant_id = os.getenv("ASSISTANT_ID", "")
        self.use_assistant_api = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"
        self.results = []
        
    async def simulate_user_question(self, question: str) -> Dict[str, Any]:
        """Simulate complete user question flow like in Slack"""
        print(f"\n{'='*80}")
        print(f"🧪 TESTING: '{question}'")
        print(f"{'='*80}")
        
        start_time = time.time()
        result = {
            "question": question,
            "timestamp": datetime.now().isoformat(),
            "routing_decision": None,
            "sql_generated": None,
            "query_executed": False,
            "data_returned": False,
            "final_response": None,
            "error": None,
            "processing_time": 0,
            "steps": []
        }
        
        try:
            # Step 1: Routing Decision
            print(f"\n📍 Step 1: Routing Decision")
            should_use_bi = should_use_bi_service(question)
            result["routing_decision"] = "BI_SERVICE" if should_use_bi else "SQL_GENERATION"
            result["steps"].append(f"Routed to: {result['routing_decision']}")
            print(f"   Decision: {result['routing_decision']}")
            
            if should_use_bi:
                # BI Service Flow (Conversational)
                print(f"\n📍 Step 2: BI Service Processing")
                try:
                    bi_response, bi_type = await process_with_bi_service(
                        question, self.test_user_id, self.test_channel_id
                    )
                    result["final_response"] = bi_response
                    result["steps"].append(f"BI Service Response: {bi_type}")
                    print(f"   BI Response Type: {bi_type}")
                    print(f"   BI Response: {bi_response[:200]}...")
                    
                except Exception as e:
                    result["error"] = f"BI Service failed: {str(e)}"
                    result["steps"].append(f"BI Service Error: {str(e)}")
                    print(f"   ❌ BI Service Error: {e}")
                    
            else:
                # SQL Generation Flow (Data Queries)
                await self._handle_sql_generation_flow(question, result)
                
        except Exception as e:
            result["error"] = str(e)
            result["steps"].append(f"Critical Error: {str(e)}")
            print(f"❌ Critical Error: {e}")
            
        result["processing_time"] = time.time() - start_time
        print(f"\n⏱️ Total processing time: {result['processing_time']:.2f}s")
        
        return result
    
    async def _handle_sql_generation_flow(self, question: str, result: Dict[str, Any]):
        """Handle the SQL generation and execution flow"""
        print(f"\n📍 Step 2: SQL Generation")
        
        if self.use_assistant_api and self.assistant_id:
            # Use Assistant API flow
            print(f"   Using Assistant API (ID: {self.assistant_id})")
            try:
                response, response_type = await handle_question(
                    question, self.test_user_id, self.test_channel_id, self.assistant_id
                )
                
                result["steps"].append(f"Assistant API Response Type: {response_type}")
                print(f"   Response Type: {response_type}")
                
                if response_type == "sql_with_data":
                    result["query_executed"] = True
                    result["data_returned"] = True
                    result["final_response"] = response
                    result["steps"].append("SQL executed with data returned")
                    print(f"   ✅ SQL executed with data")
                    print(f"   Response: {response[:300]}...")
                    
                elif response_type == "conversational":
                    result["final_response"] = response
                    result["steps"].append("Conversational response provided")
                    print(f"   💬 Conversational response")
                    print(f"   Response: {response[:200]}...")
                    
                else:
                    result["error"] = f"Unexpected response type: {response_type}"
                    result["steps"].append(f"Unexpected response type: {response_type}")
                    print(f"   ⚠️ Unexpected response type: {response_type}")
                    
            except Exception as e:
                result["error"] = f"Assistant API failed: {str(e)}"
                result["steps"].append(f"Assistant API Error: {str(e)}")
                print(f"   ❌ Assistant API Error: {e}")
                
        else:
            # Fallback to direct SQL generation
            print(f"   Using direct SQL generation")
            try:
                sql_result = await generate_sql_intelligently(
                    question, self.test_user_id, self.test_channel_id
                )
                
                if sql_result.get("success") and sql_result.get("sql"):
                    result["sql_generated"] = sql_result["sql"]
                    result["steps"].append("SQL generated successfully")
                    print(f"   ✅ SQL Generated")
                    print(f"   SQL: {sql_result['sql'][:200]}...")
                    
                    # Step 3: Execute SQL
                    await self._execute_sql_query(sql_result["sql"], result)
                    
                else:
                    result["error"] = f"SQL generation failed: {sql_result.get('error', 'Unknown error')}"
                    result["steps"].append(f"SQL generation failed: {sql_result.get('error')}")
                    print(f"   ❌ SQL Generation Failed: {sql_result.get('error')}")
                    
            except Exception as e:
                result["error"] = f"Direct SQL generation failed: {str(e)}"
                result["steps"].append(f"Direct SQL Error: {str(e)}")
                print(f"   ❌ Direct SQL Error: {e}")
    
    async def _execute_sql_query(self, sql: str, result: Dict[str, Any]):
        """Execute the SQL query and format results"""
        print(f"\n📍 Step 3: SQL Execution")
        
        try:
            # Skip connection test for now
            result["steps"].append("Skipping connection test")
            print(f"   ⏭️ Skipping database connection test")
            
            # Execute query
            query_result = await run_query(sql)
            
            if query_result.get("success"):
                result["query_executed"] = True
                result["data_returned"] = True
                
                # Format results for Slack
                formatted_response = format_result_for_slack(query_result, sql)
                result["final_response"] = formatted_response
                result["steps"].append(f"Query executed: {len(query_result.get('data', []))} rows returned")
                
                print(f"   ✅ Query Executed Successfully")
                print(f"   Rows returned: {len(query_result.get('data', []))}")
                print(f"   Formatted response: {formatted_response[:300]}...")
                
            else:
                result["error"] = f"Query execution failed: {query_result.get('error', 'Unknown error')}"
                result["steps"].append(f"Query execution failed: {query_result.get('error')}")
                print(f"   ❌ Query Failed: {query_result.get('error')}")
                
        except Exception as e:
            result["error"] = f"SQL execution error: {str(e)}"
            result["steps"].append(f"SQL execution error: {str(e)}")
            print(f"   ❌ SQL Execution Error: {e}")

    async def run_comprehensive_test_suite(self):
        """Run comprehensive test suite covering all major scenarios"""
        print(f"🚀 Starting Comprehensive E2E Test Suite")
        print(f"Assistant API: {'Enabled' if self.use_assistant_api else 'Disabled'}")
        print(f"Assistant ID: {self.assistant_id or 'Not configured'}")
        
        # Test cases covering all major scenarios
        test_cases = [
            # Organizational Metrics
            {
                "category": "Organizational Metrics",
                "questions": [
                    "What's our overall chat volume today?",
                    "Show me weekly ticket volume",
                    "How many tickets did we handle yesterday?",
                    "Total voice volume this month"
                ]
            },
            
            # Quality Metrics  
            {
                "category": "Quality Metrics",
                "questions": [
                    "What's our QA score this week?",
                    "Show me CSAT trends",
                    "What's our FCR rate?",
                    "Customer satisfaction this month"
                ]
            },
            
            # Performance Metrics
            {
                "category": "Performance Metrics", 
                "questions": [
                    "What's our AHT for chat?",
                    "Show me response time trends",
                    "Average handle time this week",
                    "Performance metrics comparison"
                ]
            },
            
            # Team Lead Performance
            {
                "category": "Team Lead Performance",
                "questions": [
                    "How is Christine Presto performing?",
                    "Show me Joan Mallari's team metrics",
                    "Team lead performance this week",
                    "Gian Gabrillo's AHT trends"
                ]
            },
            
            # Agent Performance
            {
                "category": "Agent Performance",
                "questions": [
                    "Show me John Smith's performance",
                    "How is agent Sarah performing?",
                    "Individual AHT for Mike Johnson",
                    "Agent CSAT scores"
                ]
            },
            
            # Auditor Performance
            {
                "category": "Auditor Performance", 
                "questions": [
                    "How many audits did Sarah complete?",
                    "Show me auditor productivity",
                    "QA audit volume this week",
                    "Dispute acceptance rates"
                ]
            },
            
            # Conversational Questions (should go to BI Service)
            {
                "category": "Conversational Questions",
                "questions": [
                    "How do I improve AHT?",
                    "What are best practices for CSAT?",
                    "Explain quality metrics to me",
                    "Help me understand FCR calculation"
                ]
            }
        ]
        
        all_results = []
        
        for category_data in test_cases:
            category = category_data["category"]
            questions = category_data["questions"]
            
            print(f"\n🏷️ Testing Category: {category}")
            print(f"   Questions: {len(questions)}")
            
            category_results = []
            for question in questions:
                result = await self.simulate_user_question(question)
                result["category"] = category
                category_results.append(result)
                all_results.append(result)
                
                # Brief pause between questions
                await asyncio.sleep(1)
            
            # Category summary
            successful = sum(1 for r in category_results if not r.get("error"))
            with_data = sum(1 for r in category_results if r.get("data_returned"))
            
            print(f"\n📊 {category} Summary:")
            print(f"   ✅ Successful: {successful}/{len(questions)}")
            print(f"   📈 With Data: {with_data}/{len(questions)}")
            
        # Final comprehensive report
        self._generate_final_report(all_results)
        
        return all_results
    
    def _generate_final_report(self, all_results: list):
        """Generate comprehensive test report"""
        print(f"\n{'='*80}")
        print(f"📋 FINAL E2E TEST REPORT")
        print(f"{'='*80}")
        
        total_tests = len(all_results)
        successful_tests = sum(1 for r in all_results if not r.get("error"))
        sql_routed = sum(1 for r in all_results if r.get("routing_decision") == "SQL_GENERATION")
        bi_routed = sum(1 for r in all_results if r.get("routing_decision") == "BI_SERVICE")
        queries_executed = sum(1 for r in all_results if r.get("query_executed"))
        data_returned = sum(1 for r in all_results if r.get("data_returned"))
        
        print(f"📈 Overall Statistics:")
        print(f"   Total Tests: {total_tests}")
        print(f"   Successful: {successful_tests} ({successful_tests/total_tests*100:.1f}%)")
        print(f"   Failed: {total_tests - successful_tests}")
        
        print(f"\n🚦 Routing Statistics:")
        print(f"   SQL Generation: {sql_routed} ({sql_routed/total_tests*100:.1f}%)")
        print(f"   BI Service: {bi_routed} ({bi_routed/total_tests*100:.1f}%)")
        
        print(f"\n💾 Data Execution:")
        print(f"   Queries Executed: {queries_executed}")
        print(f"   Data Returned: {data_returned}")
        
        # Category breakdown
        categories = {}
        for result in all_results:
            cat = result.get("category", "Unknown")
            if cat not in categories:
                categories[cat] = {"total": 0, "successful": 0, "with_data": 0}
            categories[cat]["total"] += 1
            if not result.get("error"):
                categories[cat]["successful"] += 1
            if result.get("data_returned"):
                categories[cat]["with_data"] += 1
        
        print(f"\n📂 By Category:")
        for cat, stats in categories.items():
            success_rate = stats["successful"] / stats["total"] * 100
            data_rate = stats["with_data"] / stats["total"] * 100
            print(f"   {cat}:")
            print(f"      Success: {stats['successful']}/{stats['total']} ({success_rate:.1f}%)")
            print(f"      Data: {stats['with_data']}/{stats['total']} ({data_rate:.1f}%)")
        
        # Error analysis
        errors = [r for r in all_results if r.get("error")]
        if errors:
            print(f"\n❌ Error Analysis:")
            error_types = {}
            for error_result in errors:
                error = error_result["error"]
                error_type = error.split(":")[0] if ":" in error else "Unknown"
                if error_type not in error_types:
                    error_types[error_type] = []
                error_types[error_type].append(error_result["question"])
            
            for error_type, questions in error_types.items():
                print(f"   {error_type}: {len(questions)} errors")
                for q in questions[:3]:  # Show first 3
                    print(f"      - {q}")
                if len(questions) > 3:
                    print(f"      ... and {len(questions) - 3} more")
        
        # Performance analysis
        avg_time = sum(r["processing_time"] for r in all_results) / len(all_results)
        max_time = max(r["processing_time"] for r in all_results)
        min_time = min(r["processing_time"] for r in all_results)
        
        print(f"\n⏱️ Performance Analysis:")
        print(f"   Average Response Time: {avg_time:.2f}s")
        print(f"   Fastest Response: {min_time:.2f}s")
        print(f"   Slowest Response: {max_time:.2f}s")
        
        # Save detailed results
        report_file = f"e2e_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(all_results, f, indent=2)
        print(f"\n💾 Detailed results saved to: {report_file}")

async def main():
    """Run the E2E test suite"""
    simulator = SlackE2ESimulator()
    
    print(f"🧪 Slack Bot E2E Test Suite")
    print(f"Time: {datetime.now().isoformat()}")
    print(f"Testing complete user experience pipeline...")
    
    try:
        results = await simulator.run_comprehensive_test_suite()
        
        print(f"\n✅ E2E Test Suite Completed!")
        print(f"📊 Total tests run: {len(results)}")
        
    except Exception as e:
        print(f"\n❌ E2E Test Suite Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())