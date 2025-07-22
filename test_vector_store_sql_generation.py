#!/usr/bin/env python3
"""
Test script to validate BI Slack bot's ability to generate SQL queries using the uploaded vector store context.
Tests specific scenarios for organizational metrics, team performance, and quality metrics.
"""

import asyncio
import os
import sys
import json
import time
from typing import Dict, List, Tuple
from datetime import datetime

# Add the app directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from dotenv import load_dotenv

# Import necessary modules
from app.llm_orchestrator import handle_question
from app.bi_service import process_with_bi_service, should_use_bi_service, get_bi_service_status
from app.conversation_manager import get_conversation_context, update_conversation_context

load_dotenv()

# Test configuration
ASSISTANT_ID = os.getenv("ASSISTANT_ID", "")
USE_ASSISTANT_API = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"
USE_BI_SERVICE = os.getenv("USE_BI_SERVICE", "false").lower() == "true"

# Test cases with expected patterns
TEST_SCENARIOS = [
    {
        "name": "ORGANIZATIONAL METRICS TEST",
        "question": "What's our overall chat volume today?",
        "expected_patterns": [
            "FCT_ZENDESK__MQR_TICKETS",
            "GROUP_ID = '17837476387479'",
            "WHERE",
            "TODAY()" or "CURRENT_DATE" or "DATE_TRUNC('day'"
        ],
        "expected_description": "Should generate SQL using FCT_ZENDESK__MQR_TICKETS with GROUP_ID = '17837476387479' and proper date filters"
    },
    {
        "name": "TEAM LEAD PERFORMANCE TEST", 
        "question": "How is Christine Presto performing this week?",
        "expected_patterns": [
            "Christine Presto",
            "supervisor",
            "JOIN",
            "WEEK" or "DATE_TRUNC('week'"
        ],
        "expected_description": "Should generate query with supervisor filtering and multi-metric joins"
    },
    {
        "name": "AGENT PERFORMANCE TEST",
        "question": "Show me John Smith's AHT performance",
        "expected_patterns": [
            "John Smith",
            "USER_NAME",
            "AHT" or "AVERAGE_HANDLE_TIME",
            "Dim Zendesk Users" or "DIM_ZENDESK_USERS"
        ],
        "expected_description": "Should use 'Dim Zendesk Users - Assignee'.'USER_NAME' filtering"
    },
    {
        "name": "QUALITY METRICS TEST",
        "question": "What's our QA score this week?",
        "expected_patterns": [
            "Klaus" or "KLAUS",
            "scorecard",
            "WEEK" or "DATE_TRUNC('week'",
            "QA" or "quality"
        ],
        "expected_description": "Should use Klaus CTE with scorecard filtering"
    },
    {
        "name": "AUDITOR PERFORMANCE TEST", 
        "question": "How many audits did Sarah complete?",
        "expected_patterns": [
            "Sarah",
            "REVIEWER_NAME",
            "COUNT(DISTINCT REVIEW_ID)" or "COUNT(DISTINCT",
            "audit" or "AUDIT"
        ],
        "expected_description": "Should use REVIEWER_NAME filtering and COUNT(DISTINCT REVIEW_ID)"
    }
]

class VectorStoreTestResults:
    def __init__(self):
        self.results = []
        self.successful_tests = 0
        self.failed_tests = 0
        self.incomplete_tests = 0
        
    def add_result(self, test_name: str, question: str, response: str, response_type: str, 
                   patterns_found: List[str], patterns_missing: List[str], 
                   vector_store_accessed: bool, error: str = None):
        """Add test result"""
        result = {
            "test_name": test_name,
            "question": question, 
            "response": response,
            "response_type": response_type,
            "patterns_found": patterns_found,
            "patterns_missing": patterns_missing,
            "vector_store_accessed": vector_store_accessed,
            "error": error,
            "timestamp": datetime.now().isoformat(),
            "success": len(patterns_missing) == 0 and error is None
        }
        
        self.results.append(result)
        
        if result["success"]:
            self.successful_tests += 1
        elif error:
            self.failed_tests += 1
        else:
            self.incomplete_tests += 1
    
    def get_summary(self) -> Dict:
        """Get test summary"""
        return {
            "total_tests": len(self.results),
            "successful": self.successful_tests,
            "failed": self.failed_tests, 
            "incomplete": self.incomplete_tests,
            "success_rate": f"{(self.successful_tests / len(self.results) * 100):.1f}%" if self.results else "0%"
        }

def check_sql_patterns(sql_text: str, expected_patterns: List[str]) -> Tuple[List[str], List[str]]:
    """Check if SQL contains expected patterns"""
    sql_upper = sql_text.upper()
    patterns_found = []
    patterns_missing = []
    
    for pattern in expected_patterns:
        pattern_variations = []
        if isinstance(pattern, str):
            pattern_variations = [pattern]
        else:
            pattern_variations = pattern if isinstance(pattern, list) else [pattern]
            
        found = False
        for variation in pattern_variations:
            if variation.upper() in sql_upper:
                patterns_found.append(variation)
                found = True
                break
                
        if not found:
            patterns_missing.append(str(pattern_variations[0]))
    
    return patterns_found, patterns_missing

def check_vector_store_access(response: str, sql: str) -> bool:
    """Check if response indicates vector store was accessed"""
    # Look for indicators that vector store context was used
    indicators = [
        "FCT_ZENDESK__MQR_TICKETS",  # Specific table names from knowledge base
        "GROUP_ID = '17837476387479'",  # Specific org filter
        "Klaus",  # QA system
        "REVIEWER_NAME",  # Auditor fields
        "Dim Zendesk Users",  # Specific dimension tables
        "-- Based on",  # SQL comments indicating context
        "Using knowledge base",  # Explicit mentions
        "WOPS_",  # Table prefixes
        "AHT" # Performance metrics
    ]
    
    combined_text = (response + " " + sql).upper()
    return any(indicator.upper() in combined_text for indicator in indicators)

async def test_sql_generation_scenario(scenario: Dict, test_user_id: str = "test_user") -> Dict:
    """Test a single SQL generation scenario"""
    print(f"\n{'='*60}")
    print(f"🧪 Testing: {scenario['name']}")
    print(f"❓ Question: {scenario['question']}")
    print(f"{'='*60}")
    
    test_channel_id = "test_channel"
    start_time = time.time()
    
    try:
        # Check if we should use BI Service
        if should_use_bi_service(scenario['question']):
            print(f"🔧 Routing to BI Service")
            response, response_type = await process_with_bi_service(
                scenario['question'], test_user_id, test_channel_id
            )
            sql_content = response  # BI Service response is the AI answer
        else:
            # Use Assistant API for SQL generation
            if USE_ASSISTANT_API and ASSISTANT_ID:
                print(f"🤖 Using Assistant API with vector store")
                response, response_type = await handle_question(
                    scenario['question'], test_user_id, test_channel_id, ASSISTANT_ID
                )
            else:
                print(f"❌ No Assistant API configured")
                return {
                    "error": "Assistant API not configured",
                    "response": "",
                    "response_type": "error"
                }
            
            sql_content = response
        
        execution_time = time.time() - start_time
        
        print(f"⏱️  Response time: {execution_time:.2f}s")
        print(f"📊 Response type: {response_type}")
        print(f"📝 Response preview: {response[:200]}...")
        
        # Check for expected SQL patterns
        patterns_found, patterns_missing = check_sql_patterns(sql_content, scenario['expected_patterns'])
        
        # Check if vector store was accessed
        vector_store_accessed = check_vector_store_access(response, sql_content)
        
        print(f"✅ Patterns found: {patterns_found}")
        print(f"❌ Patterns missing: {patterns_missing}")
        print(f"🗂️  Vector store accessed: {'Yes' if vector_store_accessed else 'No'}")
        
        return {
            "response": response,
            "response_type": response_type,
            "patterns_found": patterns_found,
            "patterns_missing": patterns_missing,
            "vector_store_accessed": vector_store_accessed,
            "execution_time": execution_time,
            "error": None
        }
        
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"💥 Error: {str(e)}")
        return {
            "response": "",
            "response_type": "error",
            "patterns_found": [],
            "patterns_missing": scenario['expected_patterns'],
            "vector_store_accessed": False,
            "execution_time": execution_time,
            "error": str(e)
        }

async def run_all_tests():
    """Run all test scenarios"""
    print("🚀 Starting Vector Store SQL Generation Tests")
    print(f"📋 Assistant ID: {ASSISTANT_ID}")
    print(f"🤖 Assistant API enabled: {USE_ASSISTANT_API}")
    print(f"🔧 BI Service enabled: {USE_BI_SERVICE}")
    
    # Check BI Service status
    bi_status = get_bi_service_status()
    print(f"📊 BI Service status: {json.dumps(bi_status, indent=2)}")
    
    results = VectorStoreTestResults()
    test_user_id = f"test_user_{int(time.time())}"
    
    for i, scenario in enumerate(TEST_SCENARIOS, 1):
        print(f"\n🔄 Running test {i}/{len(TEST_SCENARIOS)}")
        
        # Run the test
        test_result = await test_sql_generation_scenario(scenario, test_user_id)
        
        # Record results
        results.add_result(
            test_name=scenario['name'],
            question=scenario['question'], 
            response=test_result['response'],
            response_type=test_result['response_type'],
            patterns_found=test_result['patterns_found'],
            patterns_missing=test_result['patterns_missing'],
            vector_store_accessed=test_result['vector_store_accessed'],
            error=test_result['error']
        )
        
        # Small delay between tests
        await asyncio.sleep(1)
    
    return results

def generate_test_report(results: VectorStoreTestResults) -> str:
    """Generate comprehensive test report"""
    summary = results.get_summary()
    
    report = f"""
# Vector Store SQL Generation Test Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Executive Summary
- **Total Tests**: {summary['total_tests']}  
- **Successful**: {summary['successful']} 
- **Failed**: {summary['failed']}
- **Incomplete**: {summary['incomplete']}
- **Success Rate**: {summary['success_rate']}

## Test Results

"""
    
    for result in results.results:
        status = "✅ PASS" if result['success'] else ("❌ FAIL" if result['error'] else "⚠️ INCOMPLETE")
        
        report += f"""### {result['test_name']} - {status}

**Question**: {result['question']}
**Response Type**: {result['response_type']}
**Vector Store Accessed**: {'✅ Yes' if result['vector_store_accessed'] else '❌ No'}
**Patterns Found**: {', '.join(result['patterns_found']) if result['patterns_found'] else 'None'}
**Patterns Missing**: {', '.join(result['patterns_missing']) if result['patterns_missing'] else 'None'}

"""
        
        if result['error']:
            report += f"**Error**: {result['error']}\n\n"
        else:
            # Show first 500 chars of response
            preview = result['response'][:500] + "..." if len(result['response']) > 500 else result['response']
            report += f"**Response Preview**:\n```\n{preview}\n```\n\n"
    
    # Analysis section
    report += """## Analysis

### Which queries worked correctly?
"""
    successful_tests = [r for r in results.results if r['success']]
    for test in successful_tests:
        report += f"- ✅ **{test['test_name']}**: All expected patterns found\n"
    
    report += """\n### Which ones failed or were incomplete?
"""
    failed_tests = [r for r in results.results if not r['success']]
    for test in failed_tests:
        if test['error']:
            report += f"- ❌ **{test['test_name']}**: Error - {test['error']}\n"
        else:
            report += f"- ⚠️ **{test['test_name']}**: Missing patterns: {', '.join(test['patterns_missing'])}\n"
    
    report += """\n### Vector Store Access Analysis
"""
    vector_accessed = len([r for r in results.results if r['vector_store_accessed']])
    report += f"- **Tests with vector store access**: {vector_accessed}/{len(results.results)}\n"
    
    if vector_accessed < len(results.results):
        report += "- ⚠️ Some tests may not be accessing the comprehensive knowledge base properly\n"
    else:
        report += "- ✅ All tests appear to be accessing vector store context\n"
    
    report += """\n### Recommendations

1. **For failed SQL generation**: Review vector store content for missing table/column patterns
2. **For missing patterns**: Update knowledge base with more specific SQL examples
3. **For vector store access issues**: Check Assistant API configuration and vector store setup
4. **For response quality**: Consider adding more detailed business context to prompts

"""
    
    return report

async def main():
    """Main test runner"""
    try:
        # Run all tests
        results = await run_all_tests()
        
        # Generate report
        report = generate_test_report(results)
        
        # Save report to file
        report_file = f"vector_store_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(report_file, 'w') as f:
            f.write(report)
        
        # Print summary
        print("\n" + "="*80)
        print("📊 TEST RESULTS SUMMARY")
        print("="*80)
        
        summary = results.get_summary()
        print(f"Total Tests: {summary['total_tests']}")
        print(f"Successful: {summary['successful']} ✅")
        print(f"Failed: {summary['failed']} ❌")
        print(f"Incomplete: {summary['incomplete']} ⚠️")
        print(f"Success Rate: {summary['success_rate']}")
        
        print(f"\n📄 Detailed report saved to: {report_file}")
        
        # Quick analysis
        vector_accessed = len([r for r in results.results if r['vector_store_accessed']])
        print(f"\n🗂️ Vector Store Access: {vector_accessed}/{len(results.results)} tests")
        
        if summary['successful'] == len(TEST_SCENARIOS):
            print("🎉 All tests passed! Vector store context is working correctly.")
        elif summary['failed'] > 0:
            print("⚠️ Some tests failed. Check error messages and Assistant API configuration.")
        else:
            print("⚠️ Tests completed but some expected patterns were missing. Review knowledge base content.")
        
    except KeyboardInterrupt:
        print("\n❌ Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Test runner error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())