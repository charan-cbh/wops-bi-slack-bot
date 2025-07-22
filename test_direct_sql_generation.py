#!/usr/bin/env python3
"""
Test script to validate direct SQL generation using Assistant API with vector store context.
This bypasses BI Service to test the core SQL generation capabilities.
"""

import asyncio
import os
import sys
import json
import time
from datetime import datetime

# Add the app directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from dotenv import load_dotenv

# Import necessary modules for direct SQL generation
from app.llm_orchestrator import handle_question, generate_sql_intelligently
from app.conversation_manager import get_conversation_context, update_conversation_context
from app.sql_generator import extract_sql_from_response

load_dotenv()

# Test configuration
ASSISTANT_ID = os.getenv("ASSISTANT_ID", "")
USE_ASSISTANT_API = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"

async def test_direct_sql_generation(question: str, test_user_id: str = "test_user") -> dict:
    """Test direct SQL generation bypassing BI Service"""
    print(f"\n{'='*60}")
    print(f"🧪 Testing Direct SQL Generation")
    print(f"❓ Question: {question}")
    print(f"{'='*60}")
    
    test_channel_id = "test_channel"
    start_time = time.time()
    
    try:
        # Force SQL generation by temporarily disabling BI Service
        original_bi_service = os.environ.get("USE_BI_SERVICE", "false")
        os.environ["USE_BI_SERVICE"] = "false"
        
        if USE_ASSISTANT_API and ASSISTANT_ID:
            print(f"🤖 Using Assistant API directly for SQL generation")
            response, response_type = await handle_question(
                question, test_user_id, test_channel_id, ASSISTANT_ID
            )
            
            # Try to extract SQL from response
            if response_type == 'sql':
                extracted_sql = extract_sql_from_response(response)
                print(f"📝 Generated SQL:\n{extracted_sql}")
            else:
                print(f"⚠️ No SQL generated, response type: {response_type}")
                extracted_sql = response
            
        else:
            print(f"❌ No Assistant API configured")
            return {"error": "Assistant API not configured"}
        
        # Restore BI Service setting
        os.environ["USE_BI_SERVICE"] = original_bi_service
        
        execution_time = time.time() - start_time
        print(f"⏱️ Response time: {execution_time:.2f}s")
        
        return {
            "response": response,
            "response_type": response_type, 
            "extracted_sql": extracted_sql if response_type == 'sql' else None,
            "execution_time": execution_time,
            "error": None
        }
        
    except Exception as e:
        # Restore BI Service setting
        os.environ["USE_BI_SERVICE"] = original_bi_service
        
        execution_time = time.time() - start_time
        print(f"💥 Error: {str(e)}")
        return {
            "response": "",
            "response_type": "error",
            "extracted_sql": None,
            "execution_time": execution_time,
            "error": str(e)
        }

async def test_intelligent_sql_generation(question: str, test_user_id: str = "test_user") -> dict:
    """Test intelligent SQL generation function directly"""
    print(f"\n{'='*60}")
    print(f"🧠 Testing Intelligent SQL Generation")
    print(f"❓ Question: {question}")
    print(f"{'='*60}")
    
    test_channel_id = "test_channel"
    start_time = time.time()
    
    try:
        if USE_ASSISTANT_API and ASSISTANT_ID:
            print(f"🤖 Using intelligent SQL generation")
            sql_result = await generate_sql_intelligently(
                question, test_user_id, test_channel_id, ASSISTANT_ID
            )
            
            print(f"📝 Generated SQL:\n{sql_result}")
            
        else:
            print(f"❌ No Assistant API configured")
            return {"error": "Assistant API not configured"}
        
        execution_time = time.time() - start_time
        print(f"⏱️ Response time: {execution_time:.2f}s")
        
        return {
            "sql": sql_result,
            "execution_time": execution_time,
            "error": None
        }
        
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"💥 Error: {str(e)}")
        return {
            "sql": "",
            "execution_time": execution_time,
            "error": str(e)
        }

async def main():
    """Run focused SQL generation tests"""
    print("🚀 Starting Direct SQL Generation Tests")
    print(f"📋 Assistant ID: {ASSISTANT_ID}")
    print(f"🤖 Assistant API enabled: {USE_ASSISTANT_API}")
    
    test_questions = [
        "What's our overall chat volume today?",
        "How is Christine Presto performing this week?", 
        "Show me John Smith's AHT performance",
        "What's our QA score this week?",
        "How many audits did Sarah complete?"
    ]
    
    test_user_id = f"test_user_{int(time.time())}"
    results = []
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n🔄 Running test {i}/{len(test_questions)}")
        
        # Test direct handle_question (should generate SQL)
        direct_result = await test_direct_sql_generation(question, test_user_id)
        
        # Test intelligent SQL generation 
        intelligent_result = await test_intelligent_sql_generation(question, test_user_id)
        
        results.append({
            "question": question,
            "direct_result": direct_result,
            "intelligent_result": intelligent_result
        })
        
        # Small delay between tests
        await asyncio.sleep(1)
    
    # Generate summary
    print("\n" + "="*80)
    print("📊 DIRECT SQL GENERATION TEST SUMMARY")
    print("="*80)
    
    sql_generated = 0
    errors = 0
    
    for i, result in enumerate(results, 1):
        print(f"\nTest {i}: {result['question']}")
        
        direct = result['direct_result']
        intelligent = result['intelligent_result']
        
        if direct.get('error') or intelligent.get('error'):
            print(f"❌ Errors occurred")
            errors += 1
        elif direct.get('response_type') == 'sql' or intelligent.get('sql'):
            print(f"✅ SQL generated successfully")
            sql_generated += 1
            
            # Show generated SQL preview
            if direct.get('extracted_sql'):
                preview = direct['extracted_sql'][:200] + "..." if len(direct['extracted_sql']) > 200 else direct['extracted_sql']
                print(f"📝 SQL Preview: {preview}")
        else:
            print(f"⚠️ No SQL generated")
    
    print(f"\n📈 Results:")
    print(f"  SQL Generated: {sql_generated}/{len(test_questions)}")
    print(f"  Errors: {errors}/{len(test_questions)}")
    print(f"  Success Rate: {(sql_generated/len(test_questions)*100):.1f}%")

if __name__ == "__main__":
    asyncio.run(main())