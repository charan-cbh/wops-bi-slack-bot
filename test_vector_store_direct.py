#!/usr/bin/env python3
"""
Direct test of Assistant API with vector store access to verify knowledge base content.
This bypasses all SQL generation and just tests if the Assistant can access vector store context.
"""

import asyncio
import os
import sys
import json
import time
from datetime import datetime

from dotenv import load_dotenv
import openai

load_dotenv()

# Test configuration
ASSISTANT_ID = os.getenv("ASSISTANT_ID", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

async def test_vector_store_access():
    """Test direct vector store access through Assistant API"""
    if not ASSISTANT_ID or not OPENAI_API_KEY:
        print("❌ Missing ASSISTANT_ID or OPENAI_API_KEY")
        return
    
    print("🤖 Testing Direct Vector Store Access")
    print(f"📋 Assistant ID: {ASSISTANT_ID}")
    
    client = openai.AsyncOpenAI(
        api_key=OPENAI_API_KEY,
        default_headers={"OpenAI-Beta": "assistants=v2"}
    )
    
    # Test questions specifically designed to check vector store content
    test_questions = [
        "What tables should I use for Zendesk ticket volume queries?",
        "Show me the table structure for FCT_ZENDESK__MQR_TICKETS",
        "What is the GROUP_ID for organizational metrics?", 
        "What tables contain Klaus QA data?",
        "How do I filter by reviewer name for audits?",
        "What fields are available in the Dim Zendesk Users table?",
        "What are the mandatory filters for Zendesk queries?",
        "Show me SQL patterns for agent performance queries"
    ]
    
    results = []
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n🔄 Test {i}/{len(test_questions)}")
        print(f"❓ Question: {question}")
        
        try:
            # Create thread
            thread = await client.beta.threads.create()
            
            # Add message
            await client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=f"""Based on your knowledge base, please answer this specific question about the data warehouse structure: 
                
{question}

Please provide specific table names, column names, and SQL patterns from your knowledge base."""
            )
            
            # Run assistant
            start_time = time.time()
            run = await client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=ASSISTANT_ID
            )
            
            # Wait for completion
            max_attempts = 60
            attempts = 0
            while run.status in ['queued', 'in_progress', 'cancelling'] and attempts < max_attempts:
                await asyncio.sleep(1)
                attempts += 1
                run = await client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            
            execution_time = time.time() - start_time
            print(f"⏱️ Response time: {execution_time:.2f}s")
            print(f"📊 Status: {run.status}")
            
            if run.status == 'completed':
                messages = await client.beta.threads.messages.list(thread_id=thread.id, limit=1)
                if messages.data:
                    content = messages.data[0].content[0]
                    if hasattr(content, 'text'):
                        response_text = content.text.value.strip()
                        print(f"✅ Response received: {len(response_text)} chars")
                        
                        # Check for specific knowledge base indicators
                        kb_indicators = [
                            "FCT_ZENDESK__MQR_TICKETS",
                            "GROUP_ID = '17837476387479'",
                            "Klaus",
                            "REVIEWER_NAME", 
                            "Dim Zendesk Users",
                            "WOPS_",
                            "ANALYTICS.DBT_PRODUCTION",
                            "mandatory filters",
                            "AHT"
                        ]
                        
                        found_indicators = [ind for ind in kb_indicators if ind.upper() in response_text.upper()]
                        
                        print(f"🗂️ Knowledge base indicators found: {found_indicators}")
                        print(f"📝 Response preview: {response_text[:200]}...")
                        
                        results.append({
                            "question": question,
                            "response": response_text,
                            "execution_time": execution_time,
                            "kb_indicators_found": found_indicators,
                            "has_specific_content": len(found_indicators) > 0,
                            "error": None
                        })
                    else:
                        print("⚠️ No text content in response")
                        results.append({
                            "question": question,
                            "response": "",
                            "execution_time": execution_time,
                            "kb_indicators_found": [],
                            "has_specific_content": False,
                            "error": "No text content"
                        })
                else:
                    print("⚠️ No messages returned")
                    results.append({
                        "question": question,
                        "response": "",
                        "execution_time": execution_time,
                        "kb_indicators_found": [],
                        "has_specific_content": False,
                        "error": "No messages returned"
                    })
            else:
                print(f"❌ Run failed with status: {run.status}")
                error_msg = f"Run failed with status: {run.status}"
                if hasattr(run, 'last_error') and run.last_error:
                    error_msg += f" - {run.last_error}"
                    
                results.append({
                    "question": question,
                    "response": "",
                    "execution_time": execution_time,
                    "kb_indicators_found": [],
                    "has_specific_content": False,
                    "error": error_msg
                })
        
        except Exception as e:
            print(f"💥 Error: {str(e)}")
            results.append({
                "question": question,
                "response": "",
                "execution_time": 0,
                "kb_indicators_found": [],
                "has_specific_content": False,
                "error": str(e)
            })
        
        # Small delay between requests
        await asyncio.sleep(1)
    
    return results

async def main():
    """Run vector store access tests"""
    print("🚀 Starting Vector Store Access Tests")
    
    try:
        results = await test_vector_store_access()
        
        if not results:
            print("❌ No results to analyze")
            return
        
        # Analyze results
        print("\n" + "="*80)
        print("📊 VECTOR STORE ACCESS TEST SUMMARY")
        print("="*80)
        
        successful = len([r for r in results if r['has_specific_content']])
        errors = len([r for r in results if r['error']])
        
        print(f"Total Tests: {len(results)}")
        print(f"Tests with Knowledge Base Content: {successful}")
        print(f"Errors: {errors}")
        print(f"Knowledge Base Access Rate: {(successful/len(results)*100):.1f}%")
        
        # Show detailed results
        print("\n📋 DETAILED RESULTS:")
        for i, result in enumerate(results, 1):
            status = "✅" if result['has_specific_content'] else ("❌" if result['error'] else "⚠️")
            print(f"\n{i}. {status} {result['question']}")
            
            if result['error']:
                print(f"   Error: {result['error']}")
            elif result['has_specific_content']:
                print(f"   KB Indicators: {', '.join(result['kb_indicators_found'])}")
                print(f"   Preview: {result['response'][:150]}...")
            else:
                print(f"   No specific KB content detected")
                print(f"   Preview: {result['response'][:150]}...")
        
        # Save detailed report
        report_file = f"vector_store_access_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n📄 Detailed report saved to: {report_file}")
        
        # Overall assessment
        if successful >= len(results) * 0.8:  # 80% success rate
            print("🎉 Vector store appears to be working well!")
        elif successful >= len(results) * 0.5:  # 50% success rate  
            print("⚠️ Vector store has some content but may be incomplete")
        else:
            print("❌ Vector store may not be properly configured or lacking content")
        
    except Exception as e:
        print(f"💥 Test runner error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())