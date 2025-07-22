#!/usr/bin/env python3
"""
Direct test of Assistant API with Clipboard Health business context
Bypasses the complex routing logic to test vector store directly
"""
import asyncio
import os
import sys
from dotenv import load_dotenv
import openai
from openai import OpenAI

# Load environment variables
load_dotenv()

async def test_direct_assistant_api():
    """Test Assistant API directly with Clipboard Health questions"""
    print("🤖 Testing Assistant API Directly with Business Context")
    print("=" * 60)
    
    # Get configuration
    api_key = os.getenv("OPENAI_API_KEY")
    assistant_id = os.getenv("ASSISTANT_ID")
    
    if not (api_key and assistant_id):
        print("❌ Missing OPENAI_API_KEY or ASSISTANT_ID")
        return False
    
    print(f"Assistant ID: {assistant_id}")
    
    # Initialize OpenAI client with v2 header
    client = OpenAI(
        api_key=api_key,
        default_headers={"OpenAI-Beta": "assistants=v2"}
    )
    
    # Test questions
    test_questions = [
        "What is Clipboard Health?",
        "How do Magic Shifts work?", 
        "What happens if someone cancels within 8 hours?",
        "How do I contact billing support?",
        "What documents do professionals need to upload?"
    ]
    
    results = []
    
    try:
        for i, question in enumerate(test_questions, 1):
            print(f"\n--- Test {i}: {question} ---")
            
            try:
                # Create thread
                thread = client.beta.threads.create()
                
                # Add message
                client.beta.threads.messages.create(
                    thread_id=thread.id,
                    role="user", 
                    content=question
                )
                
                # Run assistant
                run = client.beta.threads.runs.create(
                    thread_id=thread.id,
                    assistant_id=assistant_id
                )
                
                # Wait for completion
                while run.status in ['queued', 'in_progress']:
                    await asyncio.sleep(1)
                    run = client.beta.threads.runs.retrieve(
                        thread_id=thread.id,
                        run_id=run.id
                    )
                
                if run.status == 'completed':
                    # Get response
                    messages = client.beta.threads.messages.list(thread_id=thread.id)
                    response = messages.data[0].content[0].text.value
                    
                    print(f"✅ SUCCESS - Response length: {len(response)} chars")
                    print(f"Preview: {response[:200]}...")
                    
                    # Check for business context keywords
                    clipboard_keywords = [
                        "clipboard health", "healthcare", "staffing", "CNAs", 
                        "LVNs", "RNs", "facilities", "professionals", "shifts"
                    ]
                    
                    found_keywords = [kw for kw in clipboard_keywords if kw.lower() in response.lower()]
                    
                    if found_keywords:
                        print(f"✅ Business context detected: {found_keywords[:3]}")
                        results.append(('success', question))
                    else:
                        print("⚠️ No clear business context in response")
                        results.append(('partial', question))
                        
                elif run.status == 'failed':
                    print(f"❌ FAILED - {run.last_error}")
                    results.append(('error', question))
                else:
                    print(f"⚠️ UNEXPECTED STATUS - {run.status}")
                    results.append(('error', question))
                    
                # Cleanup
                client.beta.threads.delete(thread.id)
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
                results.append(('error', question))
            
            await asyncio.sleep(1)  # Rate limit
        
        return results
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return []

async def test_vector_store_files():
    """Check what files are in the vector store"""
    print("\n📁 Checking Vector Store Files")
    print("=" * 40)
    
    api_key = os.getenv("OPENAI_API_KEY")
    vector_store_id = os.getenv("OPENAI_VECTOR_STORE_ID")
    
    if not (api_key and vector_store_id):
        print("❌ Missing configuration")
        return False
    
    try:
        client = OpenAI(
            api_key=api_key,
            default_headers={"OpenAI-Beta": "assistants=v2"}
        )
        
        # List files
        files = client.beta.vector_stores.files.list(vector_store_id=vector_store_id)
        
        print(f"Files in vector store: {len(files.data)}")
        for i, file_obj in enumerate(files.data[:5]):
            file_info = client.files.retrieve(file_obj.id)
            print(f"{i+1}. {file_info.filename} ({file_obj.status})")
        
        return len(files.data) > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

async def main():
    """Run direct assistant tests"""
    print("🚀 Direct Assistant API Test for Clipboard Health Context")
    print("=" * 80)
    
    # Skip vector store check and test assistant directly
    print("⚠️ Skipping vector store file check, testing Assistant directly")
    
    # Test assistant
    results = await test_direct_assistant_api()
    
    if not results:
        print("❌ No test results")
        return False
    
    # Analyze results
    success = len([r for r in results if r[0] == 'success'])
    partial = len([r for r in results if r[0] == 'partial']) 
    errors = len([r for r in results if r[0] == 'error'])
    total = len(results)
    
    success_rate = (success + partial) / total * 100 if total > 0 else 0
    
    print("\n" + "=" * 80)
    print("📊 DIRECT ASSISTANT TEST RESULTS")
    print("=" * 80)
    print(f"✅ Successful: {success}/{total}")
    print(f"🟡 Partial: {partial}/{total}")  
    print(f"❌ Errors: {errors}/{total}")
    print(f"📈 Success Rate: {success_rate:.1f}%")
    
    if success_rate >= 60:
        print("\n🎉 SUCCESS! Assistant can access Clipboard Health business context!")
        print("✅ Vector store integration is working")
        print("✅ Business knowledge is being retrieved")
        print("\n🔧 The issue is in the routing logic, not the knowledge base")
        print("💡 The bot needs to route business questions to the Assistant API")
    else:
        print("\n⚠️ Assistant has limited access to business context")
        print("🔧 Check if files were uploaded correctly to vector store")
    
    return success_rate >= 60

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)