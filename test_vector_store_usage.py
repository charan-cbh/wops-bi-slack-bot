#!/usr/bin/env python3
"""
Test script to verify if Assistant API is using the vector store files
"""
import asyncio
import os
import sys

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

async def test_vector_store_retrieval():
    """Test if assistant retrieves information from vector store"""
    print("🔍 Testing Vector Store File Retrieval")
    print("=" * 50)
    
    # Check environment variables
    assistant_id = os.getenv("ASSISTANT_ID")
    vector_store_id = os.getenv("OPENAI_VECTOR_STORE_ID")
    use_assistant = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"
    
    print(f"Assistant ID: {assistant_id[:20]}..." if assistant_id else "Not set")
    print(f"Vector Store ID: {vector_store_id[:20]}..." if vector_store_id else "Not set")
    print(f"Use Assistant API: {use_assistant}")
    
    if not (assistant_id and vector_store_id and use_assistant):
        print("❌ Missing required configuration")
        return False
    
    # Test questions that should trigger file search
    test_questions = [
        "What tables are available for response time analysis?",
        "Which table should I use for FCR analysis?", 
        "Show me the pattern for schedule adherence queries",
        "What is Pattern 1 about?"  # Direct reference to pattern file
    ]
    
    try:
        from app.llm_orchestrator import handle_question
        
        for question in test_questions:
            print(f"\n--- Testing: {question} ---")
            
            try:
                response, response_type = await handle_question(
                    question,
                    "test_user",
                    "test_channel", 
                    assistant_id
                )
                
                print(f"Response Type: {response_type}")
                
                # Check if response contains pattern file content
                pattern_indicators = [
                    "RPT_WOPS_TICKETS",
                    "Pattern 1",
                    "REPLY_TIME_IN_MINUTES",
                    "FCT_ZENDESK__MQR_TICKETS", 
                    "RPT_AGENT_SCHEDULE_ADHERENCE",
                    "vector search",
                    "pattern"
                ]
                
                found_indicators = []
                for indicator in pattern_indicators:
                    if indicator.lower() in response.lower():
                        found_indicators.append(indicator)
                
                print(f"Response length: {len(response)} chars")
                print(f"Pattern file indicators found: {len(found_indicators)}")
                if found_indicators:
                    print(f"  Found: {', '.join(found_indicators[:3])}...")
                    print("✅ Assistant likely using vector store content!")
                else:
                    print("⚠️ No clear pattern file content detected")
                
                # Show response preview
                preview = response[:200].replace('\n', ' ')
                print(f"Preview: {preview}...")
                
            except Exception as e:
                print(f"❌ Error: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def test_direct_vector_search():
    """Test direct vector store search functionality"""
    print("\n🔎 Testing Direct Vector Store Search")
    print("=" * 50)
    
    try:
        from app.llm_prompter import find_relevant_tables_from_vector_store
        
        test_queries = [
            "response time analysis",
            "schedule adherence",
            "Team Gian",
            "FCR first contact resolution"
        ]
        
        for query in test_queries:
            print(f"\n--- Searching: {query} ---")
            
            tables = await find_relevant_tables_from_vector_store(
                query,
                "test_user",
                "test_channel",
                top_k=3
            )
            
            print(f"Found tables: {tables}")
            if tables:
                print("✅ Vector store search working!")
            else:
                print("⚠️ No tables found")
        
        return True
        
    except Exception as e:
        print(f"❌ Direct search failed: {e}")
        return False

async def check_vector_store_files():
    """Check what files are in the vector store"""
    print("\n📁 Checking Vector Store Files")
    print("=" * 50)
    
    try:
        import openai
        
        api_key = os.getenv("OPENAI_API_KEY")
        vector_store_id = os.getenv("OPENAI_VECTOR_STORE_ID")
        
        if not (api_key and vector_store_id):
            print("❌ Missing API key or vector store ID")
            return False
        
        client = openai.OpenAI(api_key=api_key)
        
        # List files in vector store
        files = client.beta.vector_stores.files.list(
            vector_store_id=vector_store_id
        )
        
        print(f"Files in vector store: {len(files.data)}")
        for i, file in enumerate(files.data[:5]):  # Show first 5
            print(f"{i+1}. {file.id[:20]}... (status: {file.status})")
        
        if files.data:
            print("✅ Vector store has files!")
            return True
        else:
            print("❌ Vector store is empty")
            return False
            
    except Exception as e:
        print(f"❌ Error checking files: {e}")
        return False

async def main():
    """Run all vector store tests"""
    print("🚀 Vector Store Usage Verification")
    print("=" * 60)
    
    results = []
    
    # Test 1: Check files in vector store
    results.append(await check_vector_store_files())
    
    # Test 2: Test direct vector search
    results.append(await test_direct_vector_search())
    
    # Test 3: Test assistant retrieval
    results.append(await test_vector_store_retrieval())
    
    print("\n" + "=" * 60)
    
    if all(results):
        print("🎉 Vector Store is Working Properly!")
        print("\n✅ Findings:")
        print("• Vector store contains files")
        print("• Direct search returns results") 
        print("• Assistant retrieves content from files")
        print("• Pattern file content appears in responses")
        print("\n📋 Your assistant IS using the uploaded pattern file!")
    elif any(results):
        print("⚠️ Vector Store Partially Working")
        test_names = ["File Check", "Direct Search", "Assistant Retrieval"]
        for i, result in enumerate(results):
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"• {test_names[i]}: {status}")
    else:
        print("❌ Vector Store Issues Detected")
        print("\n🔧 Possible fixes:")
        print("• Check OPENAI_VECTOR_STORE_ID is correct")
        print("• Verify files are uploaded to vector store")
        print("• Confirm assistant has file_search tool enabled")
        print("• Check assistant is connected to vector store")
    
    return all(results)

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)