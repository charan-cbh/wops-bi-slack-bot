#!/usr/bin/env python3
"""
Test Assistant API directly to see if it can access Clipboard Health context
"""
import os
import asyncio
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

async def test_assistant_direct():
    """Test Assistant API directly"""
    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        default_headers={"OpenAI-Beta": "assistants=v2"}
    )
    
    assistant_id = os.getenv("ASSISTANT_ID")
    
    print("🧪 Testing Assistant API Directly")
    print("=" * 50)
    
    try:
        # Create thread
        thread = client.beta.threads.create()
        print(f"Thread created: {thread.id}")
        
        # Ask about Clipboard Health
        question = "What is Clipboard Health? Please provide details about their platform and services."
        
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=question
        )
        
        print(f"Question: {question}")
        
        # Run assistant
        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=assistant_id
        )
        
        print("Waiting for response...")
        
        # Wait for completion
        while run.status in ['queued', 'in_progress', 'cancelling']:
            await asyncio.sleep(1)
            run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            print(f"Status: {run.status}")
        
        print(f"Final status: {run.status}")
        
        if run.status == 'completed':
            messages = client.beta.threads.messages.list(thread_id=thread.id, limit=1)
            if messages.data:
                response = messages.data[0].content[0].text.value
                print(f"\n✅ Response received ({len(response)} chars):")
                print(response[:500] + ("..." if len(response) > 500 else ""))
                
                # Check for business context
                key_terms = ["healthcare", "staffing", "platform", "shifts", "professionals", "facilities"]
                found_terms = [term for term in key_terms if term.lower() in response.lower()]
                print(f"\n📊 Business context terms found: {found_terms}")
                
                return len(found_terms) >= 2
            else:
                print("❌ No response received")
                return False
        else:
            print(f"❌ Run failed with status: {run.status}")
            if hasattr(run, 'last_error') and run.last_error:
                print(f"Error: {run.last_error}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_assistant_direct())
    print(f"\n{'✅ SUCCESS' if success else '❌ FAILED'}")