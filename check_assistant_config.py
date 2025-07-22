#!/usr/bin/env python3
"""
Check Assistant configuration and vector store connection
"""
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

def check_assistant_config():
    """Check if Assistant is properly configured with vector store"""
    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        default_headers={"OpenAI-Beta": "assistants=v2"}
    )
    
    assistant_id = os.getenv("ASSISTANT_ID")
    vector_store_id = os.getenv("OPENAI_VECTOR_STORE_ID")
    
    print(f"🔧 Checking Assistant Configuration")
    print(f"Assistant ID: {assistant_id}")
    print(f"Vector Store ID: {vector_store_id}")
    
    try:
        # Get assistant details
        assistant = client.beta.assistants.retrieve(assistant_id)
        print(f"\n📝 Assistant Details:")
        print(f"Name: {assistant.name}")
        print(f"Model: {assistant.model}")
        print(f"Instructions: {assistant.instructions[:200]}...")
        
        # Check if assistant has file search tool
        has_file_search = any(tool.type == 'file_search' for tool in assistant.tools)
        print(f"Has file_search tool: {has_file_search}")
        
        # Check tool resources
        if assistant.tool_resources and assistant.tool_resources.file_search:
            vs_ids = assistant.tool_resources.file_search.vector_store_ids
            print(f"Configured vector stores: {vs_ids}")
            
            if vector_store_id in vs_ids:
                print("✅ Vector store is properly attached to Assistant")
            else:
                print("❌ Vector store NOT attached to Assistant")
                print("Need to attach the vector store!")
                return False
        else:
            print("❌ No vector store configuration found")
            return False
            
        # Check vector store files
        try:
            files = client.beta.vector_stores.files.list(vector_store_id=vector_store_id)
            print(f"\n📁 Vector Store Files: {len(files.data)}")
            for file_obj in files.data[:3]:  # Show first 3 files
                print(f"  - {file_obj.id}: {getattr(file_obj, 'filename', 'Unknown')} ({file_obj.status})")
                
        except Exception as e:
            print(f"❌ Error checking vector store files: {e}")
            
        return True
        
    except Exception as e:
        print(f"❌ Error checking assistant: {e}")
        return False

def fix_assistant_config():
    """Fix Assistant configuration by attaching vector store"""
    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        default_headers={"OpenAI-Beta": "assistants=v2"}
    )
    
    assistant_id = os.getenv("ASSISTANT_ID")
    vector_store_id = os.getenv("OPENAI_VECTOR_STORE_ID")
    
    print(f"\n🔧 Fixing Assistant Configuration...")
    
    try:
        # Update assistant to include vector store
        assistant = client.beta.assistants.update(
            assistant_id=assistant_id,
            tools=[{"type": "file_search"}],
            tool_resources={
                "file_search": {
                    "vector_store_ids": [vector_store_id]
                }
            }
        )
        
        print("✅ Assistant updated with vector store configuration")
        return True
        
    except Exception as e:
        print(f"❌ Error updating assistant: {e}")
        return False

if __name__ == "__main__":
    config_ok = check_assistant_config()
    
    if not config_ok:
        print("\n🔧 Attempting to fix configuration...")
        if fix_assistant_config():
            print("✅ Configuration fixed! Try testing again.")
        else:
            print("❌ Could not fix configuration.")
    else:
        print("✅ Assistant configuration looks good!")