#!/usr/bin/env python3
"""
Test user recognition disabled feature
"""

import os
import sys
import asyncio
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, 'app')

async def test_user_recognition_disabled():
    print("🔍 Testing User Recognition Disabled Feature")
    print("=" * 50)
    
    # Check environment variable
    enable_user_recognition = os.getenv("ENABLE_USER_RECOGNITION", "false").lower() == "true"
    print(f"ENABLE_USER_RECOGNITION: {enable_user_recognition}")
    
    # Test user context manager
    print("\n1. Testing User Context Manager:")
    from app.user_context_manager import get_user_context, ENABLE_USER_RECOGNITION
    print(f"   ENABLE_USER_RECOGNITION flag: {ENABLE_USER_RECOGNITION}")
    
    user_context = await get_user_context("U019NNZPPME")
    print(f"   User context result: {user_context}")
    
    # Test personal question
    print("\n2. Testing Personal Question:")
    from app.llm_orchestrator import handle_question
    
    response, response_type = await handle_question(
        "how many members are there in my team?", 
        "U019NNZPPME", 
        "test_channel"
    )
    
    print(f"   Response: {response}")
    print(f"   Response type: {response_type}")
    
    # Test non-personal question (should still work)
    print("\n3. Testing Non-Personal Question:")
    response2, response_type2 = await handle_question(
        "How many agents are there in Ricardo Birck's team?", 
        "U019NNZPPME", 
        "test_channel"
    )
    
    print(f"   Response: {response2[:200]}...")
    print(f"   Response type: {response_type2}")

if __name__ == "__main__":
    asyncio.run(test_user_recognition_disabled())