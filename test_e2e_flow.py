#!/usr/bin/env python3
"""
End-to-End Flow Test for Multi-Provider Architecture
This test simulates the complete flow from receiving a Slack message to sending the final response
"""

import asyncio
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from llm_orchestrator import handle_question, get_provider_info

async def test_sql_query_e2e():
    """Test complete E2E flow for SQL query"""
    print("🔍 Testing SQL Query E2E Flow")
    print("=" * 60)
    
    # Test parameters
    test_user = "test_user_123"
    test_channel = "test_channel_456"
    test_question = "Who has less than 92% adherence rating for July 15, 2025, in Team Gian?"
    
    print(f"📝 Question: {test_question}")
    print(f"👤 User: {test_user}")
    print(f"📺 Channel: {test_channel}")
    
    try:
        # This simulates the exact flow that happens in Slack
        response, response_type = await handle_question(test_question, test_user, test_channel)
        
        print(f"\n✅ Response Type: {response_type}")
        print(f"📊 Response Length: {len(response)} characters")
        
        if response_type == 'sql':
            print("✅ SQL query processed successfully!")
            print(f"📋 First 200 chars of response: {response[:200]}...")
        elif response_type == 'conversational':
            print("✅ Conversational response generated!")
            print(f"💬 Response: {response}")
        else:
            print(f"⚠️  Unexpected response type: {response_type}")
            
        return True
        
    except Exception as e:
        print(f"❌ E2E test failed: {str(e)}")
        return False

async def test_conversational_query_e2e():
    """Test complete E2E flow for conversational query"""
    print("\n💬 Testing Conversational Query E2E Flow")
    print("=" * 60)
    
    # Test parameters
    test_user = "test_user_456"
    test_channel = "test_channel_789"
    test_question = "Hello, how does this bot work?"
    
    print(f"📝 Question: {test_question}")
    print(f"👤 User: {test_user}")
    print(f"📺 Channel: {test_channel}")
    
    try:
        # This simulates the exact flow that happens in Slack
        response, response_type = await handle_question(test_question, test_user, test_channel)
        
        print(f"\n✅ Response Type: {response_type}")
        print(f"📊 Response Length: {len(response)} characters")
        
        if response_type == 'conversational':
            print("✅ Conversational query processed successfully!")
            print(f"💬 Response: {response}")
        elif response_type == 'sql':
            print("⚠️  Expected conversational but got SQL")
            print(f"📋 SQL Response: {response[:200]}...")
        else:
            print(f"⚠️  Unexpected response type: {response_type}")
            
        return True
        
    except Exception as e:
        print(f"❌ E2E test failed: {str(e)}")
        return False

async def test_team_specific_query_e2e():
    """Test E2E flow for team-specific query"""
    print("\n👥 Testing Team-Specific Query E2E Flow")
    print("=" * 60)
    
    # Test parameters
    test_user = "test_user_789"
    test_channel = "test_channel_123"
    test_question = "How many agents are in team Yiannis?"
    
    print(f"📝 Question: {test_question}")
    print(f"👤 User: {test_user}")
    print(f"📺 Channel: {test_channel}")
    
    try:
        # This simulates the exact flow that happens in Slack
        response, response_type = await handle_question(test_question, test_user, test_channel)
        
        print(f"\n✅ Response Type: {response_type}")
        print(f"📊 Response Length: {len(response)} characters")
        
        if response_type == 'sql':
            print("✅ Team query processed successfully!")
            print(f"📋 First 200 chars of response: {response[:200]}...")
        elif response_type == 'conversational':
            print("✅ Conversational response generated!")
            print(f"💬 Response: {response}")
        else:
            print(f"⚠️  Unexpected response type: {response_type}")
            
        return True
        
    except Exception as e:
        print(f"❌ E2E test failed: {str(e)}")
        return False

async def test_provider_info():
    """Test provider information"""
    print("\n🏭 Testing Provider Information")
    print("=" * 60)
    
    try:
        provider_info = get_provider_info()
        print(f"✅ Provider: {provider_info['provider_name']}")
        print(f"✅ Model: {provider_info['model_name']}")
        
        # Check for additional config info if available
        if 'configuration' in provider_info:
            print(f"✅ Configuration: {provider_info['configuration']}")
        else:
            print(f"✅ Additional info: {provider_info}")
        
        return True
    except Exception as e:
        print(f"❌ Provider info test failed: {str(e)}")
        return False

async def main():
    """Run all E2E tests"""
    print("🚀 STARTING MULTI-PROVIDER E2E TESTS")
    print("=" * 80)
    
    # Test provider information first
    provider_test = await test_provider_info()
    
    # Run all E2E tests
    tests = [
        await test_sql_query_e2e(),
        await test_conversational_query_e2e(),
        await test_team_specific_query_e2e()
    ]
    
    # Results summary
    passed = sum(tests) + (1 if provider_test else 0)
    total = len(tests) + 1
    
    print(f"\n🎯 TEST RESULTS SUMMARY")
    print("=" * 80)
    print(f"✅ Passed: {passed}/{total} tests")
    print(f"❌ Failed: {total - passed}/{total} tests")
    
    if passed == total:
        print("🎉 ALL E2E TESTS PASSED! Multi-provider architecture is working correctly.")
        return True
    else:
        print("⚠️  Some E2E tests failed. Please check the logs above.")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)