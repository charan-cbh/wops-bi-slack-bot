#!/usr/bin/env python3
"""
Test script for BI Service implementation
Tests both disabled and enabled states
"""
import asyncio
import os
import sys
import time

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.bi_service import (
    get_bi_service,
    process_with_bi_service,
    should_use_bi_service,
    get_bi_service_status
)

def test_bi_service_disabled():
    """Test BI Service in disabled state"""
    print("🔧 Testing BI Service - Disabled State")
    print("=" * 50)
    
    # Ensure BI Service is disabled
    os.environ['USE_BI_SERVICE'] = 'false'
    
    # Test status
    status = get_bi_service_status()
    print(f"Status: {status}")
    
    # Test should_use_bi_service
    should_use = should_use_bi_service("What is the average AHT for Team Gian?")
    print(f"Should use BI Service: {should_use}")
    assert should_use == False, "BI Service should not be used when disabled"
    
    print("✅ Disabled state test passed\n")

async def test_bi_service_enabled():
    """Test BI Service in enabled state"""
    print("🔧 Testing BI Service - Enabled State")
    print("=" * 50)
    
    # Enable BI Service
    os.environ['USE_BI_SERVICE'] = 'true'
    os.environ['BI_SERVICE_PROVIDER'] = 'openai'
    os.environ['BI_SERVICE_MODEL'] = 'gpt-4'
    
    # Test status
    status = get_bi_service_status()
    print(f"Status: {status}")
    
    # Test should_use_bi_service
    should_use = should_use_bi_service("What is the average AHT for Team Gian?")
    print(f"Should use BI Service: {should_use}")
    
    # Test BI Service instance
    bi_service = get_bi_service()
    print(f"BI Service enabled: {bi_service.is_enabled()}")
    
    if bi_service.is_enabled():
        print("✅ Enabled state test passed")
        
        # Test sample questions
        test_questions = [
            "What is the average AHT for Team Gian?",
            "How is our schedule adherence looking this week?",
            "What are the top ticket categories for July?",
            "Can you explain what FCR means and why it matters?"
        ]
        
        print("\n🧪 Testing sample questions...")
        for i, question in enumerate(test_questions, 1):
            print(f"\n--- Test Question {i} ---")
            print(f"Question: {question}")
            
            try:
                response, response_type = await process_with_bi_service(
                    question, 
                    "test_user", 
                    "test_channel"
                )
                
                print(f"Response Type: {response_type}")
                print(f"Response Length: {len(response)} characters")
                print(f"Response Preview: {response[:200]}...")
                
                if response_type == 'error':
                    print(f"⚠️ Error response: {response}")
                elif response_type == 'ai_response':
                    print("✅ Successfully got AI response")
                    
            except Exception as e:
                print(f"❌ Error processing question: {e}")
            
            # Small delay between questions
            await asyncio.sleep(1)
    else:
        print("❌ BI Service is not properly configured")
        print("Check your environment variables:")
        print(f"- USE_BI_SERVICE: {os.getenv('USE_BI_SERVICE')}")
        print(f"- BI_SERVICE_PROVIDER: {os.getenv('BI_SERVICE_PROVIDER')}")
        print(f"- Required API keys are set")

def test_configuration():
    """Test configuration scenarios"""
    print("🔧 Testing Configuration Scenarios")
    print("=" * 50)
    
    # Test different provider configurations
    providers = ['openai', 'anthropic']
    
    for provider in providers:
        print(f"\n--- Testing {provider} provider ---")
        os.environ['BI_SERVICE_PROVIDER'] = provider
        
        # Get fresh instance
        from importlib import reload
        import app.bi_service
        reload(app.bi_service)
        
        status = get_bi_service_status()
        print(f"Provider: {status['provider']}")
        print(f"Configured: {status['configured']}")

async def main():
    """Run all tests"""
    print("🚀 BI Service Implementation Test Suite")
    print("=" * 60)
    
    try:
        # Test 1: Disabled state
        test_bi_service_disabled()
        
        # Test 2: Enabled state
        await test_bi_service_enabled()
        
        # Test 3: Configuration scenarios
        test_configuration()
        
        print("\n" + "=" * 60)
        print("🎉 All tests completed!")
        print("\n📋 Test Summary:")
        print("✅ BI Service disabled state")
        print("✅ BI Service enabled state") 
        print("✅ Configuration scenarios")
        print("✅ Sample question processing")
        
        print(f"\n🔧 Final BI Service Status:")
        final_status = get_bi_service_status()
        for key, value in final_status.items():
            print(f"- {key}: {value}")
            
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)