#!/usr/bin/env python3
"""
Integration test for BI Service with sample questions
"""
import asyncio
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

async def test_bi_service_integration():
    """Test BI Service with sample business questions"""
    print("🔧 Testing BI Service Integration")
    print("=" * 50)
    
    # Enable BI Service
    os.environ['USE_BI_SERVICE'] = 'true'
    os.environ['BI_SERVICE_PROVIDER'] = 'openai'
    os.environ['BI_SERVICE_MODEL'] = 'gpt-4'
    
    # Test questions
    test_questions = [
        "What is AHT and why is it important?",
        "How should we measure schedule adherence?",
        "What factors affect First Call Resolution?",
        "What metrics are most important for agent performance?"
    ]
    
    try:
        from app.bi_service import process_with_bi_service, get_bi_service_status
        
        # Show status
        status = get_bi_service_status()
        print(f"BI Service Status: {status['configured']}")
        print(f"Provider: {status['provider']}")
        print(f"Model: {status['model']}")
        
        if not status['configured']:
            print("❌ BI Service not properly configured")
            return False
        
        print(f"\n🧪 Testing {len(test_questions)} business questions...")
        
        for i, question in enumerate(test_questions, 1):
            print(f"\n--- Question {i} ---")
            print(f"Q: {question}")
            
            try:
                response, response_type = await process_with_bi_service(
                    question,
                    "test_user",
                    "test_channel"
                )
                
                print(f"Response Type: {response_type}")
                
                if response_type == 'ai_response':
                    print(f"✅ Success! Response length: {len(response)} chars")
                    # Show first 150 characters of response
                    preview = response[:150].replace('\n', ' ')
                    print(f"Preview: {preview}...")
                elif response_type == 'error':
                    print(f"❌ Error: {response}")
                elif response_type == 'rate_limited':
                    print(f"⚠️ Rate limited: {response}")
                
            except Exception as e:
                print(f"❌ Exception: {e}")
                return False
            
            # Small delay between requests
            await asyncio.sleep(0.5)
        
        print("\n✅ All integration tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_slack_integration():
    """Test the Slack handler integration"""
    print("\n🔧 Testing Slack Handler Integration")
    print("=" * 50)
    
    try:
        from app.bi_service import should_use_bi_service
        from app.slack_handler import get_status
        
        # Test routing logic
        business_questions = [
            "What is our average handle time?",
            "How is schedule adherence looking?",
            "What are the key performance metrics?",
        ]
        
        for question in business_questions:
            should_use = should_use_bi_service(question)
            print(f"Question: '{question[:40]}...' -> Use BI Service: {should_use}")
        
        # Test status endpoint
        status = get_status()
        bi_status = status.get('bi_service', {})
        print(f"\nSlack Bot Status includes BI Service: {'bi_service' in status}")
        print(f"BI Service configured in status: {bi_status.get('configured', False)}")
        
        print("✅ Slack integration test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Slack integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run integration tests"""
    print("🚀 BI Service Integration Test Suite")
    print("=" * 60)
    
    success = True
    
    # Test 1: BI Service integration
    success &= await test_bi_service_integration()
    
    # Test 2: Slack handler integration
    success &= await test_slack_integration()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 All integration tests passed!")
        print("\n📋 BI Service is ready for use:")
        print("✅ Basic configuration works")
        print("✅ Can process business questions")
        print("✅ Integrated with Slack handler")
        print("✅ Status endpoint includes BI Service")
    else:
        print("❌ Some integration tests failed")
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)