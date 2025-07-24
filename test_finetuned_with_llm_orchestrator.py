#!/usr/bin/env python3
"""
Test fine-tuned model using existing LLM orchestrator infrastructure
"""
import os
import asyncio
import sys

# Set environment variables for fine-tuned model (you need to set OPENAI_API_KEY externally)
os.environ['USE_ASSISTANT_API'] = 'false'
os.environ['OPENAI_MODEL'] = 'ft:gpt-4o-mini-2024-07-18:clipboard-health:wops-bi-bot:BwumQjzx'
os.environ['MODEL_PROVIDER'] = 'openai'

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

async def test_critical_team_vs_individual():
    """Test the critical team vs individual query distinction"""
    
    print("🎯 Testing Fine-Tuned Model - Team vs Individual Queries")
    print("=" * 60)
    
    # Check if API key is available
    if not os.getenv('OPENAI_API_KEY'):
        print("❌ OPENAI_API_KEY not set. Please set it and run again.")
        print("   Example: export OPENAI_API_KEY=your_key && python3 test_finetuned_with_llm_orchestrator.py")
        return
    
    try:
        from app.llm_orchestrator import llm_orchestrator
        
        # Check provider info
        provider_info = llm_orchestrator.get_model_provider_info()
        print(f"✅ Provider: {provider_info.get('provider_name', 'Unknown')}")
        print(f"✅ Model: {provider_info.get('model_name', 'Unknown')}")
        print(f"✅ Assistant API: {provider_info.get('use_assistant_api', 'Unknown')}")
        
        # Test the critical individual vs team distinction that was failing
        test_scenarios = [
            {
                'question': "What are Yiannis's individual QA scores this week?",
                'expected': "Individual query - should use ASSIGNEE_NAME LIKE '%Yiannis%'",
                'type': 'individual'
            },
            {
                'question': "What are the QA scores for Yiannis's team this week?", 
                'expected': "Team query - should use ASSIGNEE_SUPERVISOR LIKE '%Yiannis%'",
                'type': 'team'
            },
            {
                'question': "Show me QA scores for Sarah's team",
                'expected': "Team query - should use ASSIGNEE_SUPERVISOR LIKE '%Sarah%'",
                'type': 'team'
            },
            {
                'question': "What is Lisa's individual performance?",
                'expected': "Individual query - should use ASSIGNEE_NAME LIKE '%Lisa%'", 
                'type': 'individual'
            }
        ]
        
        print(f"\n🧪 Testing Critical Team vs Individual Patterns:")
        print("-" * 60)
        
        test_user_id = "test_user_123"
        test_channel_id = "test_channel_456"
        
        for scenario in test_scenarios:
            print(f"\n📋 Question: {scenario['question']}")
            print(f"   Expected: {scenario['expected']}")
            
            try:
                # Use the orchestrator to handle the question
                response, response_type = await llm_orchestrator.handle_question(
                    scenario['question'], test_user_id, test_channel_id
                )
                
                print(f"   Response Type: {response_type}")
                print(f"   Response (first 150 chars): {response[:150]}...")
                
                # Check if response contains SQL for analysis
                if "SELECT" in response.upper():
                    if scenario['type'] == 'team' and "ASSIGNEE_SUPERVISOR" in response:
                        print(f"   ✅ CORRECT: Team query uses ASSIGNEE_SUPERVISOR")
                    elif scenario['type'] == 'individual' and "ASSIGNEE_NAME" in response and "ASSIGNEE_SUPERVISOR" not in response:
                        print(f"   ✅ CORRECT: Individual query uses ASSIGNEE_NAME only")
                    elif scenario['type'] == 'team' and "ASSIGNEE_SUPERVISOR" not in response:
                        print(f"   ❌ INCORRECT: Team query should use ASSIGNEE_SUPERVISOR but doesn't")
                    elif scenario['type'] == 'individual' and "ASSIGNEE_SUPERVISOR" in response:
                        print(f"   ❌ INCORRECT: Individual query uses ASSIGNEE_SUPERVISOR incorrectly")
                    else:
                        print(f"   ❓ UNCLEAR: Unable to determine if pattern is correct")
                else:
                    print(f"   ℹ️  No SQL in response - may be conversational or processed result")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        print(f"\n✅ Fine-Tuned Model Team vs Individual Test Complete!")
        print(f"\n💡 Key Success Indicators:")
        print(f"   - Team queries should use ASSIGNEE_SUPERVISOR LIKE '%Name%'")
        print(f"   - Individual queries should use ASSIGNEE_NAME LIKE '%Name%'")
        print(f"   - This was the core issue fixed by the fine-tuned model")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_critical_team_vs_individual())