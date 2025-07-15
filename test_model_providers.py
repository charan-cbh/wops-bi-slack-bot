#!/usr/bin/env python3
"""
Test script for model providers
"""

import os
import asyncio
from app.model_providers.provider_factory import get_model_provider, get_provider_info, provider_factory

def test_provider_factory():
    """Test the provider factory configuration"""
    
    print("🧪 Testing Model Provider Factory")
    print("=" * 50)
    
    # Test available providers
    available = provider_factory.get_available_providers()
    print(f"Available providers: {available}")
    
    # Test current provider detection
    current_provider = os.getenv('MODEL_PROVIDER', 'openai')
    print(f"Current provider setting: {current_provider}")
    
    # Test provider info (without creating actual provider)
    try:
        info = get_provider_info()
        print(f"Provider info: {info}")
    except Exception as e:
        print(f"Provider info error: {e}")
    
    print("\n🔄 Testing Provider Switching")
    print("-" * 30)
    
    # Test switching providers (without API keys)
    for provider_name in ['openai', 'anthropic']:
        print(f"\nTesting {provider_name} provider:")
        
        # Temporarily set provider
        original = os.getenv('MODEL_PROVIDER')
        os.environ['MODEL_PROVIDER'] = provider_name
        
        try:
            # Clear cache to force recreation
            provider_factory._provider = None
            
            # Test creation (will fail due to missing API keys, but should show proper error handling)
            provider = get_model_provider()
            print(f"  ✅ {provider_name} provider created: {provider.__class__.__name__}")
            
        except Exception as e:
            print(f"  ❌ {provider_name} provider error (expected): {e}")
        
        # Restore original
        if original:
            os.environ['MODEL_PROVIDER'] = original
        else:
            os.environ.pop('MODEL_PROVIDER', None)
    
    print("\n✅ Provider factory architecture test completed!")

async def test_provider_interfaces():
    """Test that provider interfaces work correctly"""
    
    print("\n🔍 Testing Provider Interfaces")
    print("=" * 50)
    
    # Mock test data
    test_messages = [{"role": "user", "content": "Hello"}]
    test_question = "How many agents are there?"
    test_results = "Results: 25 agents found"
    
    # Test each provider type
    for provider_name in ['openai', 'anthropic']:
        print(f"\nTesting {provider_name} interface:")
        
        os.environ['MODEL_PROVIDER'] = provider_name
        provider_factory._provider = None  # Clear cache
        
        try:
            provider = get_model_provider()
            
            # Test method existence
            methods = ['generate_response', 'generate_sql', 'summarize_results', 'classify_question', 'handle_conversational']
            for method in methods:
                if hasattr(provider, method):
                    print(f"  ✅ {method} method exists")
                else:
                    print(f"  ❌ {method} method missing")
            
            # Test token estimation
            token_count = provider.estimate_tokens("Hello world")
            print(f"  ✅ Token estimation: {token_count} tokens")
            
        except Exception as e:
            print(f"  ❌ Error testing {provider_name}: {e}")

def test_environment_configuration():
    """Test environment configuration"""
    
    print("\n⚙️ Testing Environment Configuration")
    print("=" * 50)
    
    # Check required environment variables
    env_vars = {
        'MODEL_PROVIDER': os.getenv('MODEL_PROVIDER', 'Not set'),
        'OPENAI_API_KEY': 'Set' if os.getenv('OPENAI_API_KEY') else 'Not set',
        'ANTHROPIC_API_KEY': 'Set' if os.getenv('ANTHROPIC_API_KEY') and os.getenv('ANTHROPIC_API_KEY') != 'your_anthropic_api_key_here' else 'Not set',
        'CLAUDE_MODEL': os.getenv('CLAUDE_MODEL', 'Not set'),
        'USE_ASSISTANT_API': os.getenv('USE_ASSISTANT_API', 'Not set'),
        'ASSISTANT_ID': 'Set' if os.getenv('ASSISTANT_ID') else 'Not set'
    }
    
    for var, value in env_vars.items():
        print(f"  {var}: {value}")
    
    print("\n📋 Provider Requirements:")
    print("  OpenAI: Requires OPENAI_API_KEY")
    print("  Anthropic: Requires ANTHROPIC_API_KEY and CLAUDE_MODEL")

if __name__ == "__main__":
    test_environment_configuration()
    test_provider_factory()
    asyncio.run(test_provider_interfaces())
    
    print("\n🎉 All tests completed!")
    print("\nTo enable full functionality:")
    print("1. Set MODEL_PROVIDER=anthropic in .env file")
    print("2. Add your Anthropic API key to ANTHROPIC_API_KEY in .env file")
    print("3. Optionally adjust CLAUDE_MODEL if needed")