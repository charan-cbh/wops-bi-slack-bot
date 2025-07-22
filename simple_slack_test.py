#!/usr/bin/env python3
"""
Simple test to verify deployment configuration
Run this on your deployment server to check configuration
"""
import os
from dotenv import load_dotenv

load_dotenv()

def check_deployment_config():
    """Check if all required environment variables are set"""
    print("🔍 Deployment Configuration Check")
    print("=" * 50)
    
    required_vars = {
        'USE_BI_SERVICE': os.getenv('USE_BI_SERVICE'),
        'USE_ASSISTANT_API': os.getenv('USE_ASSISTANT_API'),
        'ASSISTANT_ID': os.getenv('ASSISTANT_ID'),
        'OPENAI_VECTOR_STORE_ID': os.getenv('OPENAI_VECTOR_STORE_ID'),
        'OPENAI_API_KEY': '***' if os.getenv('OPENAI_API_KEY') else None,
        'BI_SERVICE_PROVIDER': os.getenv('BI_SERVICE_PROVIDER'),
        'BI_SERVICE_MODEL': os.getenv('BI_SERVICE_MODEL')
    }
    
    print("📋 Environment Variables:")
    all_set = True
    for key, value in required_vars.items():
        status = "✅" if value else "❌"
        print(f"{status} {key}: {value}")
        if not value:
            all_set = False
    
    return all_set

def test_imports():
    """Test if all required modules can be imported"""
    print("\n📦 Import Test:")
    
    try:
        from app.bi_service import get_bi_service_status, should_use_bi_service
        print("✅ BI Service imports OK")
        
        status = get_bi_service_status()
        print(f"✅ BI Service status: {status['configured']}")
        
        should_use = should_use_bi_service("test")
        print(f"✅ BI Service routing: {should_use}")
        
        return True
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

def create_debug_endpoint():
    """Create a debug message for Slack testing"""
    print("\n🔧 Slack Debug Commands:")
    print("Try these commands in your Slack:")
    print("1. @bot debug config")
    print("2. @bot What is Clipboard Health?")
    print("3. @bot How do Magic Shifts work?")
    print("\nExpected: Detailed responses about Clipboard Health platform")

if __name__ == "__main__":
    print("🚀 Deployment Configuration Test")
    print("Copy this script to your deployment server and run it")
    print("=" * 60)
    
    config_ok = check_deployment_config()
    imports_ok = test_imports()
    
    print("\n" + "=" * 60)
    if config_ok and imports_ok:
        print("✅ Deployment configuration looks good!")
        print("🔍 If Slack bot still not working, check:")
        print("  - Bot restart after code deployment") 
        print("  - Slack workspace permissions")
        print("  - Network connectivity to OpenAI API")
        create_debug_endpoint()
    else:
        print("❌ Deployment configuration issues found")
        print("🔧 Fix the issues above and redeploy")