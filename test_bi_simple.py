#!/usr/bin/env python3
"""
Simple test for BI Service configuration
"""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

def test_basic_import():
    """Test basic import and configuration"""
    print("🔧 Testing BI Service Import")
    
    # Test disabled state
    os.environ['USE_BI_SERVICE'] = 'false'
    
    try:
        from app.bi_service import get_bi_service_status, should_use_bi_service
        
        status = get_bi_service_status()
        print(f"Status (disabled): {status}")
        
        should_use = should_use_bi_service("test question")
        print(f"Should use BI Service (disabled): {should_use}")
        
        # Test enabled state
        os.environ['USE_BI_SERVICE'] = 'true'
        os.environ['BI_SERVICE_PROVIDER'] = 'openai'
        
        # Reload to pick up new env vars
        import importlib
        import app.bi_service
        importlib.reload(app.bi_service)
        
        from app.bi_service import get_bi_service_status, should_use_bi_service
        
        status = get_bi_service_status()
        print(f"Status (enabled): {status}")
        
        should_use = should_use_bi_service("test question")
        print(f"Should use BI Service (enabled): {should_use}")
        
        print("✅ Basic configuration test passed")
        return True
        
    except Exception as e:
        print(f"❌ Import or configuration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_basic_import()
    print(f"\nTest result: {'PASSED' if success else 'FAILED'}")