#!/usr/bin/env python3
"""
Test script to verify Clipboard Health business context knowledge
Tests if the bot can answer questions from the support site using vector store
"""
import asyncio
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

async def test_clipboard_health_knowledge():
    """Test if bot can answer Clipboard Health business context questions"""
    print("🏥 Testing Clipboard Health Business Context Knowledge")
    print("=" * 60)
    
    # Check environment variables
    assistant_id = os.getenv("ASSISTANT_ID")
    vector_store_id = os.getenv("OPENAI_VECTOR_STORE_ID")
    use_assistant = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"
    
    print(f"Assistant ID: {assistant_id[:20]}..." if assistant_id else "Not set")
    print(f"Vector Store ID: {vector_store_id[:20]}..." if vector_store_id else "Not set")
    print(f"Use Assistant API: {use_assistant}")
    
    if not (assistant_id and vector_store_id and use_assistant):
        print("❌ Missing required configuration")
        return False
    
    # Test questions from Clipboard Health support site
    test_questions = [
        {
            "question": "What is Clipboard Health?",
            "expected_keywords": ["healthcare staffing platform", "CNAs", "LVNs", "RNs", "Wei Deng", "marketplace"],
            "category": "Platform Overview"
        },
        {
            "question": "What happens if a professional cancels within 8 hours of their shift?",
            "expected_keywords": ["urgent shifts", "premium rates", "0-8 hours", "automatic", "no additional cost"],
            "category": "Cancellation Policy"
        },
        {
            "question": "How do Magic Shifts work and what is the guarantee?",
            "expected_keywords": ["99%", "$100 invoice credit", "CNA/LVN/LPN/RN", "1 hour in advance", "limited"],
            "category": "Advanced Features"
        },
        {
            "question": "What documents are required for healthcare professionals?",
            "expected_keywords": ["driver's license", "background check", "professional license", "SSN", "selfie verification"],
            "category": "Document Requirements"
        },
        {
            "question": "I can't log into my account, what should I do?",
            "expected_keywords": ["latest app version", "Chrome browser", "hcp.clipboard.health", "spam folder", "(408) 837-0116"],
            "category": "Technical Support"
        },
        {
            "question": "How does instant payment work?",
            "expected_keywords": ["100%", "eligible", "$2.99", "debit card", "Stripe", "timesheet"],
            "category": "Payment System"
        },
        {
            "question": "Who should I contact for billing issues?",
            "expected_keywords": ["(415) 604-3272", "billing@clipboardhealth.com"],
            "category": "Contact Information"
        },
        {
            "question": "How does block booking work?",
            "expected_keywords": ["1 or more weeks", "80%", "mutual commitment", "Blocks tab", "no rate negotiation"],
            "category": "Block Booking"
        },
        {
            "question": "What is the Clipboard Score system?",
            "expected_keywords": ["attendance score", "100 points", "unlimited", "priority access", "restrictions"],
            "category": "Performance Management"
        },
        {
            "question": "How do digital timesheets work?",
            "expected_keywords": ["GPS", "NFC", "facility staff signature", "location services", "geofencing"],
            "category": "Digital Timesheets"
        }
    ]
    
    try:
        from app.llm_orchestrator import handle_question
        
        results = []
        
        for i, test in enumerate(test_questions, 1):
            print(f"\n--- Test {i}: {test['category']} ---")
            print(f"Question: {test['question']}")
            
            try:
                response, response_type = await handle_question(
                    test['question'],
                    "test_user",
                    "test_channel", 
                    assistant_id
                )
                
                print(f"Response Type: {response_type}")
                print(f"Response Length: {len(response)} characters")
                
                # Check if response contains expected keywords
                found_keywords = []
                for keyword in test['expected_keywords']:
                    if keyword.lower() in response.lower():
                        found_keywords.append(keyword)
                
                keyword_score = len(found_keywords) / len(test['expected_keywords'])
                
                # Evaluate response quality
                if keyword_score >= 0.4:  # At least 40% of keywords found
                    print(f"✅ EXCELLENT - Found {len(found_keywords)}/{len(test['expected_keywords'])} keywords")
                    results.append(('excellent', test['category']))
                elif keyword_score >= 0.2:  # At least 20% of keywords found
                    print(f"🟡 PARTIAL - Found {len(found_keywords)}/{len(test['expected_keywords'])} keywords")
                    results.append(('partial', test['category']))
                else:
                    print(f"❌ POOR - Found {len(found_keywords)}/{len(test['expected_keywords'])} keywords")
                    results.append(('poor', test['category']))
                
                if found_keywords:
                    print(f"   Keywords found: {', '.join(found_keywords[:3])}...")
                
                # Show response preview
                preview = response[:300].replace('\n', ' ')
                print(f"   Preview: {preview}...")
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
                results.append(('error', test['category']))
            
            # Small delay between requests
            await asyncio.sleep(1)
        
        return results
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return []

async def test_bi_service_clipboard_integration():
    """Test if BI service can handle Clipboard Health questions"""
    print("\n🔧 Testing BI Service with Clipboard Health Context")
    print("=" * 60)
    
    try:
        from app.bi_service import get_bi_service, should_use_bi_service, process_with_bi_service
        
        # Enable BI Service for test
        os.environ['USE_BI_SERVICE'] = 'true'
        
        bi_questions = [
            "What is Clipboard Health's business model?",
            "Explain how urgent shifts reduce facility costs",
            "How does the platform ensure compliance with healthcare regulations?",
        ]
        
        for question in bi_questions:
            print(f"\n--- BI Test: {question} ---")
            
            should_use = should_use_bi_service(question)
            print(f"Should use BI Service: {should_use}")
            
            if should_use:
                try:
                    response, response_type = await process_with_bi_service(
                        question,
                        "test_user",
                        "test_channel"
                    )
                    
                    print(f"Response Type: {response_type}")
                    print(f"Response Length: {len(response)} characters")
                    print(f"Preview: {response[:200]}...")
                    
                except Exception as e:
                    print(f"❌ BI Service error: {e}")
            
            await asyncio.sleep(1)
        
        return True
        
    except Exception as e:
        print(f"❌ BI Service test failed: {e}")
        return False

async def main():
    """Run all Clipboard Health context tests"""
    print("🚀 Clipboard Health Business Context Test Suite")
    print("=" * 80)
    
    # Test 1: Knowledge base retrieval
    context_results = await test_clipboard_health_knowledge()
    
    # Test 2: BI Service integration
    bi_result = await test_bi_service_clipboard_integration()
    
    print("\n" + "=" * 80)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 80)
    
    if context_results:
        excellent = sum(1 for result, _ in context_results if result == 'excellent')
        partial = sum(1 for result, _ in context_results if result == 'partial')
        poor = sum(1 for result, _ in context_results if result == 'poor')
        errors = sum(1 for result, _ in context_results if result == 'error')
        
        total = len(context_results)
        success_rate = (excellent + partial) / total * 100 if total > 0 else 0
        
        print(f"📈 Knowledge Base Test Results:")
        print(f"  ✅ Excellent responses: {excellent}/{total}")
        print(f"  🟡 Partial responses: {partial}/{total}")
        print(f"  ❌ Poor responses: {poor}/{total}")
        print(f"  🔥 Errors: {errors}/{total}")
        print(f"  📊 Success Rate: {success_rate:.1f}%")
        
        print(f"\n📋 Category Breakdown:")
        categories = {}
        for result, category in context_results:
            if category not in categories:
                categories[category] = []
            categories[category].append(result)
        
        for category, results in categories.items():
            excellent_cat = results.count('excellent')
            partial_cat = results.count('partial')
            total_cat = len(results)
            print(f"  • {category}: {excellent_cat + partial_cat}/{total_cat} successful")
        
        print(f"\n🎯 Overall Assessment:")
        if success_rate >= 80:
            print("🎉 EXCELLENT - Bot has comprehensive Clipboard Health knowledge!")
            print("✅ Vector store integration is working properly")
            print("✅ Business context is being retrieved accurately")
        elif success_rate >= 60:
            print("🟡 GOOD - Bot has solid Clipboard Health knowledge with some gaps")
            print("⚠️ Some improvement needed in vector store retrieval")
        elif success_rate >= 40:
            print("⚠️ FAIR - Bot has basic Clipboard Health knowledge but needs improvement")
            print("🔧 Vector store may need optimization or more comprehensive content")
        else:
            print("❌ POOR - Bot lacks sufficient Clipboard Health knowledge")
            print("🔧 Check vector store configuration and content quality")
    
    bi_status = "✅ Working" if bi_result else "❌ Issues"
    print(f"\n🔧 BI Service Integration: {bi_status}")
    
    return success_rate >= 60 if context_results else False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)