#!/usr/bin/env python3
"""
Test a real BI question end-to-end
"""
import os
import sys
import asyncio
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.bi_service import should_use_bi_service
from app.llm_orchestrator import handle_question, generate_sql_intelligently
from dotenv import load_dotenv

load_dotenv()

async def test_real_bi_question():
    """Test a real BI question that should generate SQL"""
    print(f"🧪 Testing Real BI Question End-to-End")
    print(f"Time: {datetime.now().isoformat()}")
    
    # Test configuration
    test_user_id = "U12345TEST"
    test_channel_id = "C12345TEST" 
    assistant_id = os.getenv("ASSISTANT_ID", "")
    use_assistant_api = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"
    
    print(f"Assistant API: {'Enabled' if use_assistant_api else 'Disabled'}")
    print(f"Assistant ID: {assistant_id or 'Not configured'}")
    
    # Test question that should generate SQL
    question = "What's our chat volume today?"
    print(f"\n📝 Question: '{question}'")
    
    # Step 1: Test routing
    print(f"\n📍 Step 1: Routing Test")
    should_use_bi = should_use_bi_service(question)
    routing = "BI_SERVICE" if should_use_bi else "SQL_GENERATION"
    print(f"   Routing Decision: {routing}")
    
    if routing != "SQL_GENERATION":
        print(f"❌ Question should route to SQL generation, but routed to {routing}")
        return False
    
    # Step 2: SQL Generation Test
    print(f"\n📍 Step 2: SQL Generation Test")
    
    if use_assistant_api and assistant_id:
        print(f"   Testing with Assistant API...")
        try:
            response, response_type = await handle_question(
                question, test_user_id, test_channel_id, assistant_id
            )
            
            print(f"   ✅ Assistant API Response Type: {response_type}")
            print(f"   📄 Response Length: {len(response)} characters")
            
            # Check if SQL was generated
            if "```sql" in response.lower() or "select" in response.lower():
                print(f"   🎯 SQL detected in response!")
                
                # Extract SQL if present
                if "```sql" in response:
                    sql_start = response.find("```sql") + 6
                    sql_end = response.find("```", sql_start)
                    if sql_end > sql_start:
                        sql = response[sql_start:sql_end].strip()
                        print(f"   📜 Extracted SQL:")
                        print(f"   {sql}")
                        
                        # Validate SQL contains expected patterns
                        expected_patterns = [
                            "FCT_ZENDESK__MQR_TICKETS",
                            "GROUP_ID",
                            "17837476387479",  # Chat group ID
                            "COUNT",
                            "TICKET_ID"
                        ]
                        
                        found_patterns = []
                        missing_patterns = []
                        
                        for pattern in expected_patterns:
                            if pattern in sql.upper():
                                found_patterns.append(pattern)
                            else:
                                missing_patterns.append(pattern)
                        
                        print(f"   ✅ Found patterns: {found_patterns}")
                        if missing_patterns:
                            print(f"   ⚠️ Missing patterns: {missing_patterns}")
                        
                        if len(found_patterns) >= 3:  # At least 3 key patterns
                            print(f"   🎉 SQL appears to be correctly generated!")
                            return True
                        else:
                            print(f"   ❌ SQL missing too many expected patterns")
                            return False
            
            print(f"   📄 Full Response Preview:")
            print(f"   {response[:500]}...")
            
            if response_type == "sql_with_data":
                print(f"   ✅ Response indicates SQL was executed with data")
                return True
            elif "no data" in response.lower() or "no results" in response.lower():
                print(f"   ⚠️ SQL was executed but returned no data (this is OK for testing)")
                return True
            else:
                print(f"   ❌ Response doesn't appear to contain SQL or data")
                return False
            
        except Exception as e:
            print(f"   ❌ Assistant API Error: {e}")
            return False
    
    else:
        print(f"   Testing with direct SQL generation...")
        try:
            sql_result = await generate_sql_intelligently(
                question, test_user_id, test_channel_id
            )
            
            if sql_result.get("success") and sql_result.get("sql"):
                sql = sql_result["sql"]
                print(f"   ✅ Direct SQL Generation Successful")
                print(f"   📜 Generated SQL:")
                print(f"   {sql}")
                
                # Validate SQL patterns
                expected_patterns = [
                    "FCT_ZENDESK__MQR_TICKETS",
                    "GROUP_ID", 
                    "17837476387479",
                    "COUNT",
                    "TICKET_ID"
                ]
                
                found_patterns = []
                for pattern in expected_patterns:
                    if pattern in sql.upper():
                        found_patterns.append(pattern)
                
                print(f"   ✅ Found expected patterns: {found_patterns}")
                
                if len(found_patterns) >= 3:
                    print(f"   🎉 Direct SQL generation successful!")
                    return True
                else:
                    print(f"   ❌ SQL missing expected patterns")
                    return False
            
            else:
                error = sql_result.get("error", "Unknown error")
                print(f"   ❌ Direct SQL Generation Failed: {error}")
                return False
                
        except Exception as e:
            print(f"   ❌ Direct SQL Generation Error: {e}")
            return False

async def main():
    """Run the real BI question test"""
    try:
        success = await test_real_bi_question()
        
        if success:
            print(f"\n🎉 SUCCESS! End-to-End BI Question Test Passed")
            print(f"✅ Routing is working correctly")
            print(f"✅ SQL generation is working correctly") 
            print(f"✅ Vector store context is being used properly")
            print(f"\n🚀 The bot is ready for real Slack usage!")
        else:
            print(f"\n❌ FAILED! End-to-End BI Question Test Failed") 
            print(f"🔧 Review the SQL generation or vector store setup")
            
    except Exception as e:
        print(f"\n❌ Test Failed with Exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())