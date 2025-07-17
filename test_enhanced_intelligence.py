#!/usr/bin/env python3
"""
Test Enhanced Intelligence System
Test the new business intelligence capabilities with real scenarios
"""

import asyncio
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

async def test_user_specific_question():
    """Test the exact question that the user reported having issues with"""
    print("🧪 Testing User-Specific Question")
    print("=" * 80)
    
    # The exact question from the user
    question = "Who is the lowest 3 agents from team Kim for the month of June including the metrics of AHT, QA and CSAT?"
    
    print(f"📝 Question: {question}")
    print()
    
    try:
        # Test business metrics intelligence
        print("📊 Testing Business Metrics Intelligence...")
        from business_metrics_intelligence import business_metrics_intelligence
        
        context = business_metrics_intelligence.analyze_question_context(question)
        print(f"✅ Business Context: {context}")
        print()
        
        # Test SQL structure generation
        print("🔧 Testing SQL Structure Generation...")
        sql_structure = business_metrics_intelligence.generate_intelligent_sql_structure(
            question, 'RPT_WOPS_AGENT_PERFORMANCE', {}
        )
        print(f"✅ SQL Structure: {sql_structure}")
        print()
        
        # Test enhanced intelligent analyst
        print("🧠 Testing Enhanced Intelligent Analyst...")
        from intelligent_data_analyst import intelligent_data_analyst
        
        intent_analysis = {
            'question_type': 'performance_ranking',
            'required_table': 'RPT_WOPS_AGENT_PERFORMANCE',
            'confidence': 100,
            'is_personal': False,
            'personal_context': None
        }
        
        sql, explanation = await intelligent_data_analyst.generate_enhanced_intelligent_sql(
            question, intent_analysis, {}, None
        )
        
        print("✅ Enhanced SQL Generated:")
        print(f"```sql\n{sql}\n```")
        print()
        
        print("📋 Business Intelligence Explanation:")
        print(explanation)
        print()
        
        # Validate the generated SQL
        print("🔍 Validating SQL Business Logic...")
        validation = business_metrics_intelligence.validate_query_business_logic(sql, question)
        print(f"✅ Validation Result: {validation}")
        print()
        
        # Check if the SQL addresses the user's concerns
        print("🎯 Checking if SQL addresses user concerns:")
        concerns_addressed = []
        
        # Check 1: Does it aggregate data properly?
        if "GROUP BY ASSIGNEE_NAME" in sql:
            concerns_addressed.append("✅ Proper aggregation for multi-period data")
        else:
            concerns_addressed.append("❌ Missing proper aggregation")
        
        # Check 2: Does it handle AHT business logic correctly?
        if "avg_aht_minutes DESC" in sql:
            concerns_addressed.append("✅ Correct AHT business logic (DESC for lowest performing)")
        else:
            concerns_addressed.append("❌ Incorrect AHT business logic")
        
        # Check 3: Does it filter by team Kim?
        if "ASSIGNEE_SUPERVISOR ILIKE '%Kim%'" in sql:
            concerns_addressed.append("✅ Proper team filtering")
        else:
            concerns_addressed.append("❌ Missing team filtering")
        
        # Check 4: Does it filter by June?
        if "EXTRACT(MONTH FROM SOLVED_WEEK) = 6" in sql:
            concerns_addressed.append("✅ Proper month filtering")
        else:
            concerns_addressed.append("❌ Missing month filtering")
        
        # Check 5: Does it limit to 3 results?
        if "LIMIT 3" in sql:
            concerns_addressed.append("✅ Proper result limiting")
        else:
            concerns_addressed.append("❌ Missing result limiting")
        
        for concern in concerns_addressed:
            print(concern)
        
        print()
        
        # Final assessment
        successful_checks = sum(1 for c in concerns_addressed if c.startswith("✅"))
        total_checks = len(concerns_addressed)
        
        print(f"🎯 FINAL ASSESSMENT: {successful_checks}/{total_checks} checks passed")
        
        if successful_checks == total_checks:
            print("🎉 SUCCESS: All user concerns have been addressed!")
            return True
        else:
            print("⚠️ Some concerns still need attention.")
            return False
            
    except Exception as e:
        print(f"❌ Error testing user question: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_various_business_scenarios():
    """Test various business scenarios to ensure comprehensive intelligence"""
    print("\n📊 Testing Various Business Scenarios")
    print("=" * 80)
    
    test_scenarios = [
        {
            'name': 'AHT Worst Performers',
            'question': 'Who are the 5 worst performing agents by AHT in July?',
            'expected_logic': 'AHT DESC (high AHT = poor performance)'
        },
        {
            'name': 'QA Best Performers',
            'question': 'Show me the top 3 agents with highest QA scores',
            'expected_logic': 'QA_SCORE DESC (high QA = good performance)'
        },
        {
            'name': 'Multi-Metric Performance',
            'question': 'Who are the bottom 5 agents considering AHT, QA, and CSAT?',
            'expected_logic': 'Multi-metric comparison with proper business logic'
        },
        {
            'name': 'Team Performance Analysis',
            'question': 'How is team Yiannis performing in terms of AHT and QA?',
            'expected_logic': 'Team filtering with proper aggregation'
        }
    ]
    
    results = []
    
    for scenario in test_scenarios:
        print(f"\n🧪 Testing: {scenario['name']}")
        print(f"📝 Question: {scenario['question']}")
        print(f"🎯 Expected Logic: {scenario['expected_logic']}")
        
        try:
            from business_metrics_intelligence import business_metrics_intelligence
            from intelligent_data_analyst import intelligent_data_analyst
            
            # Analyze context
            context = business_metrics_intelligence.analyze_question_context(scenario['question'])
            
            # Generate SQL
            intent_analysis = {
                'question_type': 'performance_analysis',
                'required_table': 'RPT_WOPS_AGENT_PERFORMANCE',
                'confidence': 100,
                'is_personal': False
            }
            
            sql, explanation = await intelligent_data_analyst.generate_enhanced_intelligent_sql(
                scenario['question'], intent_analysis, {}, None
            )
            
            # Validate logic
            validation = business_metrics_intelligence.validate_query_business_logic(sql, scenario['question'])
            
            success = validation['is_valid'] and len(validation['errors']) == 0
            results.append({
                'scenario': scenario['name'],
                'success': success,
                'sql_preview': sql[:100] + "..." if len(sql) > 100 else sql
            })
            
            print(f"✅ Result: {'SUCCESS' if success else 'NEEDS WORK'}")
            if not success:
                print(f"⚠️ Issues: {validation['errors']}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            results.append({
                'scenario': scenario['name'],
                'success': False,
                'error': str(e)
            })
    
    # Summary
    print(f"\n📊 BUSINESS SCENARIOS SUMMARY:")
    print("=" * 80)
    
    successful_scenarios = sum(1 for r in results if r['success'])
    total_scenarios = len(results)
    
    for result in results:
        status = "✅ PASS" if result['success'] else "❌ FAIL"
        print(f"{status} {result['scenario']}")
        if not result['success'] and 'error' in result:
            print(f"    Error: {result['error']}")
    
    print(f"\n🎯 OVERALL RESULT: {successful_scenarios}/{total_scenarios} scenarios passed")
    
    return successful_scenarios == total_scenarios

async def test_data_analyzer():
    """Test the pre-query data analyzer"""
    print("\n🔍 Testing Pre-Query Data Analyzer")
    print("=" * 80)
    
    try:
        from pre_query_data_analyzer import pre_query_analyzer
        
        # Test question analysis
        question = "Who are the lowest 3 agents from team Kim for June including AHT, QA and CSAT?"
        
        analysis = pre_query_analyzer.analyze_question_data_requirements(question, 'RPT_WOPS_AGENT_PERFORMANCE')
        
        print(f"📋 Question Analysis: {analysis}")
        
        # Test comprehensive analysis
        comprehensive = pre_query_analyzer.generate_comprehensive_analysis(question, 'RPT_WOPS_AGENT_PERFORMANCE')
        
        print(f"🔬 Comprehensive Analysis: {comprehensive}")
        
        print("✅ Data analyzer working correctly")
        return True
        
    except Exception as e:
        print(f"❌ Error testing data analyzer: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all enhanced intelligence tests"""
    print("🚀 TESTING ENHANCED INTELLIGENCE SYSTEM")
    print("=" * 80)
    
    tests = [
        ("User-Specific Question", test_user_specific_question),
        ("Data Analyzer", test_data_analyzer),
        ("Business Scenarios", test_various_business_scenarios)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running {test_name} Test...")
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test failed: {e}")
            results.append((test_name, False))
    
    # Final summary
    print("\n🎯 FINAL TEST RESULTS")
    print("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n📊 SUMMARY: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! Enhanced intelligence is working correctly.")
        print("\n💡 Key Features Implemented:")
        print("• Business metrics intelligence with proper interpretation")
        print("• Smart aggregation for multi-period data")
        print("• Contextual analysis for performance queries")
        print("• Query validation with business logic")
        print("• Pre-query data structure analysis")
        return True
    else:
        print("⚠️ Some tests failed. Enhanced intelligence needs refinement.")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)