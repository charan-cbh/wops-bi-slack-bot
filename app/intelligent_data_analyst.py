#!/usr/bin/env python3
"""
Enhanced Intelligent Data Analyst System
Acts as a business intelligence expert with deep understanding of data relationships,
proper metrics interpretation, and smart aggregation capabilities
"""

import re
from typing import Dict, List, Any, Tuple, Optional


class IntelligentDataAnalyst:
    """
    Enhanced data analyst system that understands business context,
    table relationships, generates accurate SQL queries with proper metrics interpretation,
    and smart aggregation for multi-period data
    """
    
    def __init__(self):
        # Import enhanced intelligence systems
        try:
            from app.business_metrics_intelligence import business_metrics_intelligence
            from app.pre_query_data_analyzer import pre_query_analyzer
            self.business_metrics = business_metrics_intelligence
            self.data_analyzer = pre_query_analyzer
            self.enhanced_intelligence = True
            print("✅ Enhanced intelligence systems loaded successfully")
        except ImportError as e:
            print(f"⚠️ Enhanced intelligence systems not available: {e}")
            self.enhanced_intelligence = False
        # Table relationships and purposes based on schema analysis
        self.table_intelligence = {
            'RPT_WOPS_AGENT_PERFORMANCE': {
                'purpose': 'Agent performance metrics with supervisor relationships',
                'best_for': ['agent_count', 'team_questions', 'supervisor_questions', 'agent_performance'],
                'key_columns': {
                    'agent_identifier': 'ASSIGNEE_NAME',
                    'supervisor': 'ASSIGNEE_SUPERVISOR', 
                    'metrics': ['NUM_TICKETS', 'AHT_MINUTES', 'FCR_PERCENTAGE', 'QA_SCORE'],
                    'time_dimension': 'SOLVED_WEEK'
                },
                'data_grain': 'agent_week',
                'business_context': 'Pre-aggregated weekly agent performance with supervisor relationships'
            },
            
            'RPT_WOPS_TL_PERFORMANCE': {
                'purpose': 'Team lead/supervisor performance metrics',
                'best_for': ['supervisor_performance', 'team_lead_metrics', 'management_analytics'],
                'key_columns': {
                    'supervisor': 'SUPERVISOR',
                    'metrics': ['NUM_TICKETS', 'AHT_MINUTES', 'FCR_PERCENTAGE', 'QA_SCORE'],
                    'time_dimension': 'SOLVED_WEEK'
                },
                'data_grain': 'supervisor_week',
                'business_context': 'Pre-aggregated weekly team lead performance'
            },
            
            'ZENDESK_TICKET_AGENT__HANDLE_TIME': {
                'purpose': 'Detailed ticket-level agent activity with handle times',
                'best_for': ['handle_time', 'efficiency', 'detailed_agent_activity'],
                'key_columns': {
                    'agent_identifier': 'USER_NAME',
                    'supervisor': 'SUPERVISOR',
                    'metrics': ['HANDLE_TIME_IN_MINUTES', 'AMAZON_CONNECT_CALL_DURATION_IN_MINUTES'],
                    'time_dimension': 'CREATED_AT_PST'
                },
                'data_grain': 'ticket_agent',
                'business_context': 'Ticket-level detail for handle time and efficiency analysis'
            },
            
            'RPT_WOPS_TICKETS': {
                'purpose': 'Comprehensive ticket analytics with response times',
                'best_for': ['ticket_volume', 'response_time', 'ticket_analytics'],
                'key_columns': {
                    'agent_identifier': 'AGENT_NAME',
                    'metrics': ['REPLY_TIME_IN_MINUTES', 'FIRST_RESOLUTION_TIME_IN_MINUTES'],
                    'time_dimension': 'CREATED_AT_PST'
                },
                'data_grain': 'ticket',
                'business_context': 'Pre-calculated response times and ticket metrics'
            }
        }
        
        # Question type intelligence
        self.question_intelligence = {
            'schedule_adherence': {
                'keywords': ['schedule adherence', 'adherence rate', 'schedule compliance', 'schedule variance', 'offline time', 'break adherence', 'schedule patterns', 'adherence trends', 'schedule analysis', 'schedule performance', 'adherence metrics', 'schedule monitoring', 'schedule effectiveness', 'adherence dashboard', 'adherence comparison', 'schedule following', 'time tracking', 'work schedule', 'attendance patterns', 'adherent', 'adherence'],
                'required_table': 'RPT_AGENT_SCHEDULE_ADHERENCE',
                'required_columns': ['AGENT_NAME', 'ADHERENCE_PERCENTAGE', 'SCHEDULED_MINUTES', 'ADHERENT_MINUTES', 'OFFLINE_MINUTES'],
                'aggregation': 'AVG(CAST(ADHERENCE_PERCENTAGE AS FLOAT))',
                'business_logic': 'Schedule adherence analysis requires schedule compliance data with JOIN to performance table for supervisor info'
            },
            
            'agent_count': {
                'keywords': ['how many agents', 'count agents', 'number of agents', 'agent count'],
                'required_table': 'RPT_WOPS_AGENT_PERFORMANCE',
                'required_columns': ['ASSIGNEE_NAME'],
                'aggregation': 'COUNT(DISTINCT ASSIGNEE_NAME)',
                'business_logic': 'Agent counting requires DISTINCT because agents may have multiple weeks of data'
            },
            
            'team_agent_count': {
                'keywords': ["'s team", 'how many agents', 'agents work', 'agents in team'],
                'required_table': 'RPT_WOPS_AGENT_PERFORMANCE', 
                'required_columns': ['ASSIGNEE_SUPERVISOR', 'ASSIGNEE_NAME'],
                'aggregation': 'COUNT(DISTINCT ASSIGNEE_NAME)',
                'business_logic': 'Team agent counting needs supervisor-agent relationship data with DISTINCT'
            },
            
            'team_questions': {
                'keywords': ['team', 'supervisor', 'manager', 'reports to', 'under'],
                'required_table': 'RPT_WOPS_AGENT_PERFORMANCE', 
                'required_columns': ['ASSIGNEE_SUPERVISOR', 'ASSIGNEE_NAME'],
                'filter_logic': 'possessive_supervisor_matching',
                'business_logic': 'Team questions need agent-supervisor relationship data'
            },
            
            'handle_time': {
                'keywords': ['handle time', 'aht', 'efficiency', 'average handle time'],
                'required_table': 'ZENDESK_TICKET_AGENT__HANDLE_TIME',
                'required_columns': ['HANDLE_TIME_IN_MINUTES', 'USER_NAME'],
                'aggregation': 'AVG(HANDLE_TIME_IN_MINUTES)',
                'business_logic': 'Handle time analysis needs ticket-level detail'
            },
            
            'response_time': {
                'keywords': ['response time', 'reply time', 'resolution time', 'sla'],
                'required_table': 'RPT_WOPS_TICKETS',
                'required_columns': ['REPLY_TIME_IN_MINUTES', 'FIRST_RESOLUTION_TIME_IN_MINUTES'],
                'aggregation': 'AVG(REPLY_TIME_IN_MINUTES)',
                'business_logic': 'Response times are pre-calculated in tickets table'
            }
        }
    
    def analyze_question_intent(self, question: str) -> Dict[str, Any]:
        """Analyze question with business intelligence to determine optimal approach"""
        
        question_lower = question.lower()
        
        analysis = {
            'primary_intent': None,
            'question_type': None,
            'required_table': None,
            'required_columns': [],
            'business_context': '',
            'confidence': 0,
            'supervisor_context': False,
            'agent_context': False,
            'count_query': False
        }
        
        # Detect question type with high precision
        max_confidence = 0
        
        for intent_type, config in self.question_intelligence.items():
            confidence = 0
            matched_keywords = []
            
            for keyword in config['keywords']:
                if keyword in question_lower:
                    confidence += 100
                    matched_keywords.append(keyword)
            
            if confidence > max_confidence:
                max_confidence = confidence
                analysis['primary_intent'] = intent_type
                analysis['question_type'] = intent_type
                analysis['required_table'] = config['required_table']
                analysis['required_columns'] = config['required_columns']
                analysis['business_context'] = config['business_logic']
                analysis['confidence'] = confidence
                analysis['matched_keywords'] = matched_keywords
        
        # Enhanced context detection
        analysis['supervisor_context'] = "'s team" in question_lower or any(
            word in question_lower for word in ['supervisor', 'manager', 'reports to', 'under']
        )
        analysis['agent_context'] = 'agent' in question_lower
        analysis['count_query'] = any(phrase in question_lower for phrase in ['how many', 'count', 'number of'])
        
        return analysis
    
    def extract_supervisor_from_possessive(self, question: str) -> str:
        """Extract supervisor name from possessive constructs"""
        patterns = [
            r"([A-Z][a-z]+\s+[A-Z][a-z]+)'s\s+team",
            r"work\s+(?:in|for|under)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'s\s+team",
            r"agents?\s+(?:in|under|for)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'s\s+team",
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            if matches:
                return matches[0]
        
        return None
    
    def extract_supervisor_from_team_question(self, question: str) -> str:
        """Extract supervisor name from team questions like 'team Yiannis' or 'for team Yiannis'"""
        patterns = [
            r"team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:between|for|in|on|during|over|within)",
            r"team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)$",
            r"for\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:between|for|in|on|during|over|within)",
            r"for\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)$",
            r"in\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:between|for|in|on|during|over|within)",
            r"in\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)$",
            r"under\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:between|for|in|on|during|over|within)",
            r"under\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)$",
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            if matches:
                return matches[0]
        
        return None
    
    async def generate_enhanced_intelligent_sql(self, question: str, intent_analysis: Dict, schema: Dict, user_id: str = None) -> Tuple[str, str]:
        """
        Generate SQL with enhanced business intelligence, proper metrics interpretation,
        and smart aggregation for multi-period data
        """
        if not self.enhanced_intelligence:
            # Fallback to original method if enhanced systems not available
            return await self.generate_intelligent_sql(question, intent_analysis, schema, user_id)
        
        print(f"🧠 Enhanced Intelligence: Analyzing question with business context...")
        
        # Step 1: Analyze business context and metrics
        business_context = self.business_metrics.analyze_question_context(question)
        print(f"📊 Business Context: {business_context}")
        
        # Step 2: Determine target table
        required_table = intent_analysis.get('required_table', 'RPT_WOPS_AGENT_PERFORMANCE')
        
        # Step 3: Analyze data structure requirements
        data_requirements = self.data_analyzer.analyze_question_data_requirements(question, required_table)
        print(f"📋 Data Requirements: {data_requirements}")
        
        # Step 4: Generate intelligent SQL structure
        sql_structure = self.business_metrics.generate_intelligent_sql_structure(question, required_table, schema)
        print(f"🔧 SQL Structure: {sql_structure}")
        
        # Step 5: Handle specific question types with enhanced intelligence
        if self._is_performance_ranking_question(question, business_context):
            return await self._generate_performance_ranking_sql(question, business_context, sql_structure, required_table)
        elif self._is_team_analysis_question(question, business_context):
            return await self._generate_team_analysis_sql(question, business_context, sql_structure, required_table)
        elif self._is_metrics_comparison_question(question, business_context):
            return await self._generate_metrics_comparison_sql(question, business_context, sql_structure, required_table)
        else:
            # Default enhanced SQL generation
            return await self._generate_default_enhanced_sql(question, business_context, sql_structure, required_table)
    
    def _is_performance_ranking_question(self, question: str, context: Dict) -> bool:
        """Check if question is asking for performance ranking"""
        return (context['ranking_requested'] and 
                context['performance_context'] in ['poor_performance', 'good_performance'] and
                len(context['metrics_mentioned']) > 0)
    
    def _is_team_analysis_question(self, question: str, context: Dict) -> bool:
        """Check if question is asking for team analysis"""
        return context['team_context'] is not None
    
    def _is_metrics_comparison_question(self, question: str, context: Dict) -> bool:
        """Check if question is asking for metrics comparison"""
        return len(context['metrics_mentioned']) > 1
    
    async def _generate_performance_ranking_sql(self, question: str, context: Dict, sql_structure: Dict, table: str) -> Tuple[str, str]:
        """Generate SQL for performance ranking questions with proper business logic"""
        
        # Extract team context if present
        team_filter = ""
        if context['team_context']:
            team_filter = f"AND ASSIGNEE_SUPERVISOR ILIKE '%{context['team_context']}%'"
        
        # Extract time context if present
        time_filter = ""
        if context['time_period'] == 'monthly':
            # Extract month from question
            month_match = re.search(r'(january|february|march|april|may|june|july|august|september|october|november|december)', question.lower())
            if month_match:
                month_name = month_match.group(1)
                month_number = {
                    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
                    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
                }.get(month_name, 6)  # Default to June if not found
                time_filter = f"AND EXTRACT(MONTH FROM SOLVED_WEEK) = {month_number} AND EXTRACT(YEAR FROM SOLVED_WEEK) = 2025"
        
        # Build SELECT clause with proper aggregation
        select_columns = ["ASSIGNEE_NAME"]
        metrics_columns = []
        
        for metric in context['metrics_mentioned']:
            if metric == 'aht':
                metrics_columns.append("AVG(AHT_MINUTES) as avg_aht_minutes")
            elif metric == 'qa_score':
                metrics_columns.append("AVG(QA_SCORE) as avg_qa_score")
            elif metric == 'csat':
                metrics_columns.append("AVG(POSITIVE_RES_CSAT * 100) as avg_csat_percentage")
            elif metric == 'fcr':
                metrics_columns.append("AVG(FCR_PERCENTAGE * 100) as avg_fcr_percentage")
        
        # Build ORDER BY clause with proper business logic
        order_by_parts = []
        
        for metric in context['metrics_mentioned']:
            if metric == 'aht':
                # For AHT, "lowest performing" means HIGHEST values (more time = worse performance)
                if context['performance_context'] == 'poor_performance':
                    order_by_parts.append("avg_aht_minutes DESC")
                else:
                    order_by_parts.append("avg_aht_minutes ASC")
            elif metric in ['qa_score', 'csat', 'fcr']:
                # For QA, CSAT, FCR, "lowest performing" means LOWEST values
                if context['performance_context'] == 'poor_performance':
                    order_by_parts.append(f"avg_{metric} ASC")
                else:
                    order_by_parts.append(f"avg_{metric} DESC")
        
        # Determine limit
        limit_clause = ""
        if sql_structure.get('limit_clause'):
            limit_clause = f"LIMIT {sql_structure['limit_clause']}"
        
        # Build final SQL
        sql = f"""
SELECT 
    {', '.join(select_columns + metrics_columns)}
FROM ANALYTICS.DBT_PRODUCTION.{table}
WHERE ASSIGNEE_NAME IS NOT NULL
{team_filter}
{time_filter}
GROUP BY ASSIGNEE_NAME
ORDER BY {', '.join(order_by_parts)}
{limit_clause}
""".strip()
        
        # Generate business explanation
        business_explanation = f"""
🧠 ENHANCED BUSINESS INTELLIGENCE APPLIED:

📊 QUESTION ANALYSIS:
✅ Type: Performance ranking for {context['performance_context']}
✅ Metrics: {', '.join(context['metrics_mentioned'])}
✅ Team Context: {context['team_context'] or 'All teams'}
✅ Time Period: {context['time_period'] or 'All time'}

🔍 DATA INTELLIGENCE:
✅ Multi-period data detected - using proper aggregation
✅ GROUP BY ASSIGNEE_NAME to avoid duplicate agent entries
✅ AVG() aggregation for accurate performance metrics

⚡ BUSINESS LOGIC APPLIED:
{'✅ AHT Logic: Higher values = WORSE performance (sorted DESC for lowest performing)' if 'aht' in context['metrics_mentioned'] else ''}
{'✅ QA/CSAT Logic: Higher values = BETTER performance (sorted ASC for lowest performing)' if any(m in context['metrics_mentioned'] for m in ['qa_score', 'csat']) else ''}

🎯 CRITICAL INTELLIGENCE:
• This query addresses the exact issue where same agent appears multiple times
• Proper aggregation ensures accurate performance comparison
• Business logic ensures "lowest performing" is interpreted correctly for each metric

📈 BUSINESS CONTEXT:
{sql_structure.get('business_explanation', 'Performance analysis with proper business logic')}

⚠️ CRITICAL WARNINGS:
{chr(10).join(sql_structure.get('critical_warnings', []))}
"""
        
        return sql, business_explanation
    
    async def _generate_team_analysis_sql(self, question: str, context: Dict, sql_structure: Dict, table: str) -> Tuple[str, str]:
        """Generate SQL for team analysis questions"""
        
        team_filter = f"ASSIGNEE_SUPERVISOR ILIKE '%{context['team_context']}%'"
        
        # Build comprehensive team analysis
        sql = f"""
SELECT 
    ASSIGNEE_NAME,
    AVG(AHT_MINUTES) as avg_aht_minutes,
    AVG(QA_SCORE) as avg_qa_score,
    AVG(POSITIVE_RES_CSAT * 100) as avg_csat_percentage,
    AVG(FCR_PERCENTAGE * 100) as avg_fcr_percentage,
    SUM(NUM_TICKETS) as total_tickets,
    COUNT(DISTINCT SOLVED_WEEK) as weeks_active
FROM ANALYTICS.DBT_PRODUCTION.{table}
WHERE ASSIGNEE_NAME IS NOT NULL
AND {team_filter}
GROUP BY ASSIGNEE_NAME
ORDER BY avg_aht_minutes DESC, avg_qa_score ASC, avg_csat_percentage ASC
""".strip()
        
        business_explanation = f"""
🧠 ENHANCED TEAM ANALYSIS:

👥 TEAM CONTEXT: {context['team_context']}
📊 COMPREHENSIVE METRICS: AHT, QA, CSAT, FCR, Volume, Activity
🔄 SMART AGGREGATION: Proper averaging across multiple weeks
⚡ BUSINESS LOGIC: Sorted by performance indicators (AHT DESC = worst first)
"""
        
        return sql, business_explanation
    
    async def _generate_metrics_comparison_sql(self, question: str, context: Dict, sql_structure: Dict, table: str) -> Tuple[str, str]:
        """Generate SQL for metrics comparison questions"""
        
        # Build comparison query with all requested metrics
        metrics_columns = []
        
        for metric in context['metrics_mentioned']:
            if metric == 'aht':
                metrics_columns.append("AVG(AHT_MINUTES) as avg_aht_minutes")
            elif metric == 'qa_score':
                metrics_columns.append("AVG(QA_SCORE) as avg_qa_score")
            elif metric == 'csat':
                metrics_columns.append("AVG(POSITIVE_RES_CSAT * 100) as avg_csat_percentage")
            elif metric == 'fcr':
                metrics_columns.append("AVG(FCR_PERCENTAGE * 100) as avg_fcr_percentage")
        
        sql = f"""
SELECT 
    ASSIGNEE_NAME,
    {', '.join(metrics_columns)}
FROM ANALYTICS.DBT_PRODUCTION.{table}
WHERE ASSIGNEE_NAME IS NOT NULL
GROUP BY ASSIGNEE_NAME
ORDER BY ASSIGNEE_NAME
""".strip()
        
        business_explanation = f"""
🧠 ENHANCED METRICS COMPARISON:

📊 METRICS ANALYZED: {', '.join(context['metrics_mentioned'])}
🔄 SMART AGGREGATION: Proper averaging for accurate comparison
⚡ BUSINESS LOGIC: All metrics included for comprehensive analysis
"""
        
        return sql, business_explanation
    
    async def _generate_default_enhanced_sql(self, question: str, context: Dict, sql_structure: Dict, table: str) -> Tuple[str, str]:
        """Generate default enhanced SQL with business intelligence"""
        
        # Use the original method as fallback but with enhanced explanation
        original_sql, original_explanation = await self.generate_intelligent_sql(question, {'required_table': table}, {})
        
        enhanced_explanation = f"""
🧠 ENHANCED INTELLIGENCE FALLBACK:

📊 BUSINESS CONTEXT: {context.get('business_intelligence', {}).get('business_context', 'Standard analysis')}
⚡ ORIGINAL ANALYSIS: {original_explanation}

🔄 ENHANCEMENTS APPLIED:
• Business metrics understanding
• Data structure analysis
• Performance context interpretation
"""
        
        return original_sql, enhanced_explanation
    
    async def generate_intelligent_sql(self, question: str, intent_analysis: Dict, schema: Dict, user_id: str = None) -> str:
        """Generate SQL with business intelligence and data understanding"""
        
        # Handle personal context if user_id is provided
        user_context = None
        user_filters = {}
        if user_id:
            try:
                from app.user_context_manager import get_user_context, get_personal_filters, ENABLE_USER_RECOGNITION
                if ENABLE_USER_RECOGNITION:
                    user_context = await get_user_context(user_id)
                    user_filters = await get_personal_filters(user_id)
                else:
                    print("⚠️ User recognition disabled - personal context not available")
            except ImportError:
                print("⚠️ User context manager not available")
        
        # Handle different intent analysis structures
        question_type = intent_analysis.get('question_type') or intent_analysis.get('primary_intent')
        required_table = intent_analysis.get('required_table')
        is_personal = intent_analysis.get('is_personal', False)
        personal_context = intent_analysis.get('personal_context', None)
        
        # If intent comes from question analyzer (has is_personal), prioritize personal context
        if is_personal:
            question_type = 'personal_question'
        
        # Handle PERSONAL CONTEXT questions first
        if (is_personal and user_context) or question_type == 'personal_question':
            return self._generate_personal_sql(question, intent_analysis, schema, user_context, user_filters)
        
        if question_type == 'team_agent_count' or (question_type in ['agent_count', 'team_questions'] and "'s team" in question.lower()):
            # Agent counting under supervisor - HIGHEST PRECISION
            supervisor = self.extract_supervisor_from_possessive(question)
            
            if supervisor:
                sql = f"""
SELECT COUNT(DISTINCT ASSIGNEE_NAME) as agent_count
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
WHERE ASSIGNEE_SUPERVISOR ILIKE '%{supervisor}%'
AND ASSIGNEE_NAME IS NOT NULL
AND ASSIGNEE_SUPERVISOR IS NOT NULL
"""
                
                business_explanation = f"""
BUSINESS INTELLIGENCE APPLIED:
✅ TABLE: RPT_WOPS_AGENT_PERFORMANCE (agent-supervisor relationships)
✅ METRIC: COUNT(DISTINCT ASSIGNEE_NAME) (avoids duplicate weeks)
✅ FILTER: ASSIGNEE_SUPERVISOR ILIKE '%{supervisor}%' (fuzzy supervisor matching)
✅ CONTEXT: Possessive form "{supervisor}'s team" indicates supervisor relationship

BUSINESS LOGIC:
- Agent counting requires DISTINCT because agents have multiple week records
- Agent-supervisor relationships are in RPT_WOPS_AGENT_PERFORMANCE table
- ASSIGNEE_SUPERVISOR column contains supervisor names
- ASSIGNEE_NAME contains agent names
- This gives actual team composition, not ticket volume

CRITICAL CORRECTION:
- Previous bot used TICKET_USER_ID which counts tickets (wrong!)
- Previous bot used wrong table (ZENDESK_TICKET_AGENT__HANDLE_TIME)
- This corrects to proper agent counting from performance table
"""
                
                return sql.strip(), business_explanation
        
        elif question_type == 'handle_time':
            # Handle time analysis
            sql = f"""
SELECT 
    USER_NAME,
    SUPERVISOR,
    AVG(HANDLE_TIME_IN_MINUTES) as avg_handle_time,
    COUNT(*) as tickets_handled
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE HANDLE_TIME_IN_MINUTES IS NOT NULL
GROUP BY USER_NAME, SUPERVISOR
ORDER BY avg_handle_time
"""
            
            business_explanation = """
BUSINESS INTELLIGENCE APPLIED:
✅ TABLE: ZENDESK_TICKET_AGENT__HANDLE_TIME (ticket-level detail)
✅ METRIC: AVG(HANDLE_TIME_IN_MINUTES) (efficiency measurement)
✅ GRAIN: Ticket-level data for precise handle time analysis
"""
            
            return sql.strip(), business_explanation
        
        elif question_type == 'response_time':
            # Response time analysis
            sql = f"""
SELECT 
    AVG(REPLY_TIME_IN_MINUTES) as avg_response_time,
    AVG(FIRST_RESOLUTION_TIME_IN_MINUTES) as avg_resolution_time,
    COUNT(*) as total_tickets
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE REPLY_TIME_IN_MINUTES IS NOT NULL
AND CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7
"""
            
            business_explanation = """
BUSINESS INTELLIGENCE APPLIED:
✅ TABLE: RPT_WOPS_TICKETS (pre-calculated response times)
✅ METRICS: REPLY_TIME_IN_MINUTES, FIRST_RESOLUTION_TIME_IN_MINUTES
✅ TIME FILTER: Last 7 days in PST timezone
"""
            
            return sql.strip(), business_explanation
        
        elif 'improvement' in question.lower() and ('qa' in question.lower() or 'quality' in question.lower()):
            # QA improvement analysis
            supervisor = self.extract_supervisor_from_possessive(question) or self.extract_supervisor_from_team_question(question)
            
            if supervisor:
                sql = f"""
WITH agent_qa_trend AS (
    SELECT 
        ASSIGNEE_NAME,
        ASSIGNEE_SUPERVISOR,
        SOLVED_WEEK,
        QA_SCORE,
        LAG(QA_SCORE, 1) OVER (PARTITION BY ASSIGNEE_NAME ORDER BY SOLVED_WEEK) as prev_qa_score,
        LAG(QA_SCORE, 2) OVER (PARTITION BY ASSIGNEE_NAME ORDER BY SOLVED_WEEK) as prev_2_qa_score,
        ROW_NUMBER() OVER (PARTITION BY ASSIGNEE_NAME ORDER BY SOLVED_WEEK DESC) as week_rank
    FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
    WHERE ASSIGNEE_SUPERVISOR ILIKE '%{supervisor}%'
    AND QA_SCORE IS NOT NULL
    AND SOLVED_WEEK >= CURRENT_DATE - INTERVAL '21 days'
),
improvement_calc AS (
    SELECT 
        ASSIGNEE_NAME,
        ASSIGNEE_SUPERVISOR,
        QA_SCORE as current_qa_score,
        prev_qa_score,
        prev_2_qa_score,
        CASE 
            WHEN prev_2_qa_score IS NOT NULL THEN QA_SCORE - prev_2_qa_score
            WHEN prev_qa_score IS NOT NULL THEN QA_SCORE - prev_qa_score
            ELSE 0
        END as qa_improvement
    FROM agent_qa_trend
    WHERE week_rank = 1
)
SELECT 
    ASSIGNEE_NAME,
    ASSIGNEE_SUPERVISOR,
    ROUND(current_qa_score, 2) as current_qa_score,
    ROUND(prev_qa_score, 2) as previous_qa_score,
    ROUND(qa_improvement, 2) as qa_improvement_points
FROM improvement_calc
WHERE qa_improvement > 0
ORDER BY qa_improvement DESC
LIMIT 5
"""
            else:
                # General QA improvement without specific supervisor
                sql = f"""
WITH agent_qa_trend AS (
    SELECT 
        ASSIGNEE_NAME,
        ASSIGNEE_SUPERVISOR,
        SOLVED_WEEK,
        QA_SCORE,
        LAG(QA_SCORE, 1) OVER (PARTITION BY ASSIGNEE_NAME ORDER BY SOLVED_WEEK) as prev_qa_score,
        LAG(QA_SCORE, 2) OVER (PARTITION BY ASSIGNEE_NAME ORDER BY SOLVED_WEEK) as prev_2_qa_score,
        ROW_NUMBER() OVER (PARTITION BY ASSIGNEE_NAME ORDER BY SOLVED_WEEK DESC) as week_rank
    FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
    WHERE QA_SCORE IS NOT NULL
    AND SOLVED_WEEK >= CURRENT_DATE - INTERVAL '21 days'
),
improvement_calc AS (
    SELECT 
        ASSIGNEE_NAME,
        ASSIGNEE_SUPERVISOR,
        QA_SCORE as current_qa_score,
        prev_qa_score,
        prev_2_qa_score,
        CASE 
            WHEN prev_2_qa_score IS NOT NULL THEN QA_SCORE - prev_2_qa_score
            WHEN prev_qa_score IS NOT NULL THEN QA_SCORE - prev_qa_score
            ELSE 0
        END as qa_improvement
    FROM agent_qa_trend
    WHERE week_rank = 1
)
SELECT 
    ASSIGNEE_NAME,
    ASSIGNEE_SUPERVISOR,
    ROUND(current_qa_score, 2) as current_qa_score,
    ROUND(prev_qa_score, 2) as previous_qa_score,
    ROUND(qa_improvement, 2) as qa_improvement_points
FROM improvement_calc
WHERE qa_improvement > 0
ORDER BY qa_improvement DESC
LIMIT 10
"""
            
            business_explanation = f"""
BUSINESS INTELLIGENCE APPLIED:
✅ TABLE: RPT_WOPS_AGENT_PERFORMANCE (QA score tracking)
✅ METRIC: QA_SCORE improvement over last 3 weeks
✅ FILTER: {'ASSIGNEE_SUPERVISOR ILIKE "%' + supervisor + '%"' if supervisor else 'All supervisors'}
✅ LOGIC: Compare current QA score to previous weeks using LAG window function
✅ RANKING: Ordered by improvement points (highest improvement first)

BUSINESS CONTEXT:
- QA improvement shows agents who are getting better at quality metrics
- Uses 3-week window to capture meaningful improvement trends
- Positive improvement indicates better quality performance
- Helps identify agents who are responding well to coaching
"""
            
            return sql.strip(), business_explanation
        
        elif any(phrase in question.lower() for phrase in ['how many agents', 'team performance', 'team - ', 'team performing']):
            # Team count and performance analysis
            supervisor = self.extract_supervisor_from_possessive(question) or self.extract_supervisor_from_team_question(question)
            
            if supervisor:
                # Check if it's asking for last X weeks/days
                if 'last' in question.lower() and ('week' in question.lower() or 'day' in question.lower()):
                    time_filter = "AND SOLVED_WEEK >= CURRENT_DATE - INTERVAL '21 days'"
                else:
                    time_filter = ""
                
                sql = f"""
SELECT 
    COUNT(DISTINCT ASSIGNEE_NAME) as agent_count,
    AVG(QA_SCORE) as avg_qa_score,
    AVG(POSITIVE_RES_CSAT * 100) as avg_csat_percentage,
    AVG(AHT_MINUTES) as avg_aht_minutes,
    AVG(FCR_PERCENTAGE * 100) as avg_fcr_percentage,
    SUM(NUM_TICKETS) as total_tickets
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
WHERE ASSIGNEE_SUPERVISOR ILIKE '%{supervisor}%'
AND ASSIGNEE_NAME IS NOT NULL
{time_filter}
"""
                
                # Add individual agent performance if requested
                if 'performing' in question.lower() or 'performance' in question.lower():
                    sql = f"""
SELECT 
    ASSIGNEE_NAME,
    AVG(QA_SCORE) as avg_qa_score,
    AVG(POSITIVE_RES_CSAT * 100) as avg_csat_percentage,
    AVG(AHT_MINUTES) as avg_aht_minutes,
    AVG(FCR_PERCENTAGE * 100) as avg_fcr_percentage,
    SUM(NUM_TICKETS) as total_tickets
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
WHERE ASSIGNEE_SUPERVISOR ILIKE '%{supervisor}%'
AND ASSIGNEE_NAME IS NOT NULL
{time_filter}
GROUP BY ASSIGNEE_NAME
ORDER BY avg_qa_score DESC
"""
                
                business_explanation = f"""
BUSINESS INTELLIGENCE APPLIED:
✅ TABLE: RPT_WOPS_AGENT_PERFORMANCE (agent performance metrics)
✅ SUPERVISOR: {supervisor}
✅ METRICS: QA Score, CSAT, AHT, FCR, Ticket Volume
✅ TIME FILTER: {'Last 3 weeks' if 'last' in question.lower() else 'All time'}
✅ AGGREGATION: Proper averaging for performance metrics

BUSINESS CONTEXT:
- Team performance analysis with key metrics
- Aggregated data to show overall team health
- Individual agent breakdown if performance details requested
- Follows SQL aggregation rules with proper GROUP BY
"""
                
                return sql.strip(), business_explanation
        
        elif any(phrase in question.lower() for phrase in ['schedule adherence', 'adherence', 'adherent', 'schedule compliance', 'offline time']):
            # Schedule adherence analysis
            supervisor = self.extract_supervisor_from_possessive(question) or self.extract_supervisor_from_team_question(question)
            
            if supervisor:
                # Team-specific adherence - JOIN with performance table for supervisor data
                if any(phrase in question.lower() for phrase in ['most', 'least', 'best', 'worst', 'top', 'bottom']):
                    # Individual agent ranking within team
                    sql = f"""
SELECT 
    s.AGENT_NAME,
    AVG(CAST(s.ADHERENCE_PERCENTAGE AS FLOAT)) as avg_adherence_rate,
    SUM(s.SCHEDULED_MINUTES) as total_scheduled_minutes,
    SUM(s.ADHERENT_MINUTES) as total_adherent_minutes,
    SUM(s.OFFLINE_MINUTES) as total_offline_minutes,
    COUNT(*) as schedule_periods
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE s
INNER JOIN ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE p 
    ON UPPER(TRIM(s.AGENT_NAME)) = UPPER(TRIM(p.ASSIGNEE_NAME))
WHERE p.ASSIGNEE_SUPERVISOR ILIKE '%{supervisor}%'
AND s.ADHERENCE_DATE >= CURRENT_DATE - INTERVAL '30 days'
AND s.ADHERENCE_PERCENTAGE != '-'
GROUP BY s.AGENT_NAME
ORDER BY avg_adherence_rate DESC
"""
                else:
                    # Team summary
                    sql = f"""
SELECT 
    COUNT(DISTINCT s.AGENT_NAME) as agent_count,
    AVG(CAST(s.ADHERENCE_PERCENTAGE AS FLOAT)) as team_avg_adherence_rate,
    SUM(s.SCHEDULED_MINUTES) as total_scheduled_minutes,
    SUM(s.ADHERENT_MINUTES) as total_adherent_minutes,
    SUM(s.OFFLINE_MINUTES) as total_offline_minutes
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE s
INNER JOIN ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE p 
    ON UPPER(TRIM(s.AGENT_NAME)) = UPPER(TRIM(p.ASSIGNEE_NAME))
WHERE p.ASSIGNEE_SUPERVISOR ILIKE '%{supervisor}%'
AND s.ADHERENCE_DATE >= CURRENT_DATE - INTERVAL '30 days'
AND s.ADHERENCE_PERCENTAGE != '-'
"""
            else:
                # General adherence analysis
                sql = f"""
SELECT 
    AGENT_NAME,
    AVG(CAST(ADHERENCE_PERCENTAGE AS FLOAT)) as avg_adherence_rate,
    SUM(SCHEDULED_MINUTES) as total_scheduled_minutes,
    SUM(ADHERENT_MINUTES) as total_adherent_minutes,
    SUM(OFFLINE_MINUTES) as total_offline_minutes,
    COUNT(*) as schedule_periods
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE
WHERE ADHERENCE_DATE >= CURRENT_DATE - INTERVAL '30 days'
AND ADHERENCE_PERCENTAGE != '-'
GROUP BY AGENT_NAME
ORDER BY avg_adherence_rate DESC
LIMIT 20
"""
            
            business_explanation = f"""
BUSINESS INTELLIGENCE APPLIED:
✅ TABLE: RPT_AGENT_SCHEDULE_ADHERENCE (schedule compliance metrics)
✅ JOIN: RPT_WOPS_AGENT_PERFORMANCE (for supervisor relationships)
✅ SUPERVISOR: {supervisor if supervisor else 'All teams'}
✅ METRICS: Adherence Rate, Scheduled/Adherent/Offline Minutes
✅ TIME FILTER: Last 30 days
✅ FOCUS: Schedule compliance and time tracking

BUSINESS CONTEXT:
- Schedule adherence measures how well agents follow their assigned schedules
- Adherence percentage shows compliance rate (filtered for valid data, excluding '-')
- Offline time indicates time not following schedule
- Critical for workforce management and productivity analysis
- Uses JOIN with performance table to get supervisor relationships

TECHNICAL NOTE:
- ADHERENCE_PERCENTAGE is stored as TEXT, so CAST to FLOAT for calculations
- Filters out invalid entries with ADHERENCE_PERCENTAGE != '-'
"""
            
            return sql.strip(), business_explanation
        
        # Default fallback
        return "-- Unable to generate intelligent SQL for this question type", "Business context not recognized"
    
    def get_table_recommendation(self, question: str) -> Dict[str, Any]:
        """Get intelligent table recommendation based on question analysis"""
        
        intent = self.analyze_question_intent(question)
        
        recommendation = {
            'recommended_table': intent['required_table'],
            'confidence': intent['confidence'],
            'reasoning': intent['business_context'],
            'alternative_tables': [],
            'table_intelligence': self.table_intelligence.get(intent['required_table'], {})
        }
        
        return recommendation
    
    def explain_data_relationships(self) -> str:
        """Explain key data relationships for bot intelligence"""
        
        explanation = """
🧠 DATA ANALYST INTELLIGENCE SUMMARY:

📊 KEY TABLE PURPOSES:
• RPT_WOPS_AGENT_PERFORMANCE: Agent-supervisor relationships, team composition
• ZENDESK_TICKET_AGENT__HANDLE_TIME: Ticket-level efficiency, handle times  
• RPT_WOPS_TICKETS: Response times, ticket volume
• RPT_WOPS_TL_PERFORMANCE: Supervisor/manager performance

🎯 CRITICAL BUSINESS RULES:
• Agent counting → Use ASSIGNEE_NAME from RPT_WOPS_AGENT_PERFORMANCE
• Team questions → Use ASSIGNEE_SUPERVISOR for supervisor relationships
• Handle time → Use USER_NAME from ZENDESK_TICKET_AGENT__HANDLE_TIME
• Response time → Use RPT_WOPS_TICKETS (pre-calculated metrics)

⚠️ COMMON MISTAKES TO AVOID:
• Never count TICKET_USER_ID for agent counting (counts tickets, not agents)
• Never use GROUP_NAME for supervisor questions (use ASSIGNEE_SUPERVISOR)
• Never use handle time table for agent counting (wrong granularity)
• Always use DISTINCT for agent counting (agents have multiple records)

✅ CORRECT PATTERNS:
• "How many agents in X's team?" → COUNT(DISTINCT ASSIGNEE_NAME) WHERE ASSIGNEE_SUPERVISOR ILIKE '%X%'
• "Agent handle time" → AVG(HANDLE_TIME_IN_MINUTES) FROM ZENDESK_TICKET_AGENT__HANDLE_TIME
• "Response time" → AVG(REPLY_TIME_IN_MINUTES) FROM RPT_WOPS_TICKETS
"""
        
        return explanation
    
    def _generate_personal_sql(self, question: str, intent_analysis: Dict, schema: Dict, user_context, user_filters: Dict) -> str:
        """Generate SQL for personal context questions (my team, my performance, etc.)"""
        
        question_lower = question.lower()
        
        # MY TEAM questions
        if any(phrase in question_lower for phrase in ['my team', 'our team', 'how many members', 'team members']):
            if user_context.role in ['supervisor', 'manager']:
                # Show team members for supervisors
                team_members = user_context.direct_reports
                if team_members:
                    member_list = "', '".join(team_members)
                    sql = f"""
SELECT COUNT(DISTINCT ASSIGNEE_NAME) as team_member_count
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
WHERE ASSIGNEE_SUPERVISOR ILIKE '%{user_context.name}%'
AND ASSIGNEE_NAME IS NOT NULL
"""
                else:
                    sql = "SELECT 0 as team_member_count -- No team members found"
                
                business_explanation = f"""
🏢 PERSONAL CONTEXT APPLIED:
✅ USER: {user_context.name} ({user_context.role})
✅ TEAM: Showing YOUR direct reports
✅ LOGIC: Count agents where you are the supervisor

BUSINESS INTELLIGENCE:
- You have {len(user_context.direct_reports)} direct reports
- Using ASSIGNEE_SUPERVISOR to find your team
- DISTINCT ensures no duplicate counting
"""
                return sql.strip(), business_explanation
            
            elif user_context.role == 'agent':
                # Show team size for agents (peers under same supervisor)
                sql = f"""
SELECT COUNT(DISTINCT ASSIGNEE_NAME) as team_size
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
WHERE ASSIGNEE_SUPERVISOR ILIKE '%{user_context.supervisor}%'
AND ASSIGNEE_NAME IS NOT NULL
"""
                business_explanation = f"""
🏢 PERSONAL CONTEXT APPLIED:
✅ USER: {user_context.name} (Agent)
✅ TEAM: Showing your team under supervisor {user_context.supervisor}
✅ LOGIC: Count all agents under your supervisor

BUSINESS INTELLIGENCE:
- You are part of {user_context.supervisor}'s team
- This shows total team size including you
"""
                return sql.strip(), business_explanation
        
        # MY PERFORMANCE / MY KPIS questions
        elif any(phrase in question_lower for phrase in ['my performance', 'my kpis', 'my metrics', 'my stats']):
            sql = f"""
SELECT 
    ASSIGNEE_NAME,
    AVG(AHT_MINUTES) as avg_handle_time,
    AVG(FCR_PERCENTAGE) as avg_fcr_rate,
    AVG(QA_SCORE) as avg_qa_score,
    SUM(NUM_TICKETS) as total_tickets,
    COUNT(DISTINCT SOLVED_WEEK) as weeks_active
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
WHERE ASSIGNEE_NAME ILIKE '%{user_context.name}%'
AND SOLVED_WEEK >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY ASSIGNEE_NAME
"""
            business_explanation = f"""
📊 PERSONAL PERFORMANCE METRICS:
✅ USER: {user_context.name}
✅ TIMEFRAME: Last 30 days
✅ METRICS: AHT, FCR, QA Score, Ticket Volume

BUSINESS INTELLIGENCE:
- Personal performance dashboard
- Filtered to YOUR data only
- Time-bounded for relevance
"""
            return sql.strip(), business_explanation
        
        # MY TICKETS questions
        elif any(phrase in question_lower for phrase in ['my tickets', 'tickets i solved', 'how many tickets did i']):
            if 'today' in question_lower:
                time_filter = "AND DATE(SOLVED_AT_PST) = CURRENT_DATE"
                timeframe = "today"
            elif 'week' in question_lower:
                time_filter = "AND SOLVED_AT_PST >= DATE_TRUNC('week', CURRENT_DATE)"
                timeframe = "this week"
            else:
                time_filter = "AND SOLVED_AT_PST >= CURRENT_DATE - INTERVAL '7 days'"
                timeframe = "last 7 days"
            
            sql = f"""
SELECT 
    COUNT(*) as tickets_solved,
    AVG(HANDLE_TIME_IN_MINUTES) as avg_handle_time
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE USER_NAME ILIKE '%{user_context.name}%'
{time_filter}
"""
            business_explanation = f"""
🎫 PERSONAL TICKET METRICS:
✅ USER: {user_context.name}
✅ TIMEFRAME: {timeframe}
✅ SCOPE: Tickets YOU solved

BUSINESS INTELLIGENCE:
- Individual productivity tracking
- Handle time efficiency
- Personal performance focus
"""
            return sql.strip(), business_explanation
        
        # MY TEAM'S PERFORMANCE questions
        elif any(phrase in question_lower for phrase in ['my team solve', 'team performance', 'team tickets']):
            if user_context.role in ['supervisor', 'manager']:
                sql = f"""
SELECT 
    COUNT(*) as team_tickets_solved,
    AVG(HANDLE_TIME_IN_MINUTES) as team_avg_handle_time,
    COUNT(DISTINCT USER_NAME) as active_agents
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE SUPERVISOR ILIKE '%{user_context.name}%'
AND SOLVED_AT_PST >= CURRENT_DATE - INTERVAL '7 days'
"""
                business_explanation = f"""
👥 TEAM PERFORMANCE METRICS:
✅ SUPERVISOR: {user_context.name}
✅ TEAM SIZE: {len(user_context.direct_reports)} direct reports
✅ TIMEFRAME: Last 7 days

BUSINESS INTELLIGENCE:
- Team productivity overview
- Supervisor dashboard view
- Team efficiency metrics
"""
                return sql.strip(), business_explanation
        
        # Default personal SQL
        sql = f"""
SELECT 
    'Personal data access configured for {user_context.name}' as message,
    '{user_context.role}' as role,
    '{user_context.team}' as team
"""
        business_explanation = f"""
👤 USER CONTEXT RECOGNIZED:
✅ USER: {user_context.name}
✅ ROLE: {user_context.role}
✅ TEAM: {user_context.team}

Please be more specific about what personal data you'd like to see.
Examples: "my performance", "my team", "my tickets", "my KPIs"
"""
        
        return sql.strip(), business_explanation


# Global instance
intelligent_data_analyst = IntelligentDataAnalyst()