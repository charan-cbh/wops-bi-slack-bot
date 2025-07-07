import re
from typing import Dict, List, Optional, Tuple
import json


class PatternMatcher:
    """Matches user questions to documented query patterns"""

    def __init__(self):
        self.patterns = self._load_patterns()

    def _load_patterns(self) -> List[Dict]:
        """Load query patterns with their configurations based on the comprehensive pattern.md"""
        return [
            {
                "id": "wops_tickets_response_time",
                "name": "WOPS Tickets Comprehensive Analysis ⭐ RESPONSE TIME LEADER",
                "table": "ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS",
                "description": "EXCLUSIVE pattern for response time analysis and general ticket metrics. Pre-calculated response times, business-ready data.",
                "priority": "HIGH",
                "exclusive_for": ["response time", "reply time", "resolution time", "SLA compliance", "SLA",
                                  "turnaround time"],
                "keywords": [
                    "response time", "reply time", "resolution time", "SLA compliance", "SLA",
                    "turnaround time", "time to respond", "time to resolve", "average response",
                    "response distribution", "response trends", "response speed", "response performance",
                    "time metrics", "resolution speed", "reply speed", "first response time",
                    "resolution analysis", "response benchmarks", "response rates", "how long does it take",
                    "time between", "response window", "ticket volume", "ticket count", "how many tickets",
                    "ticket trends", "ticket distribution", "ticket analysis", "ticket breakdown",
                    "tickets created", "tickets solved", "ticket metrics", "ticket patterns",
                    "volume analysis", "daily tickets", "weekly tickets", "monthly tickets",
                    "ticket statistics", "workload", "case volume", "issue volume", "contact channel"
                ],
                "questions": [
                    "response time", "reply time", "resolution time", "SLA compliance", "turnaround time",
                    "how long does it take", "time to respond", "time to resolve", "average response",
                    "response distribution", "response speed", "response trends", "response benchmarks",
                    "how many tickets", "ticket volume", "tickets created", "ticket distribution",
                    "tickets by", "show tickets", "ticket trends", "contact channel", "daily tickets",
                    "tickets today", "tickets yesterday", "tickets this week", "tickets last month",
                    "ticket count", "ticket metrics", "ticket statistics", "volume analysis"
                ],
                "business_context": """This is the EXCLUSIVE table for response time questions and primary table for ticket analysis.

Key Business Rules:
- EXCLUSIVE for response time questions - NEVER use other tables for response time
- Pre-calculated response time metrics: REPLY_TIME_IN_MINUTES, FIRST_RESOLUTION_TIME_IN_MINUTES, FULL_RESOLUTION_TIME_IN_MINUTES
- Pre-filtered business-ready data - no complex WHERE clauses needed
- Direct Contact_Channel field (Web, Chat, Voice, Other)
- All standard business filters already applied
- Simply add date filtering and specific criteria""",
                "standard_filters": """-- No standard filters needed - data is pre-filtered
-- Simply add specific criteria like:
-- WHERE CREATED_AT_PST >= CURRENT_DATE - 7    -- Date filtering
-- AND AGENT_NAME = 'John Smith'             -- Agent filtering  
-- AND CONTACT_CHANNEL = 'Chat'              -- Channel filtering""",
                "key_columns": {
                    "identifiers": ["TICKET_ID", "ASSIGNEE_ID", "WORKER_ID"],
                    "people": ["WORKER_NAME", "WORKER_EMAIL", "AGENT_NAME", "AGENT_EMAIL", "TEAM_LEAD"],
                    "dimensions": ["GROUP_ID", "NATIVE_ZENDESK_CHANNEL", "CONTACT_CHANNEL", "TICKET_STATUS",
                                   "TICKET_TYPE", "TICKET_SUB_TYPE"],
                    "timestamps": ["CREATED_AT_PST", "INITIALLY_ASSIGNED_AT_PST", "ASSIGNED_AT_PST",
                                   "SOLVED_AT_PST"],
                    "response_time_metrics": ["REPLY_TIME_IN_MINUTES", "FIRST_RESOLUTION_TIME_IN_MINUTES",
                                              "FULL_RESOLUTION_TIME_IN_MINUTES"],
                    "other_metrics": ["HANDLE_TIME", "LAST_TOUCH_HANDLE_TIME", "CONNECT_HANDLE_TIME_TOTAL_MIN",
                                      "REOPENS", "REPLIES", "PROVIDED_SCORE"],
                    "categories": ["WAIVER_REQUEST_DRIVER", "PRODUCT_HELP_CATEGORY", "PAYMENTS_CATEGORY",
                                   "URGENT_SHIFTS_TYPE", "ESCALATION_TEAM"]
                },
                "derived_fields": {
                    "SLA_Response_Compliance": """CASE WHEN REPLY_TIME_IN_MINUTES <= 60 THEN 1 ELSE 0 END""",
                    "SLA_Resolution_Compliance": """CASE WHEN FIRST_RESOLUTION_TIME_IN_MINUTES <= 1440 THEN 1 ELSE 0 END""",
                    "Response_Time_Bucket": """CASE 
  WHEN REPLY_TIME_IN_MINUTES <= 15 THEN '0-15 min (Excellent)'
  WHEN REPLY_TIME_IN_MINUTES <= 60 THEN '15-60 min (Good)'
  WHEN REPLY_TIME_IN_MINUTES <= 240 THEN '1-4 hours (Needs Improvement)'
  ELSE '4+ hours (Critical)'
END"""
                },
                "confidence_boost": 150  # Extra points for exclusive patterns
            },
            {
                "id": "agent_handle_time",
                "name": "Agent Handle Time Analysis ⭐ HANDLE TIME LEADER",
                "table": "ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME",
                "description": "PRIORITY pattern for handle time, AHT, efficiency analysis. Pre-calculated metrics with voice channel details.",
                "priority": "HIGH",
                "keywords": [
                    "handle time", "AHT", "average handle time", "handling time", "efficiency",
                    "agent efficiency", "time per ticket", "call duration", "talk time",
                    "hold time", "efficiency metrics", "productivity metrics", "speed metrics",
                    "time analysis", "duration analysis", "how long agents take", "agent speed",
                    "ticket processing time", "work efficiency", "time management", "agent velocity",
                    "voice metrics", "amazon connect", "call time", "call handling", "voice handling"
                ],
                "questions": [
                    "average handle time", "aht", "handle time by agent", "aht by agent",
                    "longest handle time", "handle time trend", "call duration", "voice metrics",
                    "agent efficiency", "talk time", "hold time", "aht by channel", "aht by team",
                    "handle time distribution", "efficiency rankings", "call analytics",
                    "voice channel metrics", "agent time analysis", "handling time", "agent speed"
                ],
                "business_context": """PRIORITY table for handle time and agent efficiency analysis.

Key Business Rules:
- Pre-calculated handle time in both seconds and minutes
- Voice channel has Amazon Connect metrics (call duration, talk time, hold time)
- Agent-level granularity for detailed efficiency analysis
- No complex calculations needed - all metrics pre-calculated
- Consider excluding outliers (>120 minutes) for averages""",
                "standard_filters": """USER_NAME IS NOT NULL 
  AND USER_NAME != ''
  AND HANDLE_TIME_IN_MINUTES IS NOT NULL""",
                "key_columns": {
                    "identifiers": ["TICKET_ID", "USER_ID", "TICKET_USER_ID"],
                    "agent_info": ["USER_NAME", "USER_EMAIL", "SUPERVISOR"],
                    "dimensions": ["GROUP_NAME", "CONTACT_CHANNEL"],
                    "timestamps": ["CREATED_AT", "CREATED_AT_PST", "SOLVED_AT", "SOLVED_AT_PST"],
                    "handle_time_metrics": ["HANDLE_TIME_IN_MINUTES", "HANDLE_TIME_IN_SECONDS"],
                    "voice_metrics": ["AMAZON_CONNECT_CALL_DURATION_IN_MINUTES",
                                      "AMAZON_CONNECT_HOLD_TIME_IN_MINUTES", "AMAZON_CONNECT_TALK_TIME_IN_MINUTES"],
                    "categories": ["WOPS_TICKET_TYPE_A", "ESCALATION_TEAM"]
                },
                "derived_fields": {
                    "Handle_Time_Bucket": """CASE 
  WHEN HANDLE_TIME_IN_MINUTES < 5 THEN '0-5 min'
  WHEN HANDLE_TIME_IN_MINUTES < 10 THEN '5-10 min'
  WHEN HANDLE_TIME_IN_MINUTES < 20 THEN '10-20 min'
  WHEN HANDLE_TIME_IN_MINUTES < 30 THEN '20-30 min'
  ELSE '30+ min'
END""",
                    "Voice_Efficiency_Ratio": """CASE 
  WHEN AMAZON_CONNECT_CALL_DURATION_IN_MINUTES > 0 
  THEN AMAZON_CONNECT_TALK_TIME_IN_MINUTES / AMAZON_CONNECT_CALL_DURATION_IN_MINUTES 
  ELSE NULL 
END"""
                },
                "confidence_boost": 125
            },
            {
                "id": "fcr_analysis",
                "name": "First Contact Resolution (FCR) Analysis ⭐ FCR LEADER",
                "table": "ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS",
                "description": "PRIORITY pattern for FCR rates, repeat contact analysis, channel switching. Requires window functions.",
                "priority": "HIGH",
                "keywords": [
                    "FCR", "first contact resolution", "repeat contact", "channel switching",
                    "callback", "call back", "same issue", "resolved first time",
                    "multiple contacts", "customer contacted again", "repeat ticket",
                    "followup ticket", "follow up", "one call resolution", "contact again",
                    "resolution effectiveness", "repeat analysis", "channel switching patterns",
                    "customer calling back", "multiple touch", "contact multiple times"
                ],
                "questions": [
                    "fcr rate", "first contact resolution", "repeat contacts", "channel switching",
                    "resolved first time", "fcr by agent", "callback rate", "repeat customers",
                    "follow up contacts", "fcr by channel", "fcr trends", "resolution effectiveness",
                    "which agents best fcr", "fcr by team", "contact patterns", "same issue repeat"
                ],
                "business_context": """PRIORITY table for FCR analysis using 24-hour repeat contact definition.

Key Business Rules:
- FCR = customer does not create another ticket within 24 hours
- Requires LEAD() window functions for customer tracking
- Pre-applied filters for solved/closed, native_messaging, Chat/Voice groups
- Complex but necessary for accurate FCR calculation
- Time range: typically last 6 weeks including current week""",
                "standard_filters": """status IN ('solved', 'closed')
  AND brand_id IN ('360002340693', '29186504989207')
  AND channel = 'native_messaging'
  AND group_id IN ('17837476387479', '28949203098007')
  AND (NOT LOWER(ticket_tags) LIKE '%email_blocked%' OR ticket_tags IS NULL)
  AND (assignee_name <> 'TechOps Bot' OR assignee_name IS NULL)""",
                "key_columns": {
                    "identifiers": ["TICKET_ID", "REQUESTER_ID"],
                    "dimensions": ["ASSIGNEE_NAME", "CHANNEL", "GROUP_ID"],
                    "timestamps": ["CREATED_AT_PST", "SOLVED_AT_PST"],
                    "categories": ["ISSUE_TYPE", "SHIFT_ID_S"]
                },
                "window_functions": {
                    "next_ticket_detection": "LEAD(ticket_id) OVER (PARTITION BY requester_id ORDER BY created_at_pst)",
                    "next_ticket_date": "LEAD(created_at_pst) OVER (PARTITION BY requester_id ORDER BY created_at_pst)",
                    "fcr_calculation": "CASE WHEN created_at_pst + INTERVAL '24 HOUR' >= LEAD(created_at_pst) OVER (PARTITION BY requester_id ORDER BY created_at_pst) THEN 0 ELSE 1 END"
                },
                "derived_fields": {
                    "Contact_Channel": """CASE
  WHEN group_id = '17837476387479' THEN 'Chat'
  WHEN group_id = '28949203098007' THEN 'Voice'
  ELSE 'Other'
END""",
                    "Is_FCR_Success": """CASE
  WHEN created_at_pst + INTERVAL '24 HOUR' >= LEAD(created_at_pst) OVER (PARTITION BY requester_id ORDER BY created_at_pst) THEN 0
  ELSE 1
END"""
                },
                "requires_window_functions": True,
                "confidence_boost": 125
            },
            {
                "id": "wops_agent_performance",
                "name": "WOPS Agent Performance (Weekly Aggregated) ⭐ AGENT PERFORMANCE LEADER",
                "table": "ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE",
                "description": "PRIORITY pattern for agent performance dashboards, rankings, KPIs. Pre-aggregated weekly metrics.",
                "priority": "HIGH",
                "keywords": [
                    "agent performance", "agent metrics", "agent productivity", "agent efficiency",
                    "agent statistics", "agent dashboard", "agent comparison", "agent ranking",
                    "which agent", "best agent", "top agent", "agent analysis", "individual agent",
                    "agent scores", "agent evaluation", "agent effectiveness", "weekly agent performance",
                    "agent trends", "agent quality", "agent KPIs", "agent benchmarks", "agent leaderboard",
                    "performance dashboard", "top performers", "performance trends", "performance summary",
                    "weekly stats", "performance metrics", "agent scorecard", "performance comparison"
                ],
                "questions": [
                    "agent performance", "top performing agents", "weekly performance", "performance trends",
                    "agent rankings", "performance dashboard", "who are the best agents", "agent stats",
                    "weekly stats", "performance comparison", "week over week", "agent scorecard",
                    "individual agent performance", "performance metrics", "agent kpi", "agent qa scores",
                    "agent quality", "individual agent", "agent leaderboard", "performance summary"
                ],
                "business_context": """PRIORITY table for agent performance analysis with pre-aggregated weekly KPIs.

Key Business Rules:
- All KPIs pre-calculated: volume (NUM_TICKETS), efficiency (AHT_MINUTES), quality (QA_SCORE), effectiveness (FCR_PERCENTAGE)
- Weekly aggregated data - no complex calculations needed
- Filter out null/empty agent names and system accounts
- Current week = MAX(SOLVED_WEEK)
- Prioritize agents with complete data for rankings""",
                "standard_filters": """ASSIGNEE_NAME IS NOT NULL 
  AND ASSIGNEE_NAME != '' 
  AND ASSIGNEE_NAME != 'None'
  AND LOWER(ASSIGNEE_NAME) != 'null'""",
                "key_columns": {
                    "identifiers": ["SOLVED_WEEK_ASSIGNEE_ID"],
                    "dimensions": ["ASSIGNEE_NAME", "SOLVED_WEEK"],
                    "kpi_metrics": ["NUM_TICKETS", "AHT_MINUTES", "FCR_PERCENTAGE", "QA_SCORE"],
                    "csat_metrics": ["POSITIVE_RES_CSAT", "NEGATIVE_RES_CSAT"],
                    "timestamps": ["SOLVED_WEEK"]
                },
                "derived_fields": {
                    "CSAT_Rate": """CASE 
  WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN NULL
  ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100
END""",
                    "Performance_Score": """(FCR_PERCENTAGE * 0.3 + QA_SCORE * 0.3 + 
   CASE WHEN AHT_MINUTES <= 10 THEN 100 ELSE GREATEST(0, 100 - (AHT_MINUTES - 10) * 5) END * 0.2 +
   CASE WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN 50 
        ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 END * 0.2)""",
                    "Data_Completeness_Priority": """CASE WHEN QA_SCORE IS NOT NULL AND FCR_PERCENTAGE IS NOT NULL THEN 0 ELSE 1 END"""
                },
                "confidence_boost": 125
            },
            {
                "id": "wops_tl_performance",
                "name": "WOPS Team Lead Performance (Weekly Aggregated) ⭐ TEAM LEAD LEADER",
                "table": "ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE",
                "description": "PRIORITY pattern for team lead/supervisor performance, team-level metrics, leadership analysis.",
                "priority": "HIGH",
                "keywords": [
                    "team lead performance", "supervisor metrics", "manager performance", "team leader analysis",
                    "supervisor analysis", "manager analysis", "leadership metrics", "team lead dashboard",
                    "team performance", "supervisor performance", "team lead ranking", "team lead comparison",
                    "team lead trends", "team lead evaluation", "team lead effectiveness",
                    "weekly team performance",
                    "supervisor dashboard", "manager dashboard", "team metrics", "leadership analysis",
                    "team stats", "team rankings", "team capacity", "team volume", "cross team", "multi team"
                ],
                "questions": [
                    "team performance", "team lead performance", "supervisor performance", "team rankings",
                    "team stats", "team dashboard", "team metrics", "top performing teams", "team comparison",
                    "team benchmarking", "team capacity", "team volume", "workload distribution",
                    "cross team analysis", "multi team", "team qa scores", "team fcr", "team aht",
                    "team satisfaction", "supervisor rankings", "team lead rankings", "team weekly performance"
                ],
                "business_context": """PRIORITY table for team lead performance with pre-aggregated team-level weekly KPIs.

Key Business Rules:
- Team-level aggregated metrics from all agents under each supervisor
- Pre-calculated team KPIs: volume, efficiency, quality, effectiveness
- Filter out null/empty supervisor names
- Use for supervisor effectiveness and team comparisons
- Current week = MAX(SOLVED_WEEK)""",
                "standard_filters": """SUPERVISOR IS NOT NULL 
  AND SUPERVISOR != '' 
  AND SUPERVISOR != 'None'
  AND LOWER(SUPERVISOR) != 'null'""",
                "key_columns": {
                    "identifiers": ["SOLVED_WEEK_SUPERVISOR_ID"],
                    "dimensions": ["SUPERVISOR", "SOLVED_WEEK"],
                    "team_kpi_metrics": ["NUM_TICKETS", "AHT_MINUTES", "FCR_PERCENTAGE", "QA_SCORE"],
                    "team_csat_metrics": ["POSITIVE_RES_CSAT", "NEGATIVE_RES_CSAT"],
                    "timestamps": ["SOLVED_WEEK"]
                },
                "derived_fields": {
                    "Team_CSAT_Rate": """CASE 
  WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN NULL
  ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100
END""",
                    "Estimated_Tickets_Per_Agent": """CASE WHEN NUM_TICKETS = 0 THEN 0 ELSE NUM_TICKETS / 8.0 END""",
                    "Team_Performance_Score": """(FCR_PERCENTAGE * 0.3 + QA_SCORE * 0.3 + 
   CASE WHEN AHT_MINUTES <= 10 THEN 100 ELSE GREATEST(0, 100 - (AHT_MINUTES - 10) * 5) END * 0.4)"""
                },
                "confidence_boost": 125
            },
            {
                "id": "schedule_adherence",
                "name": "Agent Schedule Adherence Analysis ⭐ ADHERENCE LEADER",
                "table": "ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE",
                "description": "EXCLUSIVE pattern for schedule adherence, compliance, time tracking analysis. Pre-calculated adherence metrics.",
                "priority": "HIGH",
                "exclusive_for": ["schedule adherence", "adherence rate", "schedule compliance"],
                "keywords": [
                    "schedule adherence", "adherence rate", "schedule compliance", "schedule variance",
                    "offline time", "break adherence", "schedule patterns", "adherence trends",
                    "schedule analysis", "schedule performance", "adherence metrics", "schedule monitoring",
                    "schedule effectiveness", "adherence dashboard", "adherence comparison", "schedule following",
                    "time tracking", "work schedule", "attendance patterns", "schedule variance",
                    "non adherent", "adherent minutes", "scheduled minutes"
                ],
                "questions": [
                    "schedule adherence", "adherence rate", "schedule compliance", "schedule variance",
                    "offline time", "break adherence", "schedule patterns", "adherence trends",
                    "schedule analysis", "schedule performance", "adherence metrics", "which agents poor adherence",
                    "schedule monitoring", "adherence dashboard", "adherence comparison", "time tracking",
                    "attendance patterns", "schedule following", "adherence by team", "adherence by agent"
                ],
                "business_context": """EXCLUSIVE table for schedule adherence analysis with pre-calculated metrics.

Key Business Rules:
- Pre-calculated ADHERENCE_PERCENTAGE (0-100)
- Time metrics: SCHEDULED_MINUTES, ADHERENT_MINUTES, OFFLINE_MINUTES
- Agent-level daily adherence data
- No complex calculations needed - all metrics ready to use
- Use for time tracking and compliance monitoring""",
                "standard_filters": """AGENT_NAME IS NOT NULL 
  AND AGENT_NAME != ''
  AND ADHERENCE_DATE IS NOT NULL""",
                "key_columns": {
                    "identifiers": ["AGENT_NAME", "ADHERENCE_DATE"],
                    "dimensions": ["AGENT_NAME", "SUPERVISOR", "TEAM"],
                    "adherence_metrics": ["ADHERENCE_PERCENTAGE", "SCHEDULED_MINUTES", "ADHERENT_MINUTES",
                                          "OFFLINE_MINUTES", "NON_ADHERENT_LOGGED_MINUTES",
                                          "TOTAL_NON_ADHERENT_MINUTES"],
                    "timestamps": ["ADHERENCE_DATE", "ADHERENCE_DATETIME"],
                    "categories": ["SCHEDULED_TASK_TYPE", "NON_ADHERENT_ACTIVITY"]
                },
                "derived_fields": {
                    "Adherence_Grade": """CASE 
  WHEN ADHERENCE_PERCENTAGE >= 95 THEN 'Excellent'
  WHEN ADHERENCE_PERCENTAGE >= 90 THEN 'Good'
  WHEN ADHERENCE_PERCENTAGE >= 85 THEN 'Needs Improvement'
  ELSE 'Critical'
END""",
                    "Non_Adherence_Rate": """100 - ADHERENCE_PERCENTAGE""",
                    "Offline_Percentage": """CASE 
  WHEN SCHEDULED_MINUTES > 0 THEN (OFFLINE_MINUTES / SCHEDULED_MINUTES) * 100 
  ELSE 0 
END"""
                },
                "confidence_boost": 150  # Extra points for exclusive patterns
            }
        ]

    def match_pattern(self, question: str) -> Optional[Dict]:
        """Find the best matching pattern for a question"""
        question_lower = question.lower()
        scores = []

        print(f"🔍 Pattern matching for: '{question}'")

        for pattern in self.patterns:
            score = 0
            matched_items = []

            # Check exact question matches
            for q in pattern["questions"]:
                if q in question_lower:
                    score += 10
                    matched_items.append(f"question:'{q}'")

            # Check keyword matches
            for keyword in pattern["keywords"]:
                if keyword in question_lower:
                    score += 3
                    matched_items.append(f"keyword:'{keyword}'")

            # Enhanced bonuses for specific patterns
            if pattern["id"] == "wops_agent_performance":
                performance_indicators = [
                    ("best", 8), ("top", 8), ("performing", 8), ("performance", 8),
                    ("agent", 5), ("weekly", 6), ("last week", 8), ("this week", 6),
                    ("rankings", 8), ("ranking", 8), ("dashboard", 6),
                    ("trends", 6), ("scorecard", 6), ("kpi", 6), ("individual", 5)
                ]

                # Exclude team-related terms
                if not any(term in question_lower for term in ["team", "supervisor", "lead"]):
                    for indicator, points in performance_indicators:
                        if indicator in question_lower:
                            score += points
                            matched_items.append(f"agent_performance:'{indicator}'({points})")

            elif pattern["id"] == "wops_tl_performance":
                team_indicators = [
                    ("team", 10), ("supervisor", 10), ("team lead", 12), ("team leader", 12),
                    ("team performance", 15), ("supervisor performance", 15),
                    ("team stats", 12), ("team metrics", 12), ("team rankings", 12),
                    ("cross team", 10), ("multi team", 10), ("team comparison", 12),
                    ("team capacity", 10), ("workload", 8)
                ]

                for indicator, points in team_indicators:
                    if indicator in question_lower:
                        score += points
                        matched_items.append(f"team_performance:'{indicator}'({points})")

            elif pattern["id"] == "wops_klaus_qa_ata":
                qa_detail_indicators = [
                    ("qa component", 15), ("qa breakdown", 15), ("qa details", 12),
                    ("auto fail", 15), ("auto-fail", 15), ("qa review", 10),
                    ("component", 8), ("resolution score", 12), ("communication score", 12),
                    ("reviewer", 10), ("scorecard", 8), ("ata", 10),
                    ("individual qa", 12), ("detailed qa", 12)
                ]

                for indicator, points in qa_detail_indicators:
                    if indicator in question_lower:
                        score += points
                        matched_items.append(f"qa_detail:'{indicator}'({points})")

            elif pattern["id"] == "wops_tickets":
                if any(term in question_lower for term in ["ticket", "volume", "created", "how many"]):
                    # Boost for daily/operational queries
                    if any(term in question_lower for term in ["today", "yesterday", "daily", "count"]):
                        score += 8
                        matched_items.append("daily_tickets_bonus")
                    else:
                        score += 5
                        matched_items.append("tickets_bonus")

            elif pattern["id"] == "handle_time":
                if any(term in question_lower for term in ["handle time", "aht", "duration"]):
                    score += 5
                    matched_items.append("aht_bonus")

            elif pattern["id"] == "fcr":
                if any(term in question_lower for term in ["fcr", "first contact", "resolution rate"]):
                    score += 5
                    matched_items.append("fcr_bonus")

            # Log scoring details
            if score > 0:
                print(f"   {pattern['id']}: {score} points - {', '.join(matched_items)}")

            scores.append((pattern, score))

        # Return best match if score is high enough
        best_match = max(scores, key=lambda x: x[1])

        print(f"🎯 Best match: {best_match[0]['id']} with {best_match[1]} points")

        if best_match[1] >= 3:
            print(f"✅ Pattern matched: {best_match[0]['name']}")
            return best_match[0]
        else:
            print(f"❌ No pattern match (highest score: {best_match[1]}, need ≥3)")
            return None


class PatternBasedQueryHelper:
    """Builds HELPER SQL queries for OpenAI using documented patterns"""

    def __init__(self):
        self.pattern_matcher = PatternMatcher()

    def build_helper_sql(self, question: str, pattern: Dict, intent: Dict) -> str:
        """Build HELPER SQL query based on pattern and intent - OpenAI will refine this"""
        question_lower = question.lower()
        table = pattern["table"]

        # Start with base query structure
        sql_parts = {
            "select": [],
            "from": table,
            "where": [],
            "group_by": [],
            "order_by": [],
            "limit": ""
        }

        # Apply standard filters if available
        if pattern.get("standard_filters"):
            sql_parts["where"].append(pattern["standard_filters"].strip())

        # Build helper query based on intent and pattern
        if pattern["id"] == "wops_agent_performance":
            sql = self._build_wops_performance_helper(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "wops_tl_performance":
            sql = self._build_wops_tl_performance_helper(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "wops_klaus_qa_ata":
            sql = self._build_wops_klaus_qa_ata_helper(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "wops_tickets":
            sql = self._build_wops_tickets_helper(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "handle_time":
            sql = self._build_handle_time_helper(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "fcr":
            sql = self._build_fcr_helper(question_lower, pattern, intent, sql_parts)
        else:
            sql = self._build_generic_helper(question_lower, pattern, intent, sql_parts)

        return sql

    def _build_wops_performance_helper(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build HELPER SQL for WOPS agent performance queries"""

        # Current week top performers
        if any(phrase in question for phrase in ["top performers", "best agents", "top agents", "best performing"]):
            sql_parts["select"] = [
                "ASSIGNEE_NAME",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE",
                "CASE WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) > 0 THEN POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 ELSE NULL END as csat_rate"
            ]
            sql_parts["where"].append(
                "SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE)")

            # Smart ordering: prioritize agents with complete data
            sql_parts["order_by"] = [
                "CASE WHEN QA_SCORE IS NOT NULL AND FCR_PERCENTAGE IS NOT NULL THEN 0 ELSE 1 END",
                "QA_SCORE DESC",
                "FCR_PERCENTAGE DESC",
                "NUM_TICKETS DESC"
            ]
            sql_parts["limit"] = "LIMIT 10"

        # Performance trends
        elif "trends" in question or "week over week" in question:
            sql_parts["select"] = [
                "SOLVED_WEEK",
                "ASSIGNEE_NAME",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE"
            ]
            sql_parts["order_by"] = ["SOLVED_WEEK DESC"]
            sql_parts["limit"] = "LIMIT 12"

        # Performance rankings
        elif "ranking" in question or "rankings" in question:
            sql_parts["select"] = [
                "ASSIGNEE_NAME",
                "AVG(NUM_TICKETS) as avg_weekly_tickets",
                "AVG(AHT_MINUTES) as avg_aht",
                "AVG(FCR_PERCENTAGE) as avg_fcr",
                "AVG(QA_SCORE) as avg_qa_score",
                "COUNT(*) as weeks_active",
                "ROW_NUMBER() OVER (ORDER BY AVG(QA_SCORE) DESC, AVG(FCR_PERCENTAGE) DESC) as overall_rank"
            ]
            sql_parts["where"].append("SOLVED_WEEK >= CURRENT_DATE - INTERVAL '12 weeks'")
            sql_parts["where"].append("QA_SCORE IS NOT NULL")
            sql_parts["group_by"] = ["ASSIGNEE_NAME"]
            sql_parts["order_by"] = ["overall_rank"]

        # Weekly performance summary
        elif "weekly" in question or "this week" in question or "last week" in question:
            sql_parts["select"] = [
                "ASSIGNEE_NAME",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE"
            ]
            if "last week" in question:
                sql_parts["where"].append(
                    "SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE) - INTERVAL '7 days'")
            else:
                sql_parts["where"].append(
                    "SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE)")
            sql_parts["order_by"] = ["QA_SCORE DESC"]

        # Default: recent performance data
        else:
            sql_parts["select"] = [
                "ASSIGNEE_NAME",
                "SOLVED_WEEK",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE"
            ]
            sql_parts["where"].append("(QA_SCORE IS NOT NULL OR FCR_PERCENTAGE IS NOT NULL)")
            sql_parts["order_by"] = ["SOLVED_WEEK DESC", "QA_SCORE DESC"]
            sql_parts["limit"] = "LIMIT 50"

        return self._assemble_sql(sql_parts)

    def _build_wops_tl_performance_helper(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build HELPER SQL for WOPS team lead performance queries"""

        # Current week top performing teams
        if any(phrase in question for phrase in ["top performing teams", "best teams", "top teams", "team rankings"]):
            sql_parts["select"] = [
                "SUPERVISOR",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE",
                "CASE WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) > 0 THEN POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 ELSE NULL END as team_csat_rate",
                "NUM_TICKETS / 8.0 as estimated_tickets_per_agent"
            ]
            sql_parts["where"].append(
                "SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE)")
            sql_parts["order_by"] = ["QA_SCORE DESC", "FCR_PERCENTAGE DESC"]
            sql_parts["limit"] = "LIMIT 10"

        # Team performance trends
        elif "team trends" in question or "team performance trends" in question:
            sql_parts["select"] = [
                "SOLVED_WEEK",
                "SUPERVISOR",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE"
            ]
            sql_parts["order_by"] = ["SOLVED_WEEK DESC"]
            sql_parts["limit"] = "LIMIT 12"

        # Team capacity analysis
        elif "capacity" in question or "workload" in question:
            sql_parts["select"] = [
                "SUPERVISOR",
                "AVG(NUM_TICKETS) as avg_weekly_volume",
                "MAX(NUM_TICKETS) as peak_weekly_volume",
                "MIN(NUM_TICKETS) as min_weekly_volume",
                "AVG(NUM_TICKETS) / 8.0 as estimated_tickets_per_agent"
            ]
            sql_parts["where"].append("SOLVED_WEEK >= CURRENT_DATE - INTERVAL '8 weeks'")
            sql_parts["group_by"] = ["SUPERVISOR"]
            sql_parts["order_by"] = ["avg_weekly_volume DESC"]

        # Default: recent team performance
        else:
            sql_parts["select"] = [
                "SUPERVISOR",
                "SOLVED_WEEK",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE"
            ]
            sql_parts["order_by"] = ["SOLVED_WEEK DESC", "QA_SCORE DESC"]
            sql_parts["limit"] = "LIMIT 50"

        return self._assemble_sql(sql_parts)

    def _build_wops_klaus_qa_ata_helper(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build HELPER SQL for WOPS Klaus QA & ATA detailed reviews"""

        # QA component breakdown
        if any(phrase in question for phrase in ["component breakdown", "qa components", "component analysis"]):
            sql_parts["select"] = [
                "REVIEWEE_NAME",
                "COUNT(*) as total_reviews",
                "AVG(OVERALL_SCORE) as avg_overall_score",
                "AVG(CASE WHEN RESOLUTION_BASE > 0 THEN (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100 END) as avg_resolution_pct",
                "AVG(CASE WHEN COMMUNICATION_BASE > 0 THEN (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100 END) as avg_communication_pct",
                "AVG(CASE WHEN HANDLING_BASE > 0 THEN (HANDLING_RATING_SCORE / HANDLING_BASE) * 100 END) as avg_handling_pct",
                "SUM(CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1 ELSE 0 END) as auto_fail_count"
            ]
            sql_parts["where"].append("REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'")
            sql_parts["group_by"] = ["REVIEWEE_NAME"]
            sql_parts["order_by"] = ["avg_overall_score DESC"]

        # Auto-fail analysis
        elif "auto fail" in question or "auto-fail" in question:
            sql_parts["select"] = [
                "REVIEWEE_NAME",
                "REVIEWER_NAME",
                "TICKET_ID",
                "OVERALL_SCORE",
                "SCORECARD_NAME",
                "REVIEW_CREATED_AT",
                "REVIEW_COMMENT"
            ]
            sql_parts["where"].append("NO_AUTO_FAIL_RATING_SCORE < 100")
            sql_parts["where"].append("REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'")
            sql_parts["order_by"] = ["REVIEW_CREATED_AT DESC"]

        # Reviewer performance
        elif "reviewer" in question:
            sql_parts["select"] = [
                "REVIEWER_NAME",
                "COUNT(*) as reviews_conducted",
                "AVG(OVERALL_SCORE) as avg_score_given",
                "STDDEV(OVERALL_SCORE) as score_std_dev",
                "SUM(CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1 ELSE 0 END) / COUNT(*) * 100 as auto_fail_rate"
            ]
            sql_parts["where"].append("REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'")
            sql_parts["group_by"] = ["REVIEWER_NAME"]
            sql_parts["order_by"] = ["score_std_dev ASC"]

        # QA by channel
        elif "by channel" in question or "channel" in question:
            sql_parts["select"] = [
                "CONTACT_CHANNEL",
                "COUNT(*) as review_count",
                "AVG(OVERALL_SCORE) as avg_overall_score",
                "SUM(CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1 ELSE 0 END) / COUNT(*) * 100 as auto_fail_rate"
            ]
            sql_parts["where"].append("REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'")
            sql_parts["where"].append("CONTACT_CHANNEL IS NOT NULL")
            sql_parts["group_by"] = ["CONTACT_CHANNEL"]
            sql_parts["order_by"] = ["avg_overall_score DESC"]

        # Default: recent reviews
        else:
            sql_parts["select"] = [
                "REVIEWEE_NAME",
                "REVIEWER_NAME",
                "OVERALL_SCORE",
                "SCORECARD_NAME",
                "CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 'Yes' ELSE 'No' END as auto_fail",
                "REVIEW_CREATED_AT"
            ]
            sql_parts["order_by"] = ["REVIEW_CREATED_AT DESC"]
            sql_parts["limit"] = "LIMIT 50"

        return self._assemble_sql(sql_parts)

    def _build_wops_tickets_helper(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build HELPER SQL for WOPS tickets queries"""

        # Ticket count queries
        if any(phrase in question for phrase in ["how many tickets", "ticket count", "tickets created"]):
            sql_parts["select"] = ["COUNT(*) as ticket_count"]

            # Add time filter
            time_filter = self._get_time_filter(question, "CREATED_AT")
            if time_filter:
                sql_parts["where"].append(time_filter)

        # Volume by dimension
        elif "by" in question or "per" in question:
            sql_parts["select"] = ["COUNT(*) as ticket_count"]

            # Determine grouping
            if "channel" in question:
                contact_channel = pattern["derived_fields"]["Contact_Channel"]
                sql_parts["select"].insert(0, f"({contact_channel}) AS Contact_Channel")
                sql_parts["group_by"] = ["Contact_Channel"]
            elif "group" in question:
                sql_parts["select"].insert(0, "GROUP_NAME")
                sql_parts["group_by"] = ["GROUP_NAME"]
            elif "agent" in question:
                sql_parts["select"].insert(0, "ASSIGNEE_NAME")
                sql_parts["group_by"] = ["ASSIGNEE_NAME"]

            sql_parts["order_by"] = ["ticket_count DESC"]

        # Default: show recent tickets
        else:
            sql_parts["select"] = [
                "TICKET_ID",
                "ASSIGNEE_NAME",
                "GROUP_NAME",
                "STATUS",
                "CREATED_AT"
            ]
            sql_parts["order_by"] = ["CREATED_AT DESC"]
            sql_parts["limit"] = "LIMIT 100"

        return self._assemble_sql(sql_parts)

    def _build_handle_time_helper(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build HELPER SQL for handle time queries"""

        if "average" in question or "aht" in question.lower():
            sql_parts["select"] = ["AVG(HANDLE_TIME_IN_MINUTES) as avg_handle_time_minutes"]

            if "by agent" in question:
                sql_parts["select"].insert(0, "USER_NAME")
                sql_parts["group_by"] = ["USER_NAME"]
                sql_parts["select"].append("COUNT(*) as tickets_handled")

            sql_parts["order_by"] = ["avg_handle_time_minutes"]
        else:
            # Default: recent handle times
            sql_parts["select"] = [
                "TICKET_ID",
                "USER_NAME",
                "HANDLE_TIME_IN_MINUTES",
                "CONTACT_CHANNEL",
                "CREATED_AT"
            ]
            sql_parts["order_by"] = ["CREATED_AT DESC"]
            sql_parts["limit"] = "LIMIT 100"

        return self._assemble_sql(sql_parts)

    def _build_fcr_helper(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build HELPER SQL for FCR queries - basic structure"""

        # FCR requires complex window functions - provide basic structure
        sql_parts["select"] = [
            "ASSIGNEE_NAME",
            "COUNT(*) as total_tickets"
        ]
        sql_parts["group_by"] = ["ASSIGNEE_NAME"]
        sql_parts["order_by"] = ["total_tickets DESC"]
        sql_parts["limit"] = "LIMIT 20"

        # Add comment for OpenAI
        sql = self._assemble_sql(sql_parts)
        sql = f"-- FCR requires window functions - this is basic structure\n{sql}"

        return sql

    def _build_generic_helper(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build generic helper query"""
        sql_parts["select"] = ["*"]
        sql_parts["limit"] = "LIMIT 10"
        return self._assemble_sql(sql_parts)

    def _get_time_filter(self, question: str, date_column: str) -> Optional[str]:
        """Extract time filter from question"""
        if "today" in question:
            return f"DATE({date_column}) = CURRENT_DATE()"
        elif "yesterday" in question:
            return f"DATE({date_column}) = DATEADD(day, -1, CURRENT_DATE())"
        elif "last week" in question:
            return f"{date_column} >= DATEADD(week, -1, CURRENT_DATE())"
        elif "this week" in question:
            return f"WEEK({date_column}) = WEEK(CURRENT_DATE()) AND YEAR({date_column}) = YEAR(CURRENT_DATE())"
        elif "last month" in question:
            return f"{date_column} >= DATEADD(month, -1, CURRENT_DATE())"
        elif "this month" in question:
            return f"MONTH({date_column}) = MONTH(CURRENT_DATE()) AND YEAR({date_column}) = YEAR(CURRENT_DATE())"
        return None

    def _assemble_sql(self, parts: Dict) -> str:
        """Assemble SQL from parts"""
        sql = f"SELECT {', '.join(parts['select'])}\n"
        sql += f"FROM {parts['from']}\n"

        if parts['where']:
            sql += f"WHERE {' AND '.join(parts['where'])}\n"

        if parts['group_by']:
            sql += f"GROUP BY {', '.join(parts['group_by'])}\n"

        if parts['order_by']:
            sql += f"ORDER BY {', '.join(parts['order_by'])}\n"

        if parts['limit']:
            sql += f"{parts['limit']}\n"

        return sql.strip()