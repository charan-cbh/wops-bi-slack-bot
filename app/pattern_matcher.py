import re
from typing import Dict, List, Optional, Tuple
import json


class PatternMatcher:
    """Matches user questions to documented query patterns"""

    def __init__(self):
        self.patterns = self._load_patterns()

    def _load_patterns(self) -> List[Dict]:
        """Load query patterns with their configurations"""
        return [
            {
                "id": "wops_agent_performance",
                "name": "WOPS Agent Performance (Weekly Aggregated)",
                "table": "ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE",
                "description": "Use for weekly agent performance summaries, rankings, trends, and executive dashboards",
                "keywords": [
                    "agent performance", "weekly performance", "performance dashboard",
                    "top performers", "agent rankings", "performance trends",
                    "agent stats", "performance summary", "weekly stats",
                    "performance metrics", "agent scorecard", "individual agent",
                    "performance comparison", "agent kpi", "weekly kpi",
                    "csat", "customer satisfaction", "performance correlation",
                    "individual performance", "agent weekly", "weekly agent"
                ],
                "questions": [
                    "agent performance", "top performing agents", "weekly performance",
                    "performance trends", "agent rankings", "performance dashboard",
                    "who are the best agents", "agent stats", "weekly stats",
                    "performance comparison", "week over week", "agent scorecard",
                    "individual agent performance", "performance metrics", "agent kpi",
                    "agent qa scores", "agent quality", "individual agent"
                ],
                "business_context": """This table contains pre-aggregated weekly performance metrics per agent.

Key Business Rules:
- Always filter out null/empty agent names: ASSIGNEE_NAME IS NOT NULL AND ASSIGNEE_NAME != ''
- Exclude system accounts: ASSIGNEE_NAME NOT IN ('None', 'null', 'Automated Update')
- None of the fields should be null
- Dont create new columns or assumptions, use the existing data to determine anything and analysis
- For rankings, prioritize agents with complete data (non-null QA_SCORE and FCR_PERCENTAGE)
- Current week: SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM table)
- Order by data completeness first, then performance metrics""",
                "standard_filters": """ASSIGNEE_NAME IS NOT NULL 
  AND ASSIGNEE_NAME != '' 
  AND ASSIGNEE_NAME != 'None'
  AND LOWER(ASSIGNEE_NAME) != 'null'""",
                "key_columns": {
                    "identifiers": ["SOLVED_WEEK_ASSIGNEE_ID"],
                    "dimensions": ["ASSIGNEE_NAME", "SOLVED_WEEK"],
                    "metrics": ["NUM_TICKETS", "AHT_MINUTES", "FCR_PERCENTAGE",
                                "QA_SCORE", "POSITIVE_RES_CSAT", "NEGATIVE_RES_CSAT"],
                    "timestamps": ["SOLVED_WEEK"]
                },
                "derived_fields": {
                    "CSAT_Rate": """CASE 
  WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN NULL
  ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100
END""",
                    "Data_Completeness_Priority": """CASE 
  WHEN QA_SCORE IS NOT NULL AND FCR_PERCENTAGE IS NOT NULL THEN 0 
  ELSE 1 
END"""
                }
            },
            {
                "id": "wops_tl_performance",
                "name": "WOPS Team Lead Performance (Weekly Aggregated)",
                "table": "ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE",
                "description": "Use for team lead/supervisor performance, team-level metrics, multi-team analysis",
                "keywords": [
                    "team lead", "team leader", "supervisor", "team performance",
                    "team stats", "team metrics", "team rankings", "team dashboard",
                    "supervisor performance", "team qa", "team fcr", "team aht",
                    "team capacity", "team volume", "cross team", "multi team",
                    "team comparison", "team benchmarking", "team trends",
                    "supervisor rankings", "team lead rankings", "team weekly",
                    "team satisfaction", "team csat", "workload distribution"
                ],
                "questions": [
                    "team performance", "team lead performance", "supervisor performance",
                    "team rankings", "team stats", "team dashboard", "team metrics",
                    "top performing teams", "team comparison", "team benchmarking",
                    "team capacity", "team volume", "workload distribution",
                    "cross team analysis", "multi team", "team qa scores",
                    "team fcr", "team aht", "team satisfaction", "supervisor rankings",
                    "team lead rankings", "team weekly performance"
                ],
                "business_context": """This table contains pre-aggregated weekly performance metrics per team/supervisor.

Key Business Rules:
- Always filter out null/empty supervisor names: SUPERVISOR IS NOT NULL AND SUPERVISOR != ''
- Use for team-level analysis and supervisor effectiveness
- Team metrics are aggregated from all agents under each supervisor
- For rankings, prioritize teams with complete data
- Current week: SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM table)""",
                "standard_filters": """SUPERVISOR IS NOT NULL 
  AND SUPERVISOR != '' 
  AND SUPERVISOR != 'None'
  AND LOWER(SUPERVISOR) != 'null'""",
                "key_columns": {
                    "identifiers": ["SOLVED_WEEK_SUPERVISOR_ID"],
                    "dimensions": ["SUPERVISOR", "SOLVED_WEEK"],
                    "metrics": ["NUM_TICKETS", "AHT_MINUTES", "FCR_PERCENTAGE",
                                "QA_SCORE", "POSITIVE_RES_CSAT", "NEGATIVE_RES_CSAT"],
                    "timestamps": ["SOLVED_WEEK"]
                },
                "derived_fields": {
                    "Team_CSAT_Rate": """CASE 
  WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN NULL
  ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100
END""",
                    "Estimated_Tickets_Per_Agent": """CASE 
  WHEN NUM_TICKETS = 0 THEN 0
  ELSE NUM_TICKETS / 8.0
END"""
                }
            },
            {
                "id": "wops_klaus_qa_ata",
                "name": "WOPS Klaus QA & ATA Detailed Reviews",
                "table": "ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA",
                "description": "Use for detailed QA component analysis, individual review investigation, auto-fail analysis",
                "keywords": [
                    "qa component", "qa breakdown", "qa details", "individual qa",
                    "auto fail", "auto-fail", "qa review", "qa investigation",
                    "resolution score", "communication score", "handling score",
                    "clerical score", "qa components", "reviewer performance",
                    "scorecard analysis", "ata review", "qa vs ata",
                    "component analysis", "qa coaching", "qa failure",
                    "review details", "qa by ticket type", "qa by channel"
                ],
                "questions": [
                    "qa component breakdown", "qa components", "auto fail analysis",
                    "qa review details", "individual qa reviews", "qa investigation",
                    "resolution component", "communication component", "handling component",
                    "clerical component", "reviewer performance", "reviewer consistency",
                    "scorecard analysis", "qa vs ata", "ata reviews",
                    "qa coaching priorities", "qa failure analysis", "qa by ticket type",
                    "qa by channel", "component scores", "detailed qa"
                ],
                "business_context": """This table contains individual QA review details with component breakdowns.

Key Business Rules:
- Each row represents one QA review with detailed component scores
- Component scores have both rating_score and base values for percentage calculation
- Auto-fail detection: NO_AUTO_FAIL_RATING_SCORE < 100
- Different scorecards (QA vs ATA) have different passing thresholds
- Use for detailed coaching and root cause analysis""",
                "standard_filters": """REVIEWEE_NAME IS NOT NULL 
  AND REVIEWEE_NAME != ''
  AND REVIEW_CREATED_AT IS NOT NULL""",
                "key_columns": {
                    "identifiers": ["REVIEW_ID", "TICKET_ID", "SCORECARD_ID"],
                    "dimensions": ["REVIEWEE_NAME", "REVIEWER_NAME", "SCORECARD_NAME", "CONTACT_CHANNEL"],
                    "timestamps": ["REVIEW_CREATED_AT", "REVIEW_UPDATED_AT"],
                    "metrics": ["OVERALL_SCORE", "NO_AUTO_FAIL_RATING_SCORE",
                                "RESOLUTION_RATING_SCORE", "COMMUNICATION_RATING_SCORE",
                                "HANDLING_RATING_SCORE", "CLERICAL_RATING_SCORE"],
                    "categories": ["WOPS_TICKET_TYPE_A", "PAYMENTS_CATEGORY_B"]
                },
                "derived_fields": {
                    "Resolution_Percentage": """CASE 
  WHEN RESOLUTION_BASE = 0 THEN NULL
  ELSE (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100
END""",
                    "Communication_Percentage": """CASE 
  WHEN COMMUNICATION_BASE = 0 THEN NULL
  ELSE (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100  
END""",
                    "Has_Auto_Fail": """CASE
  WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1
  ELSE 0
END"""
                }
            },
            {
                "id": "wops_tickets",
                "name": "WOPS Tickets Comprehensive Analysis",
                "table": "ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS",
                "description": "Use for detailed ticket-level analysis, daily metrics, real-time operational data",
                "keywords": [
                    "ticket", "tickets", "created", "volume", "channel", "group",
                    "agent", "response time", "resolution time", "handle time",
                    "status", "escalation", "dispute", "payment", "waiver",
                    "urgent shift", "contact driver", "zendesk", "daily tickets",
                    "ticket count", "ticket trends", "ticket distribution"
                ],
                "questions": [
                    "how many tickets", "ticket volume", "tickets created",
                    "ticket distribution", "tickets by", "show tickets",
                    "ticket trends", "contact channel", "daily tickets",
                    "escalation patterns", "ticket count", "tickets today",
                    "tickets yesterday", "ticket investigation"
                ],
                "business_context": """This table contains all ticket data with comprehensive business filters applied.

Use for real-time daily metrics and detailed ticket investigation.""",
                "standard_filters": """STATUS IN ('closed', 'solved')
  AND (ASSIGNEE_NAME <> 'Automated Update' OR ASSIGNEE_NAME IS NULL)
  AND (NOT LOWER(TICKET_TAGS) LIKE '%email_blocked%' OR TICKET_TAGS IS NULL)
  AND CHANNEL IN ('api', 'email', 'native_messaging', 'web')
  AND (ASSIGNEE_NAME <> 'TechOps Bot' OR ASSIGNEE_NAME IS NULL)
  AND BRAND_ID IN ('29186504989207', '360002340693')
  AND (NOT LOWER(TICKET_TAGS) LIKE '%bulk_email_tool%' OR TICKET_TAGS IS NULL)
  AND (NOT LOWER(TICKET_TAGS) LIKE '%seattle_psst_monthly_email%' OR TICKET_TAGS IS NULL)""",
                "key_columns": {
                    "identifiers": ["TICKET_ID", "REQUESTER_ID", "ASSIGNEE_ID", "WORKER_ID"],
                    "dimensions": ["GROUP_NAME", "CHANNEL", "STATUS", "PRIORITY", "TICKET_TYPE", "ASSIGNEE_NAME"],
                    "timestamps": ["CREATED_AT", "SOLVED_AT", "ASSIGNED_AT", "UPDATED_AT"],
                    "metrics": ["REPLY_TIME_IN_MINUTES", "FIRST_RESOLUTION_TIME_IN_MINUTES",
                                "FULL_RESOLUTION_TIME_IN_MINUTES", "HANDLE_TIME"],
                    "categories": ["WOPS_TICKET_TYPE_A", "PAYMENTS_CATEGORY_B", "DISPUTE_DRIVER_A", "ESCALATION_TEAM"]
                },
                "derived_fields": {
                    "Contact_Channel": """CASE 
  WHEN GROUP_ID = '5495272772503' THEN 'Web'
  WHEN GROUP_ID = '17837476387479' THEN 'Chat'
  WHEN GROUP_ID = '28949203098007' THEN 'Voice'
  ELSE 'Other'
END"""
                }
            },
            {
                "id": "handle_time",
                "name": "Agent Handle Time Analysis",
                "table": "ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME",
                "description": "Use for detailed handle time metrics, voice analytics, efficiency analysis",
                "keywords": [
                    "handle time", "aht", "average handle", "call duration",
                    "hold time", "talk time", "efficiency", "duration",
                    "voice metrics", "amazon connect", "call time"
                ],
                "questions": [
                    "average handle time", "aht", "handle time by",
                    "longest handle time", "handle time trend", "call duration",
                    "voice metrics", "agent efficiency", "talk time", "hold time"
                ],
                "business_context": """Pre-calculated handle time metrics with voice channel details.

Use for efficiency analysis and voice channel performance.""",
                "standard_filters": "",
                "key_columns": {
                    "identifiers": ["TICKET_ID", "USER_ID", "TICKET_USER_ID"],
                    "dimensions": ["USER_NAME", "USER_EMAIL", "SUPERVISOR", "GROUP_NAME", "CONTACT_CHANNEL"],
                    "timestamps": ["CREATED_AT", "CREATED_AT_PST", "SOLVED_AT", "SOLVED_AT_PST"],
                    "metrics": ["HANDLE_TIME_IN_MINUTES", "HANDLE_TIME_IN_SECONDS",
                                "AMAZON_CONNECT_CALL_DURATION_IN_MINUTES", "AMAZON_CONNECT_HOLD_TIME_IN_MINUTES"],
                    "categories": ["WOPS_TICKET_TYPE_A", "ESCALATION_TEAM"]
                }
            },
            {
                "id": "fcr",
                "name": "First Contact Resolution Analysis",
                "table": "ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS",
                "description": "Use for FCR analysis, repeat contact patterns, channel switching investigation",
                "keywords": [
                    "fcr", "first contact", "resolution rate", "repeat contact",
                    "channel switching", "resolved first time", "follow up",
                    "repeat customers", "callback", "second contact"
                ],
                "questions": [
                    "fcr rate", "first contact resolution", "repeat contacts",
                    "channel switching", "resolved first time", "which agents best fcr",
                    "callback rate", "repeat customers", "follow up contacts"
                ],
                "business_context": """Requires complex window functions to calculate FCR.

Use for understanding repeat contact patterns and resolution effectiveness.""",
                "standard_filters": """status IN ('solved', 'closed')
  AND brand_id IN ('360002340693', '29186504989207')
  AND channel = 'native_messaging'
  AND group_id IN ('17837476387479', '28949203098007')""",
                "requires_window_functions": True
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