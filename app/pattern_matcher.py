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
                    "performance metrics", "agent scorecard", "team performance",
                    "performance comparison", "agent kpi", "weekly kpi",
                    "csat", "customer satisfaction", "performance correlation"
                ],
                "questions": [
                    "agent performance", "top performing agents", "weekly performance",
                    "performance trends", "agent rankings", "performance dashboard",
                    "who are the best agents", "agent stats", "weekly stats",
                    "performance comparison", "week over week", "agent scorecard",
                    "team performance", "performance metrics", "agent kpi"
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
                "id": "wops_tickets",
                "name": "WOPS Tickets Comprehensive Analysis",
                "table": "ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS",
                "keywords": [
                    "ticket", "tickets", "created", "volume", "channel", "group",
                    "agent", "response time", "resolution time", "handle time",
                    "status", "escalation", "dispute", "payment", "waiver",
                    "urgent shift", "contact driver", "zendesk"
                ],
                "questions": [
                    "how many tickets", "ticket volume", "tickets created",
                    "ticket distribution", "tickets by", "show tickets",
                    "ticket trends", "agent performance", "contact channel",
                    "escalation patterns", "ticket count"
                ],
                "business_context": """This table contains all ticket data with comprehensive business filters applied.""",
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
                "id": "klaus_qa",
                "name": "Klaus Quality Assurance Reviews",
                "table": "ANALYTICS.DBT_PRODUCTION.FCT_KLAUS__REVIEWS",
                "keywords": [
                    "qa", "quality", "score", "review", "klaus", "pass", "fail",
                    "resolution", "communication", "handling", "auto-fail",
                    "scorecard", "ata", "component", "reviewer"
                ],
                "questions": [
                    "qa score", "quality score", "agent qa", "passed qa",
                    "failed qa", "qa breakdown", "qa component", "qa trends",
                    "qa pass rate", "auto fail", "ata score"
                ],
                "business_context": """Complex QA scoring with multiple scorecards and pass thresholds.""",
                "standard_filters": "",
                "key_columns": {
                    "identifiers": ["REVIEW_ID", "REVIEWEE_ID", "REVIEWER_ID"],
                    "dimensions": ["REVIEWEE_NAME", "REVIEWER_NAME", "SCORECARD_ID", "IS_PASS_FAIL"],
                    "timestamps": ["REVIEW_CREATED_AT"],
                    "metrics": ["Overall_Score", "Resolution", "Clear_Communication", "Handling", "No_Auto_Fail"],
                    "categories": ["SCORECARD_NAME"]
                }
            },
            {
                "id": "handle_time",
                "name": "Agent Handle Time Analysis",
                "table": "ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME",
                "keywords": [
                    "handle time", "aht", "average handle", "call duration",
                    "hold time", "talk time", "efficiency", "duration"
                ],
                "questions": [
                    "average handle time", "aht", "handle time by",
                    "longest handle time", "handle time trend", "call duration",
                    "voice metrics", "agent efficiency"
                ],
                "business_context": """Pre-calculated handle time metrics with voice channel details.""",
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
                "name": "First Contact Resolution",
                "table": "ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS",
                "keywords": [
                    "fcr", "first contact", "resolution rate", "repeat contact",
                    "channel switching", "resolved first time", "follow up"
                ],
                "questions": [
                    "fcr rate", "first contact resolution", "repeat contacts",
                    "channel switching", "resolved first time", "which agents best fcr"
                ],
                "business_context": """Requires complex window functions to calculate FCR.""",
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
                    ("trends", 6), ("scorecard", 6), ("kpi", 6)
                ]

                for indicator, points in performance_indicators:
                    if indicator in question_lower:
                        score += points
                        matched_items.append(f"performance:'{indicator}'({points})")

            elif pattern["id"] == "wops_tickets":
                if any(term in question_lower for term in ["ticket", "volume", "created", "how many"]):
                    score += 5
                    matched_items.append("tickets_bonus")

            elif pattern["id"] == "klaus_qa":
                if any(term in question_lower for term in ["qa", "quality", "score", "pass", "fail"]):
                    score += 5
                    matched_items.append("qa_bonus")

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
        elif pattern["id"] == "wops_tickets":
            sql = self._build_wops_tickets_helper(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "klaus_qa":
            sql = self._build_klaus_helper(question_lower, pattern, intent, sql_parts)
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

    def _build_klaus_helper(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build HELPER SQL for Klaus QA queries"""

        if "pass rate" in question:
            sql_parts["select"] = [
                "REVIEWEE_NAME",
                "AVG(CASE WHEN IS_PASS_FAIL = 'Pass' THEN 1.0 ELSE 0 END) * 100 as pass_rate",
                "AVG(Overall_Score) as avg_qa_score",
                "COUNT(*) as reviews_count"
            ]
            sql_parts["where"].append("REVIEW_CREATED_AT >= CURRENT_DATE - 30")
            sql_parts["group_by"] = ["REVIEWEE_NAME"]
            sql_parts["order_by"] = ["pass_rate DESC"]
        else:
            # Default: recent reviews
            sql_parts["select"] = [
                "REVIEWEE_NAME",
                "Overall_Score",
                "IS_PASS_FAIL",
                "REVIEW_CREATED_AT"
            ]
            sql_parts["order_by"] = ["REVIEW_CREATED_AT DESC"]
            sql_parts["limit"] = "LIMIT 50"

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