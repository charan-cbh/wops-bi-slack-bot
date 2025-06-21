# app/pattern_matcher.py
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
                "standard_filters": """
WHERE STATUS IN ('closed', 'solved')
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
                "standard_filters": """
WHERE status IN ('solved', 'closed')
  AND brand_id IN ('360002340693', '29186504989207')
  AND channel = 'native_messaging'
  AND group_id IN ('17837476387479', '28949203098007')""",
                "requires_window_functions": True
            },
            {
                "id": "wops_agent_performance",
                "name": "WOPS Agent Performance (Weekly Aggregated)",
                "table": "ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE",
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
                "standard_filters": "",
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
                    "Performance_Score": """(
                  (FCR_PERCENTAGE * 0.3) + 
                  (QA_SCORE * 0.3) + 
                  (CASE WHEN AHT_MINUTES <= 10 THEN 100 ELSE GREATEST(0, 100 - (AHT_MINUTES - 10) * 5) END * 0.2) +
                  (CASE WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN 50 
                        ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 END * 0.2)
                )"""
                }
            }
        ]

    def match_pattern(self, question: str) -> Optional[Dict]:
        """Find the best matching pattern for a question"""
        question_lower = question.lower()
        scores = []

        for pattern in self.patterns:
            score = 0

            # Check exact question matches
            for q in pattern["questions"]:
                if q in question_lower:
                    score += 10

            # Check keyword matches
            for keyword in pattern["keywords"]:
                if keyword in question_lower:
                    score += 2

            # Bonus for specific indicators
            if pattern["id"] == "wops_tickets" and any(
                    term in question_lower for term in ["ticket", "volume", "created"]
            ):
                score += 5

            if pattern["id"] == "klaus_qa" and any(
                    term in question_lower for term in ["qa", "quality", "score", "pass", "fail"]
            ):
                score += 5

            if pattern["id"] == "handle_time" and any(
                    term in question_lower for term in ["handle time", "aht", "duration"]
            ):
                score += 5

            if pattern["id"] == "fcr" and any(
                    term in question_lower for term in ["fcr", "first contact", "resolution rate"]
            ):
                score += 5

            scores.append((pattern, score))

        # Return best match if score is high enough
        best_match = max(scores, key=lambda x: x[1])
        if best_match[1] >= 5:  # Threshold for pattern match
            return best_match[0]

        return None

    def get_pattern_by_table(self, table_name: str) -> Optional[Dict]:
        """Get pattern configuration by table name"""
        for pattern in self.patterns:
            if pattern["table"].upper() == table_name.upper():
                return pattern
        return None


class PatternBasedQueryBuilder:
    """Builds SQL queries using documented patterns"""

    def __init__(self):
        self.pattern_matcher = PatternMatcher()

    def build_query(self, question: str, pattern: Dict, intent: Dict) -> str:
        """Build SQL query based on pattern and intent"""
        question_lower = question.lower()
        table = pattern["table"]

        # Start with base query
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
            sql_parts["where"].append(pattern["standard_filters"].strip().replace("WHERE ", ""))

        # Build query based on intent and pattern
        if pattern["id"] == "wops_tickets":
            sql = self._build_wops_query(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "klaus_qa":
            sql = self._build_klaus_query(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "handle_time":
            sql = self._build_handle_time_query(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "fcr":
            sql = self._build_fcr_query(question_lower, pattern, intent, sql_parts)
        elif pattern["id"] == "wops_agent_performance":
            sql = self._build_wops_performance_query(question_lower, pattern, intent, sql_parts)
        else:
            sql = self._build_generic_query(question_lower, pattern, intent, sql_parts)

        return sql

    def _build_wops_query(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build WOPS tickets query"""

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
                sql_parts["select"].insert(0, f"{contact_channel} AS Contact_Channel")
                sql_parts["group_by"] = ["Contact_Channel"]
            elif "group" in question:
                sql_parts["select"].insert(0, "GROUP_NAME")
                sql_parts["group_by"] = ["GROUP_NAME"]
            elif "agent" in question:
                sql_parts["select"].insert(0, "ASSIGNEE_NAME")
                sql_parts["group_by"] = ["ASSIGNEE_NAME"]
            elif "type" in question:
                sql_parts["select"].insert(0, "WOPS_TICKET_TYPE_A")
                sql_parts["group_by"] = ["WOPS_TICKET_TYPE_A"]

            sql_parts["order_by"] = ["ticket_count DESC"]

        # Response/resolution time queries
        elif any(term in question for term in ["response time", "resolution time"]):
            if "average" in question or "avg" in question:
                sql_parts["select"] = [
                    "AVG(REPLY_TIME_IN_MINUTES) as avg_reply_time",
                    "AVG(FIRST_RESOLUTION_TIME_IN_MINUTES) as avg_resolution_time"
                ]
            else:
                sql_parts["select"] = [
                    "TICKET_ID",
                    "REPLY_TIME_IN_MINUTES",
                    "FIRST_RESOLUTION_TIME_IN_MINUTES"
                ]
                sql_parts["order_by"] = ["FIRST_RESOLUTION_TIME_IN_MINUTES DESC"]
                sql_parts["limit"] = "LIMIT 20"

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

    def _build_klaus_query(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build Klaus QA query"""

        # Need to use the complex CTE structure for Klaus
        base_cte = self._get_klaus_base_cte()

        if "pass rate" in question or "pass fail" in question:
            return f"""{base_cte}
SELECT 
  REVIEWEE_NAME,
  AVG(CASE WHEN IS_PASS_FAIL = 'Pass' THEN 1.0 ELSE 0 END) * 100 as pass_rate,
  AVG(Overall_Score) as avg_qa_score,
  COUNT(*) as reviews_count
FROM klaus_scores
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - 30
GROUP BY REVIEWEE_NAME
HAVING COUNT(*) >= 3
ORDER BY pass_rate DESC"""

        elif "qa score" in question or "quality score" in question:
            group_by = "REVIEWEE_NAME"
            if "by team" in question:
                # Would need team mapping
                pass

            return f"""{base_cte}
SELECT 
  {group_by},
  AVG(Overall_Score) as avg_score,
  AVG(Resolution) as avg_resolution,
  AVG(Clear_Communication) as avg_communication,
  AVG(Handling) as avg_handling,
  COUNT(*) as review_count
FROM klaus_scores
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - 30
GROUP BY {group_by}
ORDER BY avg_score DESC"""

        else:
            # Default: recent reviews
            return f"""{base_cte}
SELECT 
  REVIEWEE_NAME,
  REVIEWER_NAME,
  Overall_Score,
  IS_PASS_FAIL,
  Resolution,
  Clear_Communication,
  Handling,
  REVIEW_CREATED_AT
FROM klaus_scores
ORDER BY REVIEW_CREATED_AT DESC
LIMIT 50"""

    def _build_handle_time_query(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build handle time query"""

        if "average" in question or "aht" in question.lower():
            sql_parts["select"] = ["AVG(HANDLE_TIME_IN_MINUTES) as avg_handle_time_minutes"]

            # Add grouping if specified
            if "by agent" in question:
                sql_parts["select"].insert(0, "USER_NAME")
                sql_parts["group_by"] = ["USER_NAME"]
                sql_parts["select"].append("COUNT(*) as tickets_handled")
            elif "by channel" in question:
                sql_parts["select"].insert(0, "CONTACT_CHANNEL")
                sql_parts["group_by"] = ["CONTACT_CHANNEL"]

            sql_parts["order_by"] = ["avg_handle_time_minutes"]

        elif "voice" in question or "call" in question:
            sql_parts["select"] = [
                "USER_NAME",
                "AVG(AMAZON_CONNECT_TALK_TIME_IN_MINUTES) as avg_talk_time",
                "AVG(AMAZON_CONNECT_HOLD_TIME_IN_MINUTES) as avg_hold_time",
                "AVG(AMAZON_CONNECT_CALL_DURATION_IN_MINUTES) as avg_call_duration",
                "COUNT(*) as calls_handled"
            ]
            sql_parts["where"].append("CONTACT_CHANNEL = 'Voice'")
            sql_parts["group_by"] = ["USER_NAME"]
            sql_parts["order_by"] = ["avg_call_duration DESC"]

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

        # Add time filter
        time_filter = self._get_time_filter(question, "CREATED_AT")
        if time_filter:
            sql_parts["where"].append(time_filter)

        return self._assemble_sql(sql_parts)

    def _build_fcr_query(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build FCR query - requires window functions"""

        base_cte = self._get_fcr_base_cte()

        if "fcr rate" in question or "first contact resolution" in question:
            if "by agent" in question:
                return f"""{base_cte}
SELECT 
  ASSIGNEE_NAME,
  AVG(IS_RESOLVED_FIRST_TIME) * 100 as fcr_rate,
  COUNT(*) as tickets_handled
FROM fcr_data
GROUP BY ASSIGNEE_NAME
HAVING COUNT(*) >= 10
ORDER BY fcr_rate DESC"""
            else:
                return f"""{base_cte}
SELECT AVG(IS_RESOLVED_FIRST_TIME) * 100 as overall_fcr_rate
FROM fcr_data"""

        elif "channel switching" in question:
            return f"""{base_cte}
SELECT 
  CONTACT_CHANNEL, 
  NEXT_CONTACT_CHANNEL, 
  COUNT(*) as switch_count
FROM fcr_data
WHERE IS_RESOLVED_FIRST_TIME = 0 
  AND NEXT_CONTACT_CHANNEL IS NOT NULL
GROUP BY CONTACT_CHANNEL, NEXT_CONTACT_CHANNEL
ORDER BY switch_count DESC"""

        else:
            return f"""{base_cte}
SELECT 
  AVG(IS_RESOLVED_FIRST_TIME) * 100 as fcr_rate,
  COUNT(*) as total_tickets
FROM fcr_data"""

    def _build_wops_performance_query(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build WOPS agent performance query with smart filtering"""

        # Add standard business filters for performance queries
        standard_filters = [
            "ASSIGNEE_NAME IS NOT NULL",  # Filter out unassigned tickets
            "ASSIGNEE_NAME != ''",  # Filter out empty names
            "NUM_TICKETS > 0"  # Only agents who actually handled tickets
        ]
        sql_parts["where"].extend(standard_filters)

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
                # Complete data first
                "QA_SCORE DESC",
                "FCR_PERCENTAGE DESC",
                "NUM_TICKETS DESC"
            ]
            sql_parts["limit"] = "LIMIT 10"

        # Performance trends for specific agent
        elif "trends" in question or "week over week" in question:
            sql_parts["select"] = [
                "SOLVED_WEEK",
                "ASSIGNEE_NAME",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE"
            ]
            # If specific agent mentioned, we'd need to parse it
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
            sql_parts["where"].extend([
                "SOLVED_WEEK >= CURRENT_DATE - INTERVAL '12 weeks'",
                "QA_SCORE IS NOT NULL"  # Only rank agents with QA data
            ])
            sql_parts["group_by"] = ["ASSIGNEE_NAME"]
            sql_parts["order_by"] = ["overall_rank"]

        # Weekly performance summary
        elif "weekly" in question or "this week" in question:
            sql_parts["select"] = [
                "ASSIGNEE_NAME",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE"
            ]
            sql_parts["where"].append(
                "SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE)")
            sql_parts["order_by"] = ["QA_SCORE DESC"]

        # Default: recent performance data with quality filtering
        else:
            sql_parts["select"] = [
                "ASSIGNEE_NAME",
                "SOLVED_WEEK",
                "NUM_TICKETS",
                "AHT_MINUTES",
                "FCR_PERCENTAGE",
                "QA_SCORE"
            ]
            # Add data quality filter for default queries
            sql_parts["where"].append("(QA_SCORE IS NOT NULL OR FCR_PERCENTAGE IS NOT NULL)")
            sql_parts["order_by"] = ["SOLVED_WEEK DESC", "QA_SCORE DESC"]
            sql_parts["limit"] = "LIMIT 50"

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
        elif "last 7 days" in question:
            return f"{date_column} >= CURRENT_DATE() - 7"
        elif "last 30 days" in question:
            return f"{date_column} >= CURRENT_DATE() - 30"
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

    def _get_klaus_base_cte(self) -> str:
        """Get the base CTE for Klaus queries"""
        # This is a simplified version - the full version would include all the complex calculations
        return """WITH klaus_scores AS (
  SELECT 
    REVIEWEE_NAME,
    REVIEWER_NAME,
    REVIEW_CREATED_AT,
    SCORECARD_ID,
    -- Complex score calculations would go here
    CASE 
      WHEN SCORECARD_ID IN (53921, 54262) AND Overall_Score >= 85 THEN 'Pass'
      WHEN SCORECARD_ID = 59144 AND Overall_Score >= 90 THEN 'Pass'
      ELSE 'Fail'
    END as IS_PASS_FAIL,
    Overall_Score,
    Resolution,
    Clear_Communication,
    Handling
  FROM ANALYTICS.DBT_PRODUCTION.FCT_KLAUS__REVIEWS
)"""

    def _get_fcr_base_cte(self) -> str:
        """Get the base CTE for FCR queries"""
        return """WITH fcr_data AS (
  SELECT
    ticket_id,
    requester_id,
    assignee_name,
    CASE
      WHEN group_id = '17837476387479' THEN 'Chat'
      WHEN group_id = '28949203098007' THEN 'Voice'
      ELSE 'Other'
    END AS contact_channel,
    LEAD(CASE
      WHEN group_id = '17837476387479' THEN 'Chat'
      WHEN group_id = '28949203098007' THEN 'Voice'
      ELSE 'Other'
    END) OVER (PARTITION BY requester_id ORDER BY created_at_pst) as next_contact_channel,
    created_at_pst,
    CASE
      WHEN created_at_pst + INTERVAL '24 HOUR' >= 
           LEAD(created_at_pst) OVER (PARTITION BY requester_id ORDER BY created_at_pst) 
      THEN 0
      ELSE 1
    END AS is_resolved_first_time
  FROM ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS
  WHERE status IN ('solved', 'closed')
    AND brand_id IN ('360002340693', '29186504989207')
    AND channel = 'native_messaging'
    AND group_id IN ('17837476387479', '28949203098007')
    AND created_at_pst >= CURRENT_DATE() - 42
)"""

    def _build_generic_query(self, question: str, pattern: Dict, intent: Dict, sql_parts: Dict) -> str:
        """Build generic query when specific pattern logic doesn't match"""
        # Default to showing sample data
        sql_parts["select"] = ["*"]
        sql_parts["limit"] = "LIMIT 10"
        return self._assemble_sql(sql_parts)