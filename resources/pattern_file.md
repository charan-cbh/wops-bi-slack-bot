# Metabase Query Patterns for Business Intelligence

This document contains proven SQL query patterns from Metabase reports that demonstrate how to answer common business questions using our Snowflake data warehouse.

## Overview
Each pattern represents a validated approach to answering specific types of business questions. These patterns have been extracted from production Metabase reports and include all necessary business logic, filters, and calculations.

---

## Pattern 1: WOPS Tickets Comprehensive Analysis

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS`

### Questions This Pattern Answers
- How many tickets were created today/yesterday/this week/last month?
- What is the ticket volume by channel/group/agent?
- Show ticket distribution by type/category/priority
- What are the response and resolution times?
- Which tickets have the longest handle time?
- Show tickets by status or escalation patterns
- Ticket trends over time
- Agent performance on ticket handling
- Contact channel breakdown
- Dispute and payment-related tickets analysis
- Urgent shift requests volume
- Waiver requests analysis

### Standard Business Filters (Always Apply)
```sql
WHERE STATUS IN ('closed', 'solved')
  AND (ASSIGNEE_NAME <> 'Automated Update' OR ASSIGNEE_NAME IS NULL)
  AND (NOT LOWER(TICKET_TAGS) LIKE '%email_blocked%' OR TICKET_TAGS IS NULL)
  AND CHANNEL IN ('api', 'email', 'native_messaging', 'web')
  AND (ASSIGNEE_NAME <> 'TechOps Bot' OR ASSIGNEE_NAME IS NULL)
  AND BRAND_ID IN ('29186504989207', '360002340693')
  AND (NOT LOWER(TICKET_TAGS) LIKE '%bulk_email_tool%' OR TICKET_TAGS IS NULL)
  AND (NOT LOWER(TICKET_TAGS) LIKE '%seattle_psst_monthly_email%' OR TICKET_TAGS IS NULL)
```

### Key Derived Fields
**Contact Channel** (derive from GROUP_ID):
```sql
CASE 
  WHEN GROUP_ID = '5495272772503' THEN 'Web'
  WHEN GROUP_ID = '17837476387479' THEN 'Chat'
  WHEN GROUP_ID = '28949203098007' THEN 'Voice'
  ELSE 'Other'
END AS Contact_Channel
```

**Ticket Sub-Type** (complex mapping based on WOPS_TICKET_TYPE_A):
```sql
CASE
  WHEN WOPS_TICKET_TYPE_A = 'wops_attendance_waiver_request' THEN WAIVER_REQUEST_DRIVER
  WHEN WOPS_TICKET_TYPE_A = 'mpt_ticket_type_payments' THEN PAYMENTS_CATEGORY_B
  WHEN WOPS_TICKET_TYPE_A = 'mpt_ticket_type_general_assistance' THEN PRODUCT_FEATURE_A
  WHEN WOPS_TICKET_TYPE_A = 'mpt_ticket_type_urgent_shifts' THEN URGENT_SHIFTS_TYPE_A
  WHEN WOPS_TICKET_TYPE_A = 'mpt_ticket_type_reactivation_request' THEN DEACTIVATION_REASON
  ELSE WOPS_TICKET_TYPE_A
END AS ticket_sub_type
```

### Important Columns
- **Identifiers**: TICKET_ID, REQUESTER_ID, ASSIGNEE_ID, WORKER_ID
- **Dimensions**: GROUP_NAME, CHANNEL, STATUS, PRIORITY, TICKET_TYPE, ASSIGNEE_NAME
- **Timestamps**: CREATED_AT, SOLVED_AT, ASSIGNED_AT, UPDATED_AT (all have _PST versions)
- **Metrics**: REPLY_TIME_IN_MINUTES, FIRST_RESOLUTION_TIME_IN_MINUTES, FULL_RESOLUTION_TIME_IN_MINUTES, HANDLE_TIME
- **Categories**: WOPS_TICKET_TYPE_A, PAYMENTS_CATEGORY_B, DISPUTE_DRIVER_A, ESCALATION_TEAM

### Sample Queries

**Daily ticket count**:
```sql
SELECT COUNT(*) as ticket_count 
FROM ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS 
WHERE DATE(CREATED_AT) = CURRENT_DATE()
  AND STATUS IN ('closed', 'solved')
  AND [other standard filters]
```

**Ticket volume by channel**:
```sql
SELECT 
  CASE 
    WHEN GROUP_ID = '5495272772503' THEN 'Web'
    WHEN GROUP_ID = '17837476387479' THEN 'Chat'
    WHEN GROUP_ID = '28949203098007' THEN 'Voice'
    ELSE 'Other'
  END AS Contact_Channel,
  COUNT(*) as ticket_count
FROM ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS
WHERE [standard filters]
GROUP BY Contact_Channel
ORDER BY ticket_count DESC
```

### Query Adaptations
- **For date filtering**: `WHERE CREATED_AT >= CURRENT_DATE - 7`
- **For specific agent**: `WHERE ASSIGNEE_NAME = 'John Smith'`
- **For ticket type**: `WHERE WOPS_TICKET_TYPE_A = 'specific_type'`
- **For volume by group**: `GROUP BY GROUP_NAME`
- **For hourly trends**: `GROUP BY DATE_TRUNC('hour', CREATED_AT_PST)`

---

## Pattern 2: Agent Handle Time Analysis

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME`

### Questions This Pattern Answers
- What is the average handle time (AHT)?
- Show AHT by agent/team/channel
- Which agents have the longest handle times?
- Handle time trends over time
- Call duration vs handle time analysis (voice channel)
- Hold time impact on total handle time
- Handle time by ticket type/category
- Voice channel metrics (talk time, hold time)
- Escalated tickets handle time
- Handle time distribution analysis
- Agent efficiency rankings
- Channel comparison for handle times
- Peak hours handle time analysis

### Pre-Calculated Metrics (No calculations needed)
- **Handle Time**: Available in both HANDLE_TIME_IN_SECONDS and HANDLE_TIME_IN_MINUTES
- **Voice Metrics**: 
  - AMAZON_CONNECT_CALL_DURATION_IN_[SECONDS/MINUTES]
  - AMAZON_CONNECT_HOLD_TIME_IN_[SECONDS/MINUTES]
  - AMAZON_CONNECT_TALK_TIME_IN_[SECONDS/MINUTES]

### Key Columns
- **Identifiers**: TICKET_ID, USER_ID, TICKET_USER_ID
- **Agent Info**: USER_NAME, USER_EMAIL, SUPERVISOR
- **Grouping**: GROUP_ID, GROUP_NAME, CONTACT_CHANNEL
- **Categories**: WOPS_TICKET_TYPE_A, PRODUCT_HELP_CATEGORY, PAYMENTS_CATEGORY_B, EA_TICKET_TYPE
- **Escalation**: ESCALATION_TEAM, ESCALATION_TYPE
- **Timestamps**: CREATED_AT, CREATED_AT_PST, SOLVED_AT, SOLVED_AT_PST

### Sample Queries

**Average AHT by agent**:
```sql
SELECT 
  USER_NAME, 
  AVG(HANDLE_TIME_IN_MINUTES) as avg_aht_minutes,
  COUNT(*) as tickets_handled
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE CREATED_AT >= CURRENT_DATE - 7
GROUP BY USER_NAME
ORDER BY avg_aht_minutes
```

**Voice channel efficiency**:
```sql
SELECT 
  USER_NAME,
  AVG(AMAZON_CONNECT_TALK_TIME_IN_MINUTES) as avg_talk_time,
  AVG(AMAZON_CONNECT_HOLD_TIME_IN_MINUTES) as avg_hold_time,
  AVG(AMAZON_CONNECT_CALL_DURATION_IN_MINUTES) as avg_call_duration
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE CONTACT_CHANNEL = 'Voice'
  AND CREATED_AT >= CURRENT_DATE - 7
GROUP BY USER_NAME
ORDER BY avg_call_duration DESC
```

**Handle time distribution**:
```sql
SELECT 
  CASE 
    WHEN HANDLE_TIME_IN_MINUTES < 5 THEN '0-5 min'
    WHEN HANDLE_TIME_IN_MINUTES < 10 THEN '5-10 min'
    WHEN HANDLE_TIME_IN_MINUTES < 20 THEN '10-20 min'
    WHEN HANDLE_TIME_IN_MINUTES < 30 THEN '20-30 min'
    ELSE '30+ min'
  END as time_bucket,
  COUNT(*) as ticket_count
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE CREATED_AT >= CURRENT_DATE - 7
GROUP BY time_bucket
ORDER BY time_bucket
```

### Query Adaptations
- **For outlier exclusion**: `WHERE HANDLE_TIME_IN_MINUTES < 120`
- **For voice only**: `WHERE CONTACT_CHANNEL = 'Voice'`
- **For time patterns**: `GROUP BY EXTRACT(HOUR FROM CREATED_AT_PST)`
- **For ticket type analysis**: `GROUP BY WOPS_TICKET_TYPE_A`

---

## Pattern 3: First Contact Resolution (FCR) Analysis

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS` (with window functions)

### Questions This Pattern Answers
- What is the FCR rate?
- Which agents have the best FCR?
- FCR by channel (Chat vs Voice)
- Which issue types have low FCR?
- Channel switching patterns (Chat to Voice, etc.)
- Repeat contact analysis
- FCR trends over time
- Impact of issue type on FCR
- Facility-specific FCR rates
- Supervisor/team FCR performance
- Same issue vs different issue follow-ups
- Time between repeat contacts

### FCR Definition
A ticket is considered "resolved first time" if the customer does not create another ticket within 24 hours.

### Window Function Logic
```sql
WITH FCR_CTE AS (
  SELECT
    ticket_id,
    LEAD(ticket_id) OVER (PARTITION BY requester_id ORDER BY created_at_pst) as next_ticket,
    requester_id,
    channel,
    LEAD(channel) OVER (PARTITION BY requester_id ORDER BY created_at_pst) as next_channel,
    -- Contact channel derivation
    CASE
      WHEN group_id = '17837476387479' THEN 'Chat'
      WHEN group_id = '28949203098007' THEN 'Voice'
      ELSE 'Other'
    END AS contact_channel,
    -- More LEAD functions for other fields...
    created_at_pst,
    LEAD(created_at_pst) OVER (PARTITION BY requester_id ORDER BY created_at_pst) AS next_ticket_date,
    -- FCR calculation
    CASE
      WHEN created_at_pst + INTERVAL '24 HOUR' >= LEAD(created_at_pst) OVER (PARTITION BY requester_id ORDER BY created_at_pst) THEN 0
      ELSE 1
    END AS is_resolved_first_time
  FROM ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS
  WHERE [filters]
)
```

### Pre-Applied Filters
- Time range: Last 6 weeks including current week
- Status: solved or closed only
- Brands: 360002340693, 29186504989207 (CBH brands)
- Channel: native_messaging only
- Groups: 17837476387479 (Chat), 28949203098007 (Voice)
- Excludes: email_blocked, bulk_email_tool, seattle_psst_monthly_email tags, TechOps Bot

### Key Derived Columns
- **Current ticket info**: CHANNEL, CONTACT_CHANNEL, ISSUE_TYPE, SHIFT_ID_S
- **Next ticket info**: NEXT_CHANNEL, NEXT_CONTACT_CHANNEL, NEXT_ISSUE_TYPE, NEXT_SHIFT_ID
- **FCR flag**: IS_RESOLVED_FIRST_TIME (1 = success, 0 = failure)

### Sample Queries

**Overall FCR rate**:
```sql
SELECT AVG(IS_RESOLVED_FIRST_TIME) * 100 as fcr_rate
FROM [FCR pattern query]
```

**FCR by agent**:
```sql
SELECT 
  ASSIGNEE_NAME,
  AVG(IS_RESOLVED_FIRST_TIME) * 100 as fcr_rate,
  COUNT(*) as tickets_handled
FROM [FCR pattern query]
GROUP BY ASSIGNEE_NAME
ORDER BY fcr_rate DESC
```

**Channel switching analysis**:
```sql
SELECT 
  CONTACT_CHANNEL, 
  NEXT_CONTACT_CHANNEL, 
  COUNT(*) as switches
FROM [FCR pattern query]
WHERE IS_RESOLVED_FIRST_TIME = 0 
  AND NEXT_CONTACT_CHANNEL IS NOT NULL
GROUP BY CONTACT_CHANNEL, NEXT_CONTACT_CHANNEL
```

**Same issue repeat rate**:
```sql
SELECT 
  ISSUE_TYPE,
  AVG(CASE WHEN ISSUE_TYPE = NEXT_ISSUE_TYPE THEN 1 ELSE 0 END) * 100 as same_issue_rate
FROM [FCR pattern query]
WHERE IS_RESOLVED_FIRST_TIME = 0
GROUP BY ISSUE_TYPE
```

---

## Pattern 4: WOPS Agent Performance (Weekly Aggregated)

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE`

### Questions This Pattern Answers
- What is overall agent performance this week/last week?
- Which agents are top performers across all metrics?
- Show weekly performance trends for specific agents
- Compare agent performance week-over-week
- What is the correlation between volume and quality metrics?
- Which agents have the best customer satisfaction scores?
- Show team performance rankings
- What are the performance benchmarks by metric?
- Which agents need performance coaching?
- Weekly performance dashboard metrics
- Agent performance distribution analysis
- Top/bottom performers identification
- Performance consistency analysis over time
- Customer satisfaction vs quality score correlation
- Handle time vs volume relationship analysis
- FCR leaders and improvement opportunities
- **QA score trends and agent quality rankings** (replaces detailed Klaus analysis)

### Pre-Aggregated Metrics (No Calculations Needed)
This table contains **pre-calculated weekly metrics** - no complex aggregations required.

- **Volume**: `NUM_TICKETS` (total tickets solved per week)
- **Efficiency**: `AHT_MINUTES` (average handle time in minutes)
- **Quality**: `QA_SCORE` (average QA score 0-100) - *replaces Klaus detailed analysis*
- **Effectiveness**: `FCR_PERCENTAGE` (first contact resolution rate 0-100)
- **Customer Satisfaction**: `POSITIVE_RES_CSAT`, `NEGATIVE_RES_CSAT` (satisfaction counts)

### Important Columns
- **Identifiers**: SOLVED_WEEK_ASSIGNEE_ID (unique weekly performance record)
- **Dimensions**: ASSIGNEE_NAME, SOLVED_WEEK (week ending date)
- **Volume Metrics**: NUM_TICKETS
- **Efficiency Metrics**: AHT_MINUTES
- **Quality Metrics**: QA_SCORE (0-100 scale) - *aggregated from all QA sources*
- **Effectiveness Metrics**: FCR_PERCENTAGE (0-100 scale)
- **Satisfaction Metrics**: POSITIVE_RES_CSAT, NEGATIVE_RES_CSAT

### Key Derived Fields for Analysis

**CSAT Rate**:
```sql
CASE 
  WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN NULL
  ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100
END AS csat_percentage
```

**Overall Performance Score** (composite metric):
```sql
(
  (FCR_PERCENTAGE * 0.3) + 
  (QA_SCORE * 0.3) + 
  (CASE WHEN AHT_MINUTES <= 10 THEN 100 ELSE GREATEST(0, 100 - (AHT_MINUTES - 10) * 5) END * 0.2) +
  (CASE WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN 50 
        ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 END * 0.2)
) AS performance_score
```

### Sample Queries

**Current week top performers**:
```sql
SELECT 
  ASSIGNEE_NAME,
  NUM_TICKETS,
  AHT_MINUTES,
  FCR_PERCENTAGE,
  QA_SCORE,
  POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 as csat_rate
FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE
WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE)
ORDER BY QA_SCORE DESC, FCR_PERCENTAGE DESC
LIMIT 10
```

**Weekly QA performance trends for specific agent**:
```sql
SELECT 
  SOLVED_WEEK,
  NUM_TICKETS,
  QA_SCORE,
  CASE
    WHEN QA_SCORE >= 90 THEN 'Excellent'
    WHEN QA_SCORE >= 80 THEN 'Good' 
    WHEN QA_SCORE >= 70 THEN 'Needs Improvement'
    ELSE 'Critical'
  END as qa_tier
FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE
WHERE ASSIGNEE_NAME = 'John Smith'
ORDER BY SOLVED_WEEK DESC
LIMIT 12  -- Last 12 weeks
```

**Week-over-week performance comparison**:
```sql
WITH current_week AS (
  SELECT * FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE
  WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE)
),
previous_week AS (
  SELECT * FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE
  WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE) - INTERVAL '7 days'
)
SELECT 
  c.ASSIGNEE_NAME,
  c.NUM_TICKETS as current_tickets,
  p.NUM_TICKETS as previous_tickets,
  c.NUM_TICKETS - p.NUM_TICKETS as ticket_change,
  c.QA_SCORE as current_qa,
  p.QA_SCORE as previous_qa,
  c.QA_SCORE - p.QA_SCORE as qa_change,
  c.FCR_PERCENTAGE - p.FCR_PERCENTAGE as fcr_change
FROM current_week c
LEFT JOIN previous_week p ON c.ASSIGNEE_NAME = p.ASSIGNEE_NAME
ORDER BY qa_change DESC
```

**Quality performance distribution analysis**:
```sql
SELECT 
  CASE 
    WHEN QA_SCORE >= 90 THEN 'Excellent (90+)'
    WHEN QA_SCORE >= 80 THEN 'Good (80-89)'
    WHEN QA_SCORE >= 70 THEN 'Needs Improvement (70-79)'
    ELSE 'Critical (<70)'
  END as qa_tier,
  CASE
    WHEN FCR_PERCENTAGE >= 80 THEN 'High FCR (80+)'
    WHEN FCR_PERCENTAGE >= 70 THEN 'Medium FCR (70-79)'
    ELSE 'Low FCR (<70)'
  END as fcr_tier,
  COUNT(*) as agent_count,
  AVG(NUM_TICKETS) as avg_volume
FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE
WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL '4 weeks'
GROUP BY qa_tier, fcr_tier
ORDER BY qa_tier, fcr_tier
```

**Agent quality rankings**:
```sql
SELECT 
  ASSIGNEE_NAME,
  AVG(QA_SCORE) as avg_qa_score,
  AVG(FCR_PERCENTAGE) as avg_fcr,
  AVG(NUM_TICKETS) as avg_weekly_tickets,
  COUNT(*) as weeks_active,
  ROW_NUMBER() OVER (ORDER BY AVG(QA_SCORE) DESC) as qa_rank
FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE
WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL '12 weeks'
  AND QA_SCORE IS NOT NULL
GROUP BY ASSIGNEE_NAME
HAVING COUNT(*) >= 8  -- At least 8 weeks of data
ORDER BY qa_rank
```

### Query Adaptations
- **For current week only**: `WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE)`
- **For last N weeks**: `WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL 'N weeks'`
- **For specific agent**: `WHERE ASSIGNEE_NAME = 'Agent Name'`
- **For QA analysis**: `WHERE QA_SCORE IS NOT NULL` (filter out weeks without QA data)
- **For quality trends**: `ORDER BY SOLVED_WEEK ASC/DESC`
- **For quality rankings**: Use `ROW_NUMBER() OVER (ORDER BY QA_SCORE DESC)`
- **For quality thresholds**: Use CASE statements for QA score tiers

### Business Intelligence Use Cases

**QA Performance Management** (replaces Klaus detailed analysis):
- Weekly QA score trends across all agents
- Quality rankings and tier distribution
- QA performance correlation with other metrics
- Quality improvement tracking over time

**Executive Dashboard Metrics**:
- Weekly performance summary across all agents
- Top/bottom performers identification
- Performance trend analysis
- Correlation insights between metrics

**Agent Coaching & Development**:
- Individual agent performance profiles
- Week-over-week improvement tracking
- Strength/weakness identification
- Performance goal tracking

---

## Cross-Pattern Analysis Guidelines

### For Complete Performance Analysis
Choose the appropriate level and pattern combination:

**Option A: Team Lead/Supervisor Analysis (Pattern 5)**
Use Pattern 5 (WOPS Team Lead Performance) for:
- Team performance summaries and rankings
- Supervisor effectiveness analysis
- Team capacity and workload planning
- Cross-team benchmarking
- Team-level QA and performance trends

**Option B: Individual Agent Analysis (Pattern 4)**
Use Pattern 4 (WOPS Agent Performance) for:
- Individual agent performance summaries
- Agent rankings and comparisons
- Personal performance trends over time
- Agent coaching insights
- Individual QA score analysis (weekly aggregates)

**Option C: Detailed QA Component Analysis (Pattern 6)** 🆕
Use Pattern 6 (WOPS Klaus QA & ATA) for:
- Individual QA review details and component breakdowns
- Auto-fail incident analysis
- Reviewer performance and consistency
- Scorecard-specific analysis (QA vs ATA)
- Channel and ticket type QA impact
- Root cause analysis for QA failures

**Option D: Detailed Operational Analysis (Patterns 1-3)**
Combine Patterns 1-3 when granular, real-time data is needed:
1. **Volume**: Use WOPS Tickets pattern (ticket-level details)
2. **Efficiency**: Use Handle Time pattern (detailed AHT metrics)
3. **Effectiveness**: Use FCR pattern (contact resolution analysis)

**Multi-Level QA Analysis Flow**:
```sql
-- Team QA Summary (Pattern 5)
-- → Agent QA Weekly (Pattern 4) 
-- → Individual Review Details (Pattern 6)
-- → Related Ticket Analysis (Pattern 1)
```

### For Channel Comparison
- Use Contact Channel derivation (same logic across patterns)
- Compare metrics across Chat, Voice, and Web channels

### For Time-Based Analysis
- All patterns have PST timestamp columns
- Use DATE_TRUNC for hourly/daily/weekly aggregations
- Consider business hours filtering when relevant

---

## Pattern 5: WOPS Team Lead Performance (Weekly Aggregated)

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE`

### Questions This Pattern Answers
- What is team lead performance this week/last week?
- Which team leads are top performers across all metrics?
- Show weekly team lead performance trends for specific supervisors
- Compare team lead performance week-over-week
- What is the correlation between team volume and team quality metrics?
- Which team leads have the best customer satisfaction scores?
- Show supervisor performance rankings
- What are the performance benchmarks by team lead?
- Which teams need performance coaching or support?
- Weekly team lead dashboard metrics
- Team lead performance distribution analysis
- Top/bottom performing teams identification
- Team performance consistency analysis over time
- Team-level customer satisfaction vs quality score correlation
- Team handle time vs volume relationship analysis
- Team FCR leaders and improvement opportunities
- Supervisor QA score trends and team quality rankings
- Team capacity and workload analysis
- Multi-team comparison and benchmarking

### Pre-Aggregated Metrics (No Calculations Needed)
This table contains **pre-calculated weekly team-level metrics** - no complex aggregations required.

- **Volume**: `NUM_TICKETS` (total tickets solved per week by team)
- **Efficiency**: `AHT_MINUTES` (average handle time in minutes for the team)
- **Quality**: `QA_SCORE` (average QA score 0-100 for the team)
- **Effectiveness**: `FCR_PERCENTAGE` (first contact resolution rate 0-100 for the team)
- **Customer Satisfaction**: `POSITIVE_RES_CSAT`, `NEGATIVE_RES_CSAT` (team satisfaction counts)

### Important Columns
- **Identifiers**: SOLVED_WEEK_SUPERVISOR_ID (unique weekly team performance record)
- **Dimensions**: SUPERVISOR (team lead name), SOLVED_WEEK (week ending date)
- **Volume Metrics**: NUM_TICKETS (team total)
- **Efficiency Metrics**: AHT_MINUTES (team average)
- **Quality Metrics**: QA_SCORE (team average, 0-100 scale)
- **Effectiveness Metrics**: FCR_PERCENTAGE (team rate, 0-100 scale)
- **Satisfaction Metrics**: POSITIVE_RES_CSAT, NEGATIVE_RES_CSAT (team totals)

### Key Derived Fields for Analysis

**Team CSAT Rate**:
```sql
CASE 
  WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN NULL
  ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100
END AS team_csat_percentage
```

**Team Performance Score** (composite metric):
```sql
(
  (FCR_PERCENTAGE * 0.3) + 
  (QA_SCORE * 0.3) + 
  (CASE WHEN AHT_MINUTES <= 12 THEN 100 ELSE GREATEST(0, 100 - (AHT_MINUTES - 12) * 4) END * 0.2) +
  (CASE WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN 50 
        ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 END * 0.2)
) AS team_performance_score
```

**Tickets Per Agent Estimate** (assuming average team size):
```sql
CASE 
  WHEN NUM_TICKETS = 0 THEN 0
  ELSE NUM_TICKETS / 8.0  -- Assuming average team size of 8 agents
END AS estimated_tickets_per_agent
```

### Sample Queries

**Current week top performing teams**:
```sql
SELECT 
  SUPERVISOR,
  NUM_TICKETS,
  AHT_MINUTES,
  FCR_PERCENTAGE,
  QA_SCORE,
  POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 as team_csat_rate,
  NUM_TICKETS / 8.0 as estimated_tickets_per_agent
FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE
WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE)
ORDER BY QA_SCORE DESC, FCR_PERCENTAGE DESC
LIMIT 10
```

**Weekly team performance trends for specific team lead**:
```sql
SELECT 
  SOLVED_WEEK,
  NUM_TICKETS,
  AHT_MINUTES,
  FCR_PERCENTAGE,
  QA_SCORE,
  CASE
    WHEN QA_SCORE >= 90 THEN 'Excellent Team'
    WHEN QA_SCORE >= 80 THEN 'Good Team' 
    WHEN QA_SCORE >= 70 THEN 'Needs Support'
    ELSE 'Critical Support Needed'
  END as team_qa_tier
FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE
WHERE SUPERVISOR = 'Team Lead Name'
ORDER BY SOLVED_WEEK DESC
LIMIT 12  -- Last 12 weeks
```

**Week-over-week team performance comparison**:
```sql
WITH current_week AS (
  SELECT * FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE
  WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE)
),
previous_week AS (
  SELECT * FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE
  WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE) - INTERVAL '7 days'
)
SELECT 
  c.SUPERVISOR,
  c.NUM_TICKETS as current_team_tickets,
  p.NUM_TICKETS as previous_team_tickets,
  c.NUM_TICKETS - p.NUM_TICKETS as team_ticket_change,
  c.QA_SCORE as current_team_qa,
  p.QA_SCORE as previous_team_qa,
  c.QA_SCORE - p.QA_SCORE as team_qa_change,
  c.FCR_PERCENTAGE - p.FCR_PERCENTAGE as team_fcr_change,
  c.AHT_MINUTES - p.AHT_MINUTES as team_aht_change
FROM current_week c
LEFT JOIN previous_week p ON c.SUPERVISOR = p.SUPERVISOR
ORDER BY team_qa_change DESC
```

**Team performance distribution analysis**:
```sql
SELECT 
  CASE 
    WHEN QA_SCORE >= 90 THEN 'Excellent Teams (90+)'
    WHEN QA_SCORE >= 80 THEN 'Good Teams (80-89)'
    WHEN QA_SCORE >= 70 THEN 'Teams Need Support (70-79)'
    ELSE 'Critical Support Needed (<70)'
  END as team_qa_tier,
  CASE
    WHEN FCR_PERCENTAGE >= 80 THEN 'High Team FCR (80+)'
    WHEN FCR_PERCENTAGE >= 70 THEN 'Medium Team FCR (70-79)'
    ELSE 'Low Team FCR (<70)'
  END as team_fcr_tier,
  COUNT(*) as team_count,
  AVG(NUM_TICKETS) as avg_team_volume,
  SUM(NUM_TICKETS) as total_tickets_in_tier
FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE
WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL '4 weeks'
GROUP BY team_qa_tier, team_fcr_tier
ORDER BY team_qa_tier, team_fcr_tier
```

**Team lead performance rankings**:
```sql
SELECT 
  SUPERVISOR,
  AVG(NUM_TICKETS) as avg_weekly_team_tickets,
  AVG(AHT_MINUTES) as avg_team_aht,
  AVG(FCR_PERCENTAGE) as avg_team_fcr,
  AVG(QA_SCORE) as avg_team_qa_score,
  AVG(POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100) as avg_team_csat_rate,
  COUNT(*) as weeks_active,
  ROW_NUMBER() OVER (ORDER BY AVG(QA_SCORE) DESC, AVG(FCR_PERCENTAGE) DESC) as team_rank
FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE
WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY SUPERVISOR
HAVING COUNT(*) >= 8  -- At least 8 weeks of data
ORDER BY team_rank
```

**Team capacity and workload analysis**:
```sql
SELECT 
  SUPERVISOR,
  AVG(NUM_TICKETS) as avg_weekly_volume,
  MAX(NUM_TICKETS) as peak_weekly_volume,
  MIN(NUM_TICKETS) as min_weekly_volume,
  STDDEV(NUM_TICKETS) as volume_consistency,
  AVG(NUM_TICKETS) / 8.0 as estimated_tickets_per_agent,
  CASE
    WHEN AVG(NUM_TICKETS) >= 200 THEN 'High Capacity Team'
    WHEN AVG(NUM_TICKETS) >= 120 THEN 'Medium Capacity Team'
    ELSE 'Low Capacity Team'
  END as capacity_tier
FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE
WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL '8 weeks'
GROUP BY SUPERVISOR
ORDER BY avg_weekly_volume DESC
```

**Team performance correlation analysis**:
```sql
SELECT 
  CORR(NUM_TICKETS, QA_SCORE) as team_volume_qa_correlation,
  CORR(AHT_MINUTES, FCR_PERCENTAGE) as team_aht_fcr_correlation,
  CORR(QA_SCORE, FCR_PERCENTAGE) as team_qa_fcr_correlation,
  CORR(NUM_TICKETS, AHT_MINUTES) as team_volume_aht_correlation,
  AVG(NUM_TICKETS) as avg_team_volume,
  AVG(QA_SCORE) as avg_team_qa_score
FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE
WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL '12 weeks'
```

### Query Adaptations
- **For current week only**: `WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE)`
- **For last N weeks**: `WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL 'N weeks'`
- **For specific team lead**: `WHERE SUPERVISOR = 'Team Lead Name'`
- **For team QA analysis**: `WHERE QA_SCORE IS NOT NULL` (filter out weeks without QA data)
- **For team performance trends**: `ORDER BY SOLVED_WEEK ASC/DESC`
- **For team rankings**: Use `ROW_NUMBER() OVER (ORDER BY QA_SCORE DESC)`
- **For capacity analysis**: Focus on NUM_TICKETS metrics and volume patterns
- **For multi-team comparison**: Use multiple WHERE conditions or GROUP BY rollups

### Business Intelligence Use Cases

**Team Lead Management Dashboard**:
- Weekly team performance summary across all supervisors
- Top/bottom performing teams identification
- Team performance trend analysis
- Capacity planning and workload distribution

**Operations Management**:
- Team capacity analysis and resource allocation
- Cross-team performance benchmarking
- Team efficiency vs quality balance
- Supervisor coaching and development priorities

**Executive Team Insights**:
- Organization-wide team performance health
- Team-level correlation between metrics
- Capacity utilization and optimization opportunities
- Customer satisfaction by team/supervisor

**Workforce Planning**:
- Team volume trends for staffing decisions
- Performance consistency analysis
- Team lead effectiveness evaluation
- Resource allocation optimization

### Integration with Other Patterns

**Drill-Down Analysis**:
- **From Team to Agent**: Use Pattern 4 (Agent Performance) filtered by supervisor
- **From Team to Tickets**: Use Pattern 1 (WOPS Tickets) filtered by team/supervisor
- **From Team to Handle Time**: Use Pattern 2 (Handle Time) with supervisor filters

**Cross-Level Analysis**:
```sql
-- Compare team lead results with individual agent results
SELECT 
  tl.SUPERVISOR,
  tl.QA_SCORE as team_qa_score,
  AVG(ap.QA_SCORE) as individual_agents_avg_qa,
  tl.QA_SCORE - AVG(ap.QA_SCORE) as team_vs_individual_qa_diff
FROM ANALYTICS.DBT_PRODUCTION.WOPS_TL_PERFORMANCE tl
LEFT JOIN ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE ap 
  ON ap.SOLVED_WEEK = tl.SOLVED_WEEK 
  AND ap.ASSIGNEE_NAME IN (SELECT agent_name FROM team_roster WHERE supervisor = tl.SUPERVISOR)
WHERE tl.SOLVED_WEEK >= CURRENT_DATE - INTERVAL '4 weeks'
GROUP BY tl.SUPERVISOR, tl.QA_SCORE
```

### Performance Benchmarks (Team-Level Suggested Thresholds)
- **Excellent Team QA**: 85+ score (higher threshold than individual)
- **Good Team FCR**: 75%+ resolution rate
- **Efficient Team AHT**: <12 minutes average
- **Active Team Volume**: 120+ tickets per week
- **Strong Team CSAT**: 80%+ positive rate
- **Consistent Performance**: Low standard deviation across weeks

---

---

## Pattern 6: WOPS Klaus QA & ATA Detailed Reviews

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA`

### Questions This Pattern Answers
- What are individual QA review details and scores?
- Show QA component breakdown (Resolution, Communication, Handling, Clerical)
- Which reviews had auto-fail incidents?
- What are the most common QA failures by component?
- QA score distribution by scorecard type
- Reviewer performance and consistency analysis
- Agent QA performance on specific components
- Detailed QA trends by ticket type and channel
- ATA (Agent Training Assessment) vs QA comparison
- QA review details for specific tickets
- Component score correlation analysis
- Scorecard-specific performance analysis
- QA failure root cause analysis
- Channel-specific QA performance
- Ticket type impact on QA scores

### Scorecard Types Available
Based on SCORECARD_ID and SCORECARD_NAME columns, this table contains multiple scorecard types including QA and ATA assessments.

### Component Scoring Structure
Each component has both a **rating score** and a **base score**:
- **Resolution**: `RESOLUTION_RATING_SCORE` / `RESOLUTION_BASE`
- **Communication**: `COMMUNICATION_RATING_SCORE` / `COMMUNICATION_BASE`  
- **Handling**: `HANDLING_RATING_SCORE` / `HANDLING_BASE`
- **Clerical**: `CLERICAL_RATING_SCORE` / `CLERICAL_BASE`

### Important Columns
- **Identifiers**: REVIEW_ID, TICKET_ID, SCORECARD_ID
- **Review Details**: SCORECARD_NAME, SOURCE_TYPE, REVIEW_URL, TICKET_URL
- **People**: REVIEWEE_NAME, REVIEWEE_EMAIL, REVIEWER_NAME, REVIEWER_EMAIL
- **Timestamps**: REVIEW_CREATED_AT, REVIEW_UPDATED_AT, CREATED_AT_ISO, UPDATED_AT_ISO
- **Scores**: OVERALL_SCORE, NO_AUTO_FAIL_RATING_SCORE, component rating scores
- **Context**: WOPS_TICKET_TYPE_A, PAYMENTS_CATEGORY_B, CONTACT_CHANNEL
- **Details**: REVIEW_COMMENT, MARKDOWN_DETAIL

### Key Derived Fields for Analysis

**Component Percentage Scores**:
```sql
CASE 
  WHEN RESOLUTION_BASE = 0 THEN NULL
  ELSE (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100
END AS resolution_percentage,

CASE 
  WHEN COMMUNICATION_BASE = 0 THEN NULL
  ELSE (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100  
END AS communication_percentage,

CASE 
  WHEN HANDLING_BASE = 0 THEN NULL
  ELSE (HANDLING_RATING_SCORE / HANDLING_BASE) * 100
END AS handling_percentage,

CASE 
  WHEN CLERICAL_BASE = 0 THEN NULL
  ELSE (CLERICAL_RATING_SCORE / CLERICAL_BASE) * 100
END AS clerical_percentage
```

**Pass/Fail Status**:
```sql
CASE
  WHEN SCORECARD_NAME LIKE '%ATA%' AND OVERALL_SCORE >= 85 THEN 'Pass'
  WHEN SCORECARD_NAME LIKE '%QA%' AND OVERALL_SCORE >= 80 THEN 'Pass'
  WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 'Auto-Fail'
  ELSE 'Fail'
END AS review_status
```

**Auto-Fail Detection**:
```sql
CASE
  WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1
  ELSE 0
END AS has_auto_fail
```

### Sample Queries

**Agent QA component performance**:
```sql
SELECT 
  REVIEWEE_NAME,
  COUNT(*) as total_reviews,
  AVG(OVERALL_SCORE) as avg_overall_score,
  AVG(CASE WHEN RESOLUTION_BASE > 0 THEN (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100 END) as avg_resolution_pct,
  AVG(CASE WHEN COMMUNICATION_BASE > 0 THEN (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100 END) as avg_communication_pct,
  AVG(CASE WHEN HANDLING_BASE > 0 THEN (HANDLING_RATING_SCORE / HANDLING_BASE) * 100 END) as avg_handling_pct,
  AVG(CASE WHEN CLERICAL_BASE > 0 THEN (CLERICAL_RATING_SCORE / CLERICAL_BASE) * 100 END) as avg_clerical_pct,
  SUM(CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1 ELSE 0 END) as auto_fail_count
FROM ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY REVIEWEE_NAME
ORDER BY avg_overall_score DESC
```

**QA vs ATA scorecard comparison**:
```sql
SELECT 
  CASE 
    WHEN SCORECARD_NAME LIKE '%ATA%' THEN 'ATA'
    WHEN SCORECARD_NAME LIKE '%QA%' THEN 'QA'
    ELSE 'Other'
  END as scorecard_type,
  COUNT(*) as review_count,
  AVG(OVERALL_SCORE) as avg_score,
  AVG(CASE WHEN RESOLUTION_BASE > 0 THEN (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100 END) as avg_resolution,
  AVG(CASE WHEN COMMUNICATION_BASE > 0 THEN (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100 END) as avg_communication,
  SUM(CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1 ELSE 0 END) / COUNT(*) * 100 as auto_fail_rate
FROM ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY scorecard_type
ORDER BY scorecard_type
```

**Component failure analysis**:
```sql
SELECT 
  'Resolution' as component,
  AVG(CASE WHEN RESOLUTION_BASE > 0 THEN (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100 END) as avg_score,
  SUM(CASE WHEN RESOLUTION_BASE > 0 AND (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100 < 80 THEN 1 ELSE 0 END) as failure_count,
  COUNT(CASE WHEN RESOLUTION_BASE > 0 THEN 1 END) as total_scored

UNION ALL

SELECT 
  'Communication' as component,
  AVG(CASE WHEN COMMUNICATION_BASE > 0 THEN (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100 END) as avg_score,
  SUM(CASE WHEN COMMUNICATION_BASE > 0 AND (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100 < 80 THEN 1 ELSE 0 END) as failure_count,
  COUNT(CASE WHEN COMMUNICATION_BASE > 0 THEN 1 END) as total_scored

UNION ALL

SELECT 
  'Handling' as component,
  AVG(CASE WHEN HANDLING_BASE > 0 THEN (HANDLING_RATING_SCORE / HANDLING_BASE) * 100 END) as avg_score,
  SUM(CASE WHEN HANDLING_BASE > 0 AND (HANDLING_RATING_SCORE / HANDLING_BASE) * 100 < 80 THEN 1 ELSE 0 END) as failure_count,
  COUNT(CASE WHEN HANDLING_BASE > 0 THEN 1 END) as total_scored

UNION ALL

SELECT 
  'Clerical' as component,
  AVG(CASE WHEN CLERICAL_BASE > 0 THEN (CLERICAL_RATING_SCORE / CLERICAL_BASE) * 100 END) as avg_score,
  SUM(CASE WHEN CLERICAL_BASE > 0 AND (CLERICAL_RATING_SCORE / CLERICAL_BASE) * 100 < 80 THEN 1 ELSE 0 END) as failure_count,
  COUNT(CASE WHEN CLERICAL_BASE > 0 THEN 1 END) as total_scored

FROM ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY avg_score DESC
```

**Channel-specific QA performance**:
```sql
SELECT 
  CONTACT_CHANNEL,
  COUNT(*) as review_count,
  AVG(OVERALL_SCORE) as avg_overall_score,
  AVG(CASE WHEN RESOLUTION_BASE > 0 THEN (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100 END) as avg_resolution,
  AVG(CASE WHEN COMMUNICATION_BASE > 0 THEN (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100 END) as avg_communication,
  SUM(CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1 ELSE 0 END) / COUNT(*) * 100 as auto_fail_rate
FROM ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'
  AND CONTACT_CHANNEL IS NOT NULL
GROUP BY CONTACT_CHANNEL
ORDER BY avg_overall_score DESC
```

**Reviewer consistency analysis**:
```sql
SELECT 
  REVIEWER_NAME,
  COUNT(*) as reviews_conducted,
  AVG(OVERALL_SCORE) as avg_score_given,
  STDDEV(OVERALL_SCORE) as score_std_dev,
  MIN(OVERALL_SCORE) as min_score,
  MAX(OVERALL_SCORE) as max_score,
  SUM(CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1 ELSE 0 END) / COUNT(*) * 100 as auto_fail_rate
FROM ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY REVIEWER_NAME
HAVING COUNT(*) >= 10  -- Only reviewers with sufficient volume
ORDER BY score_std_dev ASC  -- Most consistent reviewers first
```

**Ticket type QA impact**:
```sql
SELECT 
  WOPS_TICKET_TYPE_A,
  COUNT(*) as review_count,
  AVG(OVERALL_SCORE) as avg_score,
  SUM(CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 1 ELSE 0 END) / COUNT(*) * 100 as auto_fail_rate,
  AVG(CASE WHEN RESOLUTION_BASE > 0 THEN (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100 END) as avg_resolution
FROM ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'
  AND WOPS_TICKET_TYPE_A IS NOT NULL
GROUP BY WOPS_TICKET_TYPE_A
ORDER BY auto_fail_rate DESC
```

**Individual review details lookup**:
```sql
SELECT 
  REVIEW_ID,
  TICKET_ID,
  REVIEWEE_NAME,
  REVIEWER_NAME,
  SCORECARD_NAME,
  OVERALL_SCORE,
  CASE WHEN NO_AUTO_FAIL_RATING_SCORE < 100 THEN 'Yes' ELSE 'No' END as auto_fail,
  CASE WHEN RESOLUTION_BASE > 0 THEN (RESOLUTION_RATING_SCORE / RESOLUTION_BASE) * 100 END as resolution_pct,
  CASE WHEN COMMUNICATION_BASE > 0 THEN (COMMUNICATION_RATING_SCORE / COMMUNICATION_BASE) * 100 END as communication_pct,
  CASE WHEN HANDLING_BASE > 0 THEN (HANDLING_RATING_SCORE / HANDLING_BASE) * 100 END as handling_pct,
  CASE WHEN CLERICAL_BASE > 0 THEN (CLERICAL_RATING_SCORE / CLERICAL_BASE) * 100 END as clerical_pct,
  REVIEW_COMMENT,
  REVIEW_CREATED_AT
FROM ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA
WHERE REVIEWEE_NAME = 'Agent Name'
  AND REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY REVIEW_CREATED_AT DESC
```

### Query Adaptations
- **For specific agent**: `WHERE REVIEWEE_NAME = 'Agent Name'`
- **For specific reviewer**: `WHERE REVIEWER_NAME = 'Reviewer Name'`
- **For scorecard type**: `WHERE SCORECARD_NAME LIKE '%QA%'` or `WHERE SCORECARD_NAME LIKE '%ATA%'`
- **For auto-fails only**: `WHERE NO_AUTO_FAIL_RATING_SCORE < 100`
- **For specific ticket**: `WHERE TICKET_ID = 'ticket_id'`
- **For channel analysis**: `WHERE CONTACT_CHANNEL = 'Chat'`
- **For date range**: `WHERE REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL 'N days'`
- **For component analysis**: Focus on specific RATING_SCORE/BASE combinations
- **For pass/fail analysis**: Use derived pass/fail logic based on scorecard thresholds

### Business Intelligence Use Cases

**Quality Assurance Management**:
- Individual QA review analysis and tracking
- Component-level performance identification
- Auto-fail incident investigation
- QA coaching and development priorities

**Agent Development**:
- Detailed component strength/weakness analysis
- QA improvement tracking over time
- Specific skill area focus identification
- Performance correlation with ticket types

**QA Program Analysis**:
- Reviewer consistency and calibration
- Scorecard effectiveness evaluation
- Channel-specific QA challenges
- Component scoring distribution analysis

**Operational Insights**:
- Ticket type impact on QA performance
- Contact channel QA correlation
- QA trend analysis and patterns
- Root cause analysis for QA failures

### Integration with Other Patterns

**Complement Weekly Summaries**:
- **From Pattern 4/5 Weekly QA**: Drill down to individual reviews with Pattern 6
- **From Pattern 6 Details**: Roll up to weekly trends with Pattern 4/5

**Cross-Analysis Opportunities**:
```sql
-- Compare detailed QA (Pattern 6) with weekly performance (Pattern 4)
SELECT 
  ap.ASSIGNEE_NAME,
  ap.SOLVED_WEEK,
  ap.QA_SCORE as weekly_qa_avg,
  AVG(qa.OVERALL_SCORE) as detailed_qa_avg,
  COUNT(qa.REVIEW_ID) as reviews_count
FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE ap
LEFT JOIN ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA qa
  ON ap.ASSIGNEE_NAME = qa.REVIEWEE_NAME
  AND qa.REVIEW_CREATED_AT >= ap.SOLVED_WEEK - INTERVAL '7 days'
  AND qa.REVIEW_CREATED_AT < ap.SOLVED_WEEK
WHERE ap.SOLVED_WEEK >= CURRENT_DATE - INTERVAL '4 weeks'
GROUP BY ap.ASSIGNEE_NAME, ap.SOLVED_WEEK, ap.QA_SCORE
ORDER BY ap.SOLVED_WEEK DESC
```

**Ticket-Level Analysis**:
```sql
-- Connect QA reviews (Pattern 6) with ticket details (Pattern 1)
SELECT 
  qa.TICKET_ID,
  qa.REVIEWEE_NAME,
  qa.OVERALL_SCORE,
  wt.WOPS_TICKET_TYPE_A,
  wt.HANDLE_TIME,
  wt.CONTACT_CHANNEL
FROM ANALYTICS.DBT_PRODUCTION.WOPS_KLAUS__QA_ATA qa
JOIN ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS wt
  ON qa.TICKET_ID = wt.TICKET_ID
WHERE qa.REVIEW_CREATED_AT >= CURRENT_DATE - INTERVAL '7 days'
```

### Performance Benchmarks (Review-Level)
- **Excellent QA**: 90+ overall score
- **Good QA**: 80-89 overall score
- **Component Target**: 80%+ on each component
- **Auto-Fail Rate**: <5% acceptable threshold
- **Review Volume**: Minimum 2-3 reviews per agent per month

---

## Pattern Selection Decision Tree

When users ask performance-related questions, use this decision tree:

```
Is the question about...

├── Detailed QA component analysis/individual reviews? 🆕
│   └── Use Pattern 6 (WOPS Klaus QA & ATA) ✅
│
├── Individual agent weekly performance/trends?
│   └── Use Pattern 4 (WOPS Agent Performance) ✅
│
├── Team lead/supervisor/team performance?
│   └── Use Pattern 5 (WOPS Team Lead Performance) ✅
│
├── Real-time/daily operational data?
│   └── Use Patterns 1-3 (detailed data)
│
└── [Other specific analyses...]
```

**Key Principle**: 
- Pattern 5 for **team lead/supervisor performance** questions
- Pattern 4 for **individual agent performance** questions  
- Pattern 6 for **detailed QA component analysis and individual review investigation** 🆕
- Patterns 1-3 for **detailed operational analysis** when weekly summaries aren't sufficient

## Priority Guidelines for AI Assistant

1. **First Choice - Team Level**: Pattern 5 for team lead, supervisor, or team performance questions
2. **First Choice - Agent Level**: Pattern 4 for individual agent performance, rankings, or weekly metrics
3. **First Choice - QA Details**: Pattern 6 for detailed QA component analysis, individual reviews, or QA investigation 🆕
4. **Second Choice**: Specific patterns (1-3) only when Patterns 4-6 don't have the required detail
5. **Multi-Level**: Start with appropriate summary pattern (4 or 5), drill down to detailed analysis (6 or 1-3) if needed

This approach provides faster, more accurate responses for the majority of business questions while still allowing detailed analysis when necessary.

## Important Notes
1. Always verify which filters are pre-applied in each pattern
2. Some patterns (like FCR) already have significant filtering
3. Handle time metrics are pre-calculated - no need for complex calculations
4. **QA scores are available as:**
   - **Weekly aggregates**: Patterns 4 and 5
   - **Individual review details**: Pattern 6 🆕
5. **Pattern 5 provides team-level insights, Pattern 4 provides agent-level insights, Pattern 6 provides review-level insights** 🆕
6. Consider joining patterns on TICKET_ID, AGENT identifiers, or SUPERVISOR for comprehensive analysis

## Common Business Questions and Pattern Usage

| Question | Primary Pattern | Additional Patterns | Notes |
|----------|----------------|-------------------|-------|
| "How many tickets today?" | WOPS Tickets (1) | - | Use for real-time daily metrics |
| "What's our AHT?" | Handle Time (2) | WOPS Performance (4/5) | Patterns 4/5 for weekly averages |
| "Agent performance dashboard" | **WOPS Agent Performance (4)** | Patterns 1-3 for details | **Start with Pattern 4** |
| "Team performance dashboard" | **WOPS TL Performance (5)** | Pattern 4 for agent details | **Start with Pattern 5** |
| "Top performing agents this week" | **WOPS Agent Performance (4)** | - | **Perfect for this question** |
| "Top performing teams this week" | **WOPS TL Performance (5)** | - | **Perfect for this question** |
| "Team lead rankings" | **WOPS TL Performance (5)** | - | **Team leader specific** |
| "Supervisor performance" | **WOPS TL Performance (5)** | Pattern 4 for team agents | **Team lead focus** |
| "Weekly performance trends" | **WOPS Performance (4/5)** | - | **Choose based on level needed** |
| "Agent QA scores" | **WOPS Agent Performance (4)** | Pattern 6 for details | **Weekly agent QA aggregates** |
| "Team QA scores" | **WOPS TL Performance (5)** | Pattern 6 for details | **Weekly team QA aggregates** |
| "QA component breakdown" | **WOPS Klaus QA & ATA (6)** 🆕 | - | **Detailed component analysis** |
| "QA review details" | **WOPS Klaus QA & ATA (6)** 🆕 | - | **Individual review investigation** |
| "Auto-fail analysis" | **WOPS Klaus QA & ATA (6)** 🆕 | - | **Auto-fail incident details** |
| "Reviewer performance" | **WOPS Klaus QA & ATA (6)** 🆕 | - | **QA reviewer consistency** |
| "QA by ticket type" | **WOPS Klaus QA & ATA (6)** 🆕 | Pattern 1 for ticket details | **Ticket type QA impact** |
| "QA by channel" | **WOPS Klaus QA & ATA (6)** 🆕 | - | **Channel-specific QA analysis** |
| "Quality rankings" | **WOPS Performance (4/5)** | Pattern 6 for details | **Choose appropriate level** |
| "Channel efficiency" | Handle Time (2) | WOPS Tickets (1) for volume | - |
| "Why are customers calling back?" | FCR (3) | WOPS Tickets (1) for issue types | - |
| "Peak hour analysis" | Handle Time (2) | WOPS Tickets (1) for volume | - |
| "Escalation patterns" | WOPS Tickets (1) | Handle Time (2) for duration | - |
| "Performance coaching insights" | **WOPS Performance (4/5)** | Pattern 6 for QA details | **Start with appropriate level** |
| "Week-over-week comparison" | **WOPS Performance (4/5)** | - | **Built for this analysis** |
| "Customer satisfaction trends" | **WOPS Performance (4/5)** | - | **Available at both levels** |
| "Team capacity planning" | **WOPS TL Performance (5)** | Pattern 4 for agent details | **Team lead specific** |
| "Workload distribution" | **WOPS TL Performance (5)** | Pattern 4 for agents | **Team level analysis** |
| "Cross-team benchmarking" | **WOPS TL Performance (5)** | - | **Multi-team comparison** |