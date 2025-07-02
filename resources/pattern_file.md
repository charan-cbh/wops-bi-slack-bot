# Metabase Query Patterns for Business Intelligence

This document contains proven SQL query patterns from Metabase reports that demonstrate how to answer common business questions using our Snowflake data warehouse.

## Overview
Each pattern represents a validated approach to answering specific types of business questions. These patterns have been extracted from production Metabase reports and include all necessary business logic, filters, and calculations.

---

## Pattern 1: WOPS Tickets Comprehensive Analysis

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS`

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
- Worker-specific ticket analysis
- Team lead performance metrics

### Pre-Filtered Business-Ready Data
✅ **RPT_WOPS_TICKETS is pre-filtered and includes only:**
- Closed/solved tickets
- Excludes automated agents (TechOps Bot, Automated Update)
- Excludes blocked emails (email_blocked, bulk_email_tool, seattle_psst_monthly_email)
- Only standard channels (api, email, native_messaging, web)
- Correct brand IDs (29186504989207, 360002340693)

**Simply add your specific criteria:**
```sql
WHERE CREATED_AT_PST >= CURRENT_DATE - 7    -- Date filtering
  AND AGENT_NAME = 'John Smith'             -- Agent filtering  
  AND CONTACT_CHANNEL = 'Chat'              -- Channel filtering
  AND TICKET_TYPE = 'General Assistance'   -- Type filtering
```

### Key Fields (Direct Access - No Derivation Needed)
**Contact Channel**: Direct field `CONTACT_CHANNEL` 
- Values: 'Web', 'Chat', 'Voice', 'Other'

**Ticket Classification**: Direct field `TICKET_SUB_TYPE`
- Pre-calculated business categories

### Important Columns
- **Identifiers**: TICKET_ID, ASSIGNEE_ID, WORKER_ID
- **People**: WORKER_NAME, WORKER_EMAIL, AGENT_NAME, AGENT_EMAIL, TEAM_LEAD
- **Dimensions**: GROUP_ID, NATIVE_ZENDESK_CHANNEL, CONTACT_CHANNEL, TICKET_STATUS, TICKET_TYPE, TICKET_SUB_TYPE
- **Categories**: 
  - WAIVER_REQUEST_DRIVER, WAIVER_REQUEST_RESOLUTION, ATTENDANCE_WAIVER_CRITERIA
  - PRODUCT_HELP_CATEGORY, PAYMENTS_CATEGORY, URGENT_SHIFTS_TYPE
  - ESCALATION_TEAM, ESCALATING_AGENT, RESOLUTION_TYPE, RESOLUTION
- **Timestamps**: CREATED_AT_PST, INITIALLY_ASSIGNED_AT_PST, ASSIGNED_AT_PST, SOLVED_AT_PST
- **Metrics**: 
  - REPLY_TIME_IN_MINUTES, FIRST_RESOLUTION_TIME_IN_MINUTES, FULL_RESOLUTION_TIME_IN_MINUTES
  - HANDLE_TIME, LAST_TOUCH_HANDLE_TIME
  - CONNECT_HANDLE_TIME_TOTAL_MIN, CONNECT_HANDLE_TIME_LAST_CALL_MIN
  - REOPENS, REPLIES, PROVIDED_SCORE

### Sample Queries

**Daily ticket count**:
```sql
SELECT COUNT(*) as ticket_count 
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS 
WHERE DATE(CREATED_AT_PST) = CURRENT_DATE()
```

**Ticket volume by contact channel**:
```sql
SELECT 
  CONTACT_CHANNEL,
  COUNT(*) as ticket_count,
  AVG(HANDLE_TIME) as avg_handle_time
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE CREATED_AT_PST >= CURRENT_DATE - 7
GROUP BY CONTACT_CHANNEL
ORDER BY ticket_count DESC
```

**Agent performance summary**:
```sql
SELECT 
  AGENT_NAME,
  TEAM_LEAD,
  COUNT(*) as tickets_handled,
  AVG(HANDLE_TIME) as avg_handle_time,
  AVG(REPLY_TIME_IN_MINUTES) as avg_reply_time,
  AVG(FIRST_RESOLUTION_TIME_IN_MINUTES) as avg_resolution_time
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE CREATED_AT_PST >= CURRENT_DATE - 7
GROUP BY AGENT_NAME, TEAM_LEAD
ORDER BY tickets_handled DESC
```

**Ticket type breakdown with resolution analysis**:
```sql
SELECT 
  TICKET_TYPE,
  TICKET_SUB_TYPE,
  COUNT(*) as ticket_count,
  AVG(FULL_RESOLUTION_TIME_IN_MINUTES) as avg_resolution_time,
  COUNT(CASE WHEN REOPENS > 0 THEN 1 END) as tickets_with_reopens
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE CREATED_AT_PST >= CURRENT_DATE - 30
GROUP BY TICKET_TYPE, TICKET_SUB_TYPE
ORDER BY ticket_count DESC
```

### Query Adaptations
- **For date filtering**: `WHERE CREATED_AT_PST >= CURRENT_DATE - 7`
- **For specific agent**: `WHERE AGENT_NAME = 'John Smith'`
- **For ticket type**: `WHERE TICKET_TYPE = 'specific_type'`
- **For team analysis**: `GROUP BY TEAM_LEAD`
- **For hourly trends**: `GROUP BY DATE_TRUNC('hour', CREATED_AT_PST)`
- **For worker-specific analysis**: `WHERE WORKER_ID = 'specific_worker_id'`

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

## Pattern 6: Agent Schedule Adherence Analysis

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE`

### Questions This Pattern Answers
- What is the overall schedule adherence rate?
- Which agents have the best/worst adherence?
- Schedule adherence by team/supervisor
- What are the most common non-adherent activities?
- How much time do agents spend offline vs scheduled?
- Schedule adherence trends over time
- Impact of scheduled task type on adherence
- Peak adherence hours analysis
- Adherence patterns by day of week
- Scheduled vs actual time analysis
- Break adherence patterns
- Training schedule adherence
- Meeting attendance rates
- Schedule variance analysis

### Key Metrics (Pre-Calculated)
- **Adherence Percentage**: Direct field `ADHERENCE_PERCENTAGE`
- **Time Metrics**: 
  - SCHEDULED_MINUTES: Total time scheduled for the task
  - ADHERENT_MINUTES: Time actually spent adhering to schedule
  - NON_ADHERENT_LOGGED_MINUTES: Time logged but not adherent to schedule
  - OFFLINE_MINUTES: Time completely offline
  - TOTAL_NON_ADHERENT_MINUTES: Total time not following schedule
  - TOTAL_OVERLAP_MINUTES: Time overlapping between scheduled and actual

### Important Columns
- **Identifiers**: ADHERENCE_KEY, AGENT_ID, AGENT_AVAILABILITY_ID, SCHEDULE_ID, TASK_ID
- **Agent Info**: AGENT_NAME, AGENT_EMAIL
- **Schedule Info**: SCHEDULED_TASK, SCHEDULED_START, SCHEDULED_END
- **Date/Time**: ADHERENCE_DATE
- **Metrics**: All time-based columns listed above

### Key Scheduled Tasks
Common values in SCHEDULED_TASK field:
- "Available" - Regular availability periods
- "Break" - Scheduled breaks
- "Lunch" - Lunch periods  
- "Meeting" - Scheduled meetings
- "Training" - Training sessions
- "Coaching" - Coaching sessions
- "Admin" - Administrative tasks

### Sample Queries

**Overall adherence rate**:
```sql
SELECT 
  AVG(ADHERENCE_PERCENTAGE) as overall_adherence_rate,
  COUNT(DISTINCT AGENT_ID) as agents_count,
  COUNT(*) as total_schedule_periods
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE
WHERE ADHERENCE_DATE >= CURRENT_DATE - 7
```

**Agent adherence ranking**:
```sql
SELECT 
  AGENT_NAME,
  AVG(ADHERENCE_PERCENTAGE) as avg_adherence_rate,
  SUM(SCHEDULED_MINUTES) as total_scheduled_minutes,
  SUM(ADHERENT_MINUTES) as total_adherent_minutes,
  SUM(OFFLINE_MINUTES) as total_offline_minutes,
  COUNT(*) as schedule_periods
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE
WHERE ADHERENCE_DATE >= CURRENT_DATE - 30
GROUP BY AGENT_NAME, AGENT_EMAIL
ORDER BY avg_adherence_rate DESC
```

**Schedule adherence by task type**:
```sql
SELECT 
  SCHEDULED_TASK,
  AVG(ADHERENCE_PERCENTAGE) as avg_adherence_rate,
  COUNT(*) as instances,
  AVG(SCHEDULED_MINUTES) as avg_scheduled_duration,
  AVG(ADHERENT_MINUTES) as avg_adherent_duration,
  AVG(OFFLINE_MINUTES) as avg_offline_duration
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE
WHERE ADHERENCE_DATE >= CURRENT_DATE - 7
GROUP BY SCHEDULED_TASK
ORDER BY avg_adherence_rate DESC
```

**Daily adherence trends**:
```sql
SELECT 
  ADHERENCE_DATE,
  AVG(ADHERENCE_PERCENTAGE) as daily_adherence_rate,
  COUNT(DISTINCT AGENT_ID) as agents_scheduled,
  SUM(SCHEDULED_MINUTES) as total_scheduled_minutes,
  SUM(ADHERENT_MINUTES) as total_adherent_minutes
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE
WHERE ADHERENCE_DATE >= CURRENT_DATE - 30
GROUP BY ADHERENCE_DATE
ORDER BY ADHERENCE_DATE
```

**Break adherence analysis**:
```sql
SELECT 
  AGENT_NAME,
  COUNT(CASE WHEN SCHEDULED_TASK = 'Break' THEN 1 END) as break_periods,
  AVG(CASE WHEN SCHEDULED_TASK = 'Break' THEN ADHERENCE_PERCENTAGE END) as avg_break_adherence,
  AVG(CASE WHEN SCHEDULED_TASK = 'Break' THEN SCHEDULED_MINUTES END) as avg_break_duration,
  AVG(CASE WHEN SCHEDULED_TASK = 'Break' THEN OFFLINE_MINUTES END) as avg_break_offline_time
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE
WHERE ADHERENCE_DATE >= CURRENT_DATE - 7
  AND SCHEDULED_TASK IN ('Break', 'Lunch')
GROUP BY AGENT_NAME
ORDER BY avg_break_adherence DESC
```

**Hourly adherence patterns**:
```sql
SELECT 
  EXTRACT(HOUR FROM SCHEDULED_START) as hour_of_day,
  AVG(ADHERENCE_PERCENTAGE) as avg_adherence_rate,
  COUNT(*) as schedule_instances,
  AVG(OFFLINE_MINUTES) as avg_offline_minutes
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE
WHERE ADHERENCE_DATE >= CURRENT_DATE - 7
  AND SCHEDULED_TASK = 'Available'
GROUP BY EXTRACT(HOUR FROM SCHEDULED_START)
ORDER BY hour_of_day
```

### Query Adaptations
- **For specific date range**: `WHERE ADHERENCE_DATE BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'`
- **For specific agents**: `WHERE AGENT_NAME IN ('Agent1', 'Agent2')`
- **For work hours only**: `WHERE EXTRACT(HOUR FROM SCHEDULED_START) BETWEEN 8 AND 17`
- **For non-break activities**: `WHERE SCHEDULED_TASK NOT IN ('Break', 'Lunch')`
- **For poor adherence**: `WHERE ADHERENCE_PERCENTAGE < 80`
- **For high offline time**: `WHERE OFFLINE_MINUTES > 30`

### Business Rules
- **Good Adherence**: >= 85%
- **Acceptable Adherence**: 80-84%
- **Poor Adherence**: < 80%
- **Excessive Offline**: > 15 minutes per hour scheduled

---

## Cross-Pattern Analysis Guidelines

### For Complete Agent Performance Analysis
Combine all six patterns:
1. **Volume**: Use WOPS Tickets pattern (tickets handled)
2. **Efficiency**: Use Handle Time pattern (AHT metrics)
3. **Effectiveness**: Use FCR pattern (first contact resolution)
4. **Performance**: Use Agent Performance pattern (weekly summaries)
5. **Leadership**: Use Team Lead Performance pattern (supervisor metrics)
6. **Adherence**: Use Schedule Adherence pattern (schedule compliance)

### For Channel Comparison
- Use Contact Channel derivation (same logic across patterns)
- Compare metrics across Chat, Voice, and Web channels

### For Time-Based Analysis
- All patterns have PST timestamp columns
- Use DATE_TRUNC for hourly/daily/weekly aggregations
- Consider business hours filtering when relevant

### For Team Performance Analysis
- Join patterns on AGENT_NAME or AGENT_EMAIL
- Use TEAM_LEAD field from RPT_WOPS_TICKETS for team grouping
- Consider schedule adherence as a leading indicator

## Important Notes
1. **RPT tables are pre-filtered** - no complex WHERE clauses needed for Pattern 1
2. **Direct field access** for CONTACT_CHANNEL, TICKET_SUB_TYPE in Pattern 1
3. Handle time metrics are pre-calculated - no need for complex calculations
4. Schedule adherence percentages are pre-calculated and ready to use
5. Consider joining patterns on AGENT_NAME/AGENT_EMAIL for comprehensive analysis
6. RPT_WOPS_TICKETS includes both WORKER_ID and AGENT_ID for different analysis perspectives

## Common Business Questions and Pattern Usage

| Question | Primary Pattern | Additional Patterns |
|----------|----------------|-------------------|
| "How many tickets today?" | WOPS Tickets (1) | - |
| "What's our AHT?" | Handle Time (2) | - |
| "Agent performance dashboard" | Agent Performance (4) | All patterns for complete view |
| "Team performance dashboard" | Team Lead Performance (5) | Pattern 4 for agent details |
| "Channel efficiency" | Handle Time (2) | WOPS Tickets (1) for volume |
| "Quality scores by team" | Team Lead Performance (5) | Agent Performance (4) |
| "Why are customers calling back?" | FCR (3) | WOPS Tickets (1) for issue types |
| "Peak hour analysis" | Handle Time (2), Schedule Adherence (6) | WOPS Tickets (1) for volume |
| "Escalation patterns" | WOPS Tickets (1) | Handle Time (2) for duration |
| "Schedule compliance issues" | Schedule Adherence (6) | - |
| "Agent coaching priorities" | Agent Performance (4), Schedule Adherence (6) | Handle Time (2), FCR (3) |
| "Workforce planning metrics" | Schedule Adherence (6), Team Lead Performance (5) | Handle Time (2) for capacity |
| "Break pattern analysis" | Schedule Adherence (6) | - |
| "Training effectiveness" | Agent Performance (4) | Schedule Adherence (6) for training sessions |
| "Adherence trends" | Schedule Adherence (6) | - |
| "Offline time analysis" | Schedule Adherence (6) | - |
| "Meeting attendance" | Schedule Adherence (6) | - |