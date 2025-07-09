# Enhanced Business Intelligence Query Patterns - Complete Edition

This document contains proven SQL query patterns optimized for vector search and comprehensive business intelligence questions.

**⚠️ CRITICAL TIMEZONE FIX: All PST column filtering now uses proper PST timezone conversion**

---

## 🔍 SEARCH KEYWORDS MASTER INDEX

### RESPONSE TIME QUESTIONS
**Keywords**: response time, reply time, resolution time, SLA compliance, SLA, turnaround time, time to respond, time to resolve, average response, response distribution, response trends, response speed, response performance, time metrics, resolution speed, reply speed, first response time, resolution analysis, response benchmarks, response rates, how long does it take, time between, response window

**PRIMARY TABLE**: `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS` (EXCLUSIVE - DO NOT USE OTHER TABLES)
**KEY COLUMNS**: REPLY_TIME_IN_MINUTES, FIRST_RESOLUTION_TIME_IN_MINUTES, FULL_RESOLUTION_TIME_IN_MINUTES
**IMPORTANT**: This table contains all pre-calculated response time metrics. Never use FCT_ZENDESK__MQR_TICKETS or ZENDESK_TICKET_AGENT__HANDLE_TIME for response time questions.

### FCR AND REPEAT CONTACT QUESTIONS  
**Keywords**: FCR, first contact resolution, repeat contact, channel switching, callback, call back, same issue, resolved first time, multiple contacts, customer contacted again, repeat ticket, followup ticket, follow up, one call resolution, contact again, resolution effectiveness, repeat analysis, channel switching patterns, customer calling back, multiple touch, contact multiple times

**PRIMARY TABLE**: `ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS` (requires window functions)
**KEY COLUMNS**: REQUESTER_ID, CREATED_AT_PST, CHANNEL, GROUP_ID

### AGENT PERFORMANCE QUESTIONS
**Keywords**: agent performance, agent metrics, agent productivity, agent efficiency, agent statistics, agent dashboard, agent comparison, agent ranking, which agent, best agent, top agent, agent analysis, individual agent, agent scores, agent evaluation, agent effectiveness, weekly agent performance, agent trends, agent quality, agent KPIs, agent benchmarks, agent leaderboard

**PRIMARY TABLE**: `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE` (weekly aggregated)
**ALTERNATIVE**: `ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME` (detailed)
**KEY COLUMNS**: ASSIGNEE_NAME, NUM_TICKETS, AHT_MINUTES, QA_SCORE, FCR_PERCENTAGE

### TEAM LEAD PERFORMANCE QUESTIONS
**Keywords**: team lead performance, supervisor metrics, manager performance, team leader analysis, supervisor analysis, manager analysis, leadership metrics, team lead dashboard, team performance, supervisor performance, team lead ranking, team lead comparison, team lead trends, team lead evaluation, team lead effectiveness, weekly team performance, supervisor dashboard, manager dashboard, team metrics, leadership analysis

**PRIMARY TABLE**: `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TL_PERFORMANCE` (weekly aggregated)
**KEY COLUMNS**: SUPERVISOR, NUM_TICKETS, AHT_MINUTES, QA_SCORE, FCR_PERCENTAGE

### HANDLE TIME AND EFFICIENCY QUESTIONS
**Keywords**: handle time, AHT, average handle time, handling time, efficiency, agent efficiency, time per ticket, call duration, talk time, hold time, efficiency metrics, productivity metrics, speed metrics, time analysis, duration analysis, how long agents take, agent speed, ticket processing time, work efficiency, time management, agent velocity

**PRIMARY TABLE**: `ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME`
**KEY COLUMNS**: HANDLE_TIME_IN_MINUTES, USER_NAME, SUPERVISOR, CONTACT_CHANNEL

### SCHEDULE ADHERENCE QUESTIONS
**Keywords**: schedule adherence, adherence rate, schedule compliance, schedule variance, offline time, break adherence, schedule patterns, adherence trends, schedule analysis, schedule performance, adherence metrics, schedule monitoring, schedule effectiveness, adherence dashboard, adherence comparison, schedule following, time tracking, work schedule, attendance patterns

**PRIMARY TABLE**: `ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE`
**KEY COLUMNS**: ADHERENCE_PERCENTAGE, SCHEDULED_MINUTES, ADHERENT_MINUTES, OFFLINE_MINUTES

### TICKET VOLUME AND GENERAL QUESTIONS
**Keywords**: ticket volume, ticket count, how many tickets, ticket trends, ticket distribution, ticket analysis, ticket breakdown, tickets created, tickets solved, ticket metrics, ticket patterns, volume analysis, daily tickets, weekly tickets, monthly tickets, ticket statistics, workload, case volume, issue volume

**PRIMARY TABLE**: `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS`
**KEY COLUMNS**: TICKET_ID, CREATED_AT_PST, CONTACT_CHANNEL, TICKET_TYPE, AGENT_NAME

---

## 🕐 PST TIMEZONE HELPER FUNCTIONS

### Critical PST Date Functions
```sql
-- Current PST Date
DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))

-- PST Date Range Examples
-- Today in PST
WHERE DATE(CREATED_AT_PST) = DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))

-- Yesterday in PST  
WHERE DATE(CREATED_AT_PST) = DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 1

-- Last 7 days in PST
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7

-- This week in PST (Monday start)
WHERE CREATED_AT_PST >= DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())))

-- Last week in PST
WHERE CREATED_AT_PST >= DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))) - 7
  AND CREATED_AT_PST < DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())))

-- This month in PST
WHERE CREATED_AT_PST >= DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())))

-- Last month in PST
WHERE CREATED_AT_PST >= DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))) - INTERVAL '1 month'
  AND CREATED_AT_PST < DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())))
```

---

## Pattern 1: WOPS Tickets Comprehensive Analysis ⭐ RESPONSE TIME LEADER

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS`

### 🎯 RESPONSE TIME ANALYSIS - PRIORITY PATTERN
**SEARCH TERMS**: response time, reply time, resolution time, SLA compliance, SLA, turnaround time, time to respond, time to resolve, average response, response distribution, response speed

This pattern is the **DEFINITIVE SOURCE** for response time questions because:
✅ **Pre-calculated response time metrics** - no complex calculations needed
✅ **Business-ready data** - all filters already applied
✅ **Multiple time metrics available** - reply, first resolution, full resolution

### Response Time Columns (Pre-Calculated)
- **REPLY_TIME_IN_MINUTES**: Time from ticket creation to first agent reply
- **FIRST_RESOLUTION_TIME_IN_MINUTES**: Time from creation to first resolution  
- **FULL_RESOLUTION_TIME_IN_MINUTES**: Total time from creation to final resolution

### Standard Response Time Query Pattern (PST CORRECTED)
```sql
SELECT 
  AVG(REPLY_TIME_IN_MINUTES) as avg_response_time,
  AVG(FIRST_RESOLUTION_TIME_IN_MINUTES) as avg_resolution_time,
  COUNT(*) as total_tickets,
  -- SLA Compliance (example: 1 hour response, 24 hour resolution)
  SUM(CASE WHEN REPLY_TIME_IN_MINUTES <= 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as response_sla_compliance,
  SUM(CASE WHEN FIRST_RESOLUTION_TIME_IN_MINUTES <= 1440 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as resolution_sla_compliance
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7  -- Last 7 days in PST
```

### Response Time by Priority (PST CORRECTED)
```sql
SELECT 
  TICKET_PRIORITY,
  AVG(REPLY_TIME_IN_MINUTES) as avg_response_time,
  AVG(FIRST_RESOLUTION_TIME_IN_MINUTES) as avg_resolution_time,
  COUNT(*) as ticket_count
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 30  -- Last 30 days in PST
GROUP BY TICKET_PRIORITY
ORDER BY avg_response_time
```

### Response Time Trends (PST CORRECTED)
```sql
SELECT 
  DATE(CREATED_AT_PST) as ticket_date,
  AVG(REPLY_TIME_IN_MINUTES) as avg_response_time,
  AVG(FIRST_RESOLUTION_TIME_IN_MINUTES) as avg_resolution_time,
  COUNT(*) as daily_tickets
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 30  -- Last 30 days in PST
GROUP BY DATE(CREATED_AT_PST)
ORDER BY ticket_date
```

### Response Time Distribution (PST CORRECTED)
```sql
SELECT 
  CASE 
    WHEN REPLY_TIME_IN_MINUTES <= 15 THEN '0-15 min (Excellent)'
    WHEN REPLY_TIME_IN_MINUTES <= 60 THEN '15-60 min (Good)'
    WHEN REPLY_TIME_IN_MINUTES <= 240 THEN '1-4 hours (Needs Improvement)'
    ELSE '4+ hours (Critical)'
  END as response_time_bucket,
  COUNT(*) as ticket_count,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7  -- Last 7 days in PST
GROUP BY response_time_bucket
ORDER BY MIN(REPLY_TIME_IN_MINUTES)
```

### Today's Tickets (PST CORRECTED)
```sql
SELECT COUNT(*) as tickets_today
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS
WHERE DATE(CREATED_AT_PST) = DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))
```

### Questions This Pattern Answers
- How many tickets were created today/yesterday/this week/last month?
- What is the ticket volume by channel/group/agent?
- Show ticket distribution by type/category/priority
- **What are the response and resolution times?** ⭐
- **Which tickets have the longest handle time?**
- Show tickets by status or escalation patterns
- **Ticket trends over time**
- **Agent performance on ticket handling**
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

**Simply add your specific criteria with PST timezone handling:**
```sql
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7    -- Date filtering
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
  - **REPLY_TIME_IN_MINUTES, FIRST_RESOLUTION_TIME_IN_MINUTES, FULL_RESOLUTION_TIME_IN_MINUTES** ⭐
  - HANDLE_TIME, LAST_TOUCH_HANDLE_TIME
  - CONNECT_HANDLE_TIME_TOTAL_MIN, CONNECT_HANDLE_TIME_LAST_CALL_MIN
  - REOPENS, REPLIES, PROVIDED_SCORE

---

## Pattern 2: Agent Handle Time Analysis ⭐ HANDLE TIME LEADER

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME`

### 🎯 HANDLE TIME ANALYSIS - PRIORITY PATTERN
**SEARCH TERMS**: handle time, AHT, average handle time, handling time, efficiency, agent efficiency, time per ticket, call duration, talk time, hold time, efficiency metrics, productivity metrics, speed metrics, agent velocity

This pattern is the **DEFINITIVE SOURCE** for handle time and agent efficiency questions because:
✅ **Pre-calculated handle time metrics** - no complex calculations needed
✅ **Agent-level detail** - individual agent performance data
✅ **Voice channel specifics** - call duration, talk time, hold time

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
- **Agent productivity analysis**
- **Individual agent efficiency metrics**
- **Supervisor team handle time performance**

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

### Sample Queries (PST CORRECTED)

**Average AHT by agent**:
```sql
SELECT 
  USER_NAME, 
  SUPERVISOR,
  AVG(HANDLE_TIME_IN_MINUTES) as avg_aht_minutes,
  COUNT(*) as tickets_handled,
  MIN(HANDLE_TIME_IN_MINUTES) as min_handle_time,
  MAX(HANDLE_TIME_IN_MINUTES) as max_handle_time
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7  -- Last 7 days in PST
GROUP BY USER_NAME, SUPERVISOR
ORDER BY avg_aht_minutes
```

**Voice channel efficiency (PST CORRECTED)**:
```sql
SELECT 
  USER_NAME,
  AVG(AMAZON_CONNECT_TALK_TIME_IN_MINUTES) as avg_talk_time,
  AVG(AMAZON_CONNECT_HOLD_TIME_IN_MINUTES) as avg_hold_time,
  AVG(AMAZON_CONNECT_CALL_DURATION_IN_MINUTES) as avg_call_duration,
  AVG(HANDLE_TIME_IN_MINUTES) as avg_total_handle_time
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE CONTACT_CHANNEL = 'Voice'
  AND CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7  -- Last 7 days in PST
GROUP BY USER_NAME
ORDER BY avg_call_duration DESC
```

**Handle time distribution (PST CORRECTED)**:
```sql
SELECT 
  CASE 
    WHEN HANDLE_TIME_IN_MINUTES < 5 THEN '0-5 min'
    WHEN HANDLE_TIME_IN_MINUTES < 10 THEN '5-10 min'
    WHEN HANDLE_TIME_IN_MINUTES < 20 THEN '10-20 min'
    WHEN HANDLE_TIME_IN_MINUTES < 30 THEN '20-30 min'
    ELSE '30+ min'
  END as time_bucket,
  COUNT(*) as ticket_count,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7  -- Last 7 days in PST
GROUP BY time_bucket
ORDER BY MIN(HANDLE_TIME_IN_MINUTES)
```

**Supervisor team efficiency (PST CORRECTED)**:
```sql
SELECT 
  SUPERVISOR,
  COUNT(DISTINCT USER_NAME) as team_size,
  AVG(HANDLE_TIME_IN_MINUTES) as team_avg_aht,
  COUNT(*) as total_tickets_handled,
  SUM(HANDLE_TIME_IN_MINUTES) as total_handle_time_minutes
FROM ANALYTICS.DBT_PRODUCTION.ZENDESK_TICKET_AGENT__HANDLE_TIME
WHERE CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7  -- Last 7 days in PST
  AND SUPERVISOR IS NOT NULL
GROUP BY SUPERVISOR
ORDER BY team_avg_aht
```

### Query Adaptations
- **For outlier exclusion**: `WHERE HANDLE_TIME_IN_MINUTES < 120`
- **For voice only**: `WHERE CONTACT_CHANNEL = 'Voice'`
- **For time patterns**: `GROUP BY EXTRACT(HOUR FROM CREATED_AT_PST)`
- **For ticket type analysis**: `GROUP BY WOPS_TICKET_TYPE_A`
- **For team analysis**: `GROUP BY SUPERVISOR`
- **For efficiency ranking**: `ORDER BY AVG(HANDLE_TIME_IN_MINUTES) ASC`

---

## Pattern 3: First Contact Resolution (FCR) Analysis ⭐ FCR LEADER

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS` (with window functions)

### 🎯 FCR ANALYSIS - PRIORITY PATTERN
**SEARCH TERMS**: FCR, first contact resolution, repeat contact, channel switching, callback, call back, same issue, resolved first time, multiple contacts, customer contacted again, repeat ticket, followup ticket

### FCR Definition
A ticket is considered "resolved first time" if the customer does not create another ticket within 24 hours.

### FCR Window Function Pattern (COMPLETE TEMPLATE - PST CORRECTED)
```sql
WITH FCR_ANALYSIS AS (
  SELECT
    TICKET_ID,
    REQUESTER_ID,
    CREATED_AT_PST,
    ASSIGNEE_NAME,
    CHANNEL,
    -- Contact Channel Derivation
    CASE
      WHEN GROUP_ID = '17837476387479' THEN 'Chat'
      WHEN GROUP_ID = '28949203098007' THEN 'Voice'
      ELSE 'Other'
    END AS CONTACT_CHANNEL,
    -- Next ticket by same customer
    LEAD(CREATED_AT_PST) OVER (PARTITION BY REQUESTER_ID ORDER BY CREATED_AT_PST) AS next_ticket_date,
    LEAD(CHANNEL) OVER (PARTITION BY REQUESTER_ID ORDER BY CREATED_AT_PST) AS next_channel,
    -- FCR Calculation
    CASE
      WHEN CREATED_AT_PST + INTERVAL '24 HOUR' >= LEAD(CREATED_AT_PST) OVER (PARTITION BY REQUESTER_ID ORDER BY CREATED_AT_PST) THEN 0
      ELSE 1
    END AS is_fcr_success
  FROM ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS
  WHERE STATUS IN ('closed', 'solved') 
    AND CHANNEL IN ('api', 'email', 'native_messaging', 'web')
    AND BRAND_ID IN ('29186504989207', '360002340693')
    AND CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 30  -- Last 30 days in PST
)
SELECT 
  AVG(is_fcr_success) * 100 as fcr_rate,
  COUNT(*) as total_tickets,
  SUM(is_fcr_success) as fcr_successes,
  COUNT(*) - SUM(is_fcr_success) as fcr_failures
FROM FCR_ANALYSIS;
```

### FCR by Agent (PST CORRECTED)
```sql
-- Use the FCR_ANALYSIS CTE from above, then:
SELECT 
  ASSIGNEE_NAME,
  AVG(is_fcr_success) * 100 as agent_fcr_rate,
  COUNT(*) as tickets_handled,
  SUM(is_fcr_success) as fcr_successes
FROM FCR_ANALYSIS
GROUP BY ASSIGNEE_NAME
ORDER BY agent_fcr_rate DESC
```

### FCR by Channel (PST CORRECTED)
```sql
-- Use the FCR_ANALYSIS CTE from above, then:
SELECT 
  CONTACT_CHANNEL,
  AVG(is_fcr_success) * 100 as channel_fcr_rate,
  COUNT(*) as tickets_handled
FROM FCR_ANALYSIS  
GROUP BY CONTACT_CHANNEL
ORDER BY channel_fcr_rate DESC
```

### Channel Switching Analysis (PST CORRECTED)
```sql
-- Use the FCR_ANALYSIS CTE from above, then:
SELECT 
  CONTACT_CHANNEL as original_channel,
  CASE
    WHEN LEAD(GROUP_ID) OVER (PARTITION BY REQUESTER_ID ORDER BY CREATED_AT_PST) = '17837476387479' THEN 'Chat'
    WHEN LEAD(GROUP_ID) OVER (PARTITION BY REQUESTER_ID ORDER BY CREATED_AT_PST) = '28949203098007' THEN 'Voice' 
    ELSE 'Other'
  END as next_channel,
  COUNT(*) as switches
FROM FCR_ANALYSIS
WHERE is_fcr_success = 0 AND next_ticket_date IS NOT NULL
GROUP BY original_channel, next_channel
ORDER BY switches DESC
```

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

---

## Pattern 4: WOPS Agent Performance (Weekly Aggregated) ⭐ AGENT PERFORMANCE LEADER

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE`

### 🎯 AGENT PERFORMANCE ANALYSIS - PRIORITY PATTERN
**SEARCH TERMS**: agent performance, agent metrics, agent productivity, agent efficiency, agent statistics, agent dashboard, agent comparison, agent ranking, which agent, best agent, top agent, weekly agent performance, agent quality, agent scores, agent KPIs

This pattern is the **DEFINITIVE SOURCE** for agent performance questions because:
✅ **Pre-aggregated weekly metrics** - no complex calculations needed
✅ **All key performance indicators** - volume, efficiency, quality, effectiveness
✅ **Business-ready data** - standardized across all agents

### Pre-Aggregated Metrics (No Calculations Needed)
- **Volume**: `NUM_TICKETS` (total tickets solved per week)
- **Efficiency**: `AHT_MINUTES` (average handle time in minutes)
- **Quality**: `QA_SCORE` (average QA score 0-100)
- **Effectiveness**: `FCR_PERCENTAGE` (first contact resolution rate 0-100)
- **Customer Satisfaction**: `POSITIVE_RES_CSAT`, `NEGATIVE_RES_CSAT`

### Agent Performance Dashboard Query (PST CORRECTED)
```sql
SELECT 
  ASSIGNEE_NAME,
  SOLVED_WEEK,
  NUM_TICKETS,
  AHT_MINUTES,
  FCR_PERCENTAGE,
  QA_SCORE,
  CASE 
    WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN NULL
    ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100
  END AS csat_percentage,
  -- Performance Score (composite)
  (FCR_PERCENTAGE * 0.3 + QA_SCORE * 0.3 + 
   CASE WHEN AHT_MINUTES <= 10 THEN 100 ELSE GREATEST(0, 100 - (AHT_MINUTES - 10) * 5) END * 0.2 +
   CASE WHEN (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) = 0 THEN 50 
        ELSE POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 END * 0.2
  ) AS performance_score
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE)
ORDER BY performance_score DESC
```

### Top Performers Query (PST CORRECTED)
```sql
SELECT 
  ASSIGNEE_NAME,
  AVG(QA_SCORE) as avg_qa_score,
  AVG(FCR_PERCENTAGE) as avg_fcr,
  AVG(NUM_TICKETS) as avg_weekly_tickets,
  AVG(AHT_MINUTES) as avg_aht,
  COUNT(*) as weeks_active
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_AGENT_PERFORMANCE
WHERE SOLVED_WEEK >= DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))) - INTERVAL '12 weeks'  -- Last 12 weeks
GROUP BY ASSIGNEE_NAME
HAVING COUNT(*) >= 8  -- At least 8 weeks of data
ORDER BY avg_qa_score DESC, avg_fcr DESC
LIMIT 10
```

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
- **QA score trends and agent quality rankings**

---

## Pattern 5: WOPS Team Lead Performance (Weekly Aggregated) ⭐ TEAM LEAD LEADER

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TL_PERFORMANCE`

### 🎯 TEAM LEAD PERFORMANCE ANALYSIS - PRIORITY PATTERN
**SEARCH TERMS**: team lead performance, supervisor metrics, manager performance, team leader analysis, supervisor analysis, team performance, team lead dashboard, team lead ranking, team metrics, supervisor performance, leadership metrics, manager dashboard

This pattern is the **DEFINITIVE SOURCE** for team lead performance questions because:
✅ **Pre-aggregated team metrics** - no complex calculations needed  
✅ **Team-level KPIs** - volume, efficiency, quality, effectiveness
✅ **Supervisor-ready data** - standardized across all teams

### Pre-Aggregated Team Metrics (No Calculations Needed)
- **Team Volume**: `NUM_TICKETS` (total tickets solved per week by team)
- **Team Efficiency**: `AHT_MINUTES` (average handle time for the team)
- **Team Quality**: `QA_SCORE` (average QA score 0-100 for the team)
- **Team Effectiveness**: `FCR_PERCENTAGE` (team FCR rate 0-100)
- **Team Satisfaction**: `POSITIVE_RES_CSAT`, `NEGATIVE_RES_CSAT`

### Team Lead Performance Dashboard Query (PST CORRECTED)
```sql
SELECT 
  SUPERVISOR,
  NUM_TICKETS as team_tickets,
  AHT_MINUTES as team_aht,
  FCR_PERCENTAGE as team_fcr,
  QA_SCORE as team_qa_score,
  POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100 as team_csat_rate,
  NUM_TICKETS / 8.0 as estimated_tickets_per_agent  -- Assuming 8 agents per team
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TL_PERFORMANCE
WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TL_PERFORMANCE)
ORDER BY QA_SCORE DESC, FCR_PERCENTAGE DESC
```

### Team Lead Rankings Query (PST CORRECTED)
```sql
SELECT 
  SUPERVISOR,
  AVG(NUM_TICKETS) as avg_weekly_team_tickets,
  AVG(AHT_MINUTES) as avg_team_aht,
  AVG(FCR_PERCENTAGE) as avg_team_fcr,
  AVG(QA_SCORE) as avg_team_qa_score,
  COUNT(*) as weeks_active,
  ROW_NUMBER() OVER (ORDER BY AVG(QA_SCORE) DESC, AVG(FCR_PERCENTAGE) DESC) as team_rank
FROM ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TL_PERFORMANCE
WHERE SOLVED_WEEK >= DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))) - INTERVAL '12 weeks'  -- Last 12 weeks
GROUP BY SUPERVISOR
HAVING COUNT(*) >= 8  -- At least 8 weeks of data
ORDER BY team_rank
```

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

---

## Pattern 6: Agent Schedule Adherence Analysis ⭐ ADHERENCE LEADER

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE`

### 🎯 SCHEDULE ADHERENCE ANALYSIS - PRIORITY PATTERN  
**SEARCH TERMS**: schedule adherence, adherence rate, schedule compliance, schedule variance, offline time, break adherence, schedule patterns, adherence trends, schedule analysis, schedule performance, adherence metrics, schedule monitoring, time tracking

### Key Metrics (Pre-Calculated)
- **Adherence Percentage**: Direct field `ADHERENCE_PERCENTAGE`
- **Time Metrics**: 
  - SCHEDULED_MINUTES, ADHERENT_MINUTES, OFFLINE_MINUTES
  - NON_ADHERENT_LOGGED_MINUTES, TOTAL_NON_ADHERENT_MINUTES

### Schedule Adherence Dashboard Query (PST CORRECTED)
```sql
SELECT 
  AGENT_NAME,
  AVG(ADHERENCE_PERCENTAGE) as avg_adherence_rate,
  SUM(SCHEDULED_MINUTES) as total_scheduled_minutes,
  SUM(ADHERENT_MINUTES) as total_adherent_minutes,
  SUM(OFFLINE_MINUTES) as total_offline_minutes,
  COUNT(*) as schedule_periods
FROM ANALYTICS.DBT_PRODUCTION.RPT_AGENT_SCHEDULE_ADHERENCE
WHERE ADHERENCE_DATE >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7  -- Last 7 days in PST
GROUP BY AGENT_NAME
ORDER BY avg_adherence_rate DESC
```

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

---

## TABLE SELECTION PRIORITY MATRIX

| Question Type | Priority 1 ⭐ | Priority 2 | Priority 3 |
|---------------|------------|------------|------------|
| **Response Time** | RPT_WOPS_TICKETS (ONLY) | - | - |
| **FCR Questions** | FCT_ZENDESK__MQR_TICKETS | RPT_WOPS_TICKETS | - |
| **Agent Performance** | RPT_WOPS_AGENT_PERFORMANCE | ZENDESK_TICKET_AGENT__HANDLE_TIME | RPT_WOPS_TICKETS |
| **Team Lead Performance** | RPT_WOPS_TL_PERFORMANCE | RPT_WOPS_AGENT_PERFORMANCE | ZENDESK_TICKET_AGENT__HANDLE_TIME |
| **Handle Time/Efficiency** | ZENDESK_TICKET_AGENT__HANDLE_TIME | RPT_WOPS_AGENT_PERFORMANCE | RPT_WOPS_TICKETS |
| **Schedule Adherence** | RPT_AGENT_SCHEDULE_ADHERENCE | - | - |
| **Ticket Volume** | RPT_WOPS_TICKETS | FCT_ZENDESK__MQR_TICKETS | - |

---

## CONFIDENCE INDICATORS FOR VECTOR SEARCH

### HIGH CONFIDENCE INDICATORS (Use These Tables First) ⭐
- **RPT_WOPS_TICKETS** → Response time (EXCLUSIVE), ticket volume, general analysis
- **RPT_WOPS_AGENT_PERFORMANCE** → Agent performance, weekly metrics  
- **RPT_WOPS_TL_PERFORMANCE** → Team lead performance, supervisor metrics
- **ZENDESK_TICKET_AGENT__HANDLE_TIME** → Handle time, agent efficiency
- **RPT_AGENT_SCHEDULE_ADHERENCE** → Schedule adherence, compliance

### MEDIUM CONFIDENCE INDICATORS
- **FCT_ZENDESK__MQR_TICKETS** → FCR analysis (requires window functions), NOT for response time

### PATTERN CONFIDENCE SCORING
Each pattern should score based on keyword matches:
- **Direct keyword match** = 100 points
- **Synonym match** = 75 points  
- **Related concept** = 50 points
- **Table name in question** = 150 points
- **Pre-calculated metric available** = 125 points

---

## BUSINESS RULES FOR BOT DECISION MAKING

### Response Time Questions → ALWAYS use RPT_WOPS_TICKETS ONLY ⭐
- Contains REPLY_TIME_IN_MINUTES, FIRST_RESOLUTION_TIME_IN_MINUTES, FULL_RESOLUTION_TIME_IN_MINUTES
- Pre-filtered and business ready - no other table needed
- No complex calculations needed
- **NEVER use FCT_ZENDESK__MQR_TICKETS or ZENDESK_TICKET_AGENT__HANDLE_TIME for response time**

### FCR Questions → ALWAYS use FCT_ZENDESK__MQR_TICKETS with window functions ⭐
- Requires REQUESTER_ID for customer tracking
- Must use LEAD() function for 24-hour analysis  
- Complex but necessary for accurate FCR

### Agent Performance → ALWAYS prefer RPT_WOPS_AGENT_PERFORMANCE ⭐
- Weekly aggregated data
- All KPIs pre-calculated (volume, efficiency, quality, effectiveness)
- No aggregation needed

### Handle Time/Efficiency → ALWAYS prefer ZENDESK_TICKET_AGENT__HANDLE_TIME ⭐
- Most detailed handle time data
- Agent-level granularity
- Voice channel specifics available

### Team Lead Performance → ALWAYS prefer RPT_WOPS_TL_PERFORMANCE ⭐  
- Team-level aggregated data
- Supervisor/manager focused metrics
- Pre-calculated team performance scores

### Schedule/Adherence → ALWAYS use RPT_AGENT_SCHEDULE_ADHERENCE ⭐
- Only source for adherence metrics
- Pre-calculated adherence percentages
- Time-based analysis ready

---

## COMMON QUESTION MAPPINGS - COMPLETE EDITION

| User Question Example | Primary Table | Pattern Number | Alternative Tables |
|-----------------------|---------------|----------------|-------------------|
| **RESPONSE TIME** |
| "What are our average response times?" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "Show me response times by priority" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "What is our SLA compliance rate?" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "Response time trends over time" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "How long does it take to respond?" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "Response time distribution" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "SLA performance metrics" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| **FCR** |
| "What is our FCR rate?" | FCT_ZENDESK__MQR_TICKETS | 3 ⭐ | - |
| "Why do customers contact us multiple times?" | FCT_ZENDESK__MQR_TICKETS | 3 ⭐ | - |
| "Channel switching analysis" | FCT_ZENDESK__MQR_TICKETS | 3 ⭐ | - |
| "First contact resolution by agent" | FCT_ZENDESK__MQR_TICKETS | 3 ⭐ | - |
| **AGENT PERFORMANCE** |
| "Show me agent performance metrics" | RPT_WOPS_AGENT_PERFORMANCE | 4 ⭐ | ZENDESK_TICKET_AGENT__HANDLE_TIME |
| "Which agents are top performers?" | RPT_WOPS_AGENT_PERFORMANCE | 4 ⭐ | - |
| "Agent performance dashboard" | RPT_WOPS_AGENT_PERFORMANCE | 4 ⭐ | - |
| "Weekly agent metrics" | RPT_WOPS_AGENT_PERFORMANCE | 4 ⭐ | - |
| "Agent quality scores" | RPT_WOPS_AGENT_PERFORMANCE | 4 ⭐ | - |
| **HANDLE TIME & EFFICIENCY** |
| "What is our average handle time?" | ZENDESK_TICKET_AGENT__HANDLE_TIME | 2 ⭐ | RPT_WOPS_AGENT_PERFORMANCE |
| "AHT by agent" | ZENDESK_TICKET_AGENT__HANDLE_TIME | 2 ⭐ | RPT_WOPS_AGENT_PERFORMANCE |
| "Which agents are most efficient?" | ZENDESK_TICKET_AGENT__HANDLE_TIME | 2 ⭐ | RPT_WOPS_AGENT_PERFORMANCE |
| "Handle time distribution" | ZENDESK_TICKET_AGENT__HANDLE_TIME | 2 ⭐ | - |
| "Voice channel call duration" | ZENDESK_TICKET_AGENT__HANDLE_TIME | 2 ⭐ | - |
| "Agent efficiency ranking" | ZENDESK_TICKET_AGENT__HANDLE_TIME | 2 ⭐ | RPT_WOPS_AGENT_PERFORMANCE |
| **TEAM LEAD PERFORMANCE** |
| "Team lead performance dashboard" | RPT_WOPS_TL_PERFORMANCE | 5 ⭐ | RPT_RPT_RPT_WOPS_AGENT_PERFORMANCE |
| "Which supervisors have the best teams?" | RPT_WOPS_TL_PERFORMANCE | 5 ⭐ | - |
| "Manager performance metrics" | RPT_WOPS_TL_PERFORMANCE | 5 ⭐ | - |
| "Team performance by supervisor" | RPT_WOPS_TL_PERFORMANCE | 5 ⭐ | ZENDESK_TICKET_AGENT__HANDLE_TIME |
| "Leadership metrics" | RPT_WOPS_TL_PERFORMANCE | 5 ⭐ | - |
| **SCHEDULE ADHERENCE** |
| "What is our schedule adherence rate?" | RPT_AGENT_SCHEDULE_ADHERENCE | 6 ⭐ | - |
| "Which agents have poor adherence?" | RPT_AGENT_SCHEDULE_ADHERENCE | 6 ⭐ | - |
| "Schedule compliance by team" | RPT_AGENT_SCHEDULE_ADHERENCE | 6 ⭐ | - |
| "Offline time analysis" | RPT_AGENT_SCHEDULE_ADHERENCE | 6 ⭐ | - |
| "Break adherence patterns" | RPT_AGENT_SCHEDULE_ADHERENCE | 6 ⭐ | - |
| **TICKET VOLUME** |
| "How many tickets today?" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "Ticket volume trends" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "Volume by channel" | RPT_WOPS_TICKETS | 1 ⭐ | - |
| "Daily ticket count" | RPT_WOPS_TICKETS | 1 ⭐ | - |

⭐ = High confidence table selection required

---

## Cross-Pattern Analysis Guidelines

### For Complete Agent Performance Analysis
Combine patterns for comprehensive insights:
1. **Volume**: Use WOPS Tickets pattern (tickets handled)
2. **Efficiency**: Use Handle Time pattern (AHT metrics) or Agent Performance pattern
3. **Quality**: Use Agent Performance pattern (QA scores)
4. **Effectiveness**: Use Agent Performance pattern (FCR) or FCR pattern (detailed)
5. **Adherence**: Use Schedule Adherence pattern (schedule compliance)

### For Channel Comparison
- Use Contact Channel derivation (same logic across patterns)
- Compare metrics across Chat, Voice, and Web channels

### For Time-Based Analysis
- **ALL PST columns must use PST timezone conversion for filtering**
- Use DATE_TRUNC for hourly/daily/weekly aggregations
- Consider business hours filtering when relevant

### For Team Performance Analysis
- Join patterns on AGENT_NAME or USER_NAME
- Use SUPERVISOR field for team grouping
- Consider schedule adherence as a leading indicator

## Important Notes
1. **PST TIMEZONE CRITICAL**: Always use `DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))` when filtering PST columns
2. **RPT tables are pre-filtered** - no complex WHERE clauses needed
3. **WOPS tables are pre-aggregated** - no complex GROUP BY needed
4. **Direct field access** for CONTACT_CHANNEL, TICKET_SUB_TYPE in Pattern 1
5. Handle time metrics are pre-calculated - no need for complex calculations
6. Schedule adherence percentages are pre-calculated and ready to use
7. Consider joining patterns on AGENT_NAME/USER_NAME for comprehensive analysis

## PST Timezone Examples Summary

### Common PST Date Filters
```sql
-- Today
DATE(CREATED_AT_PST) = DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))

-- Yesterday  
DATE(CREATED_AT_PST) = DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 1

-- Last 7 days
CREATED_AT_PST >= DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())) - 7

-- This week (Monday start)
CREATED_AT_PST >= DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())))

-- Last month
CREATED_AT_PST >= DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP()))) - INTERVAL '1 month'
  AND CREATED_AT_PST < DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP())))
```