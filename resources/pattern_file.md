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

## Pattern 2: Klaus Quality Assurance (QA) Reviews Analysis

**Primary Table**: `ANALYTICS.DBT_PRODUCTION.FCT_KLAUS__REVIEWS`

### Questions This Pattern Answers
- What are agent QA scores?
- Which agents passed/failed their QA reviews?
- What is the average QA score by team/agent?
- Show QA component breakdown (Resolution, Communication, etc.)
- What are the most common QA failures?
- QA score trends over time
- Reviewer performance and consistency
- Agent performance on specific QA components
- ATA (Agent Training Assessment) scores
- QA pass rates by scorecard type
- Which QA components need improvement?
- Compare QA scores between agents/teams
- Auto-fail incidents analysis

### Scorecard Configuration
- **Scorecard 53921 (QA v2)**: Pass threshold = 85%
- **Scorecard 54262 (ATA v2)**: Pass threshold = 85%
- **Scorecard 59144 (ATA)**: Pass threshold = 90%
- **Scorecard 60047**: Always results in 'Fail'

### Complex Calculations

**Overall Score**:
```sql
CASE
  WHEN No_Auto_Fail < 100 THEN 0
  WHEN SUM(RATING_WEIGHT) = 0 THEN NULL
  ELSE SUM(RATING_SCORE * RATING_WEIGHT) / SUM(100 * RATING_WEIGHT) * 100
END
```

**Clear Communication Score** (uses different methods):
- Old method: Direct "Effective Communication" category score
- New method: Average of 6 sub-components (IDs: 319808-319813)

**Handling Score**:
- QA method: 75% weight on Handling_1 (ID: 319826) + 25% weight on Handling_2 (ID: 319827)
- ATA method: Direct score from category 322522

### Key Components to Track
- **Behavioral**: Tone, Spelling_and_Grammar, Presence, Opening, Closing, Hold, Escalations
- **Technical**: Investigation, Delivering_Outcomes, Education, Attendance, Payments
- **Resolution**: Resolution score (category IDs: 319803, 322509)
- **ATA Specific**: ATA_Failure_Select_Correct_SOP, ATA_Failure_Follow_SOP, ATA_Critical_Thinking

### Sample Queries

**Agent QA pass rate**:
```sql
SELECT 
  REVIEWEE_NAME,
  AVG(CASE WHEN IS_PASS_FAIL = 'Pass' THEN 1.0 ELSE 0 END) * 100 as pass_rate,
  AVG(Overall_Score) as avg_qa_score,
  COUNT(*) as reviews_count
FROM [Klaus pattern query with CTEs]
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - 30
GROUP BY REVIEWEE_NAME
ORDER BY pass_rate DESC
```

**Component analysis**:
```sql
SELECT 
  AVG(Resolution) as avg_resolution,
  AVG(Clear_Communication) as avg_communication,
  AVG(Handling) as avg_handling,
  AVG(CASE WHEN No_Auto_Fail < 100 THEN 1.0 ELSE 0 END) * 100 as auto_fail_rate
FROM [Klaus pattern query]
WHERE REVIEW_CREATED_AT >= CURRENT_DATE - 7
```

### Query Adaptations
- **For date range**: `WHERE REVIEW_CREATED_AT >= CURRENT_DATE - 30`
- **For specific scorecard**: `WHERE SCORECARD_ID = 53921`
- **For pass rate**: `AVG(CASE WHEN IS_PASS_FAIL = 'Pass' THEN 1.0 ELSE 0 END) * 100`
- **For component analysis**: Focus on specific score columns
- **For team rollup**: Join with agent hierarchy tables

---

## Pattern 3: Agent Handle Time Analysis

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

## Pattern 4: First Contact Resolution (FCR) Analysis

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

## Cross-Pattern Analysis Guidelines

### For Complete Agent Performance Analysis
Combine all four patterns:
1. **Volume**: Use WOPS Tickets pattern (tickets handled)
2. **Efficiency**: Use Handle Time pattern (AHT metrics)
3. **Quality**: Use Klaus QA pattern (quality scores)
4. **Effectiveness**: Use FCR pattern (first contact resolution)

Example combined query structure:
```sql
WITH ticket_volume AS (
  -- From WOPS Tickets pattern
),
handle_time AS (
  -- From Handle Time pattern
),
qa_scores AS (
  -- From Klaus pattern
),
fcr_metrics AS (
  -- From FCR pattern
)
SELECT 
  agent_name,
  volume_metrics.*,
  efficiency_metrics.*,
  quality_metrics.*,
  effectiveness_metrics.*
FROM ticket_volume
JOIN handle_time ON ...
JOIN qa_scores ON ...
JOIN fcr_metrics ON ...
```

### For Channel Comparison
- Use Contact Channel derivation (same logic across patterns)
- Compare metrics across Chat, Voice, and Web channels

### For Time-Based Analysis
- All patterns have PST timestamp columns
- Use DATE_TRUNC for hourly/daily/weekly aggregations
- Consider business hours filtering when relevant

## Important Notes
1. Always verify which filters are pre-applied in each pattern
2. Some patterns (like FCR) already have significant filtering
3. Handle time metrics are pre-calculated - no need for complex calculations
4. QA scores have different thresholds by scorecard type
5. Consider joining patterns on TICKET_ID or AGENT identifiers for comprehensive analysis

## Common Business Questions and Pattern Usage

| Question | Primary Pattern | Additional Patterns |
|----------|----------------|-------------------|
| "How many tickets today?" | WOPS Tickets | - |
| "What's our AHT?" | Handle Time | - |
| "Agent performance dashboard" | All 4 patterns | Combined analysis |
| "Channel efficiency" | Handle Time | WOPS Tickets for volume |
| "Quality scores by team" | Klaus QA | - |
| "Why are customers calling back?" | FCR | WOPS Tickets for issue types |
| "Peak hour analysis" | Handle Time | WOPS Tickets for volume |
| "Escalation patterns" | WOPS Tickets | Handle Time for duration |