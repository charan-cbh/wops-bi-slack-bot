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

## Pattern 5: WOPS Agent Performance (Weekly Aggregated)

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

### Pre-Aggregated Metrics (No Calculations Needed)
This table contains **pre-calculated weekly metrics** - no complex aggregations required.

- **Volume**: `NUM_TICKETS` (total tickets solved per week)
- **Efficiency**: `AHT_MINUTES` (average handle time in minutes)
- **Quality**: `QA_SCORE` (average QA score 0-100)
- **Effectiveness**: `FCR_PERCENTAGE` (first contact resolution rate 0-100)
- **Customer Satisfaction**: `POSITIVE_RES_CSAT`, `NEGATIVE_RES_CSAT` (satisfaction counts)

### Important Columns
- **Identifiers**: SOLVED_WEEK_ASSIGNEE_ID (unique weekly performance record)
- **Dimensions**: ASSIGNEE_NAME, SOLVED_WEEK (week ending date)
- **Volume Metrics**: NUM_TICKETS
- **Efficiency Metrics**: AHT_MINUTES
- **Quality Metrics**: QA_SCORE (0-100 scale)
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

**Weekly performance trends for specific agent**:
```sql
SELECT 
  SOLVED_WEEK,
  NUM_TICKETS,
  AHT_MINUTES,
  FCR_PERCENTAGE,
  QA_SCORE
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

**Performance distribution analysis**:
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

**Agent performance rankings**:
```sql
SELECT 
  ASSIGNEE_NAME,
  AVG(NUM_TICKETS) as avg_weekly_tickets,
  AVG(AHT_MINUTES) as avg_aht,
  AVG(FCR_PERCENTAGE) as avg_fcr,
  AVG(QA_SCORE) as avg_qa_score,
  AVG(POSITIVE_RES_CSAT / (POSITIVE_RES_CSAT + NEGATIVE_RES_CSAT) * 100) as avg_csat_rate,
  COUNT(*) as weeks_active,
  ROW_NUMBER() OVER (ORDER BY AVG(QA_SCORE) DESC, AVG(FCR_PERCENTAGE) DESC) as overall_rank
FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE
WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY ASSIGNEE_NAME
HAVING COUNT(*) >= 8  -- At least 8 weeks of data
ORDER BY overall_rank
```

**Performance correlation analysis**:
```sql
SELECT 
  CORR(NUM_TICKETS, QA_SCORE) as volume_qa_correlation,
  CORR(AHT_MINUTES, FCR_PERCENTAGE) as aht_fcr_correlation,
  CORR(QA_SCORE, FCR_PERCENTAGE) as qa_fcr_correlation,
  CORR(NUM_TICKETS, AHT_MINUTES) as volume_aht_correlation
FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE
WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL '12 weeks'
```

### Query Adaptations
- **For current week only**: `WHERE SOLVED_WEEK = (SELECT MAX(SOLVED_WEEK) FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE)`
- **For last N weeks**: `WHERE SOLVED_WEEK >= CURRENT_DATE - INTERVAL 'N weeks'`
- **For specific agent**: `WHERE ASSIGNEE_NAME = 'Agent Name'`
- **For performance trends**: `ORDER BY SOLVED_WEEK ASC/DESC`
- **For rankings**: Use `ROW_NUMBER() OVER (ORDER BY metric DESC)`
- **For team analysis**: Join with agent hierarchy tables if available
- **For outlier exclusion**: `WHERE NUM_TICKETS >= 5` (minimum volume threshold)

### Business Intelligence Use Cases

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

**Team Management**:
- Team performance comparisons
- Capacity planning based on volume trends
- Quality vs efficiency balance analysis
- Customer satisfaction drivers

### Integration with Other Patterns

**Detailed Analysis Cross-Reference**:
- **For ticket-level details**: Use Pattern 1 (WOPS Tickets) with `WHERE ASSIGNEE_NAME = 'agent'`
- **For handle time breakdown**: Use Pattern 3 (Handle Time) with specific week filters
- **For QA review details**: Use Pattern 2 (Klaus QA) for component-level scores
- **For FCR incident analysis**: Use Pattern 4 (FCR) for repeat contact patterns

**Join Opportunities**:
```sql
-- Combine weekly summary with detailed tickets
SELECT 
  wp.ASSIGNEE_NAME,
  wp.SOLVED_WEEK,
  wp.NUM_TICKETS,
  wp.QA_SCORE,
  COUNT(wt.TICKET_ID) as detailed_ticket_count
FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE wp
LEFT JOIN ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS wt 
  ON wp.ASSIGNEE_NAME = wt.ASSIGNEE_NAME 
  AND DATE_TRUNC('week', wt.SOLVED_AT_PST) = wp.SOLVED_WEEK
GROUP BY wp.ASSIGNEE_NAME, wp.SOLVED_WEEK, wp.NUM_TICKETS, wp.QA_SCORE
```

### Performance Benchmarks (Suggested Thresholds)
- **Excellent QA**: 90+ score
- **Good FCR**: 80%+ resolution rate
- **Efficient AHT**: <10 minutes for most ticket types
- **Active Volume**: 20+ tickets per week
- **Strong CSAT**: 85%+ positive rate

---

## Cross-Pattern Analysis Guidelines

### For Complete Agent Performance Analysis
Choose between detailed analysis (Patterns 1-4) or executive summary (Pattern 5):

**Option A: Executive/Weekly View (Recommended for most questions)**
Use Pattern 5 (WOPS Agent Performance) for:
- Weekly performance summaries
- Agent rankings and comparisons
- Performance trends over time
- Executive dashboards

**Option B: Detailed Analysis (When granular data needed)**
Combine Patterns 1-4:
1. **Volume**: Use WOPS Tickets pattern (tickets handled)
2. **Efficiency**: Use Handle Time pattern (AHT metrics)
3. **Quality**: Use Klaus QA pattern (quality scores)
4. **Effectiveness**: Use FCR pattern (first contact resolution)

**Hybrid Approach: Start with Pattern 5, drill down with Patterns 1-4**
```sql
-- Executive view from Pattern 5
SELECT * FROM ANALYTICS.DBT_PRODUCTION.WOPS_AGENT_PERFORMANCE 
WHERE ASSIGNEE_NAME = 'John Smith' AND SOLVED_WEEK >= CURRENT_DATE - INTERVAL '8 weeks'

-- Then drill down with Pattern 1 for specific tickets
SELECT * FROM ANALYTICS.DBT_PRODUCTION.FCT_ZENDESK__MQR_TICKETS
WHERE ASSIGNEE_NAME = 'John Smith' AND CREATED_AT >= CURRENT_DATE - INTERVAL '7 days'
```

### For Channel Comparison
- Use Contact Channel derivation (same logic across patterns)
- Compare metrics across Chat, Voice, and Web channels

### For Time-Based Analysis
- All patterns have PST timestamp columns
- Use DATE_TRUNC for hourly/daily/weekly aggregations
- Consider business hours filtering when relevant

## Pattern Selection Decision Tree

When users ask performance-related questions, use this decision tree:

```
Is the question about...

├── Real-time/daily metrics? 
│   └── Use Patterns 1-4 (detailed data)
│
├── Weekly performance/trends/rankings?
│   └── Use Pattern 5 (WOPS Agent Performance) ✅
│
├── Specific ticket investigation?
│   └── Use Pattern 1 (WOPS Tickets)
│
├── QA review details/components?
│   └── Use Pattern 2 (Klaus QA)
│
├── Handle time breakdown/voice metrics?
│   └── Use Pattern 3 (Handle Time)
│
└── FCR failure analysis?
    └── Use Pattern 4 (FCR)
```

**Key Principle**: Pattern 5 should be the **default choice** for most agent performance questions unless specific granular details are needed.

## Priority Guidelines for AI Assistant

1. **First Choice**: Pattern 5 for any agent performance, rankings, or weekly metrics questions
2. **Second Choice**: Specific patterns (1-4) only when Pattern 5 doesn't have the required detail
3. **Combination**: Use Pattern 5 for overview, then drill down with specific patterns if needed

This approach provides faster, more accurate responses for the majority of business questions while still allowing detailed analysis when necessary.

## Important Notes
1. Always verify which filters are pre-applied in each pattern
2. Some patterns (like FCR) already have significant filtering
3. Handle time metrics are pre-calculated - no need for complex calculations
4. QA scores have different thresholds by scorecard type
5. Consider joining patterns on TICKET_ID or AGENT identifiers for comprehensive analysis

## Common Business Questions and Pattern Usage

| Question | Primary Pattern | Additional Patterns | Notes |
|----------|----------------|-------------------|-------|
| "How many tickets today?" | WOPS Tickets (1) | - | Use for real-time daily metrics |
| "What's our AHT?" | Handle Time (3) | WOPS Performance (5) | Pattern 5 for weekly averages |
| "Agent performance dashboard" | **WOPS Performance (5)** | Patterns 1-4 for details | **Start with Pattern 5** |
| "Top performing agents this week" | **WOPS Performance (5)** | - | **Perfect for this question** |
| "Weekly performance trends" | **WOPS Performance (5)** | - | **Ideal use case** |
| "Agent rankings" | **WOPS Performance (5)** | - | **Pre-calculated rankings** |
| "Channel efficiency" | Handle Time (3) | WOPS Tickets (1) for volume | - |
| "Quality scores by team" | Klaus QA (2) | WOPS Performance (5) | Pattern 5 for weekly averages |
| "Why are customers calling back?" | FCR (4) | WOPS Tickets (1) for issue types | - |
| "Peak hour analysis" | Handle Time (3) | WOPS Tickets (1) for volume | - |
| "Escalation patterns" | WOPS Tickets (1) | Handle Time (3) for duration | - |
| "Performance coaching insights" | **WOPS Performance (5)** | All patterns for details | **Start with Pattern 5** |
| "Week-over-week comparison" | **WOPS Performance (5)** | - | **Built for this analysis** |
| "Customer satisfaction trends" | **WOPS Performance (5)** | - | **New capability** |
| "Performance correlation analysis" | **WOPS Performance (5)** | - | **Volume vs Quality insights** |