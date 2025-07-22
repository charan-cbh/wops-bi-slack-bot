# Metabase Dashboard Context for BI Query Generation

## Structure Template

For each dashboard, please provide:

### Dashboard Information
- **Dashboard Name**: [Exact name in Metabase]
- **Dashboard URL**: [Metabase URL]
- **Primary Use Case**: [What business problem does this solve?]
- **Used By Teams**: [Which teams use this dashboard?]
- **Key Metrics**: [Main KPIs/metrics shown]
- **Refresh Frequency**: [How often is data updated?]

### Query Examples with Business Context

#### Example 1: [Query Purpose]
**Business Question**: "What team members ask this as:"
- "Show me agent performance for last month"
- "How is Team Gian doing on AHT?"
- "Which agents need coaching on quality scores?"

**SQL Query**:
```sql
SELECT 
    agent_name,
    team_name,
    AVG(aht_seconds) as avg_aht,
    AVG(quality_score) as avg_quality,
    COUNT(tickets) as total_tickets
FROM agent_performance 
WHERE date >= '2024-01-01' 
    AND team_name = 'Team Gian'
GROUP BY agent_name, team_name
ORDER BY avg_aht DESC
```

**Key Tables Used**: 
- `agent_performance` - Main metrics table
- `teams` - Team assignments
- `tickets` - Individual ticket data

**Business Logic Notes**:
- AHT under 300 seconds is considered good
- Quality scores range from 1-5, target is >4.0
- Only include productive hours, exclude training time
- Filter out test accounts and admin users

#### Example 2: [Another Query Purpose]
[Repeat structure...]

### Common Query Patterns

#### Time Period Filters
```sql
-- Last 7 days
WHERE date >= CURRENT_DATE - INTERVAL '7 days'

-- Current month
WHERE DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE)

-- Last complete month
WHERE DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
```

#### Team Filters
```sql
-- Specific team
WHERE team_name IN ('Team Gian', 'Team Sarah')

-- Exclude certain teams
WHERE team_name NOT IN ('Training', 'Admin')
```

### Edge Cases and Special Conditions
- **Holidays**: Exclude or handle differently
- **Training Period**: New agents have different thresholds
- **System Outages**: Filter out affected time periods
- **Data Quality Issues**: Known data gaps or corrections needed

---

## Example Dashboard Template

### Agent Performance Dashboard
- **Dashboard Name**: Agent Performance - Team Gian
- **Primary Use Case**: Track individual agent metrics for coaching and performance reviews
- **Used By Teams**: Team Leads, QA Team, Operations Managers
- **Key Metrics**: AHT, Quality Scores, FCR Rate, Schedule Adherence
- **Refresh Frequency**: Real-time (updated every hour)

#### Query Example 1: Individual Agent Performance
**Business Question**: 
- "How is agent John Smith performing this month?"
- "Show me John's AHT trend for last 30 days"
- "What's John's quality score compared to team average?"

**SQL Query**:
```sql
SELECT 
    a.agent_name,
    a.team_name,
    DATE(a.date) as date,
    AVG(a.aht_seconds) as avg_aht,
    AVG(a.quality_score) as quality_score,
    AVG(a.fcr_rate) as fcr_rate,
    AVG(a.schedule_adherence) as schedule_adherence,
    COUNT(t.ticket_id) as tickets_handled
FROM agent_performance a
LEFT JOIN tickets t ON a.agent_id = t.assigned_agent_id 
    AND DATE(t.created_at) = DATE(a.date)
WHERE a.agent_name = 'John Smith'
    AND a.date >= CURRENT_DATE - INTERVAL '30 days'
    AND a.team_name = 'Team Gian'
GROUP BY a.agent_name, a.team_name, DATE(a.date)
ORDER BY date DESC
```

**Key Tables**: 
- `agent_performance` (main metrics by day)
- `tickets` (individual ticket details)
- `agents` (agent info and team assignments)

**Business Logic**:
- AHT target: <280 seconds for Team Gian
- Quality score target: >4.2
- FCR target: >85%
- Only count productive hours (exclude breaks, training)

#### Query Example 2: Team Comparison
**Business Question**:
- "How does Team Gian compare to other teams?"
- "Show team performance ranking for this quarter"

**SQL Query**:
```sql
SELECT 
    team_name,
    COUNT(DISTINCT agent_id) as team_size,
    AVG(aht_seconds) as avg_team_aht,
    AVG(quality_score) as avg_team_quality,
    AVG(fcr_rate) as avg_team_fcr,
    SUM(tickets_handled) as total_tickets
FROM agent_performance 
WHERE date >= DATE_TRUNC('quarter', CURRENT_DATE)
    AND team_name NOT IN ('Training', 'Admin')
GROUP BY team_name
ORDER BY avg_team_quality DESC, avg_team_aht ASC
```

### Common Variations:
- **Time periods**: last 7/30/90 days, current/last month/quarter
- **Comparisons**: individual vs team, team vs company, current vs previous period
- **Filters**: by team, by agent, by ticket category, by customer type

---

## Instructions for Adding Your Dashboards

Please provide the above information for each of your Metabase dashboards:

1. **Agent Performance Dashboards**
2. **Team Performance Dashboards** 
3. **Operational Metrics Dashboards**
4. **Quality Assurance Dashboards**
5. **Customer Satisfaction Dashboards**
6. **Productivity Dashboards**
7. **Any other BI dashboards your teams use**

The more examples you provide with business context, the better the bot will become at generating the exact SQL queries your teams need!