
# Vector Store SQL Generation Test Report
Generated: 2025-07-22 20:58:50

## Executive Summary
- **Total Tests**: 5  
- **Successful**: 0 
- **Failed**: 0
- **Incomplete**: 5
- **Success Rate**: 0.0%

## Test Results

### ORGANIZATIONAL METRICS TEST - ⚠️ INCOMPLETE

**Question**: What's our overall chat volume today?
**Response Type**: ai_response
**Vector Store Accessed**: ✅ Yes
**Patterns Found**: WHERE
**Patterns Missing**: FCT_ZENDESK__MQR_TICKETS, GROUP_ID = '17837476387479', TODAY()

**Response Preview**:
```
To determine the overall chat volume for today, we need to access the relevant data from our operational metrics. This would involve querying the appropriate database table to gather the number of chat interactions that have occurred today.

### Data Analysis Process:
1. **Identify the Appropriate Table**: For general ticket volume, including chat interactions, we would typically use the `ANALYTICS.DBT_PRODUCTION.RPT_WOPS_TICKETS` table. This table contains pre-calculated and business-ready metr...
```

### TEAM LEAD PERFORMANCE TEST - ⚠️ INCOMPLETE

**Question**: How is Christine Presto performing this week?
**Response Type**: ai_response
**Vector Store Accessed**: ✅ Yes
**Patterns Found**: Christine Presto, WEEK
**Patterns Missing**: supervisor, JOIN

**Response Preview**:
```
To evaluate Christine Presto's performance this week, we would focus on several key metrics related to agent performance. These metrics would typically include:

1. **Number of Tickets Handled**: This measures the volume of work Christine has managed. It's important to see if she is handling an average or above-average number of tickets compared to her peers.

2. **Average Handle Time (AHT)**: This metric indicates the average time Christine takes to resolve a ticket. A lower AHT generally refle...
```

### AGENT PERFORMANCE TEST - ⚠️ INCOMPLETE

**Question**: Show me John Smith's AHT performance
**Response Type**: ai_response
**Vector Store Accessed**: ✅ Yes
**Patterns Found**: John Smith, AHT
**Patterns Missing**: USER_NAME, Dim Zendesk Users

**Response Preview**:
```
To analyze John Smith's AHT (Average Handle Time) performance, we would focus on the following key metrics related to agent performance:

1. **Average Handle Time (AHT)**: This metric measures the average time John Smith spends handling a ticket. It is calculated as the sum of the talk time, hold time, and after-call work time divided by the number of tickets handled. This metric is crucial as it directly impacts customer satisfaction and operational efficiency.

2. **Number of Tickets Handled**...
```

### QUALITY METRICS TEST - ⚠️ INCOMPLETE

**Question**: What's our QA score this week?
**Response Type**: ai_response
**Vector Store Accessed**: ✅ Yes
**Patterns Found**: Klaus, WEEK, QA
**Patterns Missing**: scorecard

**Response Preview**:
```
To determine the QA score for this week, we would typically use the `ANALYTICS.DBT_PRODUCTION.FCT_KLAUS__REVIEWS` table, which provides comprehensive insights into quality assurance through Klaus reviews. The primary metric of interest here is the `AVG(Overall_Score)`, which represents the average quality score derived from various audits and reviews conducted over the week【4:4†wops_bi_complete_knowledge_base.txt】.

To get the QA score for this week, the following SQL query could be used:

```sq...
```

### AUDITOR PERFORMANCE TEST - ⚠️ INCOMPLETE

**Question**: How many audits did Sarah complete?
**Response Type**: ai_response
**Vector Store Accessed**: ✅ Yes
**Patterns Found**: Sarah, audit
**Patterns Missing**: REVIEWER_NAME, COUNT(DISTINCT REVIEW_ID)

**Response Preview**:
```
To determine how many audits Sarah completed, you would need to access specific data from the business intelligence system that tracks audit performance metrics. Typically, the relevant metrics would include:

- **Number of Audits Completed**: This would be a count of the audits attributed to Sarah.
- **Quality Scores**: Often audits are associated with quality scores which might be relevant if you're assessing performance.
- **Audit Dates and Frequency**: Knowing when these audits were complete...
```

## Analysis

### Which queries worked correctly?

### Which ones failed or were incomplete?
- ⚠️ **ORGANIZATIONAL METRICS TEST**: Missing patterns: FCT_ZENDESK__MQR_TICKETS, GROUP_ID = '17837476387479', TODAY()
- ⚠️ **TEAM LEAD PERFORMANCE TEST**: Missing patterns: supervisor, JOIN
- ⚠️ **AGENT PERFORMANCE TEST**: Missing patterns: USER_NAME, Dim Zendesk Users
- ⚠️ **QUALITY METRICS TEST**: Missing patterns: scorecard
- ⚠️ **AUDITOR PERFORMANCE TEST**: Missing patterns: REVIEWER_NAME, COUNT(DISTINCT REVIEW_ID)

### Vector Store Access Analysis
- **Tests with vector store access**: 5/5
- ✅ All tests appear to be accessing vector store context

### Recommendations

1. **For failed SQL generation**: Review vector store content for missing table/column patterns
2. **For missing patterns**: Update knowledge base with more specific SQL examples
3. **For vector store access issues**: Check Assistant API configuration and vector store setup
4. **For response quality**: Consider adding more detailed business context to prompts

