#!/usr/bin/env python3
"""
Business Metrics Intelligence System
Provides deep understanding of business metrics and their proper interpretation
"""

import re
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass


@dataclass
class MetricDefinition:
    """Defines a business metric with its characteristics"""
    name: str
    column_name: str
    better_direction: str  # 'higher' or 'lower' 
    data_type: str  # 'percentage', 'minutes', 'count', 'score'
    aggregation_method: str  # 'avg', 'sum', 'median', 'max', 'min'
    business_context: str
    interpretation: Dict[str, str]  # 'lowest_performing', 'highest_performing' interpretations


class BusinessMetricsIntelligence:
    """
    Intelligent system that understands business metrics and their proper interpretation
    """
    
    def __init__(self):
        self.metrics_definitions = {
            'aht': MetricDefinition(
                name='Average Handle Time',
                column_name='AHT_MINUTES',
                better_direction='lower',
                data_type='minutes',
                aggregation_method='avg',
                business_context='Time spent handling tickets - lower is better efficiency',
                interpretation={
                    'lowest_performing': 'highest_values',  # High AHT = poor performance
                    'highest_performing': 'lowest_values',  # Low AHT = good performance
                    'best_agents': 'lowest_values',
                    'worst_agents': 'highest_values'
                }
            ),
            'qa_score': MetricDefinition(
                name='Quality Assurance Score',
                column_name='QA_SCORE',
                better_direction='higher',
                data_type='score',
                aggregation_method='avg',
                business_context='Quality score from QA reviews - higher is better quality',
                interpretation={
                    'lowest_performing': 'lowest_values',  # Low QA = poor performance
                    'highest_performing': 'highest_values',  # High QA = good performance
                    'best_agents': 'highest_values',
                    'worst_agents': 'lowest_values'
                }
            ),
            'csat': MetricDefinition(
                name='Customer Satisfaction Score',
                column_name='POSITIVE_RES_CSAT',
                better_direction='higher',
                data_type='percentage',
                aggregation_method='avg',
                business_context='Customer satisfaction rating - higher is better satisfaction',
                interpretation={
                    'lowest_performing': 'lowest_values',  # Low CSAT = poor performance
                    'highest_performing': 'highest_values',  # High CSAT = good performance
                    'best_agents': 'highest_values',
                    'worst_agents': 'lowest_values'
                }
            ),
            'fcr': MetricDefinition(
                name='First Call Resolution',
                column_name='FCR_PERCENTAGE',
                better_direction='higher',
                data_type='percentage',
                aggregation_method='avg',
                business_context='First call resolution rate - higher is better efficiency',
                interpretation={
                    'lowest_performing': 'lowest_values',  # Low FCR = poor performance
                    'highest_performing': 'highest_values',  # High FCR = good performance
                    'best_agents': 'highest_values',
                    'worst_agents': 'lowest_values'
                }
            ),
            'adherence': MetricDefinition(
                name='Schedule Adherence',
                column_name='ADHERENCE_PERCENTAGE',
                better_direction='higher',
                data_type='percentage',
                aggregation_method='avg',
                business_context='Schedule adherence rate - higher is better compliance',
                interpretation={
                    'lowest_performing': 'lowest_values',  # Low adherence = poor performance
                    'highest_performing': 'highest_values',  # High adherence = good performance
                    'best_agents': 'highest_values',
                    'worst_agents': 'lowest_values'
                }
            ),
            'ticket_volume': MetricDefinition(
                name='Ticket Volume',
                column_name='NUM_TICKETS',
                better_direction='context_dependent',
                data_type='count',
                aggregation_method='sum',
                business_context='Number of tickets handled - context dependent interpretation',
                interpretation={
                    'lowest_performing': 'context_dependent',
                    'highest_performing': 'context_dependent',
                    'most_active': 'highest_values',
                    'least_active': 'lowest_values'
                }
            )
        }
        
        # Performance context keywords
        self.performance_keywords = {
            'poor_performance': ['lowest', 'worst', 'bottom', 'underperforming', 'struggling', 'poor'],
            'good_performance': ['highest', 'best', 'top', 'performing', 'excellent', 'outstanding'],
            'ranking': ['rank', 'ranking', 'order', 'sort', 'arrange'],
            'improvement': ['improvement', 'getting better', 'progress', 'development'],
            'decline': ['decline', 'getting worse', 'deteriorating', 'dropping']
        }
        
        # Time period patterns
        self.time_patterns = {
            'monthly': ['month', 'monthly', 'june', 'july', 'january', 'february', 'march', 'april', 'may', 'august', 'september', 'october', 'november', 'december'],
            'weekly': ['week', 'weekly', 'this week', 'last week'],
            'daily': ['day', 'daily', 'today', 'yesterday'],
            'quarterly': ['quarter', 'quarterly', 'q1', 'q2', 'q3', 'q4']
        }
    
    def analyze_question_context(self, question: str) -> Dict[str, Any]:
        """
        Analyze the business context of a question to understand what metrics and 
        interpretations are needed
        """
        question_lower = question.lower()
        
        analysis = {
            'metrics_mentioned': [],
            'performance_context': None,
            'time_period': None,
            'aggregation_needed': False,
            'ranking_requested': False,
            'team_context': None,
            'agent_context': None,
            'business_intelligence': {}
        }
        
        # Detect metrics mentioned
        for metric_key, metric_def in self.metrics_definitions.items():
            metric_keywords = [metric_key, metric_def.name.lower()]
            
            # Add specific keywords for each metric
            if metric_key == 'aht':
                metric_keywords.extend(['handle time', 'aht', 'efficiency', 'average handle time'])
            elif metric_key == 'qa_score':
                metric_keywords.extend(['qa', 'quality', 'qa score', 'quality score'])
            elif metric_key == 'csat':
                metric_keywords.extend(['csat', 'customer satisfaction', 'satisfaction'])
            elif metric_key == 'fcr':
                metric_keywords.extend(['fcr', 'first call resolution', 'resolution'])
            elif metric_key == 'adherence':
                metric_keywords.extend(['adherence', 'schedule', 'compliance'])
            
            for keyword in metric_keywords:
                if keyword in question_lower:
                    analysis['metrics_mentioned'].append(metric_key)
                    break
        
        # Detect performance context
        for context_type, keywords in self.performance_keywords.items():
            for keyword in keywords:
                if keyword in question_lower:
                    analysis['performance_context'] = context_type
                    break
            if analysis['performance_context']:
                break
        
        # Detect time period
        for period_type, patterns in self.time_patterns.items():
            for pattern in patterns:
                if pattern in question_lower:
                    analysis['time_period'] = period_type
                    break
            if analysis['time_period']:
                break
        
        # Detect if aggregation is needed
        analysis['aggregation_needed'] = self._detect_aggregation_need(question_lower)
        
        # Detect ranking
        analysis['ranking_requested'] = any(word in question_lower for word in ['top', 'bottom', 'rank', 'order', 'sort', 'first', 'last', 'lowest', 'highest', 'worst', 'best'])
        
        # Also check for number + agents pattern (e.g., "3 agents", "5 agents")
        import re
        number_agents_pattern = r'(\d+)\s+agents'
        if re.search(number_agents_pattern, question_lower):
            analysis['ranking_requested'] = True
        
        # Detect team context
        analysis['team_context'] = self._extract_team_context(question)
        
        # Detect agent context
        analysis['agent_context'] = self._detect_agent_context(question_lower)
        
        # Generate business intelligence
        analysis['business_intelligence'] = self._generate_business_intelligence(analysis, question)
        
        return analysis
    
    def _detect_aggregation_need(self, question_lower: str) -> bool:
        """Detect if the question requires data aggregation"""
        aggregation_indicators = [
            'average', 'avg', 'mean', 'total', 'sum', 'overall',
            'month', 'weekly', 'daily', 'period', 'performance',
            'multiple', 'across', 'during', 'for the'
        ]
        
        return any(indicator in question_lower for indicator in aggregation_indicators)
    
    def _extract_team_context(self, question: str) -> Optional[str]:
        """Extract team/supervisor context from question"""
        patterns = [
            r"team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            r"from\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            r"in\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'s\s+team"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, question)
            if match:
                return match.group(1)
        
        return None
    
    def _detect_agent_context(self, question_lower: str) -> str:
        """Detect if question is about individual agents or teams"""
        if any(word in question_lower for word in ['agent', 'agents', 'individual', 'person']):
            return 'individual'
        elif any(word in question_lower for word in ['team', 'group', 'department']):
            return 'team'
        else:
            return 'unclear'
    
    def _generate_business_intelligence(self, analysis: Dict, question: str) -> Dict[str, Any]:
        """Generate business intelligence recommendations"""
        intelligence = {
            'recommended_aggregation': [],
            'sort_logic': [],
            'filter_logic': [],
            'business_context': '',
            'critical_considerations': []
        }
        
        # Generate aggregation recommendations
        if analysis['aggregation_needed']:
            intelligence['recommended_aggregation'] = self._recommend_aggregation(analysis)
        
        # Generate sort logic
        if analysis['ranking_requested']:
            intelligence['sort_logic'] = self._generate_sort_logic(analysis)
        
        # Generate filter logic
        if analysis['team_context'] or analysis['time_period']:
            intelligence['filter_logic'] = self._generate_filter_logic(analysis)
        
        # Generate business context
        intelligence['business_context'] = self._generate_business_context(analysis, question)
        
        # Generate critical considerations
        intelligence['critical_considerations'] = self._generate_critical_considerations(analysis)
        
        return intelligence
    
    def _recommend_aggregation(self, analysis: Dict) -> List[str]:
        """Recommend appropriate aggregation methods"""
        recommendations = []
        
        for metric_key in analysis['metrics_mentioned']:
            metric_def = self.metrics_definitions[metric_key]
            
            if metric_def.aggregation_method == 'avg':
                recommendations.append(f"AVG({metric_def.column_name}) as avg_{metric_key}")
            elif metric_def.aggregation_method == 'sum':
                recommendations.append(f"SUM({metric_def.column_name}) as total_{metric_key}")
            elif metric_def.aggregation_method == 'median':
                recommendations.append(f"MEDIAN({metric_def.column_name}) as median_{metric_key}")
        
        # Always aggregate by agent when dealing with multi-period data
        if analysis['time_period'] in ['monthly', 'weekly']:
            recommendations.append("GROUP BY ASSIGNEE_NAME")
        
        return recommendations
    
    def _generate_sort_logic(self, analysis: Dict) -> List[str]:
        """Generate proper sort logic based on business context"""
        sort_logic = []
        
        performance_context = analysis['performance_context']
        
        # If aggregation is needed, use the aggregated column names
        if analysis['aggregation_needed']:
            for metric_key in analysis['metrics_mentioned']:
                metric_def = self.metrics_definitions[metric_key]
                
                if performance_context == 'poor_performance':
                    # For poor performance, we want the interpretation from metric definition
                    interpretation = metric_def.interpretation.get('lowest_performing', 'lowest_values')
                    if interpretation == 'highest_values':
                        sort_logic.append(f"avg_{metric_key} DESC")
                    elif interpretation == 'lowest_values':
                        sort_logic.append(f"avg_{metric_key} ASC")
                
                elif performance_context == 'good_performance':
                    # For good performance, we want the opposite
                    interpretation = metric_def.interpretation.get('highest_performing', 'highest_values')
                    if interpretation == 'highest_values':
                        sort_logic.append(f"avg_{metric_key} DESC")
                    elif interpretation == 'lowest_values':
                        sort_logic.append(f"avg_{metric_key} ASC")
        else:
            # No aggregation needed, use original column names
            for metric_key in analysis['metrics_mentioned']:
                metric_def = self.metrics_definitions[metric_key]
                
                if performance_context == 'poor_performance':
                    # For poor performance, we want the interpretation from metric definition
                    interpretation = metric_def.interpretation.get('lowest_performing', 'lowest_values')
                    if interpretation == 'highest_values':
                        sort_logic.append(f"{metric_def.column_name} DESC")
                    elif interpretation == 'lowest_values':
                        sort_logic.append(f"{metric_def.column_name} ASC")
                
                elif performance_context == 'good_performance':
                    # For good performance, we want the opposite
                    interpretation = metric_def.interpretation.get('highest_performing', 'highest_values')
                    if interpretation == 'highest_values':
                        sort_logic.append(f"{metric_def.column_name} DESC")
                    elif interpretation == 'lowest_values':
                        sort_logic.append(f"{metric_def.column_name} ASC")
        
        return sort_logic
    
    def _generate_filter_logic(self, analysis: Dict) -> List[str]:
        """Generate appropriate filter logic"""
        filters = []
        
        if analysis['team_context']:
            filters.append(f"ASSIGNEE_SUPERVISOR ILIKE '%{analysis['team_context']}%'")
        
        if analysis['time_period'] == 'monthly':
            filters.append("EXTRACT(MONTH FROM SOLVED_WEEK) = ?")
            filters.append("EXTRACT(YEAR FROM SOLVED_WEEK) = ?")
        elif analysis['time_period'] == 'weekly':
            filters.append("SOLVED_WEEK >= DATE_TRUNC('week', CURRENT_DATE)")
        elif analysis['time_period'] == 'daily':
            filters.append("DATE(SOLVED_WEEK) = CURRENT_DATE")
        
        return filters
    
    def _generate_business_context(self, analysis: Dict, question: str) -> str:
        """Generate business context explanation"""
        context_parts = []
        
        # Explain metrics
        for metric_key in analysis['metrics_mentioned']:
            metric_def = self.metrics_definitions[metric_key]
            context_parts.append(f"• {metric_def.name}: {metric_def.business_context}")
        
        # Explain performance context
        if analysis['performance_context'] == 'poor_performance':
            context_parts.append("• Looking for LOWEST PERFORMING agents (worst performance)")
        elif analysis['performance_context'] == 'good_performance':
            context_parts.append("• Looking for HIGHEST PERFORMING agents (best performance)")
        
        # Explain aggregation need
        if analysis['aggregation_needed']:
            context_parts.append("• Data aggregation needed due to multi-period data structure")
        
        return "\n".join(context_parts)
    
    def _generate_critical_considerations(self, analysis: Dict) -> List[str]:
        """Generate critical considerations for the query"""
        considerations = []
        
        # AHT specific considerations
        if 'aht' in analysis['metrics_mentioned']:
            considerations.append("⚠️  AHT: Lower values = BETTER performance (higher values = worse performance)")
        
        # QA specific considerations
        if 'qa_score' in analysis['metrics_mentioned']:
            considerations.append("✅ QA Score: Higher values = BETTER performance")
        
        # CSAT specific considerations  
        if 'csat' in analysis['metrics_mentioned']:
            considerations.append("✅ CSAT: Higher values = BETTER performance")
        
        # Aggregation considerations
        if analysis['aggregation_needed']:
            considerations.append("📊 Multi-period data requires proper aggregation (AVG for most metrics)")
            considerations.append("🔄 Same agent may appear multiple times - use GROUP BY ASSIGNEE_NAME")
        
        # Performance context considerations
        if analysis['performance_context'] == 'poor_performance':
            considerations.append("📉 'Lowest performing' means different things for different metrics")
        
        return considerations
    
    def generate_intelligent_sql_structure(self, question: str, table_name: str, schema: Dict) -> Dict[str, Any]:
        """
        Generate intelligent SQL structure based on business context analysis
        """
        analysis = self.analyze_question_context(question)
        
        sql_structure = {
            'select_columns': [],
            'aggregations': [],
            'group_by': [],
            'where_conditions': [],
            'order_by': [],
            'limit_clause': None,
            'business_explanation': '',
            'critical_warnings': []
        }
        
        # Build SELECT columns
        sql_structure['select_columns'].append('ASSIGNEE_NAME')
        
        # Add metric columns with proper aggregation
        for metric_key in analysis['metrics_mentioned']:
            metric_def = self.metrics_definitions[metric_key]
            
            if analysis['aggregation_needed']:
                if metric_def.aggregation_method == 'avg':
                    sql_structure['aggregations'].append(f"AVG({metric_def.column_name}) as avg_{metric_key}")
                elif metric_def.aggregation_method == 'sum':
                    sql_structure['aggregations'].append(f"SUM({metric_def.column_name}) as total_{metric_key}")
            else:
                sql_structure['select_columns'].append(metric_def.column_name)
        
        # Add GROUP BY if aggregation is needed
        if analysis['aggregation_needed']:
            sql_structure['group_by'].append('ASSIGNEE_NAME')
        
        # Add WHERE conditions
        sql_structure['where_conditions'].extend(analysis['business_intelligence']['filter_logic'])
        
        # Add ORDER BY with proper business logic
        order_by_columns = analysis['business_intelligence']['sort_logic']
        if order_by_columns:
            sql_structure['order_by'].extend(order_by_columns)
        
        # Add LIMIT for ranking queries
        if analysis['ranking_requested']:
            # Extract number from question (e.g., "top 3", "bottom 5", "lowest 3 agents")
            import re
            number_patterns = [
                r'(?:top|bottom|lowest|highest|worst|best)\s+(\d+)',
                r'(\d+)\s+(?:agents|performers|people)',
                r'(\d+)'  # fallback pattern
            ]
            
            for pattern in number_patterns:
                number_match = re.search(pattern, question.lower())
                if number_match:
                    sql_structure['limit_clause'] = int(number_match.group(1))
                    break
            else:
                sql_structure['limit_clause'] = 10  # Default limit
        
        # Generate business explanation
        sql_structure['business_explanation'] = self._generate_sql_explanation(analysis, question)
        
        # Add critical warnings
        sql_structure['critical_warnings'] = analysis['business_intelligence']['critical_considerations']
        
        return sql_structure
    
    def _generate_sql_explanation(self, analysis: Dict, question: str) -> str:
        """Generate detailed explanation of SQL logic"""
        explanation_parts = [
            "🧠 BUSINESS INTELLIGENCE APPLIED:",
            ""
        ]
        
        # Explain table choice
        explanation_parts.append("📊 TABLE SELECTION:")
        explanation_parts.append("✅ RPT_WOPS_AGENT_PERFORMANCE - Contains agent performance metrics with proper relationships")
        explanation_parts.append("")
        
        # Explain metrics
        explanation_parts.append("📈 METRICS ANALYSIS:")
        for metric_key in analysis['metrics_mentioned']:
            metric_def = self.metrics_definitions[metric_key]
            explanation_parts.append(f"✅ {metric_def.name}: {metric_def.business_context}")
        explanation_parts.append("")
        
        # Explain aggregation logic
        if analysis['aggregation_needed']:
            explanation_parts.append("🔄 AGGREGATION LOGIC:")
            explanation_parts.append("✅ Multi-period data detected - using proper aggregation")
            explanation_parts.append("✅ GROUP BY ASSIGNEE_NAME to avoid duplicate agent entries")
            explanation_parts.append("✅ AVG() for performance metrics to get true average performance")
            explanation_parts.append("")
        
        # Explain sorting logic
        if analysis['ranking_requested']:
            explanation_parts.append("📊 RANKING LOGIC:")
            if analysis['performance_context'] == 'poor_performance':
                explanation_parts.append("✅ 'Lowest performing' agents - sorted by worst performance first")
            elif analysis['performance_context'] == 'good_performance':
                explanation_parts.append("✅ 'Highest performing' agents - sorted by best performance first")
            explanation_parts.append("")
        
        # Explain business context
        explanation_parts.append("💡 BUSINESS CONTEXT:")
        explanation_parts.append(analysis['business_intelligence']['business_context'])
        
        return "\n".join(explanation_parts)
    
    def validate_query_business_logic(self, sql: str, question: str) -> Dict[str, Any]:
        """
        Validate generated SQL against business logic requirements
        """
        analysis = self.analyze_question_context(question)
        
        validation_result = {
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'suggestions': []
        }
        
        sql_lower = sql.lower()
        
        # Check for AHT sorting logic
        if 'aht' in analysis['metrics_mentioned']:
            if analysis['performance_context'] == 'poor_performance':
                if 'aht_minutes asc' in sql_lower:
                    validation_result['errors'].append("❌ AHT sorting error: 'lowest performing' should be DESC (high AHT = poor performance)")
                    validation_result['is_valid'] = False
        
        # Check for aggregation when needed
        if analysis['aggregation_needed']:
            if 'group by' not in sql_lower:
                validation_result['warnings'].append("⚠️  Missing GROUP BY - multi-period data may show duplicate agents")
            
            if 'avg(' not in sql_lower and 'sum(' not in sql_lower:
                validation_result['warnings'].append("⚠️  Missing aggregation functions - may show raw individual records")
        
        # Check for proper team filtering
        if analysis['team_context']:
            if 'assignee_supervisor' not in sql_lower:
                validation_result['errors'].append("❌ Missing team filter - should use ASSIGNEE_SUPERVISOR column")
                validation_result['is_valid'] = False
        
        # Generate suggestions
        if not validation_result['is_valid']:
            validation_result['suggestions'] = self._generate_fix_suggestions(analysis, sql)
        
        return validation_result
    
    def _generate_fix_suggestions(self, analysis: Dict, sql: str) -> List[str]:
        """Generate suggestions to fix SQL issues"""
        suggestions = []
        
        # AHT sorting fixes
        if 'aht' in analysis['metrics_mentioned'] and analysis['performance_context'] == 'poor_performance':
            suggestions.append("💡 For AHT 'lowest performing': Use ORDER BY AHT_MINUTES DESC (high AHT = poor performance)")
        
        # Aggregation fixes
        if analysis['aggregation_needed']:
            suggestions.append("💡 Add GROUP BY ASSIGNEE_NAME to aggregate multi-period data")
            suggestions.append("💡 Use AVG() for performance metrics to get true average performance")
        
        # Team filtering fixes
        if analysis['team_context']:
            suggestions.append(f"💡 Add WHERE ASSIGNEE_SUPERVISOR ILIKE '%{analysis['team_context']}%'")
        
        return suggestions


# Global instance
business_metrics_intelligence = BusinessMetricsIntelligence()