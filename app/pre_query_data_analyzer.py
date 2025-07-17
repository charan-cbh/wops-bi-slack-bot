#!/usr/bin/env python3
"""
Pre-Query Data Analysis System
Analyzes data structure and context before generating SQL queries
"""

import re
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
import pandas as pd


@dataclass
class DataStructureAnalysis:
    """Analysis of table data structure"""
    table_name: str
    row_count: int
    unique_agents: int
    time_periods: List[str]
    data_grain: str  # 'agent_week', 'agent_month', 'ticket_level'
    has_duplicates: bool
    aggregation_needed: bool
    recommended_grouping: List[str]
    key_insights: List[str]


class PreQueryDataAnalyzer:
    """
    Analyzes data structure and context before SQL generation to ensure
    proper understanding of aggregation needs and business logic
    """
    
    def __init__(self):
        self.table_analysis_cache = {}
        
        # Known table structures
        self.table_structures = {
            'RPT_WOPS_AGENT_PERFORMANCE': {
                'primary_key': ['ASSIGNEE_NAME', 'SOLVED_WEEK'],
                'grain': 'agent_week',
                'time_column': 'SOLVED_WEEK',
                'agent_column': 'ASSIGNEE_NAME',
                'supervisor_column': 'ASSIGNEE_SUPERVISOR',
                'typical_duplicates': 'same_agent_multiple_weeks',
                'aggregation_patterns': {
                    'agent_performance': 'GROUP BY ASSIGNEE_NAME',
                    'team_performance': 'GROUP BY ASSIGNEE_SUPERVISOR',
                    'time_series': 'GROUP BY SOLVED_WEEK'
                }
            },
            'ZENDESK_TICKET_AGENT__HANDLE_TIME': {
                'primary_key': ['TICKET_ID', 'USER_NAME'],
                'grain': 'ticket_agent',
                'time_column': 'CREATED_AT_PST',
                'agent_column': 'USER_NAME',
                'supervisor_column': 'SUPERVISOR',
                'typical_duplicates': 'ticket_level_records',
                'aggregation_patterns': {
                    'agent_performance': 'GROUP BY USER_NAME',
                    'team_performance': 'GROUP BY SUPERVISOR',
                    'time_series': 'GROUP BY DATE(CREATED_AT_PST)'
                }
            },
            'RPT_WOPS_TICKETS': {
                'primary_key': ['TICKET_ID'],
                'grain': 'ticket',
                'time_column': 'CREATED_AT_PST',
                'agent_column': 'AGENT_NAME',
                'supervisor_column': None,
                'typical_duplicates': 'none',
                'aggregation_patterns': {
                    'agent_performance': 'GROUP BY AGENT_NAME',
                    'time_series': 'GROUP BY DATE(CREATED_AT_PST)'
                }
            }
        }
    
    async def analyze_table_structure(self, table_name: str, sample_size: int = 1000) -> DataStructureAnalysis:
        """
        Analyze table structure to understand data grain and aggregation needs
        """
        # Check cache first
        if table_name in self.table_analysis_cache:
            return self.table_analysis_cache[table_name]
        
        # Get table structure info
        table_info = self.table_structures.get(table_name, {})
        
        try:
            # Get sample data to analyze structure
            sample_data = await self._get_sample_data(table_name, sample_size)
            
            if sample_data is None or sample_data.empty:
                # Return default analysis if no data
                return self._create_default_analysis(table_name, table_info)
            
            # Analyze the sample data
            analysis = self._analyze_sample_data(sample_data, table_name, table_info)
            
            # Cache the analysis
            self.table_analysis_cache[table_name] = analysis
            
            return analysis
            
        except Exception as e:
            print(f"⚠️ Error analyzing table structure for {table_name}: {e}")
            return self._create_default_analysis(table_name, table_info)
    
    async def _get_sample_data(self, table_name: str, sample_size: int) -> Optional[pd.DataFrame]:
        """Get sample data from table for analysis"""
        try:
            from app.snowflake_runner import run_query
            
            # Get sample data with key columns
            table_info = self.table_structures.get(table_name, {})
            
            # Build sample query based on table structure
            if table_name == 'RPT_WOPS_AGENT_PERFORMANCE':
                sample_query = f"""
                SELECT 
                    ASSIGNEE_NAME,
                    ASSIGNEE_SUPERVISOR,
                    SOLVED_WEEK,
                    AHT_MINUTES,
                    QA_SCORE,
                    POSITIVE_RES_CSAT,
                    NUM_TICKETS
                FROM ANALYTICS.DBT_PRODUCTION.{table_name}
                WHERE ASSIGNEE_NAME IS NOT NULL
                ORDER BY SOLVED_WEEK DESC
                LIMIT {sample_size}
                """
            elif table_name == 'ZENDESK_TICKET_AGENT__HANDLE_TIME':
                sample_query = f"""
                SELECT 
                    USER_NAME,
                    SUPERVISOR,
                    CREATED_AT_PST,
                    HANDLE_TIME_IN_MINUTES,
                    TICKET_ID
                FROM ANALYTICS.DBT_PRODUCTION.{table_name}
                WHERE USER_NAME IS NOT NULL
                ORDER BY CREATED_AT_PST DESC
                LIMIT {sample_size}
                """
            else:
                # Generic sample query
                sample_query = f"""
                SELECT *
                FROM ANALYTICS.DBT_PRODUCTION.{table_name}
                LIMIT {sample_size}
                """
            
            df = run_query(sample_query)
            
            if df.empty or 'Error' in df.columns:
                return None
            
            return df
            
        except Exception as e:
            print(f"❌ Error getting sample data for {table_name}: {e}")
            return None
    
    def _analyze_sample_data(self, df: pd.DataFrame, table_name: str, table_info: Dict) -> DataStructureAnalysis:
        """Analyze sample data to understand structure"""
        
        # Basic metrics
        row_count = len(df)
        
        # Analyze agent column
        agent_column = table_info.get('agent_column', 'ASSIGNEE_NAME')
        if agent_column in df.columns:
            unique_agents = df[agent_column].nunique()
            agent_duplicates = row_count - unique_agents
        else:
            unique_agents = 0
            agent_duplicates = 0
        
        # Analyze time periods
        time_column = table_info.get('time_column', 'SOLVED_WEEK')
        time_periods = []
        if time_column in df.columns:
            time_periods = df[time_column].unique().tolist()
        
        # Determine data grain
        grain = table_info.get('grain', 'unknown')
        
        # Detect if aggregation is needed
        has_duplicates = agent_duplicates > 0
        aggregation_needed = has_duplicates or len(time_periods) > 1
        
        # Generate recommendations
        recommended_grouping = self._recommend_grouping(df, table_info, aggregation_needed)
        
        # Generate key insights
        key_insights = self._generate_key_insights(df, table_info, unique_agents, agent_duplicates, time_periods)
        
        return DataStructureAnalysis(
            table_name=table_name,
            row_count=row_count,
            unique_agents=unique_agents,
            time_periods=time_periods,
            data_grain=grain,
            has_duplicates=has_duplicates,
            aggregation_needed=aggregation_needed,
            recommended_grouping=recommended_grouping,
            key_insights=key_insights
        )
    
    def _recommend_grouping(self, df: pd.DataFrame, table_info: Dict, aggregation_needed: bool) -> List[str]:
        """Recommend appropriate grouping columns"""
        if not aggregation_needed:
            return []
        
        grouping = []
        
        # Always group by agent for performance questions
        agent_column = table_info.get('agent_column', 'ASSIGNEE_NAME')
        if agent_column in df.columns:
            grouping.append(agent_column)
        
        return grouping
    
    def _generate_key_insights(self, df: pd.DataFrame, table_info: Dict, unique_agents: int, 
                              agent_duplicates: int, time_periods: List) -> List[str]:
        """Generate key insights about the data structure"""
        insights = []
        
        # Agent insights
        if unique_agents > 0:
            insights.append(f"📊 {unique_agents} unique agents in sample")
        
        if agent_duplicates > 0:
            insights.append(f"🔄 {agent_duplicates} duplicate agent entries detected")
            insights.append("⚠️  Aggregation required to avoid duplicate agent counts")
        
        # Time period insights
        if len(time_periods) > 1:
            insights.append(f"📅 {len(time_periods)} time periods detected")
            insights.append("📈 Multi-period data requires aggregation for accurate performance metrics")
        
        # Data grain insights
        grain = table_info.get('grain', 'unknown')
        if grain == 'agent_week':
            insights.append("📋 Data grain: Agent-Week (each agent has one row per week)")
        elif grain == 'ticket_agent':
            insights.append("📋 Data grain: Ticket-Agent (each ticket-agent interaction)")
        elif grain == 'ticket':
            insights.append("📋 Data grain: Ticket (each ticket has one row)")
        
        return insights
    
    def _create_default_analysis(self, table_name: str, table_info: Dict) -> DataStructureAnalysis:
        """Create default analysis when data is not available"""
        return DataStructureAnalysis(
            table_name=table_name,
            row_count=0,
            unique_agents=0,
            time_periods=[],
            data_grain=table_info.get('grain', 'unknown'),
            has_duplicates=table_info.get('typical_duplicates') != 'none',
            aggregation_needed=table_info.get('grain') in ['agent_week', 'ticket_agent'],
            recommended_grouping=[table_info.get('agent_column', 'ASSIGNEE_NAME')] if table_info.get('agent_column') else [],
            key_insights=[f"⚠️ Using known structure for {table_name}"]
        )
    
    def analyze_question_data_requirements(self, question: str, table_name: str) -> Dict[str, Any]:
        """
        Analyze what data processing is required for the question
        """
        question_lower = question.lower()
        
        analysis = {
            'question_type': self._classify_question_type(question_lower),
            'aggregation_level': self._determine_aggregation_level(question_lower),
            'time_scope': self._extract_time_scope(question_lower),
            'ranking_needed': self._detect_ranking_need(question_lower),
            'performance_context': self._extract_performance_context(question_lower),
            'team_context': self._extract_team_context(question),
            'data_processing_steps': []
        }
        
        # Generate data processing steps
        analysis['data_processing_steps'] = self._generate_processing_steps(analysis, table_name)
        
        return analysis
    
    def _classify_question_type(self, question_lower: str) -> str:
        """Classify the type of question being asked"""
        if any(phrase in question_lower for phrase in ['how many', 'count', 'number of']):
            return 'count_query'
        elif any(phrase in question_lower for phrase in ['performance', 'performing', 'metrics']):
            return 'performance_query'
        elif any(phrase in question_lower for phrase in ['ranking', 'top', 'bottom', 'best', 'worst']):
            return 'ranking_query'
        elif any(phrase in question_lower for phrase in ['average', 'total', 'sum']):
            return 'aggregation_query'
        else:
            return 'general_query'
    
    def _determine_aggregation_level(self, question_lower: str) -> str:
        """Determine what level of aggregation is needed"""
        if any(phrase in question_lower for phrase in ['individual', 'each agent', 'per agent']):
            return 'agent_level'
        elif any(phrase in question_lower for phrase in ['team', 'group', 'supervisor']):
            return 'team_level'
        elif any(phrase in question_lower for phrase in ['overall', 'total', 'company']):
            return 'organization_level'
        else:
            return 'agent_level'  # Default to agent level
    
    def _extract_time_scope(self, question_lower: str) -> Dict[str, Any]:
        """Extract time scope from question"""
        time_scope = {
            'type': 'unspecified',
            'value': None,
            'filter_needed': False
        }
        
        # Month patterns
        months = ['january', 'february', 'march', 'april', 'may', 'june',
                 'july', 'august', 'september', 'october', 'november', 'december']
        
        for month in months:
            if month in question_lower:
                time_scope['type'] = 'month'
                time_scope['value'] = month
                time_scope['filter_needed'] = True
                break
        
        # Week patterns
        if 'week' in question_lower:
            time_scope['type'] = 'week'
            time_scope['filter_needed'] = True
        
        # Day patterns
        if any(word in question_lower for word in ['today', 'yesterday', 'day']):
            time_scope['type'] = 'day'
            time_scope['filter_needed'] = True
        
        return time_scope
    
    def _detect_ranking_need(self, question_lower: str) -> Dict[str, Any]:
        """Detect if ranking is needed and what type"""
        ranking = {
            'needed': False,
            'direction': None,
            'limit': None,
            'criteria': []
        }
        
        # Check for ranking keywords
        if any(word in question_lower for word in ['top', 'best', 'highest', 'leading']):
            ranking['needed'] = True
            ranking['direction'] = 'desc'
        elif any(word in question_lower for word in ['bottom', 'worst', 'lowest', 'poorest']):
            ranking['needed'] = True
            ranking['direction'] = 'asc'
        
        # Extract number for limit
        import re
        number_match = re.search(r'(\d+)', question_lower)
        if number_match and ranking['needed']:
            ranking['limit'] = int(number_match.group(1))
        
        return ranking
    
    def _extract_performance_context(self, question_lower: str) -> Dict[str, Any]:
        """Extract performance context from question"""
        context = {
            'type': 'neutral',
            'metrics': [],
            'comparison': None
        }
        
        # Performance type
        if any(word in question_lower for word in ['worst', 'lowest', 'poor', 'underperforming']):
            context['type'] = 'poor_performance'
        elif any(word in question_lower for word in ['best', 'highest', 'excellent', 'top']):
            context['type'] = 'good_performance'
        
        # Metrics mentioned
        if any(word in question_lower for word in ['aht', 'handle time', 'efficiency']):
            context['metrics'].append('aht')
        if any(word in question_lower for word in ['qa', 'quality']):
            context['metrics'].append('qa_score')
        if any(word in question_lower for word in ['csat', 'satisfaction']):
            context['metrics'].append('csat')
        
        return context
    
    def _extract_team_context(self, question: str) -> Optional[str]:
        """Extract team/supervisor context"""
        patterns = [
            r"team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            r"from\s+team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'s\s+team"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, question)
            if match:
                return match.group(1)
        
        return None
    
    def _generate_processing_steps(self, analysis: Dict, table_name: str) -> List[str]:
        """Generate data processing steps based on analysis"""
        steps = []
        
        # Step 1: Data selection
        steps.append(f"1. SELECT data from {table_name}")
        
        # Step 2: Filtering
        if analysis['time_scope']['filter_needed']:
            steps.append(f"2. FILTER by time scope: {analysis['time_scope']['type']}")
        
        if analysis['team_context']:
            steps.append(f"3. FILTER by team: {analysis['team_context']}")
        
        # Step 3: Aggregation
        if analysis['aggregation_level'] == 'agent_level':
            steps.append("4. AGGREGATE by agent (GROUP BY agent_name)")
        elif analysis['aggregation_level'] == 'team_level':
            steps.append("4. AGGREGATE by team (GROUP BY supervisor)")
        
        # Step 4: Ranking
        if analysis['ranking_needed']['needed']:
            direction = analysis['ranking_needed']['direction']
            steps.append(f"5. RANK results ({direction.upper()} order)")
        
        # Step 5: Limiting
        if analysis['ranking_needed']['limit']:
            steps.append(f"6. LIMIT to top {analysis['ranking_needed']['limit']} results")
        
        return steps
    
    def generate_comprehensive_analysis(self, question: str, table_name: str) -> Dict[str, Any]:
        """
        Generate comprehensive analysis combining data structure and question requirements
        """
        return {
            'question_analysis': self.analyze_question_data_requirements(question, table_name),
            'data_structure_key': table_name,  # Will be used to get cached structure analysis
            'processing_recommendations': self._generate_processing_recommendations(question, table_name),
            'critical_warnings': self._generate_critical_warnings(question, table_name)
        }
    
    def _generate_processing_recommendations(self, question: str, table_name: str) -> List[str]:
        """Generate processing recommendations"""
        recommendations = []
        
        table_info = self.table_structures.get(table_name, {})
        
        # Table-specific recommendations
        if table_name == 'RPT_WOPS_AGENT_PERFORMANCE':
            recommendations.append("📊 Multi-week data: Use GROUP BY ASSIGNEE_NAME for agent-level metrics")
            recommendations.append("📈 Performance metrics: Use AVG() for AHT, QA_SCORE, CSAT")
        
        # Question-specific recommendations
        if 'lowest' in question.lower() and 'aht' in question.lower():
            recommendations.append("⚠️  AHT 'lowest performing': Use ORDER BY AHT_MINUTES DESC (high AHT = poor performance)")
        
        if any(word in question.lower() for word in ['team', 'supervisor']):
            recommendations.append("👥 Team context: Use ASSIGNEE_SUPERVISOR for team filtering")
        
        return recommendations
    
    def _generate_critical_warnings(self, question: str, table_name: str) -> List[str]:
        """Generate critical warnings about data processing"""
        warnings = []
        
        # AHT interpretation warnings
        if 'aht' in question.lower() and 'lowest' in question.lower():
            warnings.append("🚨 CRITICAL: AHT 'lowest performing' means HIGHEST values (more time = worse performance)")
        
        # Aggregation warnings
        if table_name == 'RPT_WOPS_AGENT_PERFORMANCE':
            warnings.append("⚠️  Agent performance table has multiple weeks per agent - aggregation required")
        
        # Team context warnings
        if 'team' in question.lower():
            warnings.append("👥 Team questions require proper supervisor relationship filtering")
        
        return warnings


# Global instance
pre_query_analyzer = PreQueryDataAnalyzer()