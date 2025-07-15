#!/usr/bin/env python3
"""
Context-Aware SQL Enhancement System
Fixes issues like "Ricardo Birck's team" being incorrectly mapped to GROUP_NAME instead of SUPERVISOR
"""

import re
from typing import Dict, List, Any, Tuple


class ContextAwareSQLEnhancer:
    """Intelligently enhance SQL generation based on linguistic context"""
    
    def __init__(self):
        pass
    
    def extract_supervisors_from_possessive(self, question: str) -> List[str]:
        """Extract supervisor names from possessive constructs like 'X's team'"""
        supervisors = []
        
        # Pattern for possessive team references
        patterns = [
            r"([A-Z][a-z]+\s+[A-Z][a-z]+)'s\s+team",
            r"work\s+in\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'s\s+team",
            r"agents?\s+(?:in|under|for)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'s\s+team",
            r"([A-Z][a-z]+\s+[A-Z][a-z]+)'s\s+(?:group|department|staff)",
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            for match in matches:
                if match and match not in supervisors:
                    supervisors.append(match)
        
        return supervisors
    
    def extract_direct_agents(self, question: str) -> List[str]:
        """Extract direct agent references"""
        agents = []
        
        patterns = [
            r"agent\s+([A-Z][a-z]+\s+[A-Z][a-z]+)",
            r"user\s+([A-Z][a-z]+\s+[A-Z][a-z]+)",
            r"person\s+([A-Z][a-z]+\s+[A-Z][a-z]+)",
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            for match in matches:
                if match and match not in agents:
                    agents.append(match)
        
        return agents
    
    def extract_team_names(self, question: str) -> List[str]:
        """Extract direct team name references (not possessive)"""
        teams = []
        
        # Only match when NOT possessive (no 's before team)
        patterns = [
            r"team\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            r"group\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            r"department\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
        ]
        
        # Skip if possessive
        if "'s team" in question.lower():
            return teams
        
        for pattern in patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            for match in matches:
                if match and match not in teams:
                    teams.append(match)
        
        return teams
    
    def extract_categories(self, question: str) -> List[str]:
        """Extract category/issue type references"""
        categories = []
        
        patterns = [
            r"([a-z]+(?:\s+[a-z]+)*)\s+(?:issues?|problems?|tickets?)",
            r"(?:issues?|problems?|tickets?)\s+(?:about|regarding|for)\s+([a-z]+(?:\s+[a-z]+)*)",
            r"(?:type|category)\s+([a-z]+(?:\s+[a-z]+)*)",
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            for match in matches:
                if match and len(match) > 2 and match not in categories:
                    categories.append(match)
        
        return categories
    
    def analyze_question_context(self, question: str) -> Dict[str, Any]:
        """Analyze the linguistic context of the question"""
        context = {
            'is_count_query': any(phrase in question.lower() for phrase in ['how many', 'count', 'number of']),
            'is_possessive_team': "'s team" in question.lower(),
            'is_direct_team': 'team' in question.lower() and "'s team" not in question.lower(),
            'is_supervisor_query': any(word in question.lower() for word in ['supervisor', 'manager', 'reports to']),
            'is_agent_query': 'agent' in question.lower(),
            'question_intent': self._determine_intent(question)
        }
        
        return context
    
    def _determine_intent(self, question: str) -> str:
        """Determine the primary intent of the question"""
        question_lower = question.lower()
        
        if any(phrase in question_lower for phrase in ['how many', 'count']):
            return 'count'
        elif any(phrase in question_lower for phrase in ['show', 'list', 'display']):
            return 'list'
        elif any(phrase in question_lower for phrase in ['performance', 'metrics', 'statistics']):
            return 'performance'
        elif any(phrase in question_lower for phrase in ['when', 'time', 'date']):
            return 'temporal'
        else:
            return 'general'
    
    def generate_intelligent_sql_conditions(self, question: str, schema: Dict) -> Tuple[List[str], Dict[str, Any]]:
        """Generate intelligent SQL conditions based on context analysis"""
        
        # Extract entities
        supervisors = self.extract_supervisors_from_possessive(question)
        agents = self.extract_direct_agents(question)
        teams = self.extract_team_names(question)
        categories = self.extract_categories(question)
        
        # Analyze context
        context = self.analyze_question_context(question)
        
        # Map columns
        columns = schema.get('columns', [])
        column_map = self._map_columns_intelligently(columns)
        
        conditions = []
        
        # Generate supervisor conditions (highest priority for possessive)
        if supervisors and context['is_possessive_team']:
            supervisor_conditions = []
            for supervisor in supervisors:
                for col in column_map['supervisor_columns']:
                    supervisor_conditions.append(f"{col} ILIKE '%{supervisor}%'")
            
            if supervisor_conditions:
                conditions.append(f"({' OR '.join(supervisor_conditions)})")
        
        # Generate agent conditions
        if agents:
            agent_conditions = []
            for agent in agents:
                for col in column_map['agent_columns']:
                    agent_conditions.append(f"{col} ILIKE '%{agent}%'")
            
            if agent_conditions:
                conditions.append(f"({' OR '.join(agent_conditions)})")
        
        # Generate team conditions (only if NOT possessive)
        if teams and not context['is_possessive_team']:
            team_conditions = []
            for team in teams:
                for col in column_map['team_columns']:
                    team_conditions.append(f"{col} ILIKE '%{team}%'")
            
            if team_conditions:
                conditions.append(f"({' OR '.join(team_conditions)})")
        
        # Generate category conditions
        if categories:
            category_conditions = []
            for category in categories:
                for col in column_map['category_columns']:
                    category_conditions.append(f"{col} ILIKE '%{category}%'")
            
            if category_conditions:
                conditions.append(f"({' OR '.join(category_conditions)})")
        
        analysis = {
            'supervisors': supervisors,
            'agents': agents,
            'teams': teams,
            'categories': categories,
            'context': context,
            'column_mapping': column_map
        }
        
        return conditions, analysis
    
    def _map_columns_intelligently(self, columns: List[str]) -> Dict[str, List[str]]:
        """Map columns to their most likely entity types"""
        mapping = {
            'supervisor_columns': [],
            'agent_columns': [],
            'team_columns': [],
            'category_columns': [],
            'channel_columns': [],
            'status_columns': [],
        }
        
        for col in columns:
            col_lower = col.lower()
            
            # Supervisor columns (highest priority for possessive context)
            if any(term in col_lower for term in ['supervisor', 'manager', 'lead', 'boss']):
                mapping['supervisor_columns'].append(col)
            
            # Agent/user columns
            elif any(term in col_lower for term in ['agent', 'user', 'assignee', 'person', 'employee']):
                mapping['agent_columns'].append(col)
            
            # Team/group columns
            elif any(term in col_lower for term in ['team', 'group', 'department', 'unit']):
                mapping['team_columns'].append(col)
            
            # Category columns
            elif any(term in col_lower for term in ['category', 'type', 'classification', 'kind']):
                mapping['category_columns'].append(col)
            
            # Channel columns
            elif any(term in col_lower for term in ['channel', 'source', 'medium', 'contact']):
                mapping['channel_columns'].append(col)
            
            # Status columns
            elif any(term in col_lower for term in ['status', 'state', 'condition', 'phase']):
                mapping['status_columns'].append(col)
        
        return mapping
    
    def enhance_sql_instructions(self, question: str, table: str, schema: Dict, intent: Dict) -> str:
        """Generate enhanced SQL instructions with context awareness"""
        
        conditions, analysis = self.generate_intelligent_sql_conditions(question, schema)
        
        if not conditions:
            return ""
        
        context = analysis['context']
        
        enhancement = f"""
CONTEXT-AWARE SQL ENHANCEMENT:

LINGUISTIC ANALYSIS:
- Question: {question}
- Intent: {context['question_intent']}
- Count Query: {context['is_count_query']}
- Possessive Team Reference: {context['is_possessive_team']}
- Direct Team Reference: {context['is_direct_team']}

EXTRACTED ENTITIES:
- Supervisors (from possessive): {analysis['supervisors']}
- Agents (direct reference): {analysis['agents']}
- Teams (non-possessive): {analysis['teams']}
- Categories: {analysis['categories']}

INTELLIGENT COLUMN MAPPING:
- Supervisor columns: {analysis['column_mapping']['supervisor_columns']}
- Agent columns: {analysis['column_mapping']['agent_columns']}
- Team columns: {analysis['column_mapping']['team_columns']}

CONTEXT-AWARE CONDITIONS:
{chr(10).join(f"- {condition}" for condition in conditions)}

CRITICAL LOGIC:
- When question contains "X's team", X is a SUPERVISOR, not a team name
- Use SUPERVISOR/MANAGER columns for possessive references
- Use TEAM/GROUP columns only for direct team name mentions
- Prioritize context over generic pattern matching
- Apply COUNT(DISTINCT) for agent counting to avoid duplicates

EXPECTED SQL PATTERN:
SELECT COUNT(DISTINCT {analysis['column_mapping']['agent_columns'][0] if analysis['column_mapping']['agent_columns'] else 'agent_column'})
FROM {table}
WHERE {' AND '.join(conditions) if conditions else 'conditions'}"""

        return enhancement


# Global instance
context_aware_enhancer = ContextAwareSQLEnhancer()