#!/usr/bin/env python3
"""
Intelligent Entity Extractor with Context Awareness
Understands roles, relationships, and context to generate accurate SQL
"""

import re
from typing import Dict, List, Any, Tuple
from difflib import SequenceMatcher


class IntelligentEntityExtractor:
    """Context-aware entity extraction for accurate SQL generation"""
    
    def __init__(self):
        # Enhanced patterns with context awareness
        self.patterns = {
            # Possessive patterns - X's team/group/department means X is likely a supervisor
            'possessive_supervisor': [
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'s\s+team\b",
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'s\s+group\b",
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'s\s+department\b",
                r"work\s+in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'s\s+team",
                r"agents?\s+(?:in|under|for)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'s\s+team",
            ],
            
            # Direct supervisor references
            'direct_supervisor': [
                r"supervisor\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"manager\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"lead(?:er)?\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"reports?\s+to\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"under\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
            ],
            
            # Agent/person references  
            'agent_person': [
                r"agent\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"(?:for|by)\s+agent\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"user\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"person\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
            ],
            
            # Team/group references (when NOT possessive)
            'team_group': [
                r"team\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"group\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
                r"department\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)",
            ],
            
            # Categories and types
            'categories': [
                r"([a-zA-Z\s]+?)\s+(?:issues?|problems?|tickets?|requests?)",
                r"(?:type|category|kind)\s+([a-zA-Z\s]+)",
                r"(?:issues?|problems?|tickets?)\s+(?:about|regarding|for)\s+([a-zA-Z\s]+)",
            ],
            
            # Channels and sources
            'channels': [
                r"(?:via|from|through)\s+([a-zA-Z\s]+)",
                r"(?:channel|source)\s+([a-zA-Z\s]+)",
                r"([a-zA-Z\s]+)\s+channel",
            ],
            
            # Status and states
            'status': [
                r"status\s+(?:is\s+)?[\"']?([a-zA-Z\s]+)[\"']?",
                r"(?:with|in)\s+status\s+[\"']?([a-zA-Z\s]+)[\"']?",
                r"[\"']([a-zA-Z\s]+)[\"']\s+status",
            ],
            
            # Products and features
            'products': [
                r"product\s+([a-zA-Z\s]+)",
                r"feature\s+([a-zA-Z\s]+)",
                r"([a-zA-Z\s]+)\s+(?:product|feature|service)",
            ],
            
            # Quoted entities
            'quoted': [
                r'"([^"]+)"',
                r"'([^']+)'",
            ]
        }
    
    def extract_entities_with_context(self, question: str) -> Dict[str, Any]:
        """Extract entities with their contextual roles"""
        question_clean = question.strip()
        
        extracted = {
            'supervisors': [],
            'agents': [],
            'teams': [],
            'categories': [],
            'channels': [],
            'statuses': [],
            'products': [],
            'quoted': [],
            'context_analysis': {}
        }
        
        # Extract each entity type with context
        for entity_type, patterns in self.patterns.items():
            for pattern in patterns:
                matches = re.findall(pattern, question_clean, re.IGNORECASE)
                for match in matches:
                    entity = match.strip()
                    if entity and len(entity) > 1:
                        self._categorize_entity(entity, entity_type, extracted)
        
        # Context analysis
        extracted['context_analysis'] = self._analyze_context(question_clean, extracted)
        
        return extracted
    
    def _categorize_entity(self, entity: str, pattern_type: str, extracted: Dict):
        """Categorize entity based on pattern type"""
        entity_clean = self._clean_entity(entity)
        
        if pattern_type in ['possessive_supervisor', 'direct_supervisor']:
            if entity_clean not in extracted['supervisors']:
                extracted['supervisors'].append(entity_clean)
        elif pattern_type == 'agent_person':
            if entity_clean not in extracted['agents']:
                extracted['agents'].append(entity_clean)
        elif pattern_type == 'team_group':
            if entity_clean not in extracted['teams']:
                extracted['teams'].append(entity_clean)
        elif pattern_type == 'categories':
            if entity_clean not in extracted['categories']:
                extracted['categories'].append(entity_clean)
        elif pattern_type == 'channels':
            if entity_clean not in extracted['channels']:
                extracted['channels'].append(entity_clean)
        elif pattern_type == 'status':
            if entity_clean not in extracted['statuses']:
                extracted['statuses'].append(entity_clean)
        elif pattern_type == 'products':
            if entity_clean not in extracted['products']:
                extracted['products'].append(entity_clean)
        elif pattern_type == 'quoted':
            if entity_clean not in extracted['quoted']:
                extracted['quoted'].append(entity_clean)
    
    def _clean_entity(self, entity: str) -> str:
        """Clean and normalize entity text"""
        # Remove common stop words and clean up
        stop_words = ['the', 'and', 'or', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']
        words = entity.split()
        cleaned_words = [w for w in words if w.lower() not in stop_words and len(w) > 1]
        return ' '.join(cleaned_words)
    
    def _analyze_context(self, question: str, entities: Dict) -> Dict:
        """Analyze question context for smarter column mapping"""
        context = {
            'is_count_query': False,
            'is_supervisor_context': False,
            'is_agent_context': False,
            'is_team_context': False,
            'suggested_priority': []
        }
        
        question_lower = question.lower()
        
        # Detect count queries
        if any(word in question_lower for word in ['how many', 'count', 'number of']):
            context['is_count_query'] = True
        
        # Detect supervisor context
        if any(phrase in question_lower for phrase in ["'s team", 'supervisor', 'manager', 'reports to', 'under']):
            context['is_supervisor_context'] = True
            context['suggested_priority'].append('supervisor_columns')
        
        # Detect agent context
        if any(word in question_lower for word in ['agent', 'person', 'user', 'employee']):
            context['is_agent_context'] = True
            context['suggested_priority'].append('agent_columns')
        
        # Detect team context (but not possessive)
        if 'team' in question_lower and "'s team" not in question_lower:
            context['is_team_context'] = True
            context['suggested_priority'].append('team_columns')
        
        return context
    
    def generate_intelligent_conditions(self, entities: Dict, schema: Dict) -> List[str]:
        """Generate intelligent WHERE conditions based on context"""
        conditions = []
        columns = schema.get('columns', [])
        sample_data = schema.get('sample_data', [])
        context = entities.get('context_analysis', {})
        
        # Map column types
        column_map = self._map_columns_by_type(columns)
        
        # Generate supervisor conditions (highest priority for possessive context)
        if entities['supervisors'] and context.get('is_supervisor_context'):
            supervisor_conditions = []
            for supervisor in entities['supervisors']:
                for col in column_map.get('supervisor_columns', []):
                    supervisor_conditions.append(f"{col} ILIKE '%{supervisor}%'")
            
            if supervisor_conditions:
                conditions.append(f"({' OR '.join(supervisor_conditions)})")
        
        # Generate agent conditions
        if entities['agents']:
            agent_conditions = []
            for agent in entities['agents']:
                for col in column_map.get('agent_columns', []):
                    agent_conditions.append(f"{col} ILIKE '%{agent}%'")
            
            if agent_conditions:
                conditions.append(f"({' OR '.join(agent_conditions)})")
        
        # Generate team conditions (only if not supervisor context)
        if entities['teams'] and not context.get('is_supervisor_context'):
            team_conditions = []
            for team in entities['teams']:
                for col in column_map.get('team_columns', []):
                    team_conditions.append(f"{col} ILIKE '%{team}%'")
            
            if team_conditions:
                conditions.append(f"({' OR '.join(team_conditions)})")
        
        # Generate other entity conditions
        for entity_type in ['categories', 'channels', 'statuses', 'products', 'quoted']:
            if entities[entity_type]:
                type_conditions = []
                column_key = f"{entity_type[:-1]}_columns" if entity_type.endswith('s') else f"{entity_type}_columns"
                
                for entity in entities[entity_type]:
                    for col in column_map.get(column_key, []):
                        type_conditions.append(f"{col} ILIKE '%{entity}%'")
                
                if type_conditions:
                    conditions.append(f"({' OR '.join(type_conditions)})")
        
        return conditions
    
    def _map_columns_by_type(self, columns: List[str]) -> Dict[str, List[str]]:
        """Map columns to their likely entity types"""
        column_map = {
            'supervisor_columns': [],
            'agent_columns': [],
            'team_columns': [],
            'category_columns': [],
            'channel_columns': [],
            'status_columns': [],
            'product_columns': [],
            'quote_columns': []
        }
        
        for col in columns:
            col_lower = col.lower()
            
            # Supervisor/manager columns
            if any(term in col_lower for term in ['supervisor', 'manager', 'lead', 'boss']):
                column_map['supervisor_columns'].append(col)
            
            # Agent/user columns
            elif any(term in col_lower for term in ['agent', 'user', 'assignee', 'person', 'employee']):
                column_map['agent_columns'].append(col)
            
            # Team/group columns
            elif any(term in col_lower for term in ['team', 'group', 'department', 'unit']):
                column_map['team_columns'].append(col)
            
            # Category columns
            elif any(term in col_lower for term in ['category', 'type', 'classification', 'kind']):
                column_map['category_columns'].append(col)
            
            # Channel columns
            elif any(term in col_lower for term in ['channel', 'source', 'medium', 'contact']):
                column_map['channel_columns'].append(col)
            
            # Status columns
            elif any(term in col_lower for term in ['status', 'state', 'condition', 'phase']):
                column_map['status_columns'].append(col)
            
            # Product columns
            elif any(term in col_lower for term in ['product', 'feature', 'service', 'tool']):
                column_map['product_columns'].append(col)
            
            # Generic text columns (fallback)
            elif any(term in col_lower for term in ['name', 'text', 'desc', 'title', 'label']):
                # Add to multiple categories as fallback
                column_map['quote_columns'].append(col)
        
        return column_map
    
    def get_intelligent_enhancement(self, question: str, table: str, schema: Dict, intent: Dict) -> str:
        """Generate intelligent SQL enhancement text"""
        entities = self.extract_entities_with_context(question)
        conditions = self.generate_intelligent_conditions(entities, schema)
        
        if not conditions:
            return ""
        
        context = entities['context_analysis']
        
        enhancement = f"""
INTELLIGENT CONTEXT-AWARE ENTITY MATCHING:

DETECTED ENTITIES:
- Supervisors: {entities['supervisors']}
- Agents: {entities['agents']}
- Teams: {entities['teams']}
- Categories: {entities['categories']}
- Channels: {entities['channels']}
- Statuses: {entities['statuses']}
- Products: {entities['products']}

CONTEXT ANALYSIS:
- Count Query: {context.get('is_count_query', False)}
- Supervisor Context: {context.get('is_supervisor_context', False)}
- Agent Context: {context.get('is_agent_context', False)}
- Team Context: {context.get('is_team_context', False)}

INTELLIGENT CONDITIONS TO APPLY:
{chr(10).join(f"- {condition}" for condition in conditions)}

CONTEXT-AWARE LOGIC:
- When someone mentions "X's team", X is likely a supervisor/manager
- Use SUPERVISOR/MANAGER columns for possessive references
- Use TEAM/GROUP columns only for direct team name references
- Prioritize context over generic pattern matching
- Apply fuzzy matching with ILIKE for partial name matches"""
        
        return enhancement


# Global instance
intelligent_entity_extractor = IntelligentEntityExtractor()