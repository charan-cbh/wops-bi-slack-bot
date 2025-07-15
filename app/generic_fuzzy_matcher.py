#!/usr/bin/env python3
"""
Generic Fuzzy Matching System for Natural Language to SQL Conversion
Handles all types of entity matching, not just names
"""

import re
from typing import Dict, List, Any, Tuple
from difflib import SequenceMatcher


class GenericFuzzyMatcher:
    """Generic fuzzy matching for natural language queries"""
    
    def __init__(self):
        self.entity_patterns = {
            # Names (people, teams, supervisors)
            'names': [
                r'(?:team|supervisor|manager|agent|lead|user)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
                r'(?:in|from|by|for)\s+team\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
                r'(?:agent|user|person|employee)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
            ],
            
            # Categories and types
            'categories': [
                r'(?:type|category|kind)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
                r'(?:ticket|issue|problem)\s+(?:type|category)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
                r'([a-zA-Z\s]+?)\s+(?:tickets|issues|problems)(?:\s|$|,|\.)',
            ],
            
            # Products and features
            'products': [
                r'(?:product|feature|service)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
                r'([a-zA-Z\s]+?)\s+(?:product|feature|service)(?:\s|$|,|\.)',
            ],
            
            # Statuses and states
            'statuses': [
                r'(?:status|state)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
                r'(?:tickets|issues)\s+(?:with|in)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
            ],
            
            # Channels and sources
            'channels': [
                r'(?:channel|source|via)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
                r'(?:from|through)\s+([a-zA-Z\s]+?)(?:\s|$|,|\.)',
            ],
            
            # Generic quoted entities
            'quoted': [
                r'"([^"]+)"',
                r"'([^']+)'",
            ],
            
            # Generic capitalized entities (likely proper nouns)
            'proper_nouns': [
                r'\b([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*)\b',
            ]
        }
    
    def extract_all_entities(self, question: str) -> Dict[str, List[str]]:
        """Extract all potential entities from the question"""
        entities = {category: [] for category in self.entity_patterns.keys()}
        entities['all_extracted'] = []
        
        question_clean = question.strip()
        
        for category, patterns in self.entity_patterns.items():
            for pattern in patterns:
                matches = re.findall(pattern, question_clean, re.IGNORECASE)
                for match in matches:
                    entity = match.strip()
                    if entity and len(entity) > 1 and not entity.lower() in ['the', 'and', 'or', 'in', 'on', 'at']:
                        entities[category].append(entity)
                        if entity not in entities['all_extracted']:
                            entities['all_extracted'].append(entity)
        
        return entities
    
    def calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using multiple methods"""
        if not str1 or not str2:
            return 0.0
        
        str1_lower = str1.lower().strip()
        str2_lower = str2.lower().strip()
        
        # Exact match
        if str1_lower == str2_lower:
            return 1.0
        
        # Substring match
        if str1_lower in str2_lower or str2_lower in str1_lower:
            return 0.8
        
        # Sequence matcher similarity
        seq_similarity = SequenceMatcher(None, str1_lower, str2_lower).ratio()
        
        # Word overlap similarity
        words1 = set(str1_lower.split())
        words2 = set(str2_lower.split())
        word_overlap = len(words1.intersection(words2)) / max(len(words1), len(words2), 1)
        
        # Return the maximum similarity
        return max(seq_similarity, word_overlap)
    
    def find_matching_columns(self, entities: Dict[str, List[str]], columns: List[str]) -> Dict[str, List[str]]:
        """Find columns that likely contain the extracted entities"""
        column_entity_map = {}
        
        # Column type mapping
        column_types = {
            'names': ['name', 'user', 'agent', 'assignee', 'supervisor', 'manager', 'lead', 'person', 'employee'],
            'categories': ['type', 'category', 'kind', 'classification', 'group', 'class'],
            'products': ['product', 'feature', 'service', 'tool', 'application', 'system'],
            'statuses': ['status', 'state', 'condition', 'phase', 'stage'],
            'channels': ['channel', 'source', 'medium', 'method', 'via', 'contact'],
            'locations': ['location', 'place', 'region', 'area', 'zone', 'facility'],
            'identifiers': ['id', 'code', 'number', 'reference', 'key']
        }
        
        for entity_type, entity_list in entities.items():
            if not entity_list or entity_type == 'all_extracted':
                continue
                
            matching_columns = []
            
            # Find columns that match the entity type
            for col in columns:
                col_lower = col.lower()
                
                # Direct type matching
                if entity_type in column_types:
                    type_keywords = column_types[entity_type]
                    if any(keyword in col_lower for keyword in type_keywords):
                        matching_columns.append(col)
                        continue
                
                # Generic text/name columns for any entity
                if any(keyword in col_lower for keyword in ['name', 'text', 'desc', 'title', 'label']):
                    matching_columns.append(col)
            
            if matching_columns:
                column_entity_map[entity_type] = matching_columns
        
        return column_entity_map
    
    def generate_fuzzy_conditions(self, entities: Dict[str, List[str]], columns: List[str], 
                                table: str, sample_data: List[Dict] = None) -> List[str]:
        """Generate fuzzy matching WHERE conditions"""
        conditions = []
        
        # Get column-entity mapping
        column_entity_map = self.find_matching_columns(entities, columns)
        
        # If we have sample data, do intelligent matching
        if sample_data:
            conditions.extend(self._generate_intelligent_conditions(entities, sample_data, columns))
        
        # Generate basic fuzzy conditions
        for entity_type, entity_list in entities.items():
            if not entity_list or entity_type == 'all_extracted':
                continue
                
            matching_columns = column_entity_map.get(entity_type, [])
            
            for entity in entity_list:
                entity_conditions = []
                
                # Try type-specific columns first
                for col in matching_columns:
                    entity_conditions.append(f"{col} ILIKE '%{entity}%'")
                
                # If no type-specific columns, try common text columns
                if not entity_conditions:
                    common_text_columns = [col for col in columns if any(
                        keyword in col.lower() for keyword in ['name', 'text', 'desc', 'title', 'label', 'value']
                    )]
                    for col in common_text_columns:
                        entity_conditions.append(f"{col} ILIKE '%{entity}%'")
                
                if entity_conditions:
                    conditions.append(f"({' OR '.join(entity_conditions)})")
        
        return conditions
    
    def _generate_intelligent_conditions(self, entities: Dict[str, List[str]], 
                                       sample_data: List[Dict], columns: List[str]) -> List[str]:
        """Generate intelligent conditions based on sample data analysis"""
        conditions = []
        
        if not sample_data:
            return conditions
        
        # Analyze sample data to find the best matches
        for entity_type, entity_list in entities.items():
            if not entity_list or entity_type == 'all_extracted':
                continue
                
            for entity in entity_list:
                best_matches = []
                
                # Check each column in sample data
                for col in columns:
                    if col not in sample_data[0]:
                        continue
                    
                    # Get unique values from this column
                    unique_values = set()
                    for row in sample_data:
                        if row.get(col) and isinstance(row[col], str):
                            unique_values.add(row[col])
                    
                    # Find best matching values
                    for value in unique_values:
                        similarity = self.calculate_similarity(entity, value)
                        if similarity > 0.3:  # Threshold for fuzzy matching
                            best_matches.append((col, value, similarity))
                
                # Sort by similarity and create conditions
                best_matches.sort(key=lambda x: x[2], reverse=True)
                
                if best_matches:
                    # Use the best matches to create more intelligent conditions
                    top_matches = best_matches[:3]  # Top 3 matches
                    match_conditions = []
                    
                    for col, value, similarity in top_matches:
                        if similarity > 0.7:
                            # High similarity - use exact match or strict partial
                            match_conditions.append(f"{col} ILIKE '%{entity}%'")
                        else:
                            # Lower similarity - use broader partial match
                            match_conditions.append(f"{col} ILIKE '%{entity}%'")
                    
                    if match_conditions:
                        conditions.append(f"({' OR '.join(match_conditions)})")
        
        return conditions
    
    def enhance_sql_with_fuzzy_matching(self, original_question: str, table: str, 
                                      schema: Dict, intent: Dict) -> str:
        """Main method to enhance SQL generation with fuzzy matching"""
        
        # Extract entities
        entities = self.extract_all_entities(original_question)
        
        if not entities['all_extracted']:
            return ""
        
        columns = schema.get('columns', [])
        sample_data = schema.get('sample_data', [])
        
        # Generate fuzzy conditions
        fuzzy_conditions = self.generate_fuzzy_conditions(entities, columns, table, sample_data)
        
        if not fuzzy_conditions:
            return ""
        
        # Build the enhancement text
        enhancement = f"""
INTELLIGENT FUZZY MATCHING:
- Detected entities: {entities['all_extracted']}
- Apply these fuzzy matching conditions:
  {' AND '.join(fuzzy_conditions)}

FUZZY MATCHING STRATEGY:
- Use ILIKE for case-insensitive partial matching
- Multiple column matching for entity types
- Handles variations like 'Liam' → 'Liam Johnson'
- Works for names, categories, products, statuses, etc.

ENTITY ANALYSIS:
"""
        
        for entity_type, entity_list in entities.items():
            if entity_list and entity_type != 'all_extracted':
                enhancement += f"- {entity_type}: {entity_list}\n"
        
        return enhancement


# Global instance
generic_fuzzy_matcher = GenericFuzzyMatcher()