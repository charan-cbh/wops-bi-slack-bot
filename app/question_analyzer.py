import hashlib
import re
import json
from typing import List, Dict, Any, Optional
from app.pattern_matcher import PatternMatcher, PatternBasedQueryHelper

# Stop words for phrase extraction
STOP_WORDS = {'the', 'is', 'at', 'which', 'on', 'and', 'a', 'an', 'as', 'are',
              'was', 'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does',
              'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must',
              'shall', 'to', 'of', 'in', 'for', 'with', 'by', 'from', 'about'}

# Import the pattern matcher
try:
    pattern_matcher = PatternMatcher()
    pattern_query_builder = PatternBasedQueryHelper()
    PATTERNS_AVAILABLE = True
except ImportError:
    PATTERNS_AVAILABLE = False
    print("⚠️ Pattern matcher not available - using standard flow")


class QuestionAnalyzer:
    """Handles question analysis, classification, and intent detection"""
    
    def __init__(self):
        self.pattern_matcher = pattern_matcher if PATTERNS_AVAILABLE else None
        self.pattern_query_builder = pattern_query_builder if PATTERNS_AVAILABLE else None
    
    def get_question_hash(self, question: str) -> str:
        """Generate hash for question to use as cache key"""
        normalized = ' '.join(question.lower().strip().split())
        return hashlib.md5(normalized.encode()).hexdigest()[:12]
    
    def extract_key_phrases(self, question: str) -> List[str]:
        """Extract key phrases from question for learning"""
        words = question.lower().split()
        phrases = []

        # Single words (excluding stop words)
        important_words = [w for w in words if len(w) > 3 and w not in STOP_WORDS]
        phrases.extend(important_words)

        # Bigrams for important patterns
        for i in range(len(words) - 1):
            bigram = f"{words[i]} {words[i + 1]}"
            # Add all bigrams that don't contain only stop words
            if not all(word in STOP_WORDS for word in [words[i], words[i + 1]]):
                phrases.append(bigram)

        # Trigrams for complex patterns
        for i in range(len(words) - 2):
            trigram = f"{words[i]} {words[i + 1]} {words[i + 2]}"
            # Add trigrams that contain at least one important word
            if any(word not in STOP_WORDS and len(word) > 3 for word in [words[i], words[i + 1], words[i + 2]]):
                phrases.append(trigram)

        return list(set(phrases))  # Remove duplicates

    def classify_question_type_fallback(self, question: str) -> str:
        """Simple fallback classification if OpenAI fails"""
        question_lower = question.lower()

        # Strong indicators for SQL queries
        strong_sql_indicators = [
            'how many', 'count', 'show me', 'list', 'who has', 'who is',
            'what is the', 'compare', 'vs', 'versus', 'ranking', 'rank',
            'improvement', 'performance', 'metrics', 'agents', 'tickets',
            'highest', 'lowest', 'best', 'worst', 'most', 'least',
            'average', 'total', 'sum', 'breakdown', 'analysis'
        ]

        # Strong indicators for conversational
        strong_conversational_indicators = [
            'what does', 'definition of', 'meaning of', 'explain',
            'what is the source', 'how do you calculate', 'methodology',
            'what tables', 'what data do you have', 'capabilities'
        ]

        # Check for strong SQL indicators first
        if any(indicator in question_lower for indicator in strong_sql_indicators):
            return 'sql_required'

        # Check for strong conversational indicators
        if any(indicator in question_lower for indicator in strong_conversational_indicators):
            return 'conversational'

        # For ambiguous cases, default to sql_required
        # Better to try SQL and fail than miss a data request
        return 'sql_required'

    def classify_question_type(self, question: str) -> str:
        """Enhanced classification for data queries vs conversational questions"""
        question_lower = question.lower()

        # FIRST: Check for follow-up indicators about previous results
        followup_about_results = [
            'what is the source', 'where does this data', 'where is this from',
            'how did you get', 'what table', 'which database',
            'explain this', 'what does this mean', 'why is it',
            'can you clarify', 'tell me more about this',
            'break this down', 'what are these', 'who are these'
        ]

        if any(indicator in question_lower for indicator in followup_about_results):
            return 'conversational'

        # NEW: Check for metadata/definition questions (EXPANDED)
        metadata_questions = [
            'what metric is used', 'which metric', 'what is the metric',
            'is the metric', 'metric being used', 'what does aht mean',
            'definition of', 'what is aht', 'what does', 'meaning of',
            'switched to', 'changed to', 'still using', 'still the metric',
            'clarify', 'clarification', 'qq for clarity', 'quick question',
            'what column', 'which column', 'column used', 'field used',
            'handle time vs', 'resolution time vs', 'difference between',
            'currently being used', 'currently using', 'what represents',
            'how is calculated', 'calculation method', 'methodology',
            'what is fcr', 'what does fcr mean', 'fcr definition',
            'what kpis', 'available kpis', 'kpi definition',
            'business logic', 'business rules', 'how do you calculate',
            'what makes', 'how is determined', 'logic behind', 'why do we'
        ]

        if any(indicator in question_lower for indicator in metadata_questions):
            return 'conversational'

        # NEW: Check for data discovery questions
        discovery_questions = [
            'what data', 'what tables', 'what columns', 'available data',
            'what information', 'what metrics', 'data sources',
            'what can you tell me', 'what do you know', 'data available'
        ]

        if any(indicator in question_lower for indicator in discovery_questions):
            return 'conversational'

        # NEW: Check for capability questions
        capability_questions = [
            'what can you', 'help', 'capabilities', 'questions can',
            'how do you work', 'how to use', 'what commands',
            'what types of questions', 'how to ask'
        ]

        if any(indicator in question_lower for indicator in capability_questions):
            return 'conversational'

        # Check for data query indicators - EXPANDED LIST
        data_indicators = [
            'how many', 'count', 'show me', 'list', 'find',
            'highest', 'lowest', 'average', 'total',
            'tickets', 'agents', 'reviews', 'performance',
            'reply time', 'response time', 'resolution',
            'which ticket type', 'what ticket type', 'driving',
            'volume', 'trend', 'compare', 'by group', 'by channel',
            'contact driver', 'handling time', 'aht', 'kpi', 'kpis',
            'determine', 'metrics', 'performance metrics',
            'created', 'today', 'yesterday', 'this week', 'last week',
            'this month', 'last month', 'zendesk', 'chat', 'email',
            'what are the', 'give me', 'provide', 'fetch',
            'calculate', 'sum', 'aggregate', 'breakdown'
        ]

        # Check for general follow-up indicators
        general_followup_indicators = [
            'why', 'what does that mean', 'explain that',
            'can you elaborate', 'tell me more',
            'what about', 'how about'
        ]

        # Check context - short questions after data results are often follow-ups
        word_count = len(question.split())

        # Special handling for "volume" questions - these are ALWAYS data queries
        if 'volume' in question_lower and any(
                word in question_lower for word in ['ticket', 'tickets', 'agent', 'chat', 'email']):
            return 'sql_required'

        # Check if it's asking for specific data (but not about definitions/metadata)
        if any(indicator in question_lower for indicator in data_indicators):
            # Double check it's not asking about the data source or definitions
            if any(meta in question_lower for meta in ['source', 'definition', 'meaning', 'what does', 'clarify', 'switched', 'changed']):
                return 'conversational'
            return 'sql_required'

        if any(indicator in question_lower for indicator in general_followup_indicators):
            # But if it also contains data indicators, it might be a data query
            if any(indicator in question_lower for indicator in data_indicators):
                return 'sql_required'
            return 'conversational'
        else:
            # For ambiguous cases, check if it contains entities or time references
            entities = ['ticket', 'agent', 'customer', 'zendesk', 'chat', 'email', 'messaging']
            time_refs = ['today', 'yesterday', 'week', 'month', 'year', 'date']

            # If asking about definitions/clarifications of entities, it's conversational
            if any(meta in question_lower for meta in ['what is', 'definition', 'meaning', 'clarify', 'switched', 'changed', 'still using']):
                return 'conversational'

            if any(entity in question_lower for entity in entities) or any(time in question_lower for time in time_refs):
                return 'sql_required'

            # Very short questions are likely follow-ups
            if word_count <= 4:
                return 'conversational'

            return 'sql_required'  # Default to trying SQL

    def analyze_question_intent(self, question_lower: str) -> dict:
        """Analyze question intent and extract key information"""
        intent = {
            'needs_filtering': False,
            'is_aggregate': False,
            'is_comparison': False,
            'is_ranking': False,
            'is_trend': False,
            'has_time_filter': False,
            'is_personal': False,
            'personal_context': None,
            'entities': [],
            'metrics': [],
            'time_references': [],
            'filters': []
        }
        
        # Personal context detection
        personal_indicators = {
            'my': ['my team', 'my agents', 'my performance', 'my kpis', 'my metrics', 'my tickets', 'my stats'],
            'our': ['our team', 'our performance', 'our tickets', 'our metrics'],
            'i': ['i solved', 'i handled', 'i worked', 'i closed', 'did i', 'how did i'],
            'me': ['show me', 'tell me', 'give me']
        }
        
        for pronoun, patterns in personal_indicators.items():
            for pattern in patterns:
                if pattern in question_lower:
                    intent['is_personal'] = True
                    if pronoun in ['my', 'our']:
                        intent['personal_context'] = 'team_or_personal'
                    elif pronoun in ['i', 'me']:
                        intent['personal_context'] = 'individual'
                    break
            if intent['is_personal']:
                break
        
        # Additional personal context patterns
        if not intent['is_personal']:
            direct_personal_patterns = [
                'how many members are there in my team',
                'show me my kpis',
                'how many tickets did i solve',
                'how many tickets did my team solve',
                'my team performance',
                'my individual metrics'
            ]
            
            for pattern in direct_personal_patterns:
                if any(part in question_lower for part in pattern.split() if len(part) > 2):
                    # Check if multiple parts match
                    matching_parts = sum(1 for part in pattern.split() if part in question_lower)
                    if matching_parts >= 2:  # At least 2 key words match
                        intent['is_personal'] = True
                        if 'my team' in pattern or 'team' in pattern:
                            intent['personal_context'] = 'team_or_personal'
                        else:
                            intent['personal_context'] = 'individual'
                        break

        # Entity detection
        entity_patterns = {
            'agents': ['agent', 'agents', 'representative', 'rep', 'staff'],
            'tickets': ['ticket', 'tickets', 'case', 'cases', 'issue', 'issues'],
            'customers': ['customer', 'customers', 'client', 'clients'],
            'teams': ['team', 'teams', 'group', 'groups', 'department']
        }

        for entity, patterns in entity_patterns.items():
            if any(pattern in question_lower for pattern in patterns):
                intent['entities'].append(entity)

        # Metric detection
        metric_patterns = {
            'count': ['how many', 'count', 'number of', 'total'],
            'average': ['average', 'avg', 'mean'],
            'time': ['time', 'duration', 'aht', 'handle time', 'resolution time'],
            'performance': ['performance', 'score', 'rating', 'quality'],
            'volume': ['volume', 'throughput', 'capacity']
        }

        for metric, patterns in metric_patterns.items():
            if any(pattern in question_lower for pattern in patterns):
                intent['metrics'].append(metric)

        # Aggregate detection
        aggregate_keywords = ['total', 'sum', 'average', 'count', 'breakdown', 'by team', 'by agent']
        intent['is_aggregate'] = any(keyword in question_lower for keyword in aggregate_keywords)

        # Comparison detection
        comparison_keywords = ['vs', 'versus', 'compare', 'between', 'difference']
        intent['is_comparison'] = any(keyword in question_lower for keyword in comparison_keywords)

        # Ranking detection
        ranking_keywords = ['highest', 'lowest', 'best', 'worst', 'top', 'bottom', 'rank']
        intent['is_ranking'] = any(keyword in question_lower for keyword in ranking_keywords)

        # Time reference detection
        time_patterns = ['today', 'yesterday', 'this week', 'last week', 'this month', 'last month']
        for pattern in time_patterns:
            if pattern in question_lower:
                intent['time_references'].append(pattern)
                intent['has_time_filter'] = True

        # Filter detection
        filter_keywords = ['where', 'for', 'with', 'having', 'only', 'exclude']
        intent['needs_filtering'] = any(keyword in question_lower for keyword in filter_keywords)

        return intent

    def test_question_classification(self):
        """Test question classification with various examples"""
        test_questions = [
            "How many tickets were created today?",
            "What does AHT mean?",
            "Show me the top 10 agents by performance",
            "What is the source of this data?",
            "Compare agent performance between teams",
            "What tables do you have access to?",
            "Who has the highest QA scores?",
            "Explain these results"
        ]

        print("🧪 Testing question classification:")
        for question in test_questions:
            classification = self.classify_question_type(question)
            fallback = self.classify_question_type_fallback(question)
            intent = self.analyze_question_intent(question.lower())
            
            print(f"Question: {question}")
            print(f"  Classification: {classification}")
            print(f"  Fallback: {fallback}")
            print(f"  Intent: {intent}")
            print()


# Global analyzer instance
question_analyzer = QuestionAnalyzer()

# Convenience functions for backward compatibility
def get_question_hash(question: str) -> str:
    """Generate hash for question"""
    return question_analyzer.get_question_hash(question)

def extract_key_phrases(question: str) -> List[str]:
    """Extract key phrases"""
    return question_analyzer.extract_key_phrases(question)

def classify_question_type_fallback(question: str) -> str:
    """Fallback classification"""
    return question_analyzer.classify_question_type_fallback(question)

def classify_question_type(question: str) -> str:
    """Enhanced classification"""
    return question_analyzer.classify_question_type(question)

def analyze_question_intent(question_lower: str) -> dict:
    """Analyze question intent"""
    return question_analyzer.analyze_question_intent(question_lower)

def test_question_classification():
    """Test classification"""
    return question_analyzer.test_question_classification()