"""
Question Classifier - Handles question analysis and classification for the BI Slack Bot
"""
import re
import hashlib
from typing import List, Dict, Optional
from openai import OpenAI
from .valkey_manager import ValkeyManager

class QuestionClassifier:
    def __init__(self, openai_client: OpenAI, valkey_manager: ValkeyManager):
        self.client = openai_client
        self.valkey_manager = valkey_manager
        
        # Stop words for phrase extraction
        self.STOP_WORDS = {'the', 'is', 'at', 'which', 'on', 'and', 'a', 'an', 'as', 'are',
                          'was', 'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does',
                          'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must',
                          'shall', 'to', 'of', 'in', 'for', 'with', 'by', 'from', 'about'}
    
    def get_question_hash(self, question: str) -> str:
        """Generate a hash for the question for caching purposes"""
        return hashlib.md5(question.lower().strip().encode()).hexdigest()
    
    def extract_key_phrases(self, question: str) -> List[str]:
        """Extract key phrases from the question for better matching"""
        # Remove punctuation and convert to lowercase
        clean_question = re.sub(r'[^\w\s]', ' ', question.lower())
        words = clean_question.split()
        
        # Filter out stop words
        key_words = [word for word in words if word not in self.STOP_WORDS and len(word) > 2]
        
        # Extract 2-3 word phrases
        phrases = []
        for i in range(len(key_words)):
            # Single words
            phrases.append(key_words[i])
            
            # Two-word phrases
            if i < len(key_words) - 1:
                phrases.append(f"{key_words[i]} {key_words[i+1]}")
            
            # Three-word phrases
            if i < len(key_words) - 2:
                phrases.append(f"{key_words[i]} {key_words[i+1]} {key_words[i+2]}")
        
        return phrases
    
    async def classify_question_with_openai(self, question: str, user_id: str, channel_id: str, context: dict = None) -> str:
        """Use OpenAI to classify the question type and intent"""
        try:
            # System prompt for classification
            system_prompt = """You are a question classifier for a business intelligence system. 
            Analyze the user's question and classify it into one of these categories:
            
            1. DATA_QUERY - Questions asking for specific data, numbers, metrics, or reports
            2. SCHEMA_EXPLORATION - Questions about what data is available, table structure, or column information
            3. COMPARISON - Questions comparing different time periods, segments, or metrics
            4. TREND_ANALYSIS - Questions about trends, changes over time, or patterns
            5. AGGREGATION - Questions asking for sums, averages, counts, or other aggregations
            6. FILTERING - Questions with specific conditions or filters
            7. CONVERSATIONAL - General questions, greetings, or non-data related queries
            
            Respond with just the category name (e.g., "DATA_QUERY")."""
            
            # Add context if available
            context_info = ""
            if context:
                context_info = f"\n\nContext from previous conversation:\n{context.get('summary', '')}"
            
            user_prompt = f"Question: {question}{context_info}"
            
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=50,
                temperature=0.1
            )
            
            classification = response.choices[0].message.content.strip()
            print(f"🔍 Question classified as: {classification}")
            return classification
            
        except Exception as e:
            print(f"❌ Error classifying question with OpenAI: {e}")
            return self.classify_question_type_fallback(question)
    
    def classify_question_type_fallback(self, question: str) -> str:
        """Fallback classification using pattern matching"""
        question_lower = question.lower()
        
        # Data query patterns
        data_patterns = [
            r'\b(show|get|find|what|how much|how many|give me|tell me)\b',
            r'\b(sales|revenue|profit|customers|orders|transactions)\b',
            r'\b(last|this|previous|current)\s+(month|quarter|year|week|day)\b'
        ]
        
        # Schema exploration patterns
        schema_patterns = [
            r'\b(what.*table|what.*column|what.*field|what.*data.*available)\b',
            r'\b(describe|explain|structure|schema|columns|fields)\b',
            r'\b(what.*contain|what.*store|what.*include)\b'
        ]
        
        # Comparison patterns
        comparison_patterns = [
            r'\b(compare|comparison|versus|vs|against|difference|differ)\b',
            r'\b(higher|lower|more|less|better|worse)\s+(than|to)\b',
            r'\b(year over year|month over month|quarter over quarter)\b'
        ]
        
        # Trend analysis patterns
        trend_patterns = [
            r'\b(trend|trending|growth|decline|increase|decrease|change)\b',
            r'\b(over time|by month|by quarter|by year|timeline)\b',
            r'\b(moving average|rolling|pattern|seasonal)\b'
        ]
        
        # Aggregation patterns
        aggregation_patterns = [
            r'\b(total|sum|average|mean|count|maximum|minimum|avg|max|min)\b',
            r'\b(aggregate|group by|grouped|summarize|summary)\b'
        ]
        
        # Filtering patterns
        filtering_patterns = [
            r'\b(where|filter|only|specific|particular|certain)\b',
            r'\b(between|from.*to|greater than|less than|equals|contains)\b',
            r'\b(top|bottom|highest|lowest|first|last)\b'
        ]
        
        # Conversational patterns
        conversational_patterns = [
            r'\b(hello|hi|hey|thanks|thank you|help|how are you)\b',
            r'\b(can you|could you|please|would you)\b',
            r'\b(what.*this|how.*work|explain.*me)\b'
        ]
        
        # Check patterns in order of specificity
        if any(re.search(pattern, question_lower) for pattern in schema_patterns):
            return "SCHEMA_EXPLORATION"
        elif any(re.search(pattern, question_lower) for pattern in comparison_patterns):
            return "COMPARISON"
        elif any(re.search(pattern, question_lower) for pattern in trend_patterns):
            return "TREND_ANALYSIS"
        elif any(re.search(pattern, question_lower) for pattern in aggregation_patterns):
            return "AGGREGATION"
        elif any(re.search(pattern, question_lower) for pattern in filtering_patterns):
            return "FILTERING"
        elif any(re.search(pattern, question_lower) for pattern in conversational_patterns):
            return "CONVERSATIONAL"
        elif any(re.search(pattern, question_lower) for pattern in data_patterns):
            return "DATA_QUERY"
        else:
            return "DATA_QUERY"  # Default to data query
    
    def classify_question_type(self, question: str) -> str:
        """Main classification function that uses fallback method"""
        return self.classify_question_type_fallback(question)
    
    def analyze_question_intent(self, question_lower: str) -> dict:
        """Analyze the question intent and extract key information"""
        intent = {
            'is_comparison': False,
            'is_trend': False,
            'is_aggregation': False,
            'is_filtering': False,
            'time_period': None,
            'comparison_type': None,
            'aggregation_type': None,
            'filter_conditions': [],
            'entities': []
        }
        
        # Time period detection
        time_patterns = {
            'daily': r'\b(today|yesterday|daily|day|days)\b',
            'weekly': r'\b(week|weekly|weeks)\b',
            'monthly': r'\b(month|monthly|months)\b',
            'quarterly': r'\b(quarter|quarterly|quarters|q1|q2|q3|q4)\b',
            'yearly': r'\b(year|yearly|years|annual|annually)\b'
        }
        
        for period, pattern in time_patterns.items():
            if re.search(pattern, question_lower):
                intent['time_period'] = period
                break
        
        # Comparison detection
        comparison_patterns = [
            r'\b(compare|comparison|versus|vs|against)\b',
            r'\b(year over year|month over month|quarter over quarter)\b',
            r'\b(higher|lower|more|less|better|worse)\s+(than|to)\b'
        ]
        
        if any(re.search(pattern, question_lower) for pattern in comparison_patterns):
            intent['is_comparison'] = True
            if re.search(r'\byear over year\b', question_lower):
                intent['comparison_type'] = 'year_over_year'
            elif re.search(r'\bmonth over month\b', question_lower):
                intent['comparison_type'] = 'month_over_month'
            elif re.search(r'\bquarter over quarter\b', question_lower):
                intent['comparison_type'] = 'quarter_over_quarter'
        
        # Trend analysis detection
        trend_patterns = [
            r'\b(trend|trending|growth|decline|increase|decrease|change)\b',
            r'\b(over time|by month|by quarter|by year|timeline)\b'
        ]
        
        if any(re.search(pattern, question_lower) for pattern in trend_patterns):
            intent['is_trend'] = True
        
        # Aggregation detection
        aggregation_patterns = {
            'sum': r'\b(total|sum|add up|altogether)\b',
            'avg': r'\b(average|mean|avg)\b',
            'count': r'\b(count|number of|how many)\b',
            'max': r'\b(maximum|max|highest|largest|most)\b',
            'min': r'\b(minimum|min|lowest|smallest|least)\b'
        }
        
        for agg_type, pattern in aggregation_patterns.items():
            if re.search(pattern, question_lower):
                intent['is_aggregation'] = True
                intent['aggregation_type'] = agg_type
                break
        
        # Filtering detection
        filter_patterns = [
            r'\b(where|filter|only|specific|particular|certain)\b',
            r'\b(between|from.*to|greater than|less than|equals|contains)\b',
            r'\b(top|bottom|highest|lowest|first|last)\s+\d+\b'
        ]
        
        if any(re.search(pattern, question_lower) for pattern in filter_patterns):
            intent['is_filtering'] = True
        
        # Entity extraction (simple patterns)
        entity_patterns = {
            'sales': r'\b(sales|revenue|income|earnings)\b',
            'customers': r'\b(customers|clients|users|buyers)\b',
            'orders': r'\b(orders|purchases|transactions|bookings)\b',
            'products': r'\b(products|items|goods|merchandise)\b',
            'regions': r'\b(region|country|state|city|location)\b',
            'departments': r'\b(department|division|team|group)\b'
        }
        
        for entity, pattern in entity_patterns.items():
            if re.search(pattern, question_lower):
                intent['entities'].append(entity)
        
        return intent