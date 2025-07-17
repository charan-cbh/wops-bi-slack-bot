import os
import json
import tiktoken
from typing import Dict, Any, Optional
from openai import OpenAI

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("ASSISTANT_ID")

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)


class ResultProcessor:
    """Handles result summarization and response formatting"""
    
    def __init__(self):
        self.client = client
    
    async def ask_llm_for_sql(self, user_question: str, model_context: str) -> str:
        """Generate SQL using OpenAI with model context"""
        prompt = f"""
        You are a SQL expert. Generate a SQL query to answer this question using the provided table context.
        
        Question: {user_question}
        
        Table Context:
        {model_context}
        
        Generate only the SQL query, no explanations.
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a SQL expert. Generate clean, efficient SQL queries."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"❌ Error generating SQL with OpenAI: {e}")
            return f"-- Error generating SQL: {str(e)}"

    def summarize_results_with_llm(self, user_question: str, result_table: str) -> str:
        """Summarize query results using OpenAI"""
        prompt = f"""
        You are a BI analyst. Summarize these query results to answer the user's question.
        
        User Question: {user_question}
        
        Query Results:
        {result_table}
        
        Provide a clear, concise summary that directly answers the question.
        Use Slack formatting:
        - Use *text* for bold (NOT **text**)
        - Use _text_ for italic
        - Use `text` for code/values
        - Use bullet points when appropriate
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful BI analyst. Provide clear, concise summaries."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=800
            )
            
            summary = response.choices[0].message.content.strip()
            # Ensure proper Slack formatting
            summary = summary.replace("**", "*")
            return summary
            
        except Exception as e:
            print(f"❌ Error summarizing results with OpenAI: {e}")
            newline_count = result_table.count('\n')
            return f"Query completed successfully with {newline_count} results. Raw data:\n{result_table[:500]}..."

    async def summarize_with_assistant(self, user_question: str, result_table: str, user_id: str, 
                                       channel_id: str, assistant_id: str, sql_query: str = None) -> str:
        """Summarize results using OpenAI Assistant API"""
        try:
            from app.conversation_manager import conversation_manager
            thread_id = await conversation_manager.get_or_create_thread(user_id, channel_id)
            if not thread_id:
                # Fallback to direct OpenAI
                return self.summarize_results_with_llm(user_question, result_table)

            instructions = """You are a BI expert summarizing query results for business users.

CRITICAL: You are ONLY summarizing provided results. Do NOT execute any SQL queries or run any tools.

CRITICAL FORMATTING REQUIREMENTS:
- Use *text* for bold (NOT **text** - this is Slack, not Markdown)
- Use _text_ for italic
- Use `code` for values, metrics, and technical terms
- Use bullet points (•) for lists
- Keep responses concise but informative

SUMMARY REQUIREMENTS:
1. Lead with the direct answer to the user's question
2. Highlight key insights and notable findings
3. Include specific numbers and metrics
4. If showing multiple results, organize clearly
5. Point out any surprising or significant patterns

TONE: Professional but conversational, like a helpful data analyst explaining findings to a business stakeholder."""

            # Include SQL query in message for context but make it clear not to execute
            sql_context = ""
            if sql_query:
                sql_context = f"""

**SQL Query Used (for reference only - DO NOT EXECUTE):**
```sql
{sql_query}
```"""
            
            message = f"""Please summarize these query results to answer the user's question:

**User Question:** {user_question}

**Query Results:**
{result_table}{sql_context}

IMPORTANT: Only summarize the provided results. Do not execute any SQL queries. Provide a clear, business-focused summary that directly answers their question."""

            response = await conversation_manager.send_message_and_run(thread_id, message, instructions)
            
            # Ensure proper Slack formatting
            response = response.replace("**", "*")
            
            # SQL query is already included in the response via the message context
            # No need to add it again
            
            return response

        except Exception as e:
            print(f"❌ Error with assistant summarization: {e}")
            # Fallback to direct OpenAI
            return self.summarize_results_with_llm(user_question, result_table)

    async def handle_conversational_question(self, user_question: str, user_id: str, channel_id: str) -> str:
        """Handle conversational/informational questions using Assistant API"""
        try:
            from app.conversation_manager import conversation_manager
            thread_id = await conversation_manager.get_or_create_thread(user_id, channel_id)
            if not thread_id:
                return "I'm having trouble processing your question right now. Please try again."

            # Get conversation context
            context = await conversation_manager.get_conversation_context(user_id, channel_id)

            # Determine question type for tailored instructions
            question_lower = user_question.lower()
            
            # Check for different types of conversational questions
            is_metadata_question = any(indicator in question_lower for indicator in [
                'what is', 'definition', 'meaning', 'what does', 'clarify',
                'metric', 'kpi', 'aht', 'fcr', 'calculate'
            ])
            
            is_followup = any(indicator in question_lower for indicator in [
                'source', 'where', 'how did you', 'explain', 'why',
                'tell me more', 'what about', 'break down'
            ]) and context.get('last_response_type') == 'sql_results'
            
            is_discovery_question = any(indicator in question_lower for indicator in [
                'what data', 'what tables', 'available', 'what can you tell me'
            ])
            
            is_capability_question = any(indicator in question_lower for indicator in [
                'what can you', 'help', 'how do you work', 'capabilities'
            ])
            
            is_business_logic = any(indicator in question_lower for indicator in [
                'how is calculated', 'business logic', 'methodology', 'why do we'
            ])

            # Build appropriate instructions based on question type
            if is_followup:
                instructions = """You are a BI expert explaining query results and data sources.

Use your knowledge of the data warehouse and business processes to explain:
- Where the data comes from (which tables, systems, processes)
- How metrics are calculated and what they represent
- Why certain results might be seen
- What the business implications are

Be specific about data sources when possible. Reference actual table/column names when relevant.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic  
- Use `text` for code/table names"""

            elif is_metadata_question:
                instructions = """You are a BI expert explaining data definitions and business metrics.

Use your knowledge of business processes and data models to explain:
- What metrics mean and how they're calculated
- Business definitions and terminology  
- Data quality considerations
- How different metrics relate to each other
- Explain the difference between handle time vs full resolution time
- Be specific about which metric is currently used based on the data model

Be specific and definitive in your explanations. Reference actual table/column names when relevant.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic  
- Use `text` for code/table names"""

            elif is_discovery_question:
                instructions = """You are a BI expert helping users understand what data is available.

Use your knowledge of the dbt manifest and table schemas to explain:
- What tables and data sources are available (Zendesk tickets, Klaus QA, handle time, etc.)
- What metrics and KPIs can be calculated (ticket volume, AHT, QA scores, FCR, etc.)
- What dimensions and attributes exist (agents, teams, channels, ticket types, etc.)
- How different data elements relate to each other

Be comprehensive but organized in your response. Give practical examples.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names
- Use bullet points with •"""

            elif is_capability_question:
                instructions = """You are a BI assistant explaining your capabilities to help users get the most value.

Explain what you can help with:
- Answering questions about data definitions and metrics
- Running SQL queries to get specific data and analytics
- Explaining business logic and calculation methods
- Providing insights about ticket volume, agent performance, QA scores, etc.
- Finding relevant tables and understanding data structure

Give examples of good questions to ask. Be helpful and encouraging.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names
- Use bullet points with •"""

            elif is_business_logic:
                instructions = """You are a BI expert explaining business logic and calculation methods.

Use your knowledge of business processes and data models to explain:
- How metrics like AHT, FCR, QA scores are calculated
- What business rules apply to ticket handling and agent performance
- Why certain logic is used in the data warehouse
- How different processes work (ticket resolution, quality assurance, etc.)

Be clear about methodology and reasoning. Reference actual calculation logic when possible.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text* for italic
- Use `text` for code/table names"""

            else:
                instructions = """You are a knowledgeable BI assistant helping users understand data and analytics.

Use your expertise to provide helpful, accurate information about:
- Data definitions and terminology
- Available metrics and dimensions  
- How to approach data analysis
- Best practices for business intelligence

Be conversational but authoritative in your response. Act as a helpful data expert.

IMPORTANT - Use Slack formatting:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- Use `text` for code/table names"""

            # Build message with appropriate context
            message_parts = [f"User question: {user_question}"]

            # Add context only for follow-ups
            if is_followup and context:
                if context.get('last_question'):
                    message_parts.append(f"\nPrevious question: {context['last_question']}")
                message_parts.append("\nNote: The user is asking a follow-up question about recent query results.")
                if context.get('last_table_used'):
                    message_parts.append(f"Table used in previous query: {context['last_table_used']}")

            # Add guidance for standalone questions
            else:
                message_parts.append(f"\nThis is a standalone informational question about data/analytics.")
                if is_metadata_question:
                    message_parts.append("Focus on providing clear definitions and explanations based on actual data model.")
                elif is_discovery_question:
                    message_parts.append("Help them understand what data and capabilities are available.")
                elif is_capability_question:
                    message_parts.append("Explain your capabilities and how to best use the BI assistant.")

            message = "\n".join(message_parts)

            response = await conversation_manager.send_message_and_run(thread_id, message, instructions)

            # Additional safety check - convert any remaining markdown bold to Slack format
            response = response.replace("**", "*")

            return response

        except Exception as e:
            print(f"❌ Error handling conversational question: {e}")
            return "I'm having trouble processing your question right now. Please try rephrasing it or contact the analytics team for help."

    async def generate_sql_intelligently(self, question: str, user_id: str, channel_id: str) -> str:
        """Generate SQL using intelligent table selection and context"""
        # This would implement the full intelligent SQL generation
        # For now, returning a placeholder
        return "-- Intelligent SQL generation not fully implemented yet"


# Global result processor instance
result_processor = ResultProcessor()

# Convenience functions for backward compatibility
async def ask_llm_for_sql(user_question: str, model_context: str) -> str:
    """Generate SQL with LLM"""
    return await result_processor.ask_llm_for_sql(user_question, model_context)

def summarize_results_with_llm(user_question: str, result_table: str) -> str:
    """Summarize results with LLM"""
    return result_processor.summarize_results_with_llm(user_question, result_table)

async def summarize_with_assistant(user_question: str, result_table: str, user_id: str, 
                                   channel_id: str, assistant_id: str, sql_query: str = None) -> str:
    """Summarize with assistant"""
    return await result_processor.summarize_with_assistant(user_question, result_table, user_id, channel_id, assistant_id, sql_query)

async def handle_conversational_question(user_question: str, user_id: str, channel_id: str) -> str:
    """Handle conversational question"""
    return await result_processor.handle_conversational_question(user_question, user_id, channel_id)

async def generate_sql_intelligently(question: str, user_id: str, channel_id: str) -> str:
    """Generate SQL intelligently"""
    return await result_processor.generate_sql_intelligently(question, user_id, channel_id)