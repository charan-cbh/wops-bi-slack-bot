"""
Table Manager - Handles table operations, schema discovery, and table selection for the BI Slack Bot
"""
import os
import json
import time
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from openai import OpenAI
from .valkey_manager import ValkeyManager

class TableManager:
    def __init__(self, openai_client: OpenAI, valkey_manager: ValkeyManager):
        self.client = openai_client
        self.valkey_manager = valkey_manager
        
        # Import Snowflake runner for schema discovery
        try:
            from app.snowflake_runner import run_query
            self.run_query = run_query
            self.SNOWFLAKE_AVAILABLE = True
        except ImportError:
            self.SNOWFLAKE_AVAILABLE = False
            print("⚠️ Snowflake runner not available for schema discovery")
    
    async def find_relevant_tables_from_vector_store(self, question: str, user_id: str, channel_id: str, top_k: int = 8) -> List[str]:
        """Find relevant tables using OpenAI vector store"""
        try:
            # Create a thread for vector search
            thread = self.client.beta.threads.create()
            
            # Add message to thread
            self.client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=f"Find tables relevant to this question: {question}"
            )
            
            # Create and run assistant
            assistant_id = os.getenv("ASSISTANT_ID")
            vector_store_id = os.getenv("OPENAI_VECTOR_STORE_ID")
            
            if not assistant_id or not vector_store_id:
                print("❌ Missing ASSISTANT_ID or VECTOR_STORE_ID")
                return []
            
            run = self.client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=assistant_id,
                instructions=f"""
                You are a table recommendation system. Based on the user's question, suggest the most relevant table names from the available data.
                
                Question: {question}
                
                Return only the table names (without schema prefix) that are most relevant to answering this question.
                Focus on the top {top_k} most relevant tables.
                
                Format your response as a simple list of table names, one per line.
                """
            )
            
            # Wait for completion
            import time
            max_wait = 30
            start_time = time.time()
            
            while run.status in ['queued', 'in_progress']:
                if time.time() - start_time > max_wait:
                    print("⏰ Vector search timeout")
                    break
                time.sleep(1)
                run = self.client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            
            if run.status == 'completed':
                messages = self.client.beta.threads.messages.list(thread_id=thread.id)
                
                for message in messages.data:
                    if message.role == "assistant":
                        content = message.content[0].text.value
                        # Extract table names from the response
                        table_names = []
                        for line in content.split('\n'):
                            line = line.strip()
                            if line and not line.startswith('#') and not line.startswith('Table'):
                                # Clean up the table name
                                table_name = line.replace('- ', '').replace('* ', '').strip()
                                if table_name:
                                    table_names.append(table_name)
                        
                        print(f"🔍 Found {len(table_names)} relevant tables from vector store")
                        return table_names[:top_k]
            
            print("❌ Vector search failed or returned no results")
            return []
            
        except Exception as e:
            print(f"❌ Error finding tables from vector store: {e}")
            return []
    
    async def sample_table_data(self, table_name: str, sample_size: int = 10) -> Dict[str, Any]:
        """Get sample data from a table for context"""
        try:
            # Check cache first
            cache_key = f"{self.valkey_manager.TABLE_SAMPLES_PREFIX}:{table_name}:{sample_size}"
            cached_sample = await self.valkey_manager.safe_valkey_get(cache_key)
            
            if cached_sample:
                print(f"✅ Using cached sample for table: {table_name}")
                return cached_sample
            
            if not self.SNOWFLAKE_AVAILABLE:
                print("❌ Snowflake not available for sampling")
                return {"error": "Snowflake not available"}
            
            # Query for sample data
            sample_query = f"""
            SELECT * FROM {table_name} 
            LIMIT {sample_size}
            """
            
            print(f"🔍 Sampling {sample_size} rows from {table_name}")
            result = await self.run_query(sample_query)
            
            if result.get("success") and result.get("data"):
                # Convert to more readable format
                df = pd.DataFrame(result["data"])
                
                sample_data = {
                    "table_name": table_name,
                    "row_count": len(df),
                    "columns": df.columns.tolist(),
                    "sample_rows": df.head(sample_size).to_dict('records'),
                    "data_types": df.dtypes.astype(str).to_dict(),
                    "success": True
                }
                
                # Cache the sample
                await self.valkey_manager.safe_valkey_set(
                    cache_key, 
                    sample_data, 
                    ex=self.valkey_manager.SCHEMA_CACHE_TTL
                )
                
                print(f"✅ Successfully sampled {table_name}: {len(df)} rows, {len(df.columns)} columns")
                return sample_data
            else:
                error_msg = result.get("error", "Unknown error during sampling")
                print(f"❌ Error sampling {table_name}: {error_msg}")
                return {
                    "table_name": table_name,
                    "error": error_msg,
                    "success": False
                }
                
        except Exception as e:
            error_msg = f"Error sampling table {table_name}: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "table_name": table_name,
                "error": error_msg,
                "success": False
            }
    
    async def select_best_table_using_samples(self, question: str, candidate_tables: List[str], user_id: str, channel_id: str) -> Tuple[str, str]:
        """Select the best table by analyzing sample data"""
        try:
            if not candidate_tables:
                return "", "No candidate tables provided"
            
            # If only one table, return it
            if len(candidate_tables) == 1:
                return candidate_tables[0], f"Only one candidate table: {candidate_tables[0]}"
            
            print(f"🔍 Analyzing {len(candidate_tables)} candidate tables with samples")
            
            # Get samples for all candidate tables
            table_samples = {}
            for table in candidate_tables:
                sample = await self.sample_table_data(table)
                table_samples[table] = sample
            
            # Build analysis prompt
            analysis_prompt = f"""
            Question: {question}
            
            I need to select the best table to answer this question. Here are the candidate tables with their sample data:
            
            """
            
            for table, sample in table_samples.items():
                analysis_prompt += f"\n--- TABLE: {table} ---\n"
                if sample.get("success"):
                    analysis_prompt += f"Columns: {', '.join(sample['columns'])}\n"
                    analysis_prompt += f"Data types: {sample['data_types']}\n"
                    analysis_prompt += f"Sample rows ({sample['row_count']} shown):\n"
                    
                    # Add sample rows
                    for i, row in enumerate(sample['sample_rows'][:3]):  # Show first 3 rows
                        analysis_prompt += f"Row {i+1}: {row}\n"
                else:
                    analysis_prompt += f"Error: {sample.get('error', 'Unknown error')}\n"
            
            analysis_prompt += f"""
            
            Based on the question and the table samples above, which table is most likely to contain the data needed to answer the question?
            
            Consider:
            1. Column names and their relevance to the question
            2. Data types and sample values
            3. Completeness of the data
            
            Respond with just the table name and a brief explanation (max 100 words).
            Format: TABLE_NAME: explanation
            """
            
            # Get recommendation from OpenAI
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data analyst helping select the best table for a query. Be concise and accurate."
                    },
                    {
                        "role": "user",
                        "content": analysis_prompt
                    }
                ],
                max_tokens=200,
                temperature=0.1
            )
            
            result = response.choices[0].message.content.strip()
            
            # Parse the result
            if ':' in result:
                selected_table, reason = result.split(':', 1)
                selected_table = selected_table.strip()
                reason = reason.strip()
                
                # Validate the selected table is in our candidates
                if selected_table in candidate_tables:
                    print(f"✅ Selected table: {selected_table}")
                    print(f"📝 Reason: {reason}")
                    return selected_table, reason
                else:
                    # Fallback to first table if parsing failed
                    print(f"⚠️ Selected table not in candidates, using first: {candidate_tables[0]}")
                    return candidate_tables[0], f"Fallback selection: {result}"
            else:
                # Fallback to first table if parsing failed
                print(f"⚠️ Could not parse table selection, using first: {candidate_tables[0]}")
                return candidate_tables[0], f"Fallback selection: {result}"
                
        except Exception as e:
            print(f"❌ Error selecting table: {e}")
            # Return first table as fallback
            return candidate_tables[0] if candidate_tables else "", f"Error during selection: {str(e)}"
    
    async def get_table_descriptions_from_manifest(self, table_names: List[str], user_id: str, channel_id: str) -> Dict[str, str]:
        """Get table descriptions from manifest (placeholder for now)"""
        try:
            # This would typically read from a manifest file or database
            # For now, return empty descriptions
            descriptions = {}
            for table_name in table_names:
                descriptions[table_name] = f"Table: {table_name}"
            
            return descriptions
            
        except Exception as e:
            print(f"❌ Error getting table descriptions: {e}")
            return {}
    
    async def cache_table_selection(self, question: str, selected_table: str, reason: str, success: bool = True):
        """Cache the table selection for future similar questions"""
        try:
            cache_key = f"{self.valkey_manager.TABLE_SELECTION_PREFIX}:{hash(question.lower())}"
            
            cache_data = {
                "question": question,
                "selected_table": selected_table,
                "reason": reason,
                "success": success,
                "timestamp": time.time()
            }
            
            await self.valkey_manager.safe_valkey_set(
                cache_key, 
                cache_data, 
                ex=self.valkey_manager.TABLE_SELECTION_CACHE_TTL
            )
            
            print(f"✅ Cached table selection: {question} -> {selected_table}")
            
        except Exception as e:
            print(f"❌ Error caching table selection: {e}")
    
    async def get_cached_table_suggestion(self, question: str) -> Optional[str]:
        """Get cached table suggestion for similar questions"""
        try:
            cache_key = f"{self.valkey_manager.TABLE_SELECTION_PREFIX}:{hash(question.lower())}"
            
            cached_data = await self.valkey_manager.safe_valkey_get(cache_key)
            
            if cached_data and cached_data.get("success"):
                print(f"✅ Found cached table suggestion: {cached_data['selected_table']}")
                return cached_data["selected_table"]
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting cached table suggestion: {e}")
            return None
    
    async def discover_table_schema(self, table_name: str) -> dict:
        """Discover table schema using Snowflake INFORMATION_SCHEMA"""
        try:
            # Check cache first
            cache_key = f"{self.valkey_manager.SCHEMA_CACHE_PREFIX}:{table_name}"
            cached_schema = await self.valkey_manager.safe_valkey_get(cache_key)
            
            if cached_schema:
                print(f"✅ Using cached schema for table: {table_name}")
                return cached_schema
            
            if not self.SNOWFLAKE_AVAILABLE:
                print("❌ Snowflake not available for schema discovery")
                return {"error": "Snowflake not available"}
            
            # Query to get table schema
            schema_query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = UPPER('{table_name}')
            ORDER BY ORDINAL_POSITION
            """
            
            print(f"🔍 Discovering schema for table: {table_name}")
            result = await self.run_query(schema_query)
            
            if result.get("success") and result.get("data"):
                # Process the schema data
                columns = []
                for row in result["data"]:
                    columns.append({
                        "name": row["COLUMN_NAME"],
                        "type": row["DATA_TYPE"],
                        "nullable": row["IS_NULLABLE"] == "YES",
                        "default": row["COLUMN_DEFAULT"],
                        "comment": row["COMMENT"]
                    })
                
                schema_info = {
                    "table_name": table_name,
                    "columns": columns,
                    "column_names": [col["name"] for col in columns],
                    "success": True
                }
                
                # Cache the schema
                await self.valkey_manager.safe_valkey_set(
                    cache_key, 
                    schema_info, 
                    ex=self.valkey_manager.SCHEMA_CACHE_TTL
                )
                
                print(f"✅ Successfully discovered schema for {table_name}: {len(columns)} columns")
                return schema_info
            else:
                error_msg = result.get("error", "Unknown error during schema discovery")
                print(f"❌ Error discovering schema for {table_name}: {error_msg}")
                return {
                    "table_name": table_name,
                    "error": error_msg,
                    "success": False
                }
                
        except Exception as e:
            error_msg = f"Error discovering schema for {table_name}: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "table_name": table_name,
                "error": error_msg,
                "success": False
            }
    
    async def rediscover_table_schema(self, table_name: str) -> dict:
        """Force rediscovery of table schema (bypass cache)"""
        try:
            # Remove from cache first
            cache_key = f"{self.valkey_manager.SCHEMA_CACHE_PREFIX}:{table_name}"
            await self.valkey_manager.safe_valkey_delete(cache_key)
            
            # Discover fresh schema
            return await self.discover_table_schema(table_name)
            
        except Exception as e:
            print(f"❌ Error rediscovering schema for {table_name}: {e}")
            return {
                "table_name": table_name,
                "error": str(e),
                "success": False
            }