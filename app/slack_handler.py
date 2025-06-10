import hmac
import hashlib
import time
import os
import re
import json
import traceback
import asyncio
from fastapi import Request, HTTPException
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

from app.llm_prompter import (
    ask_llm_for_sql,
    summarize_results_with_llm,
    summarize_with_assistant,
    update_sql_cache_with_results,
    get_cache_stats,
    get_learning_insights,
    handle_question,
    test_question_classification,
    generate_sql_intelligently,
    clear_sql_cache,
    clear_schema_cache,
    clear_thread_cache,
    clear_conversation_cache,
    clear_table_selection_cache,
    rediscover_table_schema,
    update_conversation_context,
    get_conversation_context,
    test_conversation_flow,
    debug_table_selection,
    sample_table_data,
    find_relevant_tables_from_vector_store,
    select_best_table_using_samples,
    cache_table_selection,
)
from app.manifest_index import search_relevant_models
from app.snowflake_runner import run_query, format_result_for_slack

load_dotenv()

SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
USE_ASSISTANT_API = os.getenv("USE_ASSISTANT_API", "false").lower() == "true"
ASSISTANT_ID = os.getenv("ASSISTANT_ID", "")

slack_client = WebClient(token=SLACK_BOT_TOKEN)

recent_event_ids = set()


async def handle_slack_event(request: Request):
    """Main function to handle Slack events"""
    body = await request.body()
    headers = request.headers
    body_str = body.decode("utf-8")
    payload = json.loads(body_str)

    # Handle URL verification
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge")}

    # Slack signature verification
    timestamp = headers.get("x-slack-request-timestamp")
    slack_signature = headers.get("x-slack-signature")

    if not timestamp or not slack_signature:
        raise HTTPException(status_code=400, detail="Missing Slack headers")

    if abs(time.time() - int(timestamp)) > 60 * 5:
        raise HTTPException(status_code=400, detail="Request too old")

    sig_basestring = f"v0:{timestamp}:{body_str}"
    my_signature = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode(),
        sig_basestring.encode(),
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(my_signature, slack_signature):
        raise HTTPException(status_code=403, detail="Invalid request signature")

    event = payload.get("event", {})
    event_type = event.get("type")
    event_id = payload.get("event_id")

    if event_type == "app_mention":
        if event_id in recent_event_ids:
            print(f"🔁 Skipping duplicate event: {event_id}")
            return {"message": "Duplicate event skipped"}
        recent_event_ids.add(event_id)

        # Process the mention asynchronously
        await process_app_mention(event)

    return {"message": "Slack event received"}


async def process_app_mention(event):
    """Process app mention event with smart routing"""
    user_question = event.get("text", "")
    channel_id = event.get("channel")
    user_id = event.get("user")

    try:
        # Clean the question
        clean_question = re.sub(r"<@[^>]+>", "", user_question).strip()
        print(f"🔍 Received: {clean_question}")

        # Check for special debug commands
        if clean_question.lower().startswith("debug"):
            await handle_debug_command(clean_question, channel_id, user_id)
            return

        # Send thinking indicator
        try:
            slack_client.chat_postMessage(
                channel=channel_id,
                text="🤔 Analyzing your question..."
            )
        except Exception as e:
            print(f"⚠️ Could not send thinking indicator: {e}")

        # Use smart routing with assistant API
        if USE_ASSISTANT_API and ASSISTANT_ID:
            print(f"🤖 Using Assistant API with intelligent table selection")

            # Smart routing - determines if SQL is needed or conversational response
            response, response_type = await handle_question(clean_question, user_id, channel_id, ASSISTANT_ID)

            print(f"📊 Response type: {response_type}")

            if response_type == 'sql':
                # Execute SQL and get results
                await execute_sql_and_respond(
                    clean_question, response, channel_id, user_id
                )
            elif response_type == 'error':
                # Handle error response
                await send_slack_message(channel_id, f"❌ Error generating response: {response}")
            else:
                # Send conversational response directly
                await send_slack_message(channel_id, response)
                # Update context for conversational responses
                await update_conversation_context(user_id, channel_id, clean_question, response, 'conversational')

        else:
            # Fallback to embedding search
            await handle_with_embeddings(clean_question, channel_id, user_id)

    except TypeError as te:
        print(f"❌ Type Error (likely JSON serialization): {str(te)}")
        print(f"❌ Full error details: {type(te).__name__}: {te}")
        traceback.print_exc()

        await send_slack_message(
            channel_id,
            f"❌ **Data processing error:**\n```{str(te)}```\n\nThis usually happens with timestamp data. Try:\n1. `@bot debug clear` to clear caches\n2. Ask your question again"
        )
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print(f"❌ Error type: {type(e).__name__}")
        print(f"❌ Full traceback:")
        traceback.print_exc()

        await send_slack_message(
            channel_id,
            f"❌ **Error processing your request:**\n```{str(e)}```\n\nPlease try rephrasing your question."
        )


async def handle_debug_command(clean_question: str, channel_id: str, user_id: str):
    """Handle debug commands"""
    if not (USE_ASSISTANT_API and ASSISTANT_ID):
        await send_slack_message(channel_id, "Debug only works with Assistant API enabled")
        return

    debug_query = clean_question.replace("debug", "").strip()

    if not debug_query or debug_query.lower() == "help":
        # Show available debug commands
        debug_result = """🔧 **Available Debug Commands:**

**Cache & Stats:**
• `debug cache` or `debug stats` - Show cache statistics
• `debug learning` or `debug patterns` - Show learning insights
• `debug clear` - Clear all caches
• `debug clear selection` - Clear table selection cache only

**Table Discovery:**
• `debug find QUESTION` - Find relevant tables for a question
• `debug sample TABLE_NAME` - Sample 5 rows from a table
• `debug analyze QUESTION` - Full table selection analysis
• `debug selection QUESTION` - Debug table selection process
• `debug rediscover TABLE_NAME` - Force rediscover table schema

**Testing:**
• `debug test` - Test question classification
• `debug flow` - Test conversation flow
• `debug context` - Show current conversation context

**General:**
• `debug QUERY` - Search for tables/columns related to query"""

    elif debug_query.lower() in ["cache", "stats"]:
        # Show cache statistics
        stats = await get_cache_stats()
        learning = await get_learning_insights()
        debug_result = f"📊 **Cache Statistics:**\n```{json.dumps(stats, indent=2)}```\n\n🧠 **Learning Insights:**\n```{learning}```"

    elif debug_query.lower() in ["learning", "patterns"]:
        # Show learning patterns
        learning = await get_learning_insights()
        debug_result = f"🧠 **What I've Learned:**\n```{learning}```"

    elif debug_query.lower() in ["test", "classification"]:
        # Test question classification
        test_question_classification()
        debug_result = "🧪 **Classification test complete** - check server logs for results"

    elif debug_query.lower() in ["flow", "conversation flow"]:
        # Test conversation flow
        try:
            await test_conversation_flow()
            debug_result = "🧪 **Conversation flow test complete** - check server logs for results"
        except Exception as e:
            debug_result = f"❌ **Flow test error:** {str(e)}"

    elif debug_query.lower() in ["clear cache", "clear"]:
        # Clear all caches
        await clear_sql_cache()
        await clear_schema_cache()
        await clear_thread_cache()
        await clear_conversation_cache()
        await clear_table_selection_cache()
        debug_result = "🧹 **All caches cleared!**"

    elif debug_query.lower() == "clear selection":
        # Clear only table selection cache
        await clear_table_selection_cache()
        debug_result = "🧹 **Table selection cache cleared!**"

    elif debug_query.lower() in ["context", "conversation"]:
        # Show current conversation context
        context = await get_conversation_context(user_id, channel_id)
        if context:
            debug_result = f"💬 **Current Conversation Context:**\n"
            debug_result += f"• Last question: {context.get('last_question', 'None')}\n"
            debug_result += f"• Last response type: {context.get('last_response_type', 'None')}\n"
            debug_result += f"• Context age: {int(time.time() - context.get('timestamp', 0))} seconds\n"
            if context.get('last_response'):
                debug_result += f"• Last response preview: {context['last_response'][:200]}..."
        else:
            debug_result = "💬 **No active conversation context**"

    elif debug_query.lower().startswith("find"):
        # Find relevant tables for a question
        question = debug_query.replace("find", "").strip()
        if question:
            tables = await find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k=5)
            if tables:
                debug_result = f"📊 **Relevant Tables for:** '{question}'\n\n"
                for i, table in enumerate(tables, 1):
                    debug_result += f"{i}. {table}\n"
            else:
                debug_result = "❌ **No relevant tables found**"
        else:
            debug_result = "❌ **Usage:** `debug find YOUR QUESTION HERE`"

    elif debug_query.lower().startswith("sample"):
        # Sample data from a table
        table_name = debug_query.replace("sample", "").strip()
        if table_name:
            sample = await sample_table_data(table_name, sample_size=5)
            if sample.get('error'):
                debug_result = f"❌ **Error sampling {table_name}:** {sample['error']}"
            else:
                debug_result = f"📊 **Sample from {table_name}:**\n"
                debug_result += f"Total rows: {sample.get('total_rows', 'Unknown')}\n"
                debug_result += f"Columns ({len(sample.get('columns', []))}): {', '.join(sample.get('columns', [])[:15])}\n\n"

                if sample.get('sample_data'):
                    debug_result += "**Sample row:**\n```json\n"
                    sample_row = sample['sample_data'][0]
                    # Show first 10 columns
                    shown_cols = dict(list(sample_row.items())[:10])
                    debug_result += json.dumps(shown_cols, indent=2)
                    debug_result += "\n```"
        else:
            debug_result = "❌ **Usage:** `debug sample TABLE_NAME`"

    elif debug_query.lower().startswith("analyze"):
        # Full analysis of table selection for a question
        question = debug_query.replace("analyze", "").strip()
        if question:
            debug_result = f"🔍 **Analyzing:** '{question}'\n\n"

            # Find candidate tables
            debug_result += "**1. Finding candidate tables...**\n"
            candidates = await find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k=4)

            if candidates:
                for table in candidates:
                    debug_result += f"  • {table}\n"

                # Select best table
                debug_result += "\n**2. Selecting best table...**\n"
                selected_table, reason = await select_best_table_using_samples(question, candidates, user_id,
                                                                               channel_id)
                debug_result += f"  Selected: {selected_table}\n"
                debug_result += f"  Reason: {reason}\n"
            else:
                debug_result += "  ❌ No candidates found\n"
        else:
            debug_result = "❌ **Usage:** `debug analyze YOUR QUESTION HERE`"

    elif debug_query.lower().startswith("selection"):
        # Debug table selection process
        question = debug_query.replace("selection", "").strip()
        if question:
            selection_debug = await debug_table_selection(question, user_id, channel_id)
            debug_result = f"📊 **Table Selection Debug:**\n```{selection_debug}```"
        else:
            debug_result = "❌ **Usage:** `debug selection YOUR QUESTION HERE`"

    elif debug_query.lower().startswith("rediscover"):
        # Rediscover a specific table schema
        table_name = debug_query.replace("rediscover", "").strip()
        if table_name:
            schema = await rediscover_table_schema(table_name)
            if schema.get('error'):
                debug_result = f"❌ **Failed to rediscover schema for {table_name}:**\n{schema['error']}"
            else:
                debug_result = f"✅ **Rediscovered schema for {table_name}:**\n"
                debug_result += f"- Columns: {len(schema['columns'])}\n"
                debug_result += f"- Sample columns: {', '.join(schema['columns'][:15])}"
                if len(schema['columns']) > 15:
                    debug_result += f"\n- ... and {len(schema['columns']) - 15} more columns"
        else:
            debug_result = "❌ **Usage:** `debug rediscover TABLE_NAME`"

    else:
        # Default: search for tables/columns
        # This uses the vector store to search for relevant information
        debug_result = f"🔍 **Searching for:** {debug_query}\n\n"
        tables = await find_relevant_tables_from_vector_store(debug_query, user_id, channel_id, top_k=3)
        if tables:
            debug_result += "**Found relevant tables:**\n"
            for table in tables:
                debug_result += f"  • {table}\n"
        else:
            debug_result += "No relevant tables found in vector search"

    await send_slack_message(channel_id, f"🔍 **Debug Results:**\n{debug_result}")


async def execute_sql_and_respond(clean_question: str, sql: str, channel_id: str, user_id: str):
    """Execute SQL query and send results"""
    print("⚡ Executing query...")
    await send_slack_message(channel_id, "⚡ Executing query...")

    print(f"\n{'=' * 60}")
    print(f"🧠 SQL Query to execute:")
    print(f"{sql}")
    print(f"{'=' * 60}\n")

    # Check if SQL generation failed
    if sql.strip().lower().startswith("i don't have enough") or sql.startswith("-- Error:") or sql.startswith("⚠️"):
        await send_slack_message(channel_id, f"❌ {sql}")
        return

    # Execute the SQL query
    df = run_query(sql)

    if isinstance(df, str):
        # Error message from query execution
        print(f"❌ Query execution error: {df}")

        # Check if it's a column name error
        if "invalid identifier" in df.lower():
            # Extract the problematic column name
            match = re.search(r"invalid identifier '([^']+)'", df, re.IGNORECASE)
            if match:
                bad_column = match.group(1)
                print(f"❌ Column '{bad_column}' does not exist in the table")

                # Provide helpful suggestions
                suggestions_msg = f"❌ Query error: Column '{bad_column}' does not exist in the table.\n\n"
                suggestions_msg += "**Suggestions:**\n"
                suggestions_msg += f"1. Try `@bot debug analyze {clean_question}` to see table analysis\n"
                suggestions_msg += f"2. Try `@bot debug find {clean_question}` to find relevant tables\n"
                suggestions_msg += "3. Rephrase your question with more specific details\n\n"
                suggestions_msg += f"Full error: {df}"

                result_message = suggestions_msg
            else:
                result_message = f"❌ Query error: {df}"
        else:
            result_message = f"❌ Query error: {df}"

        # Update cache with poor results (no table info)
        await update_sql_cache_with_results(clean_question, sql, 0)
    else:
        # Success - process results
        result_count = len(df) if hasattr(df, '__len__') else 0
        print(f"✅ Query successful - returned {result_count} rows, {len(df.columns)} columns")

        # Extract table from SQL for learning
        selected_table = None
        if 'FROM' in sql.upper():
            sql_upper = sql.upper()
            from_idx = sql_upper.find('FROM')
            if from_idx != -1:
                # Extract table name (handle multi-line SQL)
                after_from = sql[from_idx + 4:].strip()
                # Get first word (table name) - handle newlines and multiple spaces
                selected_table = re.split(r'[\s\n]+', after_from)[0]

        # Update cache with actual results
        await update_sql_cache_with_results(clean_question, sql, result_count, selected_table)

        # Summarize results
        if USE_ASSISTANT_API and ASSISTANT_ID:
            result_message = await summarize_with_assistant(
                clean_question,
                format_result_for_slack(df),
                user_id,
                channel_id,
                ASSISTANT_ID
            )
        else:
            result_message = summarize_results_with_llm(
                clean_question,
                format_result_for_slack(df)
            )

        # Update conversation context to indicate SQL results were returned
        await update_conversation_context(user_id, channel_id, clean_question, result_message, 'sql_results')

    await send_slack_message(channel_id, result_message)


async def handle_with_embeddings(clean_question: str, channel_id: str, user_id: str):
    """Fallback handler using embeddings"""
    print("📚 Using local embeddings with FAISS")

    relevant_models = search_relevant_models(clean_question)
    print(f"📊 Found {len(relevant_models)} relevant models")

    model_context = relevant_models[0]["context"] if relevant_models else ""
    sql = ask_llm_for_sql(clean_question, model_context)

    await execute_sql_and_respond(clean_question, sql, channel_id, user_id)


async def send_slack_message(channel_id: str, message: str):
    """Send message to Slack with error handling"""
    try:
        # Ensure message isn't too long for Slack (4000 char limit)
        if len(message) > 3900:
            message = message[:3900] + "\n\n... (truncated due to length)"

        slack_client.chat_postMessage(channel=channel_id, text=message)
        print(f"📤 Sent message to channel {channel_id}")
    except SlackApiError as e:
        print(f"❌ Slack API error: {e.response['error']}")
        # Try sending a simpler error message
        try:
            slack_client.chat_postMessage(
                channel=channel_id,
                text="❌ Sorry, I encountered an error sending the response. Please try again."
            )
        except Exception as fallback_error:
            print(f"❌ Failed to send fallback message: {fallback_error}")
    except Exception as e:
        print(f"❌ Unexpected error sending message: {e}")


# Helper function to clear assistant thread cache (useful for testing)
async def clear_assistant_cache():
    """Clear assistant thread cache"""
    await clear_thread_cache()
    print("🧹 Assistant thread cache cleared")


# Health check endpoint helper
def get_status():
    """Get current bot status"""
    return {
        "assistant_enabled": USE_ASSISTANT_API,
        "assistant_id": ASSISTANT_ID if ASSISTANT_ID else "Not configured",
        "slack_configured": bool(SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET),
        "smart_routing_enabled": True,
        "intelligent_table_selection": True,
        "vector_store_search": True,
        "learning_enabled": True,
        "conversational_context_enabled": True,
    }