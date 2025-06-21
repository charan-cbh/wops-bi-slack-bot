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
    clear_feedback_cache,
    rediscover_table_schema,
    update_conversation_context,
    get_conversation_context,
    test_conversation_flow,
    debug_table_selection,
    sample_table_data,
    find_relevant_tables_from_vector_store,
    select_best_table_using_samples,
    cache_table_selection,
    get_table_descriptions_from_manifest,
    record_feedback,
    update_conversation_context_with_sql
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
# Store message timestamps for feedback tracking
message_to_question_map = {}  # {channel_ts: {question, sql, table}}
processed_reactions = set()  # Track processed reactions to avoid duplicates


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

    # Handle app mentions
    if event_type == "app_mention":
        if event_id in recent_event_ids:
            print(f"🔁 Skipping duplicate event: {event_id}")
            return {"message": "Duplicate event skipped"}
        recent_event_ids.add(event_id)

        # Process the mention asynchronously
        asyncio.create_task(process_app_mention(event))

    # Handle reactions (emoji feedback)
    elif event_type == "reaction_added":
        asyncio.create_task(process_reaction_added(event))

    elif event_type == "reaction_removed":
        asyncio.create_task(process_reaction_removed(event))

    return {"message": "Slack event received"}


async def process_reaction_added(event):
    """Process reaction added event for feedback"""
    try:
        reaction = event.get("reaction", "")
        user = event.get("user", "")
        item = event.get("item", {})

        if item.get("type") != "message":
            return

        channel = item.get("channel")
        ts = item.get("ts")

        # Create unique key for this reaction
        reaction_key = f"{channel}_{ts}_{user}_{reaction}"

        # Check if we've already processed this reaction
        if reaction_key in processed_reactions:
            print(f"🔄 Already processed reaction from {user}, skipping")
            return

        # Check if this is a bot message we're tracking
        channel_ts = f"{channel}_{ts}"
        if channel_ts not in message_to_question_map:
            return

        message_data = message_to_question_map[channel_ts]

        # Process feedback based on emoji
        if reaction in ["white_check_mark", "heavy_check_mark", "thumbsup", "100", "fire", "rocket"]:
            # Positive feedback
            print(f"✅ Received positive feedback from {user}")
            processed_reactions.add(reaction_key)

            await record_feedback(
                message_data['question'],
                message_data['sql'],
                message_data['table'],
                'positive'
            )

            # Send acknowledgment only once
            await send_slack_message(
                channel,
                f"Thanks for the feedback! I'll remember this worked well for similar questions. ✅",
                thread_ts=ts
            )

        elif reaction in ["x", "heavy_multiplication_x", "thumbsdown", "disappointed", "confused"]:
            # Negative feedback
            print(f"❌ Received negative feedback from {user}")
            processed_reactions.add(reaction_key)

            await record_feedback(
                message_data['question'],
                message_data['sql'],
                message_data['table'],
                'negative'
            )

            # Send acknowledgment and ask for clarification
            await send_slack_message(
                channel,
                f"Thanks for the feedback. I'll avoid this approach for similar questions. ❌\n\nCould you tell me what was wrong? This helps me improve:\n• Wrong table selected?\n• Incorrect results?\n• Missing data?\n• Other issue?",
                thread_ts=ts
            )

        # Clean up old processed reactions (older than 24 hours)
        # This is done periodically to prevent memory growth
        if len(processed_reactions) > 1000:
            processed_reactions.clear()

    except Exception as e:
        print(f"❌ Error processing reaction: {e}")
        traceback.print_exc()


async def process_reaction_removed(event):
    """Process reaction removed event - could implement feedback reversal if needed"""
    # For now, we'll just log it
    reaction = event.get("reaction", "")
    user = event.get("user", "")
    print(f"🔄 User {user} removed reaction {reaction}")


async def process_app_mention(event):
    """Process app mention event with smart routing"""
    user_question = event.get("text", "")
    channel_id = event.get("channel")
    user_id = event.get("user")
    ts = event.get("ts")  # Message timestamp

    try:
        # Clean the question
        clean_question = re.sub(r"<@[^>]+>", "", user_question).strip()
        print(f"🔍 Received: {clean_question}")

        # Check for special debug commands
        if clean_question.lower().startswith("debug"):
            await handle_debug_command(clean_question, channel_id, user_id)
            return

        # Send thinking indicator
        thinking_msg = None
        try:
            response = slack_client.chat_postMessage(
                channel=channel_id,
                text="🤔 Analyzing your question..."
            )
            thinking_msg = response.get("ts")
        except Exception as e:
            print(f"⚠️ Could not send thinking indicator: {e}")

        # Use smart routing with assistant API
        if USE_ASSISTANT_API and ASSISTANT_ID:
            print(f"🤖 Using Assistant API with intelligent table selection")

            # Smart routing - determines if SQL is needed or conversational response
            response, response_type = await handle_question(clean_question, user_id, channel_id, ASSISTANT_ID)

            print(f"📊 Response type: {response_type}")

            if response_type == 'sql':
                # Delete thinking message before executing
                if thinking_msg:
                    try:
                        slack_client.chat_delete(channel=channel_id, ts=thinking_msg)
                    except:
                        pass

                # Execute SQL and get results
                await execute_sql_and_respond(
                    clean_question, response, channel_id, user_id, ts
                )
            elif response_type == 'error':
                # Handle error response
                if thinking_msg:
                    try:
                        slack_client.chat_delete(channel=channel_id, ts=thinking_msg)
                    except:
                        pass

                # Provide helpful error message
                error_msg = f"❌ {response}\n\n"
                error_msg += "**Suggestions:**\n"
                error_msg += f"• Try `@bot debug analyze {clean_question}` to see table analysis\n"
                error_msg += f"• Try `@bot debug find {clean_question}` to find relevant tables\n"
                error_msg += "• Rephrase your question with more specific details\n"
                error_msg += "• Mention specific metrics (e.g., 'ticket count', 'agent performance', 'AHT')"

                await send_slack_message(channel_id, error_msg, include_feedback_hint=False)
            else:
                # Handle conversational response
                if thinking_msg:
                    try:
                        slack_client.chat_delete(channel=channel_id, ts=thinking_msg)
                    except:
                        pass

                # CRITICAL FIX: Check if response contains SQL
                sql_detected = False
                sql_query = None

                # Check for SQL in code block
                if "```sql" in response:
                    try:
                        sql_query = response.split("```sql")[1].split("```")[0].strip()
                        sql_detected = True
                    except:
                        pass

                # Check for raw SQL
                if not sql_query and response.strip().upper().startswith("SELECT"):
                    lines = response.split('\n')
                    sql_lines = []
                    for line in lines:
                        sql_lines.append(line)
                        if line.strip().endswith(';'):
                            break
                    if sql_lines:
                        sql_query = '\n'.join(sql_lines).strip()
                        sql_detected = True

                if sql_detected and sql_query:
                    # Execute SQL instead of showing it
                    print(f"📊 Detected SQL in conversational response - executing it")
                    await execute_sql_and_respond(clean_question, sql_query, channel_id, user_id, ts)

                    # Store SQL in context for potential "share results" request
                    await update_conversation_context_with_sql(
                        user_id, channel_id, clean_question, response, 'sql_shown', None, sql_query
                    )
                else:
                    # Normal conversational response
                    await send_slack_message(channel_id, response, include_feedback_hint=False)
                    await update_conversation_context(user_id, channel_id, clean_question, response, 'conversational')

        else:
            # Fallback to embedding search
            if thinking_msg:
                try:
                    slack_client.chat_delete(channel=channel_id, ts=thinking_msg)
                except:
                    pass
            await handle_with_embeddings(clean_question, channel_id, user_id)

    except TypeError as te:
        print(f"❌ Type Error (likely JSON serialization): {str(te)}")
        print(f"❌ Full error details: {type(te).__name__}: {te}")
        traceback.print_exc()

        await send_slack_message(
            channel_id,
            f"❌ **Data processing error:**\n```{str(te)}```\n\nThis usually happens with timestamp data. Try:\n1. `@bot debug clear` to clear caches\n2. Ask your question again",
            include_feedback_hint=False
        )
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print(f"❌ Error type: {type(e).__name__}")
        print(f"❌ Full traceback:")
        traceback.print_exc()

        await send_slack_message(
            channel_id,
            f"❌ **Error processing your request:**\n```{str(e)}```\n\nPlease try rephrasing your question or try `@bot debug analyze YOUR QUESTION` to see what's happening.",
            include_feedback_hint=False
        )


async def handle_debug_command(clean_question: str, channel_id: str, user_id: str):
    """Handle debug commands"""
    if not (USE_ASSISTANT_API and ASSISTANT_ID):
        await send_slack_message(channel_id, "Debug only works with Assistant API enabled", include_feedback_hint=False)
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
• `debug clear feedback` - Clear feedback cache

**Table Discovery:**
• `debug find QUESTION` - Find relevant tables for a question
• `debug describe TABLE1 TABLE2` - Get table descriptions from dbt manifest
• `debug sample TABLE_NAME` - Sample 10 rows from a table
• `debug analyze QUESTION` - Full table selection analysis
• `debug selection QUESTION` - Debug table selection process
• `debug rediscover TABLE_NAME` - Force rediscover table schema

**Testing:**
• `debug test` - Test question classification
• `debug flow` - Test conversation flow
• `debug context` - Show current conversation context

**General:**
• `debug QUERY` - Search for tables/columns related to query

**Feedback:**
React with ✅ or ❌ to any bot response to provide feedback!"""

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
        await clear_feedback_cache()
        debug_result = "🧹 **All caches cleared!**"

    elif debug_query.lower() == "clear selection":
        # Clear only table selection cache
        await clear_table_selection_cache()
        debug_result = "🧹 **Table selection cache cleared!**"

    elif debug_query.lower() == "clear feedback":
        # Clear only feedback cache
        await clear_feedback_cache()
        debug_result = "🧹 **Feedback cache cleared!**"

    elif debug_query.lower() in ["context", "conversation"]:
        # Show current conversation context
        context = await get_conversation_context(user_id, channel_id)
        if context:
            debug_result = f"💬 **Current Conversation Context:**\n"
            debug_result += f"• Last question: {context.get('last_question', 'None')}\n"
            debug_result += f"• Last response type: {context.get('last_response_type', 'None')}\n"
            debug_result += f"• Last table used: {context.get('last_table_used', 'None')}\n"
            debug_result += f"• Context age: {int(time.time() - context.get('timestamp', 0))} seconds\n"
            if context.get('last_response'):
                debug_result += f"• Last response preview: {context['last_response'][:200]}..."
        else:
            debug_result = "💬 **No active conversation context**"

    elif debug_query.lower().startswith("describe"):
        # Get descriptions for tables
        tables_str = debug_query.replace("describe", "").strip()
        if tables_str:
            # Split by comma or space
            tables = [t.strip() for t in re.split(r'[,\s]+', tables_str) if t.strip()]
            descriptions = await get_table_descriptions_from_manifest(tables, user_id, channel_id)

            if descriptions:
                debug_result = "📊 **Table Descriptions from dbt manifest:**\n\n"
                for table, desc in descriptions.items():
                    debug_result += f"**{table}**\n{desc}\n\n"
            else:
                debug_result = "❌ **No descriptions found for these tables**"
        else:
            debug_result = "❌ **Usage:** `debug describe TABLE1 TABLE2` or `debug describe TABLE1,TABLE2`"

    elif debug_query.lower().startswith("find"):
        # Find relevant tables for a question
        question = debug_query.replace("find", "").strip()
        if question:
            tables = await find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k=5)
            if tables:
                debug_result = f"📊 **Relevant Tables for:** '{question}'\n\n"
                descriptions = await get_table_descriptions_from_manifest(tables, user_id, channel_id)

                for i, table in enumerate(tables, 1):
                    debug_result += f"{i}. **{table}**\n"
                    if table in descriptions:
                        desc = descriptions[table]
                        debug_result += f"   {desc[:200]}{'...' if len(desc) > 200 else ''}\n\n"
            else:
                debug_result = "❌ **No relevant tables found**"
        else:
            debug_result = "❌ **Usage:** `debug find YOUR QUESTION HERE`"

    elif debug_query.lower().startswith("sample"):
        # Sample data from a table
        table_name = debug_query.replace("sample", "").strip()
        if table_name:
            sample = await sample_table_data(table_name, sample_size=10)
            if sample.get('error'):
                debug_result = f"❌ **Error sampling {table_name}:** {sample['error']}"
            else:
                debug_result = f"📊 **Sample from {table_name}:**\n"
                debug_result += f"Columns ({len(sample.get('columns', []))}): {', '.join(sample.get('columns', [])[:15])}\n"

                if sample.get('audit_columns'):
                    debug_result += f"Audit Columns: {', '.join(sample['audit_columns'])}\n"

                if sample.get('sample_data'):
                    debug_result += "\n**Sample row:**\n```json\n"
                    sample_row = sample['sample_data'][0]
                    # Show first 10 columns
                    shown_cols = dict(list(sample_row.items())[:10])
                    debug_result += json.dumps(shown_cols, indent=2)
                    debug_result += "\n```"

                    if sample.get('value_stats'):
                        debug_result += "\n\n**Numeric Column Stats:**\n"
                        for col, stats in list(sample.get('value_stats', {}).items())[:5]:
                            null_pct = (stats.get('null_count', 0) / (
                                        stats.get('null_count', 0) + stats['non_null_count']) * 100) if stats.get(
                                'null_count', 0) else 0
                            debug_result += f"• {col}: min={stats['min']:.2f}, max={stats['max']:.2f}, mean={stats['mean']:.2f}, nulls={null_pct:.1f}%\n"
        else:
            debug_result = "❌ **Usage:** `debug sample TABLE_NAME`"

    elif debug_query.lower().startswith("analyze"):
        # Full analysis of table selection for a question
        question = debug_query.replace("analyze", "").strip()
        if question:
            debug_result = f"🔍 **Analyzing:** '{question}'\n\n"

            # Find candidate tables
            debug_result += "**1. Finding candidate tables...**\n"
            candidates = await find_relevant_tables_from_vector_store(question, user_id, channel_id, top_k=6)

            if candidates:
                for table in candidates:
                    debug_result += f"  • {table}\n"

                # Select best table
                debug_result += "\n**2. Selecting best table...**\n"
                selected_table, reason = await select_best_table_using_samples(question, candidates, user_id,
                                                                               channel_id)
                debug_result += f"  **Selected:** {selected_table}\n"
                debug_result += f"  **Reason:** {reason}\n"
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

    await send_slack_message(channel_id, f"🔍 **Debug Results:**\n{debug_result}", include_feedback_hint=False)


async def execute_sql_and_respond(clean_question: str, sql: str, channel_id: str, user_id: str,
                                  original_ts: str = None):
    """Execute SQL query and send results - HIDE quality analysis from users"""
    print("⚡ Executing query with intelligent auto-retry...")
    await send_slack_message(channel_id, "⚡ Executing query...", include_feedback_hint=False)

    print(f"\n{'=' * 60}")
    print(f"🧠 SQL Query to execute:")
    print(f"{sql}")
    print(f"{'=' * 60}\n")

    # Get context to find the selected table
    context = await get_conversation_context(user_id, channel_id)
    selected_table = context.get('last_table_used') if context else None

    # Check if SQL generation failed
    if sql.strip().lower().startswith("i don't have enough") or sql.startswith("-- Error:") or sql.startswith("⚠️"):
        await send_slack_message(channel_id, f"❌ {sql}", include_feedback_hint=False)
        return

    # Import the quality analysis functions
    from app.llm_prompter import execute_with_quality_analysis

    # Execute with intelligent auto-retry (happens BEFORE user sees anything)
    df, result_count, quality_analysis = await execute_with_quality_analysis(
        clean_question, sql, selected_table, user_id, channel_id
    )

    if isinstance(df, str):
        # Error message from query execution
        print(f"❌ Query execution error: {df}")

        # Check if it's a column name error and provide helpful suggestions
        if "invalid identifier" in df.lower():
            match = re.search(r"invalid identifier '([^']+)'", df, re.IGNORECASE)
            if match:
                bad_column = match.group(1)
                print(f"❌ Column '{bad_column}' does not exist in the table")

                suggestions_msg = f"❌ Query error: Column '{bad_column}' does not exist in the table.\n\n"
                suggestions_msg += "**Suggestions:**\n"
                suggestions_msg += f"• Try `@bot debug analyze {clean_question}` to see table analysis\n"
                suggestions_msg += f"• Try `@bot debug find {clean_question}` to find relevant tables\n"

                if selected_table:
                    suggestions_msg += f"• Try `@bot debug sample {selected_table}` to see actual columns\n"

                suggestions_msg += "• Rephrase your question with more specific details"
                result_message = suggestions_msg
            else:
                result_message = f"❌ Query error: {df}"
        else:
            result_message = f"❌ Query error: {df}"

        await update_sql_cache_with_results(clean_question, sql, 0, selected_table)
    else:
        # Success - process results
        print(f"✅ Query successful - returned {result_count} rows, {len(df.columns)} columns")

        # Check if auto-retry was successful
        auto_retry_success = quality_analysis.get("auto_retry_success", False) if quality_analysis else False
        quality_score = quality_analysis.get("quality_score", 100) if quality_analysis else 100

        if auto_retry_success:
            print(f"🎉 Auto-retry was successful! Quality improved to {quality_score}/100")
            # Update SQL to the improved version for caching
            sql = quality_analysis.get("improved_sql", sql)

        # Extract table from SQL if we don't have it
        if not selected_table and 'FROM' in sql.upper():
            sql_upper = sql.upper()
            from_idx = sql_upper.find('FROM')
            if from_idx != -1:
                after_from = sql[from_idx + 4:].strip()
                selected_table = re.split(r'[\s\n]+', after_from)[0]

        # Update cache with actual results
        await update_sql_cache_with_results(clean_question, sql, result_count, selected_table)

        # Summarize results (this is what the user sees)
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

        # ONLY add quality feedback if it's truly exceptional or there was an auto-retry success
        if auto_retry_success:
            # Subtle success message - user knows we improved the query
            result_message += f"\n\n_✨ Query automatically optimized for better results_"

        # DON'T show quality analysis details to users - only in debug mode
        # Users just get the clean answer

        # Update conversation context
        await update_conversation_context(user_id, channel_id, clean_question, result_message, 'sql_results',
                                          selected_table)

    # Send the result message to user
    is_success = result_count > 0
    response = await send_slack_message(channel_id, result_message, include_feedback_hint=is_success)

    # Store message info for feedback tracking if successful
    if response and is_success and selected_table:
        msg_ts = response.get("ts")
        if msg_ts:
            channel_ts = f"{channel_id}_{msg_ts}"
            message_to_question_map[channel_ts] = {
                'question': clean_question,
                'sql': sql,
                'table': selected_table,
                'timestamp': time.time(),
                'quality_score': quality_analysis.get("quality_score", 100) if quality_analysis else 100,
                'auto_retry_success': auto_retry_success
            }
            print(f"📝 Stored message {channel_ts} for feedback tracking")

            # Clean up old entries (older than 24 hours)
            current_time = time.time()
            to_remove = []
            for key, data in message_to_question_map.items():
                if current_time - data.get('timestamp', 0) > 86400:  # 24 hours
                    to_remove.append(key)
            for key in to_remove:
                del message_to_question_map[key]


async def handle_with_embeddings(clean_question: str, channel_id: str, user_id: str):
    """Fallback handler using embeddings"""
    print("📚 Using local embeddings with FAISS")

    relevant_models = search_relevant_models(clean_question)
    print(f"📊 Found {len(relevant_models)} relevant models")

    model_context = relevant_models[0]["context"] if relevant_models else ""
    sql = ask_llm_for_sql(clean_question, model_context)

    await execute_sql_and_respond(clean_question, sql, channel_id, user_id)


async def send_slack_message(channel_id: str, message: str, thread_ts: str = None, include_feedback_hint: bool = False):
    """Send message to Slack with error handling"""
    try:
        # Ensure message isn't too long for Slack (4000 char limit)
        if len(message) > 3900:
            message = message[:3900] + "\n\n... (truncated due to length)"

        # Add feedback hint only when explicitly requested
        if include_feedback_hint:
            message += "\n\n_React with ✅ if this is helpful, or ❌ if not accurate_"

        params = {
            "channel": channel_id,
            "text": message
        }

        if thread_ts:
            params["thread_ts"] = thread_ts

        response = slack_client.chat_postMessage(**params)
        print(f"📤 Sent message to channel {channel_id}")
        return response
    except SlackApiError as e:
        print(f"❌ Slack API error: {e.response['error']}")
        # Try sending a simpler error message
        try:
            response = slack_client.chat_postMessage(
                channel=channel_id,
                text="❌ Sorry, I encountered an error sending the response. Please try again."
            )
            return response
        except Exception as fallback_error:
            print(f"❌ Failed to send fallback message: {fallback_error}")
    except Exception as e:
        print(f"❌ Unexpected error sending message: {e}")

    return None


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
        "emoji_feedback_enabled": True,
        "features": {
            "question_classification": "Dynamic context-aware classification",
            "table_discovery": "Comprehensive vector search (8 candidates)",
            "table_selection": "Rigorous accuracy-focused analysis with audit column detection",
            "sql_generation": "Accuracy-driven with complete column metadata",
            "audit_columns": "Smart detection of DW audit columns for proper ordering",
            "data_sampling": "10-row samples with statistics and null analysis",
            "error_handling": "Enhanced with helpful suggestions",
            "feedback_system": "Emoji reactions for continuous improvement"
        },
        "improvements": {
            "accuracy_focus": "Prioritizes correctness over convenience",
            "audit_awareness": "Properly identifies and uses audit columns",
            "comprehensive_search": "Searches more tables to avoid missing the right one",
            "detailed_analysis": "Shows all columns and statistics for better decisions"
        }
    }