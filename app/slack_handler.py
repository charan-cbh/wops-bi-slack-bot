import hmac
import hashlib
import time
import os
import re
import json
from fastapi import Request, HTTPException
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from app.llm_prompter import (
    ask_llm_for_sql,
    summarize_results_with_llm,
    ask_assistant_generate_sql,
    summarize_with_assistant,
    debug_assistant_search,
    update_sql_cache_with_results,
    get_cache_stats,
    get_learning_insights,
    handle_question,
    test_question_classification,
)
from app.manifest_index import search_relevant_models
from app.snowflake_runner import run_query, format_result_for_slack
from dotenv import load_dotenv
import asyncio

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
        # Send "thinking" indicator to show bot is working
        try:
            slack_client.chat_postMessage(
                channel=channel_id,
                text="🤔 Analyzing your question..."
            )
        except Exception as e:
            print(f"⚠️ Could not send thinking indicator: {e}")

        clean_question = re.sub(r"<@[^>]+>", "", user_question).strip()
        print("🔍 Received question:", user_question)
        print("🧹 Cleaned question:", clean_question)
        print(f"👤 User: {user_id}, Channel: {channel_id}")

        # Check for special debug commands
        if clean_question.lower().startswith("debug"):
            if USE_ASSISTANT_API and ASSISTANT_ID:
                debug_query = clean_question.replace("debug", "").strip()

                if debug_query.lower() in ["cache", "stats"]:
                    # Show cache statistics - NOW ASYNC for Valkey
                    stats = await get_cache_stats()
                    learning = await get_learning_insights()
                    debug_result = f"📊 **Cache Statistics:**\n```{json.dumps(stats, indent=2)}```\n\n🧠 **Learning Insights:**\n```{learning}```"
                elif debug_query.lower() in ["learning", "patterns"]:
                    # Show learning patterns - NOW ASYNC for Valkey
                    learning = await get_learning_insights()
                    debug_result = f"🧠 **What I've Learned:**\n```{learning}```"
                elif debug_query.lower() in ["test", "classification"]:
                    # Test question classification
                    test_question_classification()
                    debug_result = "🧪 **Classification test complete** - check server logs for results"
                else:
                    # Original debug search
                    debug_result = await debug_assistant_search(debug_query, user_id, channel_id)

                await send_slack_message(channel_id, f"🔍 **Debug Results:**\n{debug_result}")
                return
            else:
                await send_slack_message(channel_id, "Debug only works with Assistant API enabled")
                return

        # Use smart routing instead of always generating SQL
        if USE_ASSISTANT_API and ASSISTANT_ID:
            print(f"🤖 Using Assistant API: {ASSISTANT_ID}")

            # Smart routing - determines if SQL is needed or conversational response
            response, response_type = await handle_question(clean_question, user_id, channel_id, ASSISTANT_ID)

            print(f"🔍 Question classified as: {response_type}")

            if response_type == 'sql':
                # SQL was generated, execute it
                print("📊 Executing SQL query...")
                await send_slack_message(channel_id, "⚡ Executing query...")

                sql = response
                print("🧠 Generated SQL:\n", sql)

                # Check if SQL generation failed
                if sql.strip().lower().startswith("i don't have enough") or sql.startswith("-- Error:"):
                    await send_slack_message(channel_id, f"❌ {sql}")
                    return

                # Execute the SQL query
                print("🔍 Running query...")
                df = run_query(sql)

                if isinstance(df, str):
                    # Error message from query execution
                    result_message = f"❌ Query error: {df}"
                    # Update cache with poor results - NOW ASYNC for Valkey
                    await update_sql_cache_with_results(clean_question, sql, 0)
                else:
                    print("✅ Query returned DataFrame")
                    result_count = len(df) if hasattr(df, '__len__') else 0
                    print(f"📊 Results: {result_count} rows, {df.shape[1] if hasattr(df, 'shape') else 0} columns")

                    # Update cache with actual results - NOW ASYNC for Valkey
                    await update_sql_cache_with_results(clean_question, sql, result_count)

                    # Summarize results
                    print("🤖 Using Assistant for summarization")
                    result_message = await summarize_with_assistant(
                        clean_question,
                        format_result_for_slack(df),
                        user_id,
                        channel_id,
                        ASSISTANT_ID
                    )

                await send_slack_message(channel_id, result_message)

            elif response_type == 'conversational':
                # Conversational response - send directly, no SQL needed
                print("💬 Providing conversational response...")
                await send_slack_message(channel_id, response)

            else:
                # Fallback
                print("❓ Unknown response type")
                await send_slack_message(channel_id,
                                         "I'm not sure how to help with that. Try asking about tickets, reviews, or agent data.")

        else:
            # Fallback to embedding search (original logic)
            print("📚 Using local embeddings with FAISS")
            relevant_models = search_relevant_models(clean_question)
            for match in relevant_models:
                print(f"✅ Match found: {match['model']}")
            print(f"📊 Found {len(relevant_models)} relevant models")

            model_context = relevant_models[0]["context"] if relevant_models else ""
            sql = ask_llm_for_sql(clean_question, model_context)

            print("🧠 Generated SQL:\n", sql)

            # Check if SQL generation failed
            if sql.strip().lower().startswith("i don't have enough"):
                await send_slack_message(channel_id, f"❌ {sql}")
                return

            # Execute the SQL query
            print("🔍 Running query...")
            await send_slack_message(channel_id, "⚡ Executing query...")

            df = run_query(sql)

            if isinstance(df, str):
                # Error message from query execution
                result_message = f"❌ Query error: {df}"
                # Update cache with poor results - NOW ASYNC for Valkey
                await update_sql_cache_with_results(clean_question, sql, 0)
            else:
                print("✅ Query returned DataFrame")
                result_count = len(df) if hasattr(df, '__len__') else 0
                print(f"📊 Results: {result_count} rows, {df.shape[1] if hasattr(df, 'shape') else 0} columns")

                # Update cache with actual results - NOW ASYNC for Valkey
                await update_sql_cache_with_results(clean_question, sql, result_count)

                # Summarize results
                print("📝 Using direct LLM for summarization")
                result_message = summarize_results_with_llm(
                    clean_question,
                    format_result_for_slack(df)
                )

            await send_slack_message(channel_id, result_message)

    except Exception as e:
        print("❌ Error handling Slack event:", str(e))
        await send_slack_message(
            channel_id,
            f"❌ **Error processing your request:**\n```{str(e)}```\n\nPlease try rephrasing your question or contact support."
        )


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
    from app.llm_prompter import clear_thread_cache
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
    }