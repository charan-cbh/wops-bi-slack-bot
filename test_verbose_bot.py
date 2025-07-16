#!/usr/bin/env python3
"""
Verbose bot testing script - shows all logs and processing
Usage: python test_verbose_bot.py "your question here"
"""

import asyncio
import sys
import os
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

async def test_verbose_bot(question: str) -> str:
    """Ask the bot a question with full verbose logging"""
    print("🤖 VERBOSE BOT TEST")
    print("=" * 80)
    print(f"📋 Question: {question}")
    print("=" * 80)
    
    try:
        from app.llm_orchestrator import handle_question
        
        print("🚀 Starting question processing...")
        print()
        
        response, response_type = await handle_question(
            question, 
            user_id="U123USER", 
            channel_id="C123CHANNEL",
            assistant_id=os.getenv('ASSISTANT_ID')
        )
        
        print()
        print("=" * 80)
        print("✅ FINAL RESULT")
        print("=" * 80)
        print(f"📝 Response Type: {response_type}")
        print(f"💬 Bot Response:")
        print(response)
        print("=" * 80)
        
        return response
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"❌ Error: {str(e)}"

async def main():
    """Main function"""
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
    else:
        print("Usage: python test_verbose_bot.py \"your question here\"")
        print("Example: python test_verbose_bot.py \"how many agents work in Ricardo Birck's team?\"")
        return
    
    await test_verbose_bot(question)

if __name__ == "__main__":
    asyncio.run(main())