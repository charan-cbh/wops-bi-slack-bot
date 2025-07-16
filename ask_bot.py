#!/usr/bin/env python3
"""
Clean bot question script - just ask and get the answer
Usage: python ask_bot.py "your question here"
"""

import asyncio
import sys
import os
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

async def ask_bot(question: str) -> str:
    """Ask the bot a question and return its answer"""
    try:
        from app.llm_orchestrator import handle_question
        
        response, response_type = await handle_question(
            question, 
            user_id="U123USER", 
            channel_id="C123CHANNEL",
            assistant_id=os.getenv('ASSISTANT_ID')
        )
        
        return response
        
    except Exception as e:
        return f"❌ Error: {str(e)}"

async def main():
    """Main function"""
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
    else:
        print("Usage: python ask_bot.py \"your question here\"")
        print("Example: python ask_bot.py \"How many tickets did agent Lavinia Layson solve today?\"")
        return
    
    # Get the bot's answer (suppress processing logs)
    import warnings
    warnings.filterwarnings("ignore")
    
    # Redirect print statements temporarily
    old_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')
    
    try:
        answer = await ask_bot(question)
    finally:
        sys.stdout.close()
        sys.stdout = old_stdout
    
    # Just print the answer
    print(answer)

if __name__ == "__main__":
    asyncio.run(main())