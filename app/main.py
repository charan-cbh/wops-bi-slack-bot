from fastapi import FastAPI, Request, HTTPException
from app.slack_handler import handle_slack_event, get_status
from app.llm_prompter import check_valkey_health, init_valkey_client
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="BI Slack Bot")


# Initialize Valkey on startup
@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup"""
    logger.info("Starting up BI Slack Bot...")
    try:
        await init_valkey_client()
        logger.info("Valkey client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Valkey: {e}")


# Health check endpoint
@app.get("/")
async def root():
    """Root endpoint for health checks"""
    return {
        "status": "healthy",
        "service": "bi-slack-bot",
        "version": "1.0.0"
    }


# Detailed health check
@app.get("/health")
async def health_check():
    """Detailed health check including dependencies"""

    # Check Valkey connection
    valkey_status = await check_valkey_health()

    # Check Slack configuration
    slack_status = get_status()

    # Overall health
    is_healthy = (
            valkey_status.get("status") in ["healthy", "fallback"] and
            slack_status.get("slack_configured", False)
    )

    return {
        "status": "healthy" if is_healthy else "unhealthy",
        "checks": {
            "valkey": valkey_status,
            "slack": slack_status,
            "openai": {
                "assistant_configured": bool(os.getenv("ASSISTANT_ID")),
                "vector_store_configured": bool(os.getenv("OPENAI_VECTOR_STORE_ID"))
            }
        }
    }


# Slack events endpoint
@app.post("/slack/events")
async def slack_events(request: Request):
    """Handle Slack events"""
    try:
        response = await handle_slack_event(request)
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unhandled error in slack_events: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Warmup endpoint for Lambda
@app.get("/warmup")
async def warmup():
    """Warmup endpoint to prevent cold starts"""
    return {"status": "warm"}


# Add CORS headers if needed
@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response