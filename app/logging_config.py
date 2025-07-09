"""
Logging Configuration for BI Slack Bot
Provides structured logging with different levels for different components
"""
import logging
import os
import sys
from datetime import datetime

def setup_logging():
    """Set up logging configuration for the BI Slack Bot"""
    
    # Get log level from environment variable, default to INFO
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Create custom formatter
    class BotFormatter(logging.Formatter):
        """Custom formatter with colors and emojis for better readability"""
        
        # Color codes
        COLORS = {
            'DEBUG': '\033[36m',    # Cyan
            'INFO': '\033[32m',     # Green
            'WARNING': '\033[33m',  # Yellow
            'ERROR': '\033[31m',    # Red
            'CRITICAL': '\033[35m', # Magenta
            'RESET': '\033[0m'      # Reset
        }
        
        # Emojis for different log levels
        EMOJIS = {
            'DEBUG': '🔍',
            'INFO': '✅',
            'WARNING': '⚠️',
            'ERROR': '❌',
            'CRITICAL': '🚨'
        }
        
        def format(self, record):
            # Add color and emoji
            log_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
            emoji = self.EMOJIS.get(record.levelname, '')
            reset = self.COLORS['RESET']
            
            # Custom format with timestamp, level, module, and message
            record.levelname = f"{log_color}{emoji} {record.levelname}{reset}"
            record.name = f"{log_color}{record.name}{reset}"
            
            return super().format(record)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level))
    
    # Set formatter
    formatter = BotFormatter(
        fmt='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)
    
    # Configure specific loggers with different levels
    configure_component_loggers(log_level)
    
    # Log the configuration
    logger = logging.getLogger("logging_config")
    logger.info(f"Logging configured with level: {log_level}")

def configure_component_loggers(default_level):
    """Configure specific loggers for different components"""
    
    # Bot flow loggers - these should be visible
    bot_loggers = [
        "slack_handler",
        "llm_prompter_refactored", 
        "question_classifier",
        "table_manager",
        "sql_generator",
        "conversation_manager",
        "main"
    ]
    
    for logger_name in bot_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)  # Always show bot flow
    
    # Infrastructure loggers - reduce noise
    infrastructure_loggers = [
        "valkey_manager",
        "uvicorn",
        "uvicorn.access",
        "uvicorn.error",
        "fastapi",
        "httpx",
        "openai",
        "httpcore"
    ]
    
    for logger_name in infrastructure_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.WARNING)  # Only show warnings and errors
    
    # External library loggers - minimize noise
    external_loggers = [
        "urllib3",
        "requests",
        "boto3",
        "botocore",
        "snowflake"
    ]
    
    for logger_name in external_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.ERROR)  # Only show errors

def get_bot_logger(name):
    """Get a logger for bot components with proper configuration"""
    return logging.getLogger(name)

def log_bot_flow(logger, level, message, **kwargs):
    """Log bot flow with structured format"""
    # Add context information if provided
    context_parts = []
    if kwargs.get('user_id'):
        context_parts.append(f"User:{kwargs['user_id']}")
    if kwargs.get('channel_id'):
        context_parts.append(f"Channel:{kwargs['channel_id']}")
    if kwargs.get('question'):
        context_parts.append(f"Q:'{kwargs['question'][:50]}...'")
    if kwargs.get('table'):
        context_parts.append(f"Table:{kwargs['table']}")
    if kwargs.get('sql'):
        context_parts.append(f"SQL:{kwargs['sql'][:100]}...")
    
    context = f" [{' | '.join(context_parts)}]" if context_parts else ""
    
    # Log with level
    getattr(logger, level.lower())(f"{message}{context}")

def log_step(logger, step_name, status="START", **kwargs):
    """Log a bot processing step"""
    status_emojis = {
        "START": "🚀",
        "PROGRESS": "⏳", 
        "SUCCESS": "✅",
        "ERROR": "❌",
        "SKIP": "⏭️"
    }
    
    emoji = status_emojis.get(status, "📝")
    log_bot_flow(logger, "info", f"{emoji} {step_name} - {status}", **kwargs)

# Example usage functions for different log types
def log_question_received(logger, question, user_id, channel_id):
    """Log when a question is received"""
    log_step(logger, "QUESTION_RECEIVED", "START", 
             question=question, user_id=user_id, channel_id=channel_id)

def log_table_selected(logger, table, reason, user_id, channel_id):
    """Log when a table is selected"""
    log_step(logger, f"TABLE_SELECTED", "SUCCESS", 
             table=table, user_id=user_id, channel_id=channel_id)

def log_sql_generated(logger, sql, table, user_id, channel_id):
    """Log when SQL is generated"""
    log_step(logger, "SQL_GENERATED", "SUCCESS", 
             sql=sql, table=table, user_id=user_id, channel_id=channel_id)

def log_query_executed(logger, success, row_count=None, error=None, user_id=None, channel_id=None):
    """Log when query is executed"""
    status = "SUCCESS" if success else "ERROR"
    message = f"QUERY_EXECUTED - {status}"
    if row_count:
        message += f" ({row_count} rows)"
    if error:
        message += f" - {error}"
    
    log_step(logger, message, status, user_id=user_id, channel_id=channel_id)

def log_response_sent(logger, response_length, user_id, channel_id):
    """Log when response is sent"""
    log_step(logger, f"RESPONSE_SENT", "SUCCESS", 
             user_id=user_id, channel_id=channel_id)