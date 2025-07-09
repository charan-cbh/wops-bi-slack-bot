"""
Example of how to control logging levels with environment variables
"""

# Example environment variable settings:
# export LOG_LEVEL=INFO          # Default - shows bot flow
# export LOG_LEVEL=DEBUG         # Shows everything 
# export LOG_LEVEL=WARNING       # Only warnings and errors
# export LOG_LEVEL=ERROR         # Only errors

# What you'll see at different log levels:

# LOG_LEVEL=INFO (Recommended for production):
# 2024-01-15 10:30:45 | ✅ INFO | llm_prompter_refactored | 🚀 QUESTION_RECEIVED - START [User:U12345 | Channel:C67890 | Q:'How many tickets today?']
# 2024-01-15 10:30:46 | ✅ INFO | table_manager | 📋 Found 3 candidate tables: ['RPT_WOPS_TICKETS', 'FCT_TICKETS', 'DIM_TICKETS']
# 2024-01-15 10:30:47 | ✅ INFO | table_manager | ✅ TABLE_SELECTED - SUCCESS [Table:RPT_WOPS_TICKETS]
# 2024-01-15 10:30:48 | ✅ INFO | sql_generator | ✅ SQL_GENERATED - SUCCESS [SQL:SELECT COUNT(*) FROM RPT_WOPS_TICKETS WHERE...]
# 2024-01-15 10:30:49 | ✅ INFO | llm_prompter_refactored | ✅ QUERY_EXECUTED - SUCCESS (142 rows)

# LOG_LEVEL=DEBUG:
# Shows all the above PLUS:
# 2024-01-15 10:30:45 | 🔍 DEBUG | valkey_manager | Valkey client initialized successfully
# 2024-01-15 10:30:46 | 🔍 DEBUG | question_classifier | Question classified as: DATA_QUERY
# 2024-01-15 10:30:47 | 🔍 DEBUG | table_manager | Using cached schema for table: RPT_WOPS_TICKETS

# LOG_LEVEL=WARNING:
# Only shows warnings and errors:
# 2024-01-15 10:30:47 | ⚠️ WARNING | sql_generator | Fixing timezone handling - replacing CURRENT_DATE with PST conversion
# 2024-01-15 10:30:48 | ❌ ERROR | table_manager | Error sampling table: Connection timeout

# LOG_LEVEL=ERROR:
# Only shows errors:
# 2024-01-15 10:30:48 | ❌ ERROR | snowflake_runner | Query execution failed: Invalid SQL syntax

# Infrastructure logs (uvicorn, fastapi, etc.) are automatically set to WARNING level
# External library logs (urllib3, requests, etc.) are automatically set to ERROR level