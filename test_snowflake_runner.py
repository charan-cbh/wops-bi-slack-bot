from app.snowflake_runner import run_query, format_result_for_slack

# 🔍 Test query (you can replace this with any small query in your environment)
sql = "SELECT CURRENT_DATE AS today, CURRENT_TIME AS now;"

print("\U0001F50D Running test query...")
df = run_query(sql)

print("\n\U0001F4CB Query Result:")
print(format_result_for_slack(df))