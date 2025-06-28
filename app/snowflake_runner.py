import os
import snowflake.connector
import pandas as pd
from tabulate import tabulate
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

load_dotenv()

def get_snowflake_connection():
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE").encode()

    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=private_key_passphrase,
            backend=default_backend()
        )

    pkey = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=pkey,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

def run_query(sql: str):
    try:
        print("🔍 Running query...")
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        column_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        print("🔍 Raw result from Snowflake:")
        print(rows)
        print(f"✅ Type of result: {type(rows)}")

        df = pd.DataFrame(rows, columns=column_names)

        cursor.close()
        conn.close()

        return df
    except Exception as e:
        print(f"⚠️ Snowflake Exception: {e}")
        return pd.DataFrame([{"Error": str(e)}])

def format_result_for_slack(df: pd.DataFrame, max_rows=50, max_width=60) -> str:
    if df.empty:
        return "⚠️ No data returned."

    df_trimmed = df.head(max_rows).copy()
    df_trimmed.columns = [str(col)[:max_width] for col in df_trimmed.columns]
    for col in df_trimmed.columns:
        df_trimmed[col] = df_trimmed[col].astype(str).str.slice(0, max_width)

    try:
        return "```" + tabulate(df_trimmed, headers="keys", tablefmt="github", showindex=False) + "```"
    except Exception as e:
        return f"⚠️ Formatting error: {e}"
