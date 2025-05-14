import snowflake.connector
from dotenv import dotenv_values
from pathlib import Path

# Load credentials from exact .env path
env_path = Path("c:/Users/HP/OneDrive/Desktop/Academy/Data-Infused/.env")
env = dotenv_values(dotenv_path=env_path)

print("\n🔧 Connecting to Snowflake...")

try:
    conn = snowflake.connector.connect(
        user=env["SNOWFLAKE_USER"],
        password=env["SNOWFLAKE_PASSWORD"],
        account=env["SNOWFLAKE_ACCOUNT"],
        warehouse=env["SNOWFLAKE_WAREHOUSE"],
        database=env["SNOWFLAKE_DATABASE"],
        schema=env["SNOWFLAKE_SCHEMA"]
    )
    cursor = conn.cursor()
    print("✅ Connection successful!")

    # Create a tiny test table
    cursor.execute("""
        CREATE OR REPLACE TABLE test_table (
            id INT,
            name STRING
        )
    """)
    print("✅ test_table created successfully!")

except Exception as e:
    print("❌ Failed:")
    print(e)
finally:
    try:
        cursor.close()
        conn.close()
        print("🔒 Connection closed.")
    except:
        pass
