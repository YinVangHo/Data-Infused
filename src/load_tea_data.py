import os
import logging
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Config
PARQUET_FILE = os.getenv("LOAD_FILE", "output/tea_batch.parquet")
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

def infer_category(description):
    desc = description.lower()
    if "green" in desc:
        return "Green Tea"
    elif "herbal" in desc:
        return "Herbal Tea"
    elif "black" in desc:
        return "Black Tea"
    elif "fruit" in desc:
        return "Fruit Blend"
    else:
        return "General Tea"

def main():
    # Load and prepare tea transaction data
    df = pd.read_parquet(PARQUET_FILE)
    df["InvoiceDate"] = df["InvoiceDate"].astype(str)
    df.columns = [col.upper() for col in df.columns]
    df.drop_duplicates(subset=["INVOICENO", "STOCKCODE", "CUSTOMERID"], inplace=True)
    logging.info(f"Loaded {len(df)} tea transaction rows")

    # Connect to Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    logging.info("Connected to Snowflake")

    # Create and load TEA_TRANSACTIONS table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS TEA_TRANSACTIONS (
            INVOICENO STRING,
            STOCKCODE STRING,
            DESCRIPTION STRING,
            QUANTITY NUMBER,
            INVOICEDATE STRING,
            CUSTOMERID NUMBER,
            COUNTRY STRING
        )
    """)
    cursor.executemany("""
        INSERT INTO TEA_TRANSACTIONS (
            INVOICENO, STOCKCODE, DESCRIPTION, QUANTITY, INVOICEDATE, CUSTOMERID, COUNTRY
        ) VALUES (
            %(INVOICENO)s, %(STOCKCODE)s, %(DESCRIPTION)s, %(QUANTITY)s, %(INVOICEDATE)s, %(CUSTOMERID)s, %(COUNTRY)s
        )
    """, df.to_dict(orient="records"))
    logging.info("Inserted data into TEA_TRANSACTIONS")

    # Build and load DIM_TEAS
    dim_df = df[["STOCKCODE", "DESCRIPTION"]].drop_duplicates().copy()
    dim_df["CATEGORY"] = dim_df["DESCRIPTION"].apply(infer_category)
    dim_df["ORIGIN"] = "Unknown"

    cursor.execute("""
        CREATE OR REPLACE TABLE DIM_TEAS (
            STOCKCODE STRING,
            DESCRIPTION STRING,
            CATEGORY STRING,
            ORIGIN STRING
        )
    """)
    cursor.executemany("""
        INSERT INTO DIM_TEAS (
            STOCKCODE, DESCRIPTION, CATEGORY, ORIGIN
        ) VALUES (
            %(STOCKCODE)s, %(DESCRIPTION)s, %(CATEGORY)s, %(ORIGIN)s
        )
    """, dim_df.to_dict(orient="records"))
    logging.info("Inserted data into DIM_TEAS")

    # Cleanup
    cursor.close()
    conn.close()
    logging.info("Snowflake connection closed")

if __name__ == "__main__":
    main()