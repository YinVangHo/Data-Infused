import os
import logging
from dotenv import load_dotenv
import snowflake.connector

# Load env and set logging
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Config
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

RAW_TABLE = "TEA_TRANSACTIONS"
PRODUCTS_TABLE = "DIM_TEAS"
OUTPUT_TABLE = "TEA_TRANSACTIONS_CLEANED"

TRANSFORM_QUERY = f"""
CREATE OR REPLACE TABLE {OUTPUT_TABLE} AS
SELECT
    t.INVOICENO,
    t.STOCKCODE,
    LOWER(t.DESCRIPTION) AS DESCRIPTION,
    t.QUANTITY,
    TO_DATE(t.INVOICEDATE) AS INVOICEDATE,
    EXTRACT(YEAR FROM TO_DATE(t.INVOICEDATE)) AS YEAR,
    EXTRACT(MONTH FROM TO_DATE(t.INVOICEDATE)) AS MONTH,
    t.CUSTOMERID,
    t.COUNTRY,
    d.CATEGORY,
    d.ORIGIN
FROM {RAW_TABLE} t
LEFT JOIN {PRODUCTS_TABLE} d
    ON t.STOCKCODE = d.STOCKCODE
WHERE t.QUANTITY > 0;
"""

def main():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    logging.info(f"Running transformation to create {OUTPUT_TABLE}")
    cursor.execute(TRANSFORM_QUERY)
    logging.info(f"✅ Transformation complete — created table: {OUTPUT_TABLE}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
