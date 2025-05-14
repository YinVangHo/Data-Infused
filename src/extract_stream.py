import pandas as pd
import time
import uuid
import random
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

# Load .env from root
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Constants
RAW_DATA_PATH = os.getenv("BATCH_DATA_PATH", "data/uci_online_retail.csv")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output/")
FILTER_KEYWORDS = ["tea", "green tea", "herbal tea"]

def load_source_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding='ISO-8859-1')
    mask = df["Description"].str.contains("|".join(FILTER_KEYWORDS), case=False, na=False)
    return df[mask].dropna(subset=["InvoiceNo", "StockCode", "Description", "Quantity", "CustomerID", "Country"])

def simulate_stream(df: pd.DataFrame, interval_seconds=5, num_records=10):
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    for _ in range(num_records):
        row = df.sample(n=1).copy()
        row["transaction_id"] = str(uuid.uuid4())
        row["timestamp"] = pd.Timestamp.now()
        file_name = f"tea_stream_{row['transaction_id'].iloc[0]}.parquet"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        row.to_parquet(file_path, index=False)
        logging.info(f"Emitted 1 record → {file_path}")
        time.sleep(interval_seconds)

def main():
    df = load_source_data(RAW_DATA_PATH)
    simulate_stream(df, interval_seconds=3, num_records=5)

if __name__ == "__main__":
    main()