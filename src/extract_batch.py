import pandas as pd
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
RAW_DATA_PATH = os.getenv("BATCH_DATA_PATH", "data/uci_online_retail.csv")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output/")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "tea_batch.parquet")
FILTER_KEYWORDS = ["tea", "green tea", "herbal tea"]
REQUIRED_COLUMNS = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "CustomerID", "Country"]

def load_raw_data(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding='ISO-8859-1')
        logging.info(f"Loaded {len(df)} rows from {path}")
        return df
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        raise

def filter_tea_products(df: pd.DataFrame, keywords: list) -> pd.DataFrame:
    mask = df["Description"].str.contains('|'.join(keywords), case=False, na=False)
    tea_df = df[mask].copy()
    logging.info(f"Filtered down to {len(tea_df)} tea-related rows")
    return tea_df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(subset=REQUIRED_COLUMNS)
    after_dropna = len(df)

    df.drop_duplicates(subset=["InvoiceNo", "StockCode", "CustomerID"], inplace=True)
    after_dedup = len(df)

    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df = df.reset_index(drop=True)

    logging.info(f"Dropped {before - after_dropna} rows with nulls")
    logging.info(f"Removed {after_dropna - after_dedup} duplicate rows")
    return df

def save_data(df: pd.DataFrame, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    logging.info(f"Saved output to {output_path}")

def main():
    df = load_raw_data(RAW_DATA_PATH)
    tea_df = filter_tea_products(df, FILTER_KEYWORDS)
    clean_df = clean_data(tea_df)
    save_data(clean_df, OUTPUT_FILE)

if __name__ == "__main__":
    main()
