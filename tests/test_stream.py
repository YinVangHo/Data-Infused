import os
import sys
from pathlib import Path
import pandas as pd

# Add src folder to path
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

# Now import from inside src
from extract_stream import simulate_stream

def test_stream_to_output_folder():
    df = pd.DataFrame({
        "InvoiceNo": ["001"],
        "StockCode": ["T001"],
        "Description": ["Herbal Tea"],
        "Quantity": [5],
        "CustomerID": [12345],
        "Country": ["UK"]
    })

    test_output_path = "output/"
    os.environ["OUTPUT_DIR"] = test_output_path

    simulate_stream(df, interval_seconds=0, num_records=3)

    files = list(Path(test_output_path).glob("tea_stream_*.parquet"))
    assert len(files) >= 3

    for file in files:
        df = pd.read_parquet(file)
        assert "transaction_id" in df.columns
        assert "timestamp" in df.columns

    print("✅ Stream test passed: files created successfully")