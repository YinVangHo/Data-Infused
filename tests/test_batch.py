import os
from dotenv import load_dotenv
from pathlib import Path

def test_env_file_loading():
    # Load .env from root directory
    env_path = Path(".env")
    assert env_path.exists(), ".env file does not exist in root directory"

    load_dotenv(dotenv_path=env_path)

    batch_path = os.getenv("BATCH_DATA_PATH")
    output_path = os.getenv("OUTPUT_DIR")

    assert batch_path is not None, "BATCH_DATA_PATH not set in .env"
    assert output_path is not None, "OUTPUT_DIR not set in .env"
    assert isinstance(batch_path, str), "BATCH_DATA_PATH should be a string"
    assert isinstance(output_path, str), "OUTPUT_DIR should be a string"

    print(f"✅ .env loaded successfully. BATCH_DATA_PATH: {batch_path}, OUTPUT_DIR: {output_path}")