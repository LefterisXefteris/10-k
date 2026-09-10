from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from sec_10k import compact_sec_parquet, fetch_and_save_parquet

if __name__ == "__main__":
    fetch_and_save_parquet()
    compact_sec_parquet()
