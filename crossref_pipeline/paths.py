from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
STAGING_DIR = DATA_DIR / "staging"
SILVER_DIR = DATA_DIR / "silver"


def create_data_dirs() -> None:
    """
    Create all data folders if they do not already exist.
    """
    for path in [RAW_DIR, BRONZE_DIR, STAGING_DIR, SILVER_DIR]:
        path.mkdir(parents=True, exist_ok=True)