import os
from pathlib import Path

# BASE_DIR is /home/user/projects/NaiFlow/
BASE_DIR = Path(os.getenv("PROJECT_DIR", Path(__file__).resolve().parent.parent))

# Define common sub-directories
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

# Ensure the directories exist so the script doesn't crash
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Define the specific Cache File path
# This creates /home/user/projects/NaiFlow/data/location_cache.csv
LOCATION_CACHE = DATA_DIR / "location_cache.csv"
PIPELINE_LOG = LOG_DIR / "nairobi_pipeline.log"