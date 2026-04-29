from dotenv import load_dotenv
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env.app")

API_KEY = os.getenv("API_KEY")
RATE_LIMIT = int(os.getenv("API_RATE_LIMIT_PER_MIN", "60"))
TZ = os.getenv("TZ", "UTC")

if not API_KEY:
    raise ValueError("API_KEY is required")