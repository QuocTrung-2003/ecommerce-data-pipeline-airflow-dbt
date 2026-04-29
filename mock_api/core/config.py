# ecommerce-data-pipeline-airflow-dbt/mock_api/core/config.py
# from dotenv import load_dotenv
import os

# load_dotenv()

API_KEY = os.getenv("API_KEY")   # ✅ THIẾU DÒNG NÀY

RATE_LIMIT = int(os.getenv("API_RATE_LIMIT_PER_MIN", "60"))
TZ = os.getenv("TZ", "UTC")

if not API_KEY:
    raise ValueError("API_KEY is required")