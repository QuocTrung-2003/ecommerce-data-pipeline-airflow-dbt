import os

API_BASE = os.getenv("API_BASE_URL", "http://mock-api:8000/api/v1/data")

API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    raise ValueError("API_KEY is required")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}