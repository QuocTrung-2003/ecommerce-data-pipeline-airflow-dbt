# ecommerce-data-pipeline-airflow-dbt/mock_api/core/auth.py
import os
from fastapi import Request, HTTPException
from core.config import API_KEY

API_KEY = os.getenv("API_KEY")

def require_auth(request: Request):
    auth = request.headers.get("Authorization", "")
    if not (auth.startswith("Bearer ") and auth.split(" ",1)[1] == API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized")