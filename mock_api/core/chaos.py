# ecommerce-data-pipeline-airflow-dbt/mock_api/core/chaos.py
import random, uuid
from fastapi import HTTPException

def maybe_chaos():
    if random.random() < 0.02:
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "id": str(uuid.uuid4())}
        )