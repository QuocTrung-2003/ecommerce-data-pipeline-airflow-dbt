# ecommerce-data-pipeline-airflow-dbt/mock_api/main.py
from fastapi import FastAPI
from api.routes import router

app = FastAPI(
    title="Mock OLAP Analytics API",
    version="1.0.0"
)

app.include_router(router, prefix="/api/v1")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "mock-olap-api"
    }