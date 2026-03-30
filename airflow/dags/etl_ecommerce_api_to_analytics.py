from datetime import datetime, timedelta
from airflow import DAG


DEFAULT_ARGS = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}
with DAG(
    dag_id="etl_ecommerce_api_to_analytics",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["elt", "postgres", "dbt", "bi"],
):
    []