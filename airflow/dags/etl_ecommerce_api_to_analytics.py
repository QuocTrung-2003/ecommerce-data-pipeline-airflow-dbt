<<<<<<< Updated upstream
import os, time, requests, logging
from airflow.models import Variable
=======
# ecommerce-data-pipeline-airflow-dbt\airflow\dags\etl_ecommerce_api_to_analytics.py

>>>>>>> Stashed changes
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from common.extractor import extract_api_data


DEFAULT_ARGS = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "max_retry_delay":timedelta(minutes=20),
    "retry_exponential_backoff": True
}

    # ---------------- DAG ----------------

with DAG(
    dag_id="etl_ecommerce_api_to_analytics",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["elt", "postgres", "dbt", "bi"],
)as dag:
    
    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "stg_raw.customers",
            "endpoint": "customers",
            "ts_field": "updated_at",
            "wm_name": "wm_customers",
        },
    )

    extract_products = PythonOperator(
        task_id="extract_products",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "stg_raw.products",
            "endpoint": "products",
            "ts_field": "updated_at",
            "wm_name": "wm_products",
        },
    )

    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "stg_raw.orders",
            "endpoint": "orders",
            "ts_field": "updated_at",
            "wm_name": "wm_orders",
        },
    )

    extract_order_items = PythonOperator(
        task_id="extract_order_items",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "stg_raw.order_items",
            "endpoint": "order_items",
            "ts_field": "updated_at",
            "wm_name": "wm_order_items",
        },
    )

    extract_visits = PythonOperator(
        task_id="extract_visits",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "stg_raw.visits",
            "endpoint": "visits",
            "ts_field": "updated_at",
            "wm_name": "wm_visits",
        },
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "docker exec dbt bash -lc "
            "'cd /usr/app && dbt deps --profiles-dir /root/.dbt'"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "docker exec dbt bash -lc "
            "'cd /usr/app && dbt build --fail-fast --profiles-dir /root/.dbt'"
        ),
    )


[ extract_customers, extract_products, extract_orders, extract_order_items, extract_visits ] >> dbt_deps >> dbt_build