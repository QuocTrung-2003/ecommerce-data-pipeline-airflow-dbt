import os, time, requests, logging
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil import parser
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


API_BASE = os.environ.get("API_BASE_URL", "http://mock-api:8000")
API_KEY = os.environ["API_KEY"]
if not API_KEY:
    raise ValueError("API_KEY is required")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

DEFAULT_ARGS = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "max_retry_delay":timedelta(minutes=20),
    "retry_exponential_backoff": True
}


    # ---------------- INCREMENTAL ----------------
def _get_watermark(name, default_iso):                      # Take the previous run
    return Variable.get(name, default_var=default_iso)  

def _set_watermark(name, iso):     #  Save a new run
    Variable.set(name, iso)


def _fetch_paged(endpoint, params, max_retries=5):
    page = 1
    while True:
        p = params.copy()
        p.update({"page": page, "page_size": 500})
        r = None
        for attempt in range(max_retries):
            try:
                r = requests.get(
                    f"{API_BASE}/{endpoint}",
                    headers=HEADERS,
                    params=p,
                    timeout=30,
                )
            except requests.exceptions.RequestException as e:
                logging.warning("Request to %s failed (%s)", endpoint, e)
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 429:
                retry = r.json().get("retry_after", 10)
                time.sleep(int(retry))
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            data = r.json()
            yield data
            break
        else:
            logging.error(
                "Failed to fetch %s with params %s after %s attempts (last status %s)",
                endpoint,
                p,
                max_retries,
                getattr(r, "status_code", "unknown"),
            )
            raise RuntimeError(
                f"Failed to fetch {endpoint} after {max_retries} attempts",
            )
        if data.get("next_page"):
            page = data["next_page"]
        else:
            return
        
def extract_api_data(table, endpoint, ts_field, wm_name):
    pg = PostgresHook(postgres_conn_id="postgres_default")
    iso_default = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"
    watermark = _get_watermark(wm_name, iso_default)
    max_seen = parser.isoparse(watermark)
    rows = 0
    for page in _fetch_paged(endpoint, {"updated_since": watermark}):
        data = page["data"]
        if not data:
            continue
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                if table == "raw_data.customers":
                    cur.executemany(
                        """
                        INSERT INTO raw_data.customers(customer_id,company_name,country,industry,company_size,signup_date,updated_at,is_churned)
                        VALUES(%(customer_id)s,%(company_name)s,%(country)s,%(industry)s,%(company_size)s,%(signup_date)s,%(updated_at)s,%(is_churned)s)
                        ON CONFLICT (customer_id) DO UPDATE SET
                          company_name=EXCLUDED.company_name,
                          country=EXCLUDED.country,
                          industry=EXCLUDED.industry,
                          company_size=EXCLUDED.company_size,
                          signup_date=EXCLUDED.signup_date,
                          updated_at=EXCLUDED.updated_at,
                          is_churned=EXCLUDED.is_churned
                        """,
                        data
                    )
                elif table == "raw_data.products":
                    cur.executemany(
                        """
                        INSERT INTO raw_data.products(product_id,product_name,category,price,currency,updated_at)
                        VALUES(%(product_id)s,%(product_name)s,%(category)s,%(price)s,%(currency)s,%(updated_at)s)
                        ON CONFLICT (product_id) DO UPDATE SET
                        product_name=EXCLUDED.product_name,
                        category=EXCLUDED.category,
                        price=EXCLUDED.price,
                        currency=EXCLUDED.currency,
                        updated_at=EXCLUDED.updated_at
                        """,
                        data
                    )
                elif table == "raw_data.orders":
                    cur.executemany(
                        """
                        INSERT INTO raw_data.orders(order_id,customer_id,total_amount,status,payment_method,country,created_at,updated_at)
                        VALUES(%(order_id)s,%(customer_id)s,%(total_amount)s,%(status)s,%(payment_method)s,%(country)s,%(created_at)s,%(updated_at)s)
                        ON CONFLICT (order_id) DO UPDATE SET
                        customer_id=EXCLUDED.customer_id,
                        total_amount=EXCLUDED.total_amount,
                        status=EXCLUDED.status,
                        payment_method=EXCLUDED.payment_method,
                        country=EXCLUDED.country,
                        created_at=EXCLUDED.created_at,
                        updated_at=EXCLUDED.updated_at
                        """,
                        data
                    )
                elif table == "raw_data.order_items":
                    cur.executemany(
                        """
                        INSERT INTO raw_data.order_items(order_item_id,order_id,product_id,quantity,price,total_price,created_at,updated_at)
                        VALUES(%(order_item_id)s,%(order_id)s,%(product_id)s,%(quantity)s,%(price)s,%(total_price)s,%(created_at)s,%(updated_at)s)
                        ON CONFLICT (order_item_id) DO NOTHING
                        """,
                        data
                    )
                elif table == "raw_data.visits":
                    for d in data:
                        d["bounced"] = int(bool(d.get("bounced")))
                        d["converted"] = int(bool(d.get("converted")))

                    cur.executemany(
                        """
                        INSERT INTO raw_data.visits(visit_id,customer_id,source,medium,device,country,pageviews,session_duration_s,bounced,converted,visit_start,updated_at)
                        VALUES(%(visit_id)s,%(customer_id)s,%(source)s,%(medium)s,%(device)s,%(country)s,%(pageviews)s,%(session_duration_s)s,%(bounced)s,%(converted)s,%(visit_start)s,%(updated_at)s)
                        ON CONFLICT (visit_id) DO NOTHING
                        """,
                        data
                    )
                    
        rows += len(data)
        for d in data:
            ts = parser.isoparse(d.get(ts_field))
            if ts > max_seen:
                max_seen = ts
    _set_watermark(wm_name, max_seen.isoformat())
    return rows


    # ---------------- DAG ----------------

with DAG(
    dag_id="etl_ecommerce_api_to_analytics",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["elt", "postgres", "dbt", "bi"],
)as dag:
    
    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "raw_data.customers",
            "endpoint": "customers",
            "ts_field": "updated_at",
            "wm_name": "wm_customers",
        },
    )

    extract_products = PythonOperator(
        task_id="extract_products",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "raw_data.products",
            "endpoint": "products",
            "ts_field": "updated_at",
            "wm_name": "wm_products",
        },
    )

    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "raw_data.orders",
            "endpoint": "orders",
            "ts_field": "updated_at",
            "wm_name": "wm_orders",
        },
    )

    extract_order_items = PythonOperator(
        task_id="extract_order_items",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "raw_data.order_items",
            "endpoint": "order_items",
            "ts_field": "updated_at",
            "wm_name": "wm_order_items",
        },
    )

    extract_visits = PythonOperator(
        task_id="extract_visits",
        python_callable=extract_api_data,
        op_kwargs={
            "table": "raw_data.visits",
            "endpoint": "visits",
            "ts_field": "updated_at",
            "wm_name": "wm_visits",
        },
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
    )


[ extract_customers, extract_products, extract_orders, extract_order_items, extract_visits ] >> dbt_deps >> dbt_build