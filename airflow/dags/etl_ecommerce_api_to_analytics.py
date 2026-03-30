import os, time, requests, logging
from datetime import datetime, timedelta
from airflow import DAG


API_BASE = os.environ.get("API_BASE_URL", "http://mock-api:8000")
API_KEY = os.environ["API_KEY"]
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

DEFAULT_ARGS = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "max_retry_delay":timedelta(minutes=20),
    "retry_exponential_backoff": True
    
}

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

with DAG(
    dag_id="etl_ecommerce_api_to_analytics",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["elt", "postgres", "dbt", "bi"],
)as dag:
    
    []