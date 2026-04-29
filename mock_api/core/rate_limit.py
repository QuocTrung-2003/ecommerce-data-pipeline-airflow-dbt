# ecommerce-data-pipeline-airflow-dbt/mock_api/core/rate_limit.py
import time
import threading
from core.config import RATE_LIMIT

WINDOW = 60

lock = threading.Lock()

reset_time = time.time()
count = 0


def check_rate_limit():
    global reset_time, count

    now = time.time()

    with lock:
        if now - reset_time > WINDOW:
            reset_time = now
            count = 0

        count += 1

        return count <= RATE_LIMIT