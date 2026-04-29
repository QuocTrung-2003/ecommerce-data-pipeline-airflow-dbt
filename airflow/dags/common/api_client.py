import time
import requests
import logging
from .config import API_BASE, HEADERS

def fetch_paged(endpoint, params, max_retries=5):
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
                logging.warning("Request failed %s", e)
                time.sleep(2 ** attempt)
                continue

            if r.status_code == 429:
                time.sleep(10)
                continue

            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue

            r.raise_for_status()
            break

        else:
            raise RuntimeError(f"Failed endpoint {endpoint}")

        data = r.json()
        yield data

        if not data.get("next_page"):
            return

        page = data["next_page"]