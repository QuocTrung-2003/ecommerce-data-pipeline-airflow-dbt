from datetime import datetime, timedelta
from dateutil import parser
from airflow.providers.postgres.hooks.postgres import PostgresHook

from .api_client import fetch_paged
from .watermark import get_watermark, set_watermark

def extract_api_data(table, endpoint, ts_field, wm_name):

    pg = PostgresHook(postgres_conn_id="postgres_default")

    default_iso = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"
    watermark = get_watermark(wm_name, default_iso)

    max_seen = parser.isoparse(watermark)
    rows = 0

    for page in fetch_paged(endpoint, {"updated_since": watermark}):

        data = page["data"]
        if not data:
            continue

        with pg.get_conn() as conn:
            with conn.cursor() as cur:

                if table == "stg_raw.customers":
                    cur.executemany(
                        """
                        INSERT INTO stg_raw.customers(customer_id,company_name,country,industry,company_size,signup_date,updated_at,is_churned)
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

                elif table == "stg_raw.products":
                    cur.executemany(
                        """
                        INSERT INTO stg_raw.products(product_id,product_name,category,price,currency,updated_at)
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

                elif table == "stg_raw.orders":
                    cur.executemany(
                        """
                        INSERT INTO stg_raw.orders(order_id,customer_id,total_amount,status,payment_method,country,created_at,updated_at)
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

                elif table == "stg_raw.order_items":
                    cur.executemany(
                        """
                        INSERT INTO stg_raw.order_items(order_item_id,order_id,product_id,quantity,price,total_price,created_at,updated_at)
                        VALUES(%(order_item_id)s,%(order_id)s,%(product_id)s,%(quantity)s,%(price)s,%(total_price)s,%(created_at)s,%(updated_at)s)
                        ON CONFLICT (order_item_id) DO NOTHING
                        """,
                        data
                    )
                    
                elif table == "stg_raw.visits":
                    for d in data:
                        d["bounced"] = int(bool(d.get("bounced")))
                        d["converted"] = int(bool(d.get("converted")))

                    cur.executemany(
                        """
                        INSERT INTO stg_raw.visits(visit_id,customer_id,source,medium,device,country,pageviews,session_duration_s,bounced,converted,visit_start,updated_at)
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

    set_watermark(wm_name, max_seen.isoformat())
    return rows