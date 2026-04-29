from data.generator import DataStore
from dateutil import parser

store = DataStore()

RESOURCE_MAP = {
    "customers": lambda: store.customers,
    "orders": lambda: store.orders,
    "order_items": lambda: store.order_items,
    "products": lambda: store.products,
    "visits": lambda: store.visits,
}

def get_data(resource, qs):
    items = RESOURCE_MAP[resource]()

    # ---------------- FILTER ----------------
    if qs.get("customer_id"):
        items = [i for i in items if i.get("customer_id") == qs["customer_id"]]

    if qs.get("order_id"):
        items = [i for i in items if i.get("order_id") == qs["order_id"]]

    if qs.get("status") and resource == "orders":
        items = [i for i in items if i.get("status") == qs["status"]]

    if qs.get("country"):
        items = [i for i in items if i.get("country") == qs["country"]]

    # ---------------- INCREMENTAL ----------------
    if qs.get("updated_since"):
        ts = parser.isoparse(qs["updated_since"])

        def get_ts(i):
            return i.get("updated_at") or i.get("created_at") or i.get("visit_start")

        items = [
            i for i in items
            if parser.isoparse(get_ts(i)) >= ts
        ]

    return items