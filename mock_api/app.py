from flask import Flask, request, jsonify
from dateutil import parser
import os, time, random, uuid, threading
from generator import DataStore


API_KEY = os.environ["API_KEY"]
RATE = int(os.environ.get("API_RATE_LIMIT_PER_MIN", "60"))
WINDOW = 60

app = Flask(__name__)
store = DataStore()


# ---------------- RATE LIMIT ----------------
lock = threading.Lock()
last_reset = time.time()
count = 0

def check_rate_limit():
    global last_reset, count
    now = time.time()
    with lock:
        if now - last_reset > WINDOW:
            last_reset = now
            count = 0
        count += 1
        return count <= RATE
    

# ---------------- PAGINATION ----------------
def paginate(items, page, page_size):
    total = len(items)
    total_pages = (total + page_size - 1) // page_size
    start = (page-1)*page_size
    end = start + page_size

    return {
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "next_page": page+1 if page < total_pages else None,
        "count": len(items[start:end]),
        "data": items[start:end],
    }


# ---------------- RESOURCE MAP ----------------
RESOURCE_MAP = {
    "customers": lambda: store.customers,
    "orders": lambda: store.orders,
    "order_items": lambda: store.order_items,
    "products": lambda: store.products,
    "visits": lambda: store.visits,
}


# ---------------- HEALTH ----------------
@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------- MAIN ENDPOINT ----------------
@app.get("/customers")
@app.get("/orders")
@app.get("/order_items")
@app.get("/products")
@app.get("/visits")
def list_resources():

    
    if not check_rate_limit():
        return jsonify({"error":"rate_limited","retry_after":30}), 429
    
    
    path = request.path.strip("/")
    items = RESOURCE_MAP[path]()
    qs = request.args


    # ---------------- FILTER ----------------
    customer_id = qs.get("customer_id")
    if customer_id:
        items = [i for i in items if i.get("customer_id") == customer_id]

    order_id = qs.get("order_id")
    if order_id:
        items = [i for i in items if i.get("order_id") == order_id]

    status = qs.get("status")
    if status and path == "orders":
        items = [i for i in items if i.get("status") == status]

    country = qs.get("country")
    if country:
        items = [i for i in items if i.get("country") == country]


    # ---------------- PAGINATION ----------------
    page = int(qs.get("page", 1))
    page_size = min(int(qs.get("page_size", 500)), 1000)

    return jsonify(paginate(items, page, page_size))