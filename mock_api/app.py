from flask import Flask, request, jsonify
from dateutil import parser
import os, time, random, uuid, threading
from generator import DataStore


API_KEY = os.environ["API_KEY"]
RATE = int(os.environ.get("API_RATE_LIMIT_PER_MIN", "60"))
WINDOW = 60

app = Flask(__name__)
store = DataStore()

# ---------------- RESOURCE MAP ----------------
RESOURCE_MAP = {
    "customers": lambda: store.customers,
    "orders": lambda: store.orders,
    "order_items": lambda: store.order_items,
    "products": lambda: store.products,
    "visits": lambda: store.visits,
}


# ---------------- MAIN ENDPOINT ----------------
@app.get("/customers")
@app.get("/orders")
@app.get("/order_items")
@app.get("/products")
@app.get("/visits")
def list_resources():

    
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
