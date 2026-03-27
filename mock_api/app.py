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