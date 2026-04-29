# ecommerce-data-pipeline-airflow-dbt/mock_api/data/generator.py
import uuid
import random
from datetime import datetime, timedelta, timezone


class DataStore:
    def __init__(self, seed=42):
        self.seed = seed
        self._init_constants()
        self._init_random()
        self._generate()

    # ---------------- INIT ----------------
    def _init_random(self):
        random.seed(self.seed)

    def _init_constants(self):
        self.COUNTRIES = ["VN","TH","SG","MY","ID","PH","JP","KR","IN","CN"]

        self.INDUSTRIES = ["Fashion","Electronics","Beauty","Home","Sports"]

        self.PRODUCTS = ["T-shirt","Sneakers","Smartwatch","Blender","Yoga Mat","Backpack"]

        self.CATEGORIES = ["Clothing","Gadgets","Home Appliances","Fitness","Accessories"]

        self.PRICES = {
            "T-shirt": 25,
            "Sneakers": 120,
            "Smartwatch": 200,
            "Blender": 90,
            "Yoga Mat": 40,
            "Backpack": 60
        }

        self.PAYMENT_METHODS = ["credit_card","e_wallet","cod","bank_transfer","buy_now_pay_later"]

        self.STATUSES = ["delivered","pending","cancelled","returned"]

        self.SOURCES = ["tiktok","google","instagram","youtube","affiliate","direct","email"]

        self.MEDIUMS = ["paid_ads","organic","influencer","email","referral","social"]

        self.DEVICES = ["mobile","desktop","tablet","smart_tv"]

        self.TZ = timezone.utc

    # ---------------- DATE ----------------
    def _rand_date(self, days_back=180):
        now = datetime.now(self.TZ)
        start = now - timedelta(days=days_back)
        return start + timedelta(seconds=random.randint(0, days_back * 24 * 3600))

    # ---------------- GENERATION ----------------
    def _generate(self):

        self.customers = []
        self.products = []
        self.orders = []
        self.order_items = []
        self.visits = []

        # ---------------- PRODUCTS ----------------
        for p in self.PRODUCTS:
            self.products.append({
                "product_id": str(uuid.uuid4()),
                "product_name": p,
                "category": random.choice(self.CATEGORIES),
                "price": self.PRICES[p],
                "currency": "USD",
                "updated_at": datetime.now(self.TZ).isoformat()
            })

        # ---------------- CUSTOMERS ----------------
        for i in range(1000):
            cid = str(uuid.uuid4())
            signup = self._rand_date(360)

            self.customers.append({
                "customer_id": cid,
                "company_name": f"Company {i:04d}",
                "country": random.choice(self.COUNTRIES),
                "industry": random.choice(self.INDUSTRIES),
                "company_size": random.choices(
                    ["1-10","11-50","51-200","201-500","500+"],
                    [0.35,0.3,0.2,0.1,0.05]
                )[0],
                "signup_date": signup.isoformat(),
                "updated_at": signup.isoformat(),
                "is_churned": random.random() < 0.18
            })

        # ---------------- ORDERS ----------------
        for _ in range(10000):
            c = random.choice(self.customers)

            order_id = str(uuid.uuid4())
            created = self._rand_date(180)

            total_amount = 0
            num_items = random.randint(1, 3)

            for _ in range(num_items):
                product = random.choice(self.products)
                quantity = random.randint(1, 3)
                price = product["price"]

                total = price * quantity
                total_amount += total

                self.order_items.append({
                    "order_item_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "price": price,
                    "total_price": total,
                    "created_at": created.isoformat(),
                    "updated_at": created.isoformat()
                })

            self.orders.append({
                "order_id": order_id,
                "customer_id": c["customer_id"],
                "total_amount": float(total_amount),
                "status": random.choice(self.STATUSES),
                "payment_method": random.choice(self.PAYMENT_METHODS),
                "country": c["country"],
                "created_at": created.isoformat(),
                "updated_at": created.isoformat()
            })

        # ---------------- VISITS (FIXED LOGIC) ----------------
        for _ in range(30000):
            dt = self._rand_date(180)

            if dt.weekday() in (5,6) and random.random() < 0.2:
                continue

            cust = None
            if random.random() < 0.35:
                cust = random.choice(self.customers)["customer_id"]

            pageviews = max(1, int(random.expovariate(1/2)))
            duration = int(random.gammavariate(2,45))

            bounced = 1 if (pageviews == 1 and duration < 10) else 0

            # FIX: realistic marketing attribution
            medium = random.choice(self.MEDIUMS)

            conv_prob = (
                0.02 +
                (0.03 if medium == "email" else 0) +
                (0.02 if medium == "paid_ads" else 0) +
                (0.01 if pageviews >= 3 else 0)
            )

            self.visits.append({
                "visit_id": str(uuid.uuid4()),
                "customer_id": cust,
                "source": random.choice(self.SOURCES),
                "medium": medium,
                "device": random.choice(self.DEVICES),
                "country": random.choice(self.COUNTRIES),
                "pageviews": pageviews,
                "session_duration_s": duration,
                "bounced": bounced,
                "converted": 1 if random.random() < conv_prob else 0,
                "visit_start": dt.isoformat(),
                "updated_at": dt.isoformat()
            })