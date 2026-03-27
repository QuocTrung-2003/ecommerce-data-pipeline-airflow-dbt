import uuid, random
from datetime import datetime, timedelta, timezone

COUNTRIES = ["US","GB","DE","FR","CA","AU","NL","SE","IT","ES"]
COUNTRY_W = [0.35,0.15,0.12,0.07,0.08,0.06,0.06,0.04,0.04,0.03]

INDUSTRIES = ["Retail","Technology","Education","Health","Finance"]
IND_W = [0.35,0.25,0.12,0.1,0.1]

PRODUCTS = ["Laptop","Phone","Headphones","Keyboard","Monitor","Mouse"]
CATEGORIES = ["Electronics","Accessories"]

PRICES = {
    "Laptop": 1200,
    "Phone": 800,
    "Headphones": 150,
    "Keyboard": 100,
    "Monitor": 300,
    "Mouse": 50
}

PAYMENT_METHODS=["card","bank_transfer","paypal","apple_pay","google_pay"]
PM_W=[0.6,0.15,0.15,0.05,0.05]

STATUSES = ["completed","cancelled","returned"]
STAT_W=[0.9,0.06,0.04]

SOURCES=["google","direct","facebook","linkedin","newsletter","referral","bing"]
SRC_W=[0.45,0.18,0.12,0.08,0.07,0.06,0.04]

MEDIUMS=["organic","cpc","email","social","none","referral"]
MED_W=[0.5,0.18,0.1,0.12,0.06,0.04]

DEVICES=["desktop","mobile","tablet"]
DEV_W=[0.55,0.4,0.05]

TZ = timezone.utc
random.seed(42)

def _rand_date(days_back=180):
    now = datetime.now(TZ)
    start = now - timedelta(days=days_back)
    return start + timedelta(seconds=random.randint(0, days_back*24*3600))


class DataStore:
    def __init__(self):
        self.customers = []
        self.products = []
        self.orders = []
        self.order_items = []
        self.visits = []
        self._generate()


    def _generate(self):

        # ---------------- PRODUCTS ----------------
        for p in PRODUCTS:
            self.products.append({
                "product_id": str(uuid.uuid4()),
                "product_name": p,
                "category": random.choice(CATEGORIES),
                "price": PRICES[p],
                "currency": "USD",
                "updated_at": datetime.now(timezone.utc).isoformat()  
            })

            # ---------------- CUSTOMERS ----------------
        for i in range(1000):
            cid = str(uuid.uuid4())
            signup = _rand_date(360)

            self.customers.append({
                "customer_id": cid,
                "company_name": f"Company {i:04d}",
                "country": random.choices(COUNTRIES, COUNTRY_W)[0],
                "industry": random.choices(INDUSTRIES, IND_W)[0],
                "company_size": random.choices(
                    ["1-10","11-50","51-200","201-500","500+"],
                    [0.35,0.3,0.2,0.1,0.05]
                )[0],
                "signup_date": signup.isoformat(),
                "updated_at": signup.isoformat(),
                "is_churned": random.random() < 0.18
            })
            

            # ---------------- ORDERS + ORDER ITEMS ----------------
        for _ in range(10000):
            c = random.choice(self.customers)

            order_id = str(uuid.uuid4())
            created = _rand_date(180)
            status = random.choices(STATUSES, STAT_W)[0]

            total_amount = 0
            num_items = random.randint(1, 3)


            for _ in range(num_items):
                product = random.choice(self.products)
                quantity = random.randint(1, 3)
                price = product["price"]



