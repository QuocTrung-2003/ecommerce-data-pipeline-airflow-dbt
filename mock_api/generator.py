import uuid, random
from datetime import datetime, timedelta, timezone

COUNTRIES = ["VN","TH","SG","MY","ID","PH","JP","KR","IN","CN"]
COUNTRY_W = [0.3,0.12,0.1,0.08,0.12,0.08,0.06,0.05,0.05,0.04]

INDUSTRIES = ["Fashion","Electronics","Beauty","Home","Sports"]
IND_W = [0.3,0.25,0.15,0.15,0.15]

PRODUCTS = ["T-shirt","Sneakers","Smartwatch","Blender","Yoga Mat","Backpack"]
CATEGORIES = ["Clothing","Gadgets","Home Appliances","Fitness","Accessories"]

PRICES = {
    "T-shirt": 25,
    "Sneakers": 120,
    "Smartwatch": 200,
    "Blender": 90,
    "Yoga Mat": 40,
    "Backpack": 60
}

PAYMENT_METHODS = ["credit_card","e_wallet","cod","bank_transfer","buy_now_pay_later"]
PM_W = [0.4,0.25,0.2,0.1,0.05]

STATUSES = ["delivered","pending","cancelled","returned"]
STAT_W = [0.75,0.1,0.1,0.05]

SOURCES = ["tiktok","google","instagram","youtube","affiliate","direct","email"]
SRC_W = [0.25,0.2,0.15,0.1,0.1,0.1,0.1]

MEDIUMS = ["ads","organic","influencer","email","referral","social"]
MED_W = [0.3,0.25,0.15,0.1,0.1,0.1]

DEVICES = ["mobile","desktop","tablet","smart_tv"]
DEV_W = [0.65,0.25,0.07,0.03]

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


                item_total = price * quantity
                total_amount += item_total
                

                self.order_items.append({
                    "order_item_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "price": price,
                    "total_price": item_total,
                    "created_at": created.isoformat(),
                    "updated_at": created.isoformat()
                })      



            self.orders.append({
                "order_id": order_id,
                "customer_id": c["customer_id"],
                "total_amount": float(total_amount),
                "status": status,
                "payment_method": random.choices(PAYMENT_METHODS, PM_W)[0],
                "country": c["country"],
                "created_at": created.isoformat(),
                "updated_at": created.isoformat()
            })
        
        
        # ---------------- VISITS ----------------     
        
        for _ in range(30000):
            dt = _rand_date(180)

            if dt.weekday() in (5,6) and random.random() < 0.2:
                continue

            cust = None
            if random.random() < 0.35:
                cust = random.choice(self.customers)["customer_id"]
                
            pageviews = max(1, int(random.expovariate(1/2)))
            duration = int(random.gammavariate(2,45))

            bounced = 1 if (pageviews == 1 and duration < 10) else 0

            conv_prob = (
                0.02 +
                (0.02 if random.choice(MEDIUMS) == "cpc" else 0) +
                (0.03 if random.choice(MEDIUMS) == "email" else 0) +
                (0.01 if pageviews >= 3 else 0)
            )


            self.visits.append({
                "visit_id": str(uuid.uuid4()),
                "customer_id": cust,
                "source": random.choices(SOURCES, SRC_W)[0],
                "medium": random.choices(MEDIUMS, MED_W)[0],
                "device": random.choices(DEVICES, DEV_W)[0],
                "country": random.choices(COUNTRIES, COUNTRY_W)[0],
                "pageviews": pageviews,
                "session_duration_s": duration,
                "bounced": bounced,
                "converted": 1 if conv_prob > random.random() else 0,
                "visit_start": dt.isoformat(),
                "updated_at": dt.isoformat()
            })