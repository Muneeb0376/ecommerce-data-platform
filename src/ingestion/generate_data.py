"""Generate realistic fake e-commerce data and load it into PostgreSQL.

Usage:
    python -m src.ingestion.generate_data --users 1000 --orders 5000 --events 20000
    python -m src.ingestion.generate_data --reset   # empty tables first
"""
import argparse
import os
import random
from datetime import datetime, timedelta

import psycopg
from dotenv import load_dotenv
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Beauty", "Sports",
              "Books", "Toys", "Grocery", "Automotive", "Health"]
COUNTRIES = ["Pakistan", "India", "UAE", "UK", "USA", "Canada", "Germany", "Saudi Arabia"]
PAY_METHODS = ["card", "cash_on_delivery", "bank_transfer", "wallet"]
ORDER_STATUS = ["delivered", "shipped", "paid", "pending", "cancelled", "returned"]
ORDER_WEIGHTS = [55, 15, 10, 8, 8, 4]


def connect():
    load_dotenv()
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def rand_date(days_back=365):
    return datetime.now() - timedelta(
        days=random.randint(0, days_back),
        seconds=random.randint(0, 86399))


def reset(cur):
    cur.execute("TRUNCATE events, reviews, payments, order_items, orders, "
                "products, categories, sellers, users RESTART IDENTITY CASCADE")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--users", type=int, default=1000)
    p.add_argument("--sellers", type=int, default=50)
    p.add_argument("--products", type=int, default=300)
    p.add_argument("--orders", type=int, default=5000)
    p.add_argument("--reviews", type=int, default=2000)
    p.add_argument("--events", type=int, default=20000)
    p.add_argument("--reset", action="store_true")
    a = p.parse_args()

    with connect() as conn, conn.cursor() as cur:
        if a.reset:
            reset(cur)

        cur.executemany("INSERT INTO categories(category_name) VALUES (%s)",
                        [(c,) for c in CATEGORIES])

        cur.executemany(
            "INSERT INTO sellers(seller_name, rating, created_at) VALUES (%s,%s,%s)",
            [(fake.company(), round(random.uniform(3.0, 5.0), 1), rand_date(900))
             for _ in range(a.sellers)])

        emails = set()
        users = []
        while len(users) < a.users:
            email = fake.email()
            if email in emails:
                continue
            emails.add(email)
            users.append((fake.name(), email, random.choice(COUNTRIES), rand_date(900)))
        cur.executemany(
            "INSERT INTO users(name, email, country, created_at) VALUES (%s,%s,%s,%s)", users)

        prices = {}
        products = []
        for pid in range(1, a.products + 1):
            price = round(random.uniform(3, 500), 2)
            prices[pid] = price
            products.append((random.randint(1, a.sellers),
                             random.randint(1, len(CATEGORIES)),
                             fake.catch_phrase()[:150], price, random.randint(0, 500)))
        cur.executemany(
            "INSERT INTO products(seller_id, category_id, name, price, stock) "
            "VALUES (%s,%s,%s,%s,%s)", products)

        orders, items, payments = [], [], []
        for oid in range(1, a.orders + 1):
            odate = rand_date(365)
            status = random.choices(ORDER_STATUS, ORDER_WEIGHTS)[0]
            total = 0
            for pid in random.sample(range(1, a.products + 1), random.randint(1, 4)):
                qty = random.randint(1, 3)
                total += qty * prices[pid]
                items.append((oid, pid, qty, prices[pid]))
            total = round(total, 2)
            orders.append((random.randint(1, a.users), odate, status, total))
            if status == "pending":
                pay_status, paid_at = "failed", None
            elif status in ("cancelled", "returned"):
                pay_status, paid_at = "refunded", odate + timedelta(minutes=5)
            else:
                pay_status, paid_at = "success", odate + timedelta(minutes=random.randint(1, 30))
            payments.append((oid, random.choice(PAY_METHODS), total, pay_status, paid_at))

        cur.executemany("INSERT INTO orders(user_id, order_date, status, total_amount) "
                        "VALUES (%s,%s,%s,%s)", orders)
        cur.executemany("INSERT INTO order_items(order_id, product_id, quantity, unit_price) "
                        "VALUES (%s,%s,%s,%s)", items)
        cur.executemany("INSERT INTO payments(order_id, method, amount, status, paid_at) "
                        "VALUES (%s,%s,%s,%s,%s)", payments)

        cur.executemany(
            "INSERT INTO reviews(user_id, product_id, rating, review_date) VALUES (%s,%s,%s,%s)",
            [(random.randint(1, a.users), random.randint(1, a.products),
              random.choices([1, 2, 3, 4, 5], [5, 7, 15, 33, 40])[0], rand_date(365))
             for _ in range(a.reviews)])

        cur.executemany(
            "INSERT INTO events(user_id, product_id, event_type, event_time) VALUES (%s,%s,%s,%s)",
            [(random.randint(1, a.users), random.randint(1, a.products),
              random.choices(["view", "add_to_cart", "remove_from_cart", "purchase"],
                             [70, 15, 5, 10])[0], rand_date(90))
             for _ in range(a.events)])

        conn.commit()
        for t in ["users", "sellers", "categories", "products", "orders",
                  "order_items", "payments", "reviews", "events"]:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"{t:12} {cur.fetchone()[0]:>8} rows")


if __name__ == "__main__":
    main()
