"""
Run this once to generate sample_sales_data.csv
  python data/generate_data.py
"""
import csv, random, datetime, os

random.seed(42)

PRODUCTS = [
    ("P001", "Laptop",       "Electronics",  45000),
    ("P002", "Mobile Phone", "Electronics",  18000),
    ("P003", "Headphones",   "Electronics",   2500),
    ("P004", "Desk Chair",   "Furniture",     8000),
    ("P005", "Study Table",  "Furniture",    12000),
    ("P006", "T-Shirt",      "Clothing",       799),
    ("P007", "Jeans",        "Clothing",      1499),
    ("P008", "Rice 5kg",     "Groceries",      350),
    ("P009", "Cooking Oil",  "Groceries",      200),
    ("P010", "Notebook",     "Stationery",      60),
]

CUSTOMERS = [f"C{str(i).zfill(4)}" for i in range(1, 201)]
REGIONS   = ["North", "South", "East", "West"]

rows = []
start = datetime.date(2023, 1, 1)

for i in range(1, 5001):
    pid, pname, category, base_price = random.choice(PRODUCTS)
    qty      = random.randint(1, 10)
    discount = random.choice([0, 0, 0, 5, 10, 15])          # mostly no discount
    price    = round(base_price * (1 - discount / 100), 2)
    revenue  = round(price * qty, 2)
    sale_date = start + datetime.timedelta(days=random.randint(0, 364))

    # inject ~3 % nulls and duplicates for data-quality demo
    customer = random.choice(CUSTOMERS)
    region   = random.choice(REGIONS)
    if random.random() < 0.03:
        customer = ""          # null customer
    if random.random() < 0.02:
        rows.append(rows[-1])  # duplicate row

    rows.append({
        "order_id":    f"ORD{str(i).zfill(5)}",
        "sale_date":   sale_date.strftime("%Y-%m-%d"),
        "customer_id": customer,
        "product_id":  pid,
        "product_name":pname,
        "category":    category,
        "region":      region,
        "quantity":    qty,
        "unit_price":  price,
        "discount_pct":discount,
        "revenue":     revenue,
    })

out = os.path.join(os.path.dirname(__file__), "sample_sales_data.csv")
with open(out, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} rows → {out}")
