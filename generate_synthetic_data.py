import os
import random
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

# Setup
fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_STORES = 5
NUM_VENDORS = 10
NUM_PRODUCTS = 20
NUM_PURCHASES = 50
NUM_SALES = 200
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2023, 3, 31)

# Ensure output directory exists
output_dir = "data/raw"
os.makedirs(output_dir, exist_ok=True)

print("Generating dimensions...")

# 1. Generate Stores
stores_data = []
for i in range(1, NUM_STORES + 1):
    stores_data.append({
        "store_id": i,
        "city": fake.city()
    })
df_stores = pd.DataFrame(stores_data)

# 2. Generate Vendors
vendors_data = []
for i in range(1, NUM_VENDORS + 1):
    vendors_data.append({
        "vendor_id": i,
        "vendor_name": fake.company()
    })
df_vendors = pd.DataFrame(vendors_data)

# 3. Generate Products
classifications = [1, 2] # 1: Standard, 2: Premium
sizes = ["750mL", "1000mL", "1.75L", "375mL"]
products_data = []
for i in range(1, NUM_PRODUCTS + 1):
    size = random.choice(sizes)
    vol = 750 if size == "750mL" else (1000 if size == "1000mL" else (1750 if size == "1.75L" else 375))
    products_data.append({
        "product_id": i,
        "description": fake.catch_phrase(),
        "size": size,
        "volume": vol,
        "classification": random.choice(classifications)
    })
df_products = pd.DataFrame(products_data)

print("Generating transactions...")

# Date generator helper
def random_date(start, end):
    delta = end - start
    random_days = random.randrange(delta.days)
    return start + timedelta(days=random_days)

# 4. Generate Purchases (Header)
purchases_data = []
for i in range(1, NUM_PURCHASES + 1):
    po_date = random_date(START_DATE, END_DATE - timedelta(days=10))
    invoice_date = po_date + timedelta(days=random.randint(1, 5))
    pay_date = invoice_date + timedelta(days=random.randint(5, 30))
    
    purchases_data.append({
        "purchase_id": i,
        "vendor_id": random.choice(df_vendors["vendor_id"]),
        "po_number": f"PO-{fake.unique.random_number(digits=5)}",
        "po_date": po_date.strftime("%Y-%m-%d"),
        "invoice_date": invoice_date.strftime("%Y-%m-%d"),
        "pay_date": pay_date.strftime("%Y-%m-%d")
    })
df_purchases = pd.DataFrame(purchases_data)

# 5. Generate Purchase Items (Details)
purchase_items_data = []
purchase_item_id = 1
for _, purchase in df_purchases.iterrows():
    # Each PO has 1 to 5 items
    num_items = random.randint(1, 5)
    po_date = datetime.strptime(purchase["po_date"], "%Y-%m-%d")
    receiving_date = po_date + timedelta(days=random.randint(3, 10))
    
    for _ in range(num_items):
        qty = random.randint(10, 100)
        price = round(random.uniform(10.0, 50.0), 2)
        total_amount = round(qty * price, 2)
        
        purchase_items_data.append({
            "purchase_item_id": purchase_item_id,
            "purchase_id": purchase["purchase_id"],
            "product_id": random.choice(df_products["product_id"]),
            "store_id": random.choice(df_stores["store_id"]),
            "quantity": qty,
            "purchase_price": price,
            "total_amount": total_amount,
            "receiving_date": receiving_date.strftime("%Y-%m-%d")
        })
        purchase_item_id += 1
df_purchase_items = pd.DataFrame(purchase_items_data)

# 6. Generate Sales
sales_data = []
for i in range(1, NUM_SALES + 1):
    sales_date = random_date(START_DATE + timedelta(days=15), END_DATE)
    qty = random.randint(1, 5)
    price = round(random.uniform(20.0, 100.0), 2) # Sales price usually higher than purchase
    total_amount = round(qty * price, 2)
    excise_tax = round(total_amount * 0.05, 2) # 5% tax
    
    sales_data.append({
        "sales_id": i,
        "store_id": random.choice(df_stores["store_id"]),
        "product_id": random.choice(df_products["product_id"]),
        "sales_date": sales_date.strftime("%Y-%m-%d"),
        "quantity": qty,
        "sales_price": price,
        "total_amount": total_amount,
        "excise_tax": excise_tax
    })
df_sales = pd.DataFrame(sales_data)

print("Calculating inventory...")

# 7. Generate Inventories
# We calculate inventory based on sum of purchases minus sum of sales for each store-product combo
inventory_map = {}

# Add purchases to inventory
for _, item in df_purchase_items.iterrows():
    key = (item["store_id"], item["product_id"])
    if key not in inventory_map:
        inventory_map[key] = 0
    inventory_map[key] += item["quantity"]

# Subtract sales from inventory
for _, sale in df_sales.iterrows():
    key = (sale["store_id"], sale["product_id"])
    if key not in inventory_map:
        inventory_map[key] = 0
    inventory_map[key] -= sale["quantity"]

inventories_data = []
inventory_id = 1
for (store_id, product_id), qty in inventory_map.items():
    # If inventory goes negative, reset to 0 (just for synthetic data simplicity)
    qty = max(0, qty)
    inventories_data.append({
        "inventory_id": f"{store_id}_{product_id}",
        "store_id": store_id,
        "product_id": product_id,
        "quantity": qty,
        "last_updated": END_DATE.strftime("%Y-%m-%d")
    })
    inventory_id += 1
df_inventories = pd.DataFrame(inventories_data)

print("Exporting to CSV...")

# Export to CSV
df_stores.to_csv(f"{output_dir}/stores.csv", index=False)
df_vendors.to_csv(f"{output_dir}/vendors.csv", index=False)
df_products.to_csv(f"{output_dir}/products.csv", index=False)
df_purchases.to_csv(f"{output_dir}/purchases.csv", index=False)
df_purchase_items.to_csv(f"{output_dir}/purchase_items.csv", index=False)
df_sales.to_csv(f"{output_dir}/sales.csv", index=False)
df_inventories.to_csv(f"{output_dir}/inventories.csv", index=False)

print(f"Data generation complete! Files saved in {output_dir}/")
