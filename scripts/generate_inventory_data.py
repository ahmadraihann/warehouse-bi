import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

# Inisialisasi Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_sql():
    print("Memulai pembuatan data sintetis...")
    
    # 1. KONFIGURASI VOLUME DATA
    num_products = 500
    num_vendors = 50
    num_stores = 20
    num_purchases = 4000
    num_sales = 12000  # Melebihi syarat 10.000 baris
    
    sql_statements = []

    # --- DDL: CREATE TABLES ---
    sql_statements.append("-- DDL: Source Tables\n")
    sql_statements.append("CREATE TABLE src_products (brand_id INT PRIMARY KEY, description VARCHAR(255), size VARCHAR(50), category VARCHAR(100), volume VARCHAR(50), current_unit_price DECIMAL(10,2), current_unit_cost DECIMAL(10,2));")
    sql_statements.append("CREATE TABLE src_vendors (vendor_id INT PRIMARY KEY, vendor_name VARCHAR(255), contact_name VARCHAR(255), address TEXT);")
    sql_statements.append("CREATE TABLE src_stores (store_id INT PRIMARY KEY, city VARCHAR(100), state VARCHAR(100), zip_code VARCHAR(20));")
    sql_statements.append("CREATE TABLE src_purchases (po_number VARCHAR(50), order_date DATE, vendor_id INT, store_id INT, brand_id INT, quantity INT, unit_cost DECIMAL(10,2));")
    sql_statements.append("CREATE TABLE src_receiving (receiving_id SERIAL PRIMARY KEY, received_date DATE, po_number VARCHAR(50), brand_id INT, qty_received INT, qty_damaged INT);")
    sql_statements.append("CREATE TABLE src_sales (receipt_id VARCHAR(50), sale_date DATE, store_id INT, brand_id INT, qty_sold INT, unit_price DECIMAL(10,2), promo_flag INT);\n")

    # --- GENERATE MASTER DATA ---
    
    # Vendors
    vendor_ids = list(range(1, num_vendors + 1))
    for v_id in vendor_ids:
        name = fake.company().replace("'", "''")
        contact = fake.name().replace("'", "''")
        address = fake.address().replace("\n", ", ").replace("'", "''")
        sql_statements.append(f"INSERT INTO src_vendors VALUES ({v_id}, '{name}', '{contact}', '{address}');")

    # Stores
    store_ids = list(range(1, num_stores + 1))
    for s_id in store_ids:
        city = fake.city().replace("'", "''")
        state = fake.state().replace("'", "''")
        zip_code = fake.zipcode()
        sql_statements.append(f"INSERT INTO src_stores VALUES ({s_id}, '{city}', '{state}', '{zip_code}');")

    # Products
    product_ids = list(range(1, num_products + 1))
    categories = ['Wine', 'Whiskey', 'Beer', 'Vodka', 'Gin', 'Rum']
    product_list = []
    for p_id in product_ids:
        desc = f"{fake.word().capitalize()} {random.choice(['Special', 'Reserve', 'Classic', 'Lite'])}".replace("'", "''")
        cat = random.choice(categories)
        size = random.choice(['750ml', '1L', '500ml', '1.75L'])
        cost = round(random.uniform(5.0, 50.0), 2)
        price = round(cost * random.uniform(1.2, 1.8), 2)
        product_list.append({'id': p_id, 'cost': cost, 'price': price})
        sql_statements.append(f"INSERT INTO src_products VALUES ({p_id}, '{desc}', '{size}', '{cat}', '{size}', {price}, {cost});")

    # --- GENERATE TRANSACTIONAL DATA ---

    # Purchases & Receiving
    po_list = []
    for i in range(num_purchases):
        po_num = f"PO-{1000 + i}"
        order_date = fake.date_between(start_date='-1y', end_date='today')
        v_id = random.choice(vendor_ids)
        s_id = random.choice(store_ids)
        p = random.choice(product_list)
        qty = random.randint(10, 100)
        
        sql_statements.append(f"INSERT INTO src_purchases VALUES ('{po_num}', '{order_date}', {v_id}, {s_id}, {p['id']}, {qty}, {p['cost']});")
        
        # Simulasi Receiving (Diterima 2-5 hari kemudian)
        received_date = order_date + timedelta(days=random.randint(2, 5))
        # Simulasi Outlier: Kadang barang datang kurang atau ada yang rusak
        qty_received = qty - random.randint(0, 2) if random.random() > 0.9 else qty
        qty_damaged = random.randint(1, 3) if random.random() > 0.95 else 0
        
        sql_statements.append(f"INSERT INTO src_receiving (received_date, po_number, brand_id, qty_received, qty_damaged) VALUES ('{received_date}', '{po_num}', {p['id']}, {qty_received}, {qty_damaged});")

    # Sales (Target > 10.000 baris)
    for i in range(num_sales):
        rcpt_id = f"T-{100000 + i}"
        sale_date = fake.date_between(start_date='-1y', end_date='today')
        
        # Simulasi Seasonal Outlier: Volume Desember naik 3x lipat
        if sale_date.month == 12:
            loop_count = 3
        else:
            loop_count = 1
            
        for _ in range(loop_count):
            s_id = random.choice(store_ids)
            p = random.choice(product_list)
            qty = random.randint(1, 5)
            promo = 1 if random.random() > 0.85 else 0
            price = p['price'] * 0.9 if promo == 1 else p['price']
            
            sql_statements.append(f"INSERT INTO src_sales VALUES ('{rcpt_id}', '{sale_date}', {s_id}, {p['id']}, {qty}, {round(price, 2)}, {promo});")

    # --- WRITE TO FILE ---
    with open("inventory_setup.sql", "w") as f:
        f.write("\n".join(sql_statements))
    
    print(f"Berhasil! File 'inventory_setup.sql' telah dibuat dengan total {len(sql_statements)} perintah SQL.")

if __name__ == "__main__":
    generate_sql()