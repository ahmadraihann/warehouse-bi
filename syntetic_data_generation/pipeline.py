import pandas as pd
from db import engine
import argparse
from sqlalchemy import text
from datetime import datetime
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fresh", action="store_true", help="Truncate all tables before load")
    return parser.parse_args()

def truncate_tables():
    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))

        conn.execute(text("TRUNCATE TABLE inventories"))
        conn.execute(text("TRUNCATE TABLE sales"))
        conn.execute(text("TRUNCATE TABLE purchase_items"))
        conn.execute(text("TRUNCATE TABLE purchases"))
        conn.execute(text("TRUNCATE TABLE stores"))
        conn.execute(text("TRUNCATE TABLE vendors"))
        conn.execute(text("TRUNCATE TABLE products"))

        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()

# =========================
# 🔹 LOAD DIMENSIONS
# =========================

def load_products():
    df = pd.read_csv(os.path.join(BASE_DIR, "data", "2017PurchasePricesDec.csv"))

    df = df.rename(columns={
        "Brand": "product_id",
        "Description": "description",
        "Size": "size",
        "Volume": "volume",
        "Classification": "classification"
    })

    df = df[[
        "product_id",
        "description",
        "size",
        "volume",
        "classification"
    ]]

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    df = df.drop_duplicates(subset=["product_id"])

    df.to_sql("products", engine, if_exists="append", index=False)

def load_vendors():
    import pandas as pd
    from db import engine

    df = pd.read_csv(os.path.join(BASE_DIR, "data", "PurchasesFINAL12312016.csv"))
    df.columns = df.columns.str.strip()

    vendors = df[["VendorNumber", "VendorName"]].copy()
    vendors["VendorName"] = vendors["VendorName"].str.strip()

    vendors.columns = ["vendor_id", "vendor_name"]
    vendors = vendors.drop_duplicates(subset=["vendor_id"])

    vendors.to_sql("vendors", engine, if_exists="append", index=False)

def load_stores():
    df = pd.read_csv(os.path.join(BASE_DIR, "data", "EndInvFINAL12312016.csv"))

    stores = df[["Store", "City"]].drop_duplicates()
    stores["City"] = stores["City"].str.strip()
    stores.columns = ["store_id", "city"]
    stores = stores.drop_duplicates(subset=["store_id"])

    stores.to_sql("stores", engine, if_exists="append", index=False)

# =========================
# 🔹 PURCHASES
# =========================

def load_purchases():
    df = pd.read_csv(os.path.join(BASE_DIR, "data", "InvoicePurchases12312016.csv"))

    purchases = df[[
        "VendorNumber", "PONumber", "PODate", "InvoiceDate", "PayDate"
    ]].drop_duplicates()

    purchases.columns = [
        "vendor_id", "po_number", "po_date", "invoice_date", "pay_date"
    ]

    purchases.to_sql("purchases", engine, if_exists="append", index=False)

def load_purchase_items():
    df = pd.read_csv(os.path.join(BASE_DIR, "data", "PurchasesFINAL12312016.csv"))

    items = df[[
        "Store", "Brand", "PONumber", "Quantity", "PurchasePrice", "Dollars", "ReceivingDate"
    ]]

    items.columns = [
        "store_id", "product_id", "po_number", "quantity", "purchase_price", "total_amount", "receiving_date"
    ]

    # 🔥 JOIN ke purchases untuk dapat purchase_id
    mapping = pd.read_sql(
        "SELECT purchase_id, po_number FROM purchases",
        engine
    )

    items = items.merge(mapping, on="po_number", how="inner")

    # 🔥 ambil hanya kolom yang sesuai schema
    items = items[[
        "purchase_id", "store_id", "product_id", "quantity",
        "purchase_price", "total_amount", "receiving_date"
    ]]

    items.to_sql("purchase_items", engine, if_exists="append", index=False)

# =========================
# 🔹 SALES
# =========================

def load_sales():
    df = pd.read_csv(os.path.join(BASE_DIR, "data", "SalesFINAL12312016.csv"))

    sales = df[[
        "Store", "Brand", "SalesDate", "SalesQuantity", "SalesPrice", "SalesDollars", "ExciseTax"
    ]]

    sales.columns = [
        "store_id", "product_id", "sales_date", "quantity", "sales_price", "total_amount", "excise_tax"
    ]

    for idx, row in sales.iterrows():
        sales.at[idx, "sales_date"] = datetime.strptime(
            row["sales_date"], "%m/%d/%Y"
        ).date()

    sales.to_sql("sales", engine, if_exists="append", index=False)

# =========================
# 🔹 INVENTORIES
# =========================

def load_inventories():
    query = """
    INSERT INTO inventories (store_id, product_id, quantity, last_updated)
    SELECT 
        store_id,
        product_id,
        SUM(delta_qty) AS quantity,
        MAX(last_updated) AS last_updated
    FROM (
        SELECT 
            store_id,
            product_id,
            quantity AS delta_qty,
            receiving_date AS last_updated
        FROM purchase_items

        UNION ALL

        SELECT 
            store_id,
            product_id,
            -quantity AS delta_qty,
            sales_date AS last_updated
        FROM sales
    ) t
    GROUP BY store_id, product_id
    ON DUPLICATE KEY UPDATE
        quantity = VALUES(quantity),
        last_updated = VALUES(last_updated);
    """

    with engine.begin() as conn:
        conn.execute(text(query))   # 🔥 FIX DI SINI

# =========================
# 🚀 RUN PIPELINE
# =========================

def run():
    args = parse_args()

    if args.fresh:
        print("⚠️ Fresh mode ON: clearing tables...")
        truncate_tables()
        
    print("Loading products...")
    load_products()

    print("Loading vendors...")
    load_vendors()

    print("Loading stores...")
    load_stores()

    print("Loading purchases...")
    load_purchases()

    print("Loading purchase_items...")
    load_purchase_items()

    print("Loading sales...")
    load_sales()

    print("Loading inventory...")
    load_inventories()

    print("DONE ✅")


if __name__ == "__main__":
    run()