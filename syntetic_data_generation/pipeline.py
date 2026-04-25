import pandas as pd
import duckdb
import os
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fresh", action="store_true")
    return parser.parse_args()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "../dbt_transformasi_dan_pemodelan_data", "inventory.duckdb")

def drop_tables():
    con = duckdb.connect(DB_PATH)

    tables = [
        "purchase_items",
        "sales",
        "purchases",
        "stores",
        "vendors",
        "products"
    ]

    for t in tables:
        try:
            con.execute(f"DROP TABLE IF EXISTS {t}")
            print(f"Dropped {t}")
        except Exception as e:
            print(f"Skip {t}: {e}")

    con.close()

def create_tables():
    con = duckdb.connect(DB_PATH)

    # =====================
    # PRODUCTS
    # =====================
    con.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        description TEXT,
        size TEXT,
        volume INTEGER,
        classification INTEGER
    )
    """)

    # =====================
    # VENDORS
    # =====================
    con.execute("""
    CREATE TABLE IF NOT EXISTS vendors (
        vendor_id INTEGER PRIMARY KEY,
        vendor_name TEXT
    )
    """)

    # =====================
    # STORES
    # =====================
    con.execute("""
    CREATE TABLE IF NOT EXISTS stores (
        store_id INTEGER PRIMARY KEY,
        city TEXT
    )
    """)

    # =====================
    # PURCHASES (HEADER)
    # =====================
    con.execute("""
    CREATE TABLE IF NOT EXISTS purchases (
        purchase_id INTEGER PRIMARY KEY,
        vendor_id INTEGER,
        po_number INTEGER,
        po_date DATE,
        invoice_date DATE,
        pay_date DATE
    )
    """)

    # =====================
    # PURCHASE ITEMS (FACT)
    # =====================
    con.execute("""
    CREATE TABLE IF NOT EXISTS purchase_items (
        purchase_item_id INTEGER PRIMARY KEY,
        purchase_id INTEGER,
        store_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        purchase_price DOUBLE,
        total_amount DOUBLE,
        receiving_date DATE
    )
    """)

    # =====================
    # SALES
    # =====================
    con.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        sales_id INTEGER PRIMARY KEY,
        store_id INTEGER,
        product_id INTEGER,
        sales_date DATE,
        quantity INTEGER,
        sales_price DOUBLE,
        total_amount DOUBLE,
        excise_tax DOUBLE
    )
    """)

    # =====================
    # INVENTORY SNAPSHOT
    # =====================
    con.execute("""
    CREATE TABLE IF NOT EXISTS inventories (
        store_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        last_updated DATE,
        PRIMARY KEY (store_id, product_id)
    )
    """)

    con.close()

def load_products():
    con = duckdb.connect(DB_PATH)

    # =========================
    # 1. READ SOURCE
    # =========================
    df = pd.read_csv(
        os.path.join(BASE_DIR, "data", "2017PurchasePricesDec.csv")
    )

    # =========================
    # 2. CLEAN + RENAME
    # =========================
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

    # cleaning numeric issue ("Unknown", etc)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["classification"] = pd.to_numeric(df["classification"], errors="coerce")

    df = df.drop_duplicates(subset=["product_id"])

    # =========================
    # 3. SURROGATE KEY
    # =========================
    df = df.sort_values("product_id")

    # reorder columns
    df = df[[
        "product_id",
        "description",
        "size",
        "volume",
        "classification"
    ]]

    # =========================
    # 4. LOAD TO DUCKDB
    # =========================
    con.register("df", df)

    con.execute("""
    INSERT INTO products
    SELECT * FROM df
    """)

    con.close()

    print("✅ products loaded")

def load_vendors():
    con = duckdb.connect(DB_PATH)

    df = pd.read_csv("data/PurchasesFINAL12312016.csv")
    df.columns = df.columns.str.strip()

    df = df[["VendorNumber", "VendorName"]].copy()
    df = df.drop_duplicates(subset=["VendorNumber"])

    df = df.rename(columns={
        "VendorNumber": "vendor_id",
        "VendorName": "vendor_name"
    })

    df = df.sort_values("vendor_id")

    df = df[["vendor_id","vendor_name"]]

    con.register("df", df)
    con.execute("INSERT INTO vendors SELECT * FROM df")

    con.close()

    print("✅ vendors loaded")

def load_stores():
    con = duckdb.connect(DB_PATH)

    df = pd.read_csv("data/EndInvFINAL12312016.csv")

    df = df[["Store","City"]].drop_duplicates()

    df = df.rename(columns={
        "Store": "store_id",
        "City": "city"
    })

    df["city"] = df["city"].str.strip()

    df = df.sort_values("store_id")

    df = df[["store_id","city"]]

    con.register("df", df)
    con.execute("INSERT INTO stores SELECT * FROM df")

    con.close()

    print("✅ stores loaded")

def load_purchases():
    con = duckdb.connect(DB_PATH)

    df = pd.read_csv("data/InvoicePurchases12312016.csv")

    df = df[[
        "VendorNumber","PONumber","PODate","InvoiceDate","PayDate"
    ]].drop_duplicates()

    df = df.rename(columns={
        "VendorNumber": "vendor_id",
        "PONumber": "po_number",
        "PODate": "po_date",
        "InvoiceDate": "invoice_date",
        "PayDate": "pay_date"
    })

    vendors = con.execute("SELECT vendor_id FROM vendors").df()

    df = df.merge(vendors, on="vendor_id", how="inner")

    df = df.sort_values("po_number")
    df["purchase_id"] = range(1, len(df) + 1)

    df = df[[
        "purchase_id","vendor_id","po_number",
        "po_date","invoice_date","pay_date"
    ]]

    con.register("df", df)
    con.execute("INSERT INTO purchases SELECT * FROM df")

    con.close()

    print("✅ purchases loaded")

def load_purchase_items():
    con = duckdb.connect(DB_PATH)

    df = pd.read_csv("data/PurchasesFINAL12312016.csv")

    df = df.rename(columns={
        "Store": "store_id",
        "Brand": "product_id",
        "PONumber": "po_number"
    })

    purchases = con.execute("SELECT purchase_id, po_number FROM purchases").df()
    stores = con.execute("SELECT store_id FROM stores").df()
    products = con.execute("SELECT product_id FROM products").df()

    df = df.merge(purchases, on="po_number", how="inner")
    df = df.merge(stores, on="store_id", how="inner")
    df = df.merge(products, on="product_id", how="inner")

    df = df.rename(columns={
        "Quantity": "quantity",
        "PurchasePrice": "purchase_price",
        "Dollars": "total_amount",
        "ReceivingDate": "receiving_date"
    })

    df["purchase_item_id"] = range(1, len(df) + 1)

    df = df[[
        "purchase_item_id","purchase_id",
        "store_id","product_id",
        "quantity","purchase_price",
        "total_amount","receiving_date"
    ]]

    con.register("df", df)
    con.execute("INSERT INTO purchase_items SELECT * FROM df")

    con.close()

    print("✅ purchase_items loaded")

def load_sales():
    con = duckdb.connect(DB_PATH)

    df = pd.read_csv("data/SalesFINAL12312016.csv")

    df["SalesDate"] = pd.to_datetime(
        df["SalesDate"],
        format="%m/%d/%Y",
        errors="coerce"
    ).dt.date

    df = df.rename(columns={
        "Store": "store_id",
        "Brand": "product_id",
        "SalesDate": "sales_date",
        "SalesQuantity": "quantity",
        "SalesPrice": "sales_price",
        "SalesDollars": "total_amount",
        "ExciseTax": "excise_tax"
    })

    df["sales_id"] = range(1, len(df) + 1)

    df = df[[
        "sales_id","store_id","product_id",
        "sales_date","quantity",
        "sales_price","total_amount","excise_tax"
    ]]

    con.register("df", df)
    con.execute("INSERT INTO sales SELECT * FROM df")

    con.close()

    print("✅ sales loaded")

if __name__ == "__main__":
    args = parse_args()

    if args.fresh:
        print("🔥 FRESH MODE ON - Dropping all tables...")
        drop_tables()

    create_tables()
    load_products()
    load_vendors()
    load_stores()
    load_purchases()
    load_purchase_items()
    load_sales()
    print("Schema created successfully")