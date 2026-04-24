from sqlalchemy import create_engine

DB_URL = "mysql+pymysql://root:dayatiara369@localhost:3306/inventory"

engine = create_engine(DB_URL)