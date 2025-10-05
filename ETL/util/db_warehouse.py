from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.base import Base

# Import your dimension models here
from models.Dim_Riders import Dim_Rider
from models.Dim_Products import Dim_Products
from models.Dim_Users import Dim_Users
from models.Dim_Date import Dim_Date
from models.Fact_Order_Items import Fact_Order_Items

load_dotenv()

# Data Warehouse
database_warehouse_url = os.getenv("DATABASE_WAREHOUSE_URL")
if not database_warehouse_url:
    raise ValueError("DATABASE_WAREHOUSE_URL environment variable not set")
db_warehouse_engine = create_engine(
    database_warehouse_url, echo=False
)  # set to false for production
Session_db_warehouse = sessionmaker(bind=db_warehouse_engine)
Base.metadata.create_all(db_warehouse_engine)
