from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.base import Base

# Import your dimension models here
from models.Dim_Riders import DimRider

load_dotenv()

# Data Warehouse
database_warehouse_url = os.getenv("DATABASE_WAREHOUSE_URL")
db_warehouse_engine = create_engine(database_warehouse_url, echo=True)
Session_db_warehouse = sessionmaker(bind=db_warehouse_engine)
Base.metadata.create_all(db_warehouse_engine)
