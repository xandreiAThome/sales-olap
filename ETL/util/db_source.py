from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

load_dotenv()

# Source Database
database_source_url = os.getenv("DATABASE_SOURCE_URL")
db_source_engine = create_engine(database_source_url, echo=True)
Session_db_source = sessionmaker(bind=db_source_engine)

metadata_source = MetaData()
metadata_source.reflect(bind=db_source_engine)

# Reflect tables
users = metadata_source.tables["users"]
orders = metadata_source.tables["orders"]
orderitems = metadata_source.tables["orderitems"]
products = metadata_source.tables["products"]
couriers = metadata_source.tables["couriers"]
riders = metadata_source.tables["riders"]
