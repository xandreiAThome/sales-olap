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
users = metadata_source.tables["Users"]
orders = metadata_source.tables["Orders"]
orderitems = metadata_source.tables["OrderItems"]
products = metadata_source.tables["Products"]
couriers = metadata_source.tables["Couriers"]
riders = metadata_source.tables["Riders"]
