from dotenv import load_dotenv
import os
from db import engine, Session, metadata
from sqlalchemy import text

load_dotenv()


def main():

    with engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        print("Connected! Server time:", result.scalar())

    # Reflect tables
    users = metadata.tables["users"]
    orders = metadata.tables["orders"]
    orderitems = metadata.tables["orderitems"]
    products = metadata.tables["products"]
    couriers = metadata.tables["couriers"]
    riders = metadata.tables["riders"]


if __name__ == "__main__":
    main()
