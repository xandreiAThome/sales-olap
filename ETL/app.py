from dotenv import load_dotenv
from etl_scripts.products_etl import transform_and_load_products
from etl_scripts.users_etl import transform_and_load_users
from util.db_source import db_source_engine
from util.db_warehouse import db_warehouse_engine, Session_db_warehouse
from sqlalchemy import text
from etl_scripts.rider_etl import transform_and_load_riders
from etl_scripts.order_date_etl import load_transform_date_and_order_items
import time

load_dotenv()


def main():
    start_time = time.time()
    with db_source_engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        print("Connected! Server time:", result.scalar())

    with db_warehouse_engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        print("Connected to Data Warehouse! Server time:", result.scalar())

    # Run the ETL process for riders
    transform_and_load_riders()
    transform_and_load_products()
    transform_and_load_users()
    load_transform_date_and_order_items()

    # show all the transformed data in the Dim_Riders table
    session = Session_db_warehouse()
    result_riders = session.execute(text("SELECT * FROM dim_riders LIMIT 10"))
    for row in result_riders:
        print(row)

    result_products = session.execute(text("SELECT * FROM dim_products LIMIT 10"))
    for product in result_products:
        print(product)

    result_users = session.execute(text("SELECT * FROM dim_users LIMIT 10"))
    for user in result_users:
        print(user)
    result_date = session.execute(text("SELECT * FROM dim_date LIMIT 10"))
    for date in result_date:
        print(date)
    result_order_items = session.execute(
        text("SELECT * FROM fact_order_items LIMIT 10")
    )
    for o in result_order_items:
        print(o)

    session.close()

    end_time = time.time()
    print(f"Total running time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
