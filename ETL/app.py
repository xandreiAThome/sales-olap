from dotenv import load_dotenv
from etl_scripts.products_etl import transform_and_load_products
from util.db_source import db_source_engine
from util.db_warehouse import db_warehouse_engine, Session_db_warehouse
from sqlalchemy import text
from etl_scripts.rider_etl import transform_and_load_riders

load_dotenv()


def main():
    with db_source_engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        print("Connected! Server time:", result.scalar())

    with db_warehouse_engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        print("Connected to Data Warehouse! Server time:", result.scalar())

    # Run the ETL process for riders
    # transform_and_load_riders()
    # transform_and_load_products()

    # show all the transformed data in the Dim_Riders table
    session = Session_db_warehouse()
    result_riders = session.execute(text("SELECT * FROM dim_riders"))
    for row in result_riders:
        print(row)

    result_products = session.execute(text("SELECT * FROM dim_products"))
    for product in result_products:
        print(product)

    session.close()


if __name__ == "__main__":
    main()
