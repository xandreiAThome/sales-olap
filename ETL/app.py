from dotenv import load_dotenv
import os
from util.db_source import db_source_engine, Session_db_source, metadata_source
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
    transform_and_load_riders()

    # show all the transformed data in the Dim_Riders table
    session = Session_db_warehouse()
    result = session.execute(text("SELECT * FROM dim_riders"))
    for row in result:
        print(row)
    session.close()


if __name__ == "__main__":
    main()
