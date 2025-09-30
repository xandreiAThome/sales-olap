from util.db_source import products
from sqlalchemy import select
from models.Dim_Products import Dim_Products
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)


def extract_products():
    session = Session_db_source()
    try:
        result = session.execute(products.select())
        products_data = result.fetchall()
        logging.info(f"Extracted {len(products_data)} products from source DB.")
        return products_data
    except Exception as e:
        logging.error(f"Error extracting products: {e}")
        return []
    finally:
        session.close()


def clean_products_data(data):
    try:
        df = pd.DataFrame([dict(row._mapping) for row in data])
        df.dropna(inplace=True)

        df["productCode"] = df["productCode"].str.strip()
        df["category"] = df["category"].str.lower().str.strip()
        df["description"] = df["description"].str.strip()
        df["name"] = df["name"].str.title().str.strip()

        cleaned_data = df.to_dict(orient="records")
        logging.info(f"Cleaned products data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logging.error(f"Error cleaning products data: {e}")
        return []


def transform_and_load_products():
    products_data = extract_products()
    cleaned_products = clean_products_data(products_data)

    session = Session_db_warehouse()
    try:
        product_records = [
            {
                "Product_Code": product["productCode"],
                "Product_ID": product["id"],
                "Name": product["name"],
                "Category": product["category"],
                "Description": product["description"],
                "Price": product["price"],
            }
            for product in cleaned_products
        ]

        # --- Hybrid UPSERT logic ---
        existing_ids = set(
            row[0] for row in session.execute(select(Dim_Products.Product_ID)).all()
        )

        new_records = [
            r for r in product_records if r["Product_ID"] not in existing_ids
        ]
        update_records = [r for r in product_records if r["Product_ID"] in existing_ids]

        if new_records:
            session.bulk_insert_mappings(Dim_Products, new_records)
            logging.info(f"Inserted {len(new_records)} new products.")

        if update_records:
            session.bulk_update_mappings(Dim_Products, update_records)
            logging.info(f"Updated {len(update_records)} existing products.")

        session.commit()

    except Exception as e:
        logging.error(f"Error during transform/load: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
