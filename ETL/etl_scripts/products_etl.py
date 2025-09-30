from util.db_source import products
from models.Dim_Products import dim_products
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
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
    inserted = 0
    try:
        for product in cleaned_products:
            product_record = {
                "Product_Code": product["productCode"],
                "Product_ID": product["id"],
                "Name": product["name"],
                "Category": product["category"],
                "Description": product["description"],
                "Price": product["price"],
            }
            try:
                insert_stmt = dim_products.insert().values(**product_record)
                session.execute(insert_stmt)
                inserted += 1
            except Exception as e:
                logging.error(f"Error inserting product {product_record['Product_Code']}: {e}")
        session.commit()
        logging.info(f"Inserted {inserted} products into warehouse DB.")
    except Exception as e:
        logging.error(f"Error during transform/load: {e}")
        session.rollback()
    finally:
        session.close()
