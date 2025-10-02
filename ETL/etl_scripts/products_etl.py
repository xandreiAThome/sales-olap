from util.db_source import products
from sqlalchemy.dialects.mysql import insert
from models.Dim_Products import Dim_Products
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
from util.logging_config import get_logger

logger = get_logger(__name__)


def extract_products():
    session = Session_db_source()
    try:
        result = session.execute(products.select())
        products_data = result.fetchall()
        logger.info(f"Extracted {len(products_data)} products from source DB.")
        return products_data
    except Exception as e:
        logger.error(f"Error extracting products: {e}")
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
        logger.info(f"Cleaned products data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logger.error(f"Error cleaning products data: {e}")
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

        if product_records:
            stmt = insert(Dim_Products).values(product_records)

            # Define update behavior if Product_ID already exists
            stmt = stmt.on_duplicate_key_update(
                Product_Code=stmt.inserted.Product_Code,
                Name=stmt.inserted.Name,
                Category=stmt.inserted.Category,
                Description=stmt.inserted.Description,
                Price=stmt.inserted.Price,
            )

            session.execute(stmt)
            session.commit()
            logger.info(
                f"Upserted {len(product_records)} products"
            )

    except Exception as e:
        logger.error(f"Error during transform/load: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
