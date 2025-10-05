from util.db_source import products
from sqlalchemy.dialects.mysql import insert
from models.Dim_Products import Dim_Products
import pandas as pd
from util.db_source import Session_db_source
from contextlib import contextmanager
from util.db_warehouse import Session_db_warehouse
from util.logging_config import get_logger
import itertools
import os
import gc

BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 2000)

logger = get_logger(__name__)


@contextmanager
def extract_products_stream():
    """Context-managed stream of products rows from source DB as mappings."""
    session = Session_db_source()
    result = None
    try:
        result = session.execute(
            products.select().execution_options(stream_results=True)
        ).mappings()
        yield result
    except Exception as e:
        logger.error(f"Error streaming products: {e}")
        return
    finally:
        try:
            if result is not None:
                result.close()
        except Exception:
            pass
        session.close()


def clean_products_data(data):
    try:
        # Accept either a pandas DataFrame or an iterable of row mappings
        if isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            df = pd.DataFrame([dict(r) for r in data])

        df.dropna(inplace=True)

        df["productCode"] = df["productCode"].str.strip()
        df["category"] = df["category"].str.lower().str.strip()
        df["description"] = df["description"].str.strip()
        df["name"] = df["name"].str.title().str.strip()

        df = df.rename(
            columns={
                "productCode": "Product_Code",
                "id": "Product_ID",
                "name": "Name",
                "category": "Category",
                "description": "Description",
                "price": "Price",
            }
        )
        df.drop(columns=["createdAt", "updatedAt"], errors="ignore", inplace=True)

        cleaned_data = df.to_dict(orient="records")
        # logger.info(f"Cleaned products data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logger.error(f"Error cleaning products data: {e}")
        return []


def transform_and_load_products():
    # Stream-extract -> chunk -> clean -> upsert per chunk
    conn_session = Session_db_warehouse()
    total_inserted = 0
    try:
        with extract_products_stream() as prod_iter:
            while True:
                chunk_rows = list(itertools.islice(prod_iter, BATCH_SIZE))
                if not chunk_rows:
                    break

                records = clean_products_data(chunk_rows)

                if records:
                    stmt = insert(Dim_Products).values(records)
                    stmt = stmt.on_duplicate_key_update(
                        Product_Code=stmt.inserted.Product_Code,
                        Name=stmt.inserted.Name,
                        Category=stmt.inserted.Category,
                        Description=stmt.inserted.Description,
                        Price=stmt.inserted.Price,
                    )
                    conn_session.execute(stmt)
                    conn_session.commit()
                    total_inserted += len(records)

                # cleanup chunk
                del records, chunk_rows
                gc.collect()

        logger.info(f"Upserted {total_inserted} products in batches of {BATCH_SIZE}")

    except Exception as e:
        logger.error(f"Error during transform/load: {e}", exc_info=True)
        conn_session.rollback()
        raise
    finally:
        conn_session.close()
