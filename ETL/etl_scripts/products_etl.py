from util.db_source import products
from sqlalchemy.dialects.mysql import insert
from models.Dim_Products import Dim_Products
from sqlalchemy import select, func, case
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
   
    session = Session_db_source()
    result = None
    try:
        # MySQL title case: CONCAT(UPPER(SUBSTRING(col, 1, 1)), LOWER(SUBSTRING(col, 2)))
        def title_case(column):
            return func.concat(
                func.upper(func.substring(column, 1, 1)),
                func.lower(func.substring(column, 2))
            )
        
        # SQL query with data cleaning transformations
        stmt = select(
            products.c.id.label("Product_ID"),
            func.trim(products.c.productCode).label("Product_Code"),
            func.trim(title_case(products.c.name)).label("Name"),
            # Category normalization using CASE statement
            func.lower(
                case(
                    (func.lower(products.c.category).in_(['toy', 'toys']), 'toys'),
                    (func.lower(products.c.category).in_(['makeup', 'make up']), 'makeup'),
                    (func.lower(products.c.category).in_(['bag', 'bags']), 'bags'),
                    (func.lower(products.c.category).in_(['electronics', 'gadgets', 'laptops']), 'electronics'),
                    (func.lower(products.c.category).in_(['men\'s apparel', 'clothes']), 'apparel'),
                    else_=func.trim(products.c.category)
                )
            ).label("Category"),
            func.trim(products.c.description).label("Description"),
            products.c.price.label("Price"),
        ).where(
            # Filter out rows with NULL in required fields
            products.c.id.isnot(None),
            products.c.productCode.isnot(None),
            products.c.name.isnot(None),
            products.c.category.isnot(None),
            products.c.description.isnot(None),
            products.c.price.isnot(None),
        ).execution_options(stream_results=True, yield_per=BATCH_SIZE)
        
        result = session.execute(stmt).mappings()
        yield result
    except Exception as e:
        logger.error(f"Error streaming products: {e}")
        raise
    finally:
        # Ensure result is properly closed before session
        if result is not None:
            try:
                result.close()
            except Exception as e:
                logger.warning(f"Error closing result: {e}")
        # Close session after result
        try:
            session.close()
        except Exception as e:
            logger.warning(f"Error closing session: {e}")


def transform_and_load_products():
    """
    Stream-extract -> chunk -> load directly to warehouse.
    Data cleaning is done in SQL during extraction.
    """
    conn_session = Session_db_warehouse()
    total_inserted = 0
    try:
        with extract_products_stream() as prod_iter:
            while True:
                chunk_rows = list(itertools.islice(prod_iter, BATCH_SIZE))
                if not chunk_rows:
                    break

                # Convert mappings to list of dicts (already cleaned by SQL)
                records = [dict(row) for row in chunk_rows]

                if records:
                    # Use INSERT IGNORE to skip duplicates (faster than ON DUPLICATE KEY UPDATE)
                    stmt = insert(Dim_Products).prefix_with("IGNORE").values(records)
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
