from util.db_source import products
from models.Dim_Products import Dim_Products
from sqlalchemy import select, func, case, text
from util.db_source import Session_db_source
from contextlib import contextmanager
from util.db_warehouse import db_warehouse_engine
from util.logging_config import get_logger
import os
import io
import csv

BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 50000)  # Larger batches for PostgreSQL

logger = get_logger(__name__)


@contextmanager
def extract_products_stream():
   
    session = Session_db_source()
    result = None
    try:
        # PostgreSQL/SQLAlchemy title case: CONCAT(UPPER(SUBSTRING(col, 1, 1)), LOWER(SUBSTRING(col, 2)))
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
    Load products using PostgreSQL COPY for maximum speed.
    Truncates table first for full reload.
    """
    source_session = Session_db_source()
    conn = db_warehouse_engine.connect()
    
    try:
        logger.info(f"Extracting products from source database...")
        
        # Simple fast query - no complex SQL transformations
        stmt = select(
            products.c.id,
            products.c.productCode,
            products.c.name,
            products.c.category,
            products.c.description,
            products.c.price,
        ).where(
            # Basic NULL filtering
            products.c.id.isnot(None),
            products.c.productCode.isnot(None),
            products.c.name.isnot(None),
            products.c.category.isnot(None),
            products.c.description.isnot(None),
            products.c.price.isnot(None),
        )
        
        # Fetch ALL rows at once - much faster for small dimension tables
        result = source_session.execute(stmt).fetchall()
        logger.info(f"Fetched {len(result)} products from source")
        
        # Transform ALL rows in Python
        logger.info("Transforming data in Python...")
        
        # Category normalization mapping
        category_map = {
            'toy': 'toys', 'toys': 'toys',
            'makeup': 'makeup', 'make up': 'makeup',
            'bag': 'bags', 'bags': 'bags',
            'electronics': 'electronics', 'gadgets': 'electronics', 'laptops': 'electronics',
            "men's apparel": 'apparel', 'clothes': 'apparel',
        }
        
        records = []
        for row in result:
            # Normalize category
            cat_lower = row.category.strip().lower() if row.category else ''
            normalized_cat = category_map.get(cat_lower, cat_lower)
            
            records.append({
                "Product_ID": row.id,
                "Product_Code": row.productCode.strip() if row.productCode else None,
                "Name": row.name.strip().title() if row.name else None,
                "Category": normalized_cat,
                "Description": row.description.strip() if row.description else None,
                "Price": row.price,
            })
        
        logger.info(f"Transformed {len(records)} records")
        
        # Single transaction
        with conn.begin():
            # Truncate for full reload
            logger.info("Truncating table for full reload...")
            conn.execute(text(f"TRUNCATE TABLE {Dim_Products.__tablename__} CASCADE"))
            logger.info("Table truncated")
        
            # Use PostgreSQL COPY for maximum speed
            if records:
                logger.info(f"Using PostgreSQL COPY for {len(records)} products...")
                
                # Create CSV in memory
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                
                # Write rows in the correct column order
                for record in records:
                    writer.writerow([
                        record['Product_ID'],
                        record['Product_Code'],
                        record['Name'],
                        record['Category'],
                        record['Description'],
                        record['Price'],
                    ])
                
                # Reset buffer to start
                csv_buffer.seek(0)
                
                # Use raw connection for COPY
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                
                # PostgreSQL COPY - fastest bulk load method
                cursor.copy_expert(
                    f"""
                    COPY {Dim_Products.__tablename__} (
                        "Product_ID", "Product_Code", "Name", "Category",
                        "Description", "Price"
                    ) FROM STDIN WITH CSV
                    """,
                    csv_buffer
                )
                
                logger.info("COPY completed")
                logger.info("Committing transaction...")

        logger.info(f"✅ Core insert completed! Upserted {len(records)} products")

    except Exception as e:
        logger.error(f"Error during transform/load: {e}", exc_info=True)
        raise
    finally:
        source_session.close()
        conn.close()
