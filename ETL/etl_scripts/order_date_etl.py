from util.db_source import orderitems, orders, products
from sqlalchemy import select, func, text
from models.Dim_Date import Dim_Date
from models.Fact_Order_Items import Fact_Order_Items
import pandas as pd
from util.db_source import Session_db_source
from contextlib import contextmanager
from util.db_warehouse import Session_db_warehouse
from util.logging_config import get_logger
from sqlalchemy.dialects.mysql import insert
import itertools
import gc
import os
from util.utils import parse_date

BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 10000)  # Larger batches for bulk insert performance

logger = get_logger(__name__)


def load_all_delivery_dates():
    session_src = Session_db_source()
    session_wh = Session_db_warehouse()
    
    try:
        # Fast: Get only unique dates (no join)
        logger.info("Extracting unique delivery dates from orders...")
        stmt = select(func.distinct(orders.c.deliveryDate))
        unique_dates = session_src.execute(stmt).scalars().all()
        logger.info(f"Found {len(unique_dates)} unique date values")
        
        # Parse and build date dimension records
        date_records = []
        for date_str in unique_dates:
            if date_str:
                parsed = parse_date(date_str)
                if pd.notna(parsed):
                    date_records.append({
                        "Date_ID": int(parsed.strftime("%Y%m%d")),
                        "Date": parsed.date(),
                        "Year": parsed.year,
                        "Month": parsed.month,
                        "Day": parsed.day,
                        "Quarter": parsed.quarter,
                    })
        
        # Bulk insert with IGNORE (skip duplicates)
        if date_records:
            stmt = insert(Dim_Date).prefix_with("IGNORE").values(date_records)
            session_wh.execute(stmt)
            session_wh.commit()
            logger.info(f"Loaded {len(date_records)} dates into Dim_Date")
        else:
            logger.warning("No valid dates to load")
    
    except Exception as e:
        logger.error(f"Error loading delivery dates: {e}", exc_info=True)
        session_wh.rollback()
        raise
    finally:
        session_src.close()
        session_wh.close()


@contextmanager
def extract_orders_with_items_stream():
    """Stream joined Orders + OrderItems + Products from source DB with SQL-level cleaning and revenue calculation."""
    session = Session_db_source()
    result = None
    try:
        # Extract with SQL processing including revenue calculation
        stmt = select(
            orders.c.orderNumber.label("Order_Num"),
            orders.c.userId.label("User_ID"),
            orders.c.deliveryRiderId.label("Delivery_Rider_ID"),
            orders.c.deliveryDate.label("Delivery_Date_Raw"),  # Raw string, parse in Python
            orderitems.c.ProductId.label("Product_ID"),
            orderitems.c.quantity.label("Quantity"),
            func.trim(func.coalesce(orderitems.c.notes, '')).label("Notes"),
            (orderitems.c.quantity * products.c.price).label("Total_Revenue"),  # Calculate revenue
        ).join(orderitems, orderitems.c.OrderId == orders.c.id
        ).join(products, products.c.id == orderitems.c.ProductId)

        result = session.execute(stmt.execution_options(stream_results=True, yield_per=BATCH_SIZE)).mappings()
        yield result
    except Exception as e:
        logger.error(f"Error streaming joined orders+items: {e}", exc_info=True)
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


def transform_and_load_order_items():
   
    wh_session = Session_db_warehouse()
    total_upserted = 0
    fk_checks_disabled = False
    commit_successful = False  # Track if commit succeeded

    try:
        # Disable checks and optimize MySQL settings for bulk insert
        logger.info("Optimizing MySQL settings for bulk insert...")
        wh_session.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        wh_session.execute(text("SET UNIQUE_CHECKS=0"))
        wh_session.execute(text("SET AUTOCOMMIT=0"))
        
        wh_session.commit()
        fk_checks_disabled = True
        logger.info("MySQL optimizations applied successfully")
        
        # Load date lookup once
        logger.info("Loading date dimension lookup...")
        date_lookup = dict(
            wh_session.execute(select(Dim_Date.Date, Dim_Date.Date_ID))
            .tuples()
            .all()
        )
        logger.info(f"Loaded {len(date_lookup)} dates")

        # Stream orders+items and process in chunks
        logger.info("Processing orders and order items in batches...")
        
        batch_num = 0
        with extract_orders_with_items_stream() as order_iter:
            while True:
                chunk_rows = list(itertools.islice(order_iter, BATCH_SIZE))
                
                if not chunk_rows:
                    logger.info("No more rows to process")
                    break

                batch_num += 1

                # Build fact records for this chunk
                fact_records = []
                skipped_null_dates = 0
                for row in chunk_rows:
                    row_dict = dict(row)
                    
                    # Parse delivery date in Python (handles multi-format)
                    delivery_date_raw = row_dict.get("Delivery_Date_Raw")
                    delivery_date_id = None
                    if delivery_date_raw:
                        parsed_date = parse_date(delivery_date_raw)
                        if pd.notna(parsed_date):
                            date_key = parsed_date.date()
                            delivery_date_id = date_lookup.get(date_key)
                    
                    # Skip rows with NULL Delivery_Date_ID (required for partitioning)
                    if delivery_date_id is None:
                        skipped_null_dates += 1
                        continue

                    fact_records.append({
                        "Product_ID": row_dict.get("Product_ID"),
                        "Quantity": row_dict.get("Quantity"),
                        "Notes": row_dict.get("Notes"),
                        "Delivery_Date_ID": delivery_date_id,
                        "Delivery_Rider_ID": row_dict.get("Delivery_Rider_ID"),
                        "User_ID": row_dict.get("User_ID"),
                        "Order_Num": row_dict.get("Order_Num"),
                        "Total_Revenue": row_dict.get("Total_Revenue"),  # Calculated in SQL
                    })

                # Insert chunk using INSERT IGNORE (skips duplicates)
                if fact_records:
                    stmt = insert(Fact_Order_Items).prefix_with("IGNORE").values(fact_records)
                    wh_session.execute(stmt)
                    total_upserted += len(fact_records)
                    
                    if skipped_null_dates > 0:
                        logger.warning(f"Batch {batch_num}: Skipped {skipped_null_dates} rows with NULL Delivery_Date_ID")
                    
                    logger.info(f"Batch {batch_num}: Insert completed (total: {total_upserted})")
                elif skipped_null_dates > 0:
                    logger.warning(f"Batch {batch_num}: All {skipped_null_dates} rows had NULL dates, nothing inserted")

                # Cleanup
                del chunk_rows, fact_records
                gc.collect()

        # Commit once at the end for maximum performance
        logger.info(f"All batches processed. Committing {total_upserted} total rows...")
        wh_session.commit()
        commit_successful = True  # Mark commit as successful
        logger.info(f"Commit completed! Inserted {total_upserted} fact rows total")

    except Exception as e:
        logger.error(f"Error loading order items: {e}", exc_info=True)
        # Rollback any uncommitted changes
        try:
            wh_session.rollback()
            logger.info("Transaction rolled back successfully")
        except Exception as rb_error:
            logger.error(f"Error during rollback: {rb_error}", exc_info=True)
        raise
    finally:
        # Only ensure indexes if commit was successful
        if commit_successful:
            try:
                logger.info("Ensuring custom indexes exist (this may take a few minutes)...")
                
                # idx_fact_fk
                try:
                    wh_session.execute(text("""
                        CREATE INDEX idx_fact_fk 
                        ON fact_order_items (Product_ID, User_ID, Delivery_Date_ID, Total_Revenue)
                    """))
                    logger.info("idx_fact_fk created")
                except Exception as e:
                    if "Duplicate key name" in str(e) or "already exists" in str(e):
                        logger.info("idx_fact_fk already exists")
                    else:
                        raise
                
                # idx_date_revenue
                try:
                    wh_session.execute(text("""
                        CREATE INDEX idx_date_revenue 
                        ON fact_order_items (Delivery_Date_ID, Total_Revenue)
                    """))
                    logger.info("idx_date_revenue created")
                except Exception as e:
                    if "Duplicate key name" in str(e) or "already exists" in str(e):
                        logger.info("idx_date_revenue already exists")
                    else:
                        raise
                
                # idx_rider_revenue
                try:
                    wh_session.execute(text("""
                        CREATE INDEX idx_rider_revenue 
                        ON fact_order_items (Delivery_Rider_ID, Total_Revenue)
                    """))
                    logger.info("idx_rider_revenue created")
                except Exception as e:
                    if "Duplicate key name" in str(e) or "already exists" in str(e):
                        logger.info("idx_rider_revenue already exists")
                    else:
                        raise
                
                wh_session.commit()
                logger.info("Custom indexes ensured successfully")
            except Exception as e:
                logger.error(f"Error ensuring indexes exist: {e}", exc_info=True)
                try:
                    wh_session.rollback()
                except Exception:
                    pass  # Ignore rollback errors in cleanup
        else:
            logger.warning("Commit was not successful, skipping index creation")
        
        # Always re-enable checks and restore MySQL settings
        if fk_checks_disabled:
            try:
                logger.info("Restoring MySQL settings...")
                wh_session.execute(text("SET UNIQUE_CHECKS=1"))
                wh_session.execute(text("SET FOREIGN_KEY_CHECKS=1"))
                wh_session.execute(text("SET AUTOCOMMIT=1"))
                
                wh_session.commit()
                logger.info("MySQL settings restored")
            except Exception as e:
                logger.error(f"Error restoring MySQL settings: {e}", exc_info=True)
        
        # Always close the session
        try:
            wh_session.close()
        except Exception as e:
            logger.error(f"Error closing session: {e}", exc_info=True)


def load_transform_date_and_order_items():
    """
    Main ETL function: Load dates first, then order items.
    Call this from app.py.
    """
    logger.info("Step 1: Loading delivery dates...")
    load_all_delivery_dates()
    
    logger.info("Step 2: Loading order items...")
    transform_and_load_order_items()
