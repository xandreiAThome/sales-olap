from util.db_source import orderitems, orders, products
from sqlalchemy import select, func, text
from models.Dim_Date import Dim_Date
from models.Fact_Order_Items import Fact_Order_Items
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse, db_warehouse_engine
from util.logging_config import get_logger
from sqlalchemy.dialects.postgresql import insert
import itertools
import gc
import os
from util.utils import parse_date
import io
import csv

BATCH_SIZE = int(os.getenv("BATCH_SIZE_ORDERS") or 50000)  # Batch size for streaming
# Set to True for initial bulk loads (drops/recreates indexes for 20-40% speedup)
# Set to False for incremental updates (keeps indexes for deduplication)
OPTIMIZE_INDEXES = os.getenv("OPTIMIZE_INDEXES", "false").lower() in ("true", "1", "yes")

logger = get_logger(__name__)


def load_all_delivery_dates():
    """
    Load unique delivery dates using PostgreSQL COPY.
    Uses ON CONFLICT since dates are referenced by fact table (no truncate).
    """
    session_src = Session_db_source()
    
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
        
        if not date_records:
            logger.warning("No valid dates to load")
            return
        
        logger.info(f"Parsed {len(date_records)} valid dates")
        
        # Use PostgreSQL COPY to temp table, then INSERT ON CONFLICT
        conn = db_warehouse_engine.connect()
        
        try:
            with conn.begin():
                # Create temporary table (auto-dropped at end of transaction)
                logger.info("Creating temporary table for date staging...")
                conn.execute(text("""
                    CREATE TEMP TABLE temp_dates (
                        "Date_ID" INTEGER,
                        "Date" DATE,
                        "Year" INTEGER,
                        "Month" INTEGER,
                        "Day" INTEGER,
                        "Quarter" INTEGER
                    ) ON COMMIT DROP
                """))
                
                # Use COPY to load into temp table (super fast)
                logger.info("Using COPY to load dates into temp table...")
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                
                for record in date_records:
                    writer.writerow([
                        record['Date_ID'],
                        record['Date'],
                        record['Year'],
                        record['Month'],
                        record['Day'],
                        record['Quarter'],
                    ])
                
                csv_buffer.seek(0)
                
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                
                cursor.copy_expert(
                    """
                    COPY temp_dates ("Date_ID", "Date", "Year", "Month", "Day", "Quarter")
                    FROM STDIN WITH CSV
                    """,
                    csv_buffer
                )
                
                # Insert from temp table with ON CONFLICT (handles duplicates)
                logger.info("Inserting from temp table with ON CONFLICT...")
                result = conn.execute(text(f"""
                    INSERT INTO {Dim_Date.__tablename__} ("Date_ID", "Date", "Year", "Month", "Day", "Quarter")
                    SELECT "Date_ID", "Date", "Year", "Month", "Day", "Quarter"
                    FROM temp_dates
                    ON CONFLICT ("Date_ID") DO NOTHING
                """))
                
                logger.info(f"Loaded dates into Dim_Date")
        
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error loading delivery dates: {e}", exc_info=True)
        raise
    finally:
        session_src.close()


def drop_fact_indexes(session):
    """Drop non-primary-key indexes to speed up bulk insert (optional optimization)"""
    logger.info("Dropping fact table indexes for faster bulk insert...")
    
    indexes_to_drop = [
        "idx_fact_fk",
        "idx_date_revenue", 
        "idx_rider_revenue"
    ]
    
    for index_name in indexes_to_drop:
        try:
            session.execute(text(f'DROP INDEX IF EXISTS "{index_name}"'))
            logger.info(f"  Dropped index: {index_name}")
        except Exception as e:
            logger.warning(f"  Could not drop {index_name}: {e}")
    
    session.commit()
    logger.info("Indexes dropped successfully")


def create_fact_indexes(session):
    """Create indexes after bulk insert - faster on complete data (optional optimization)"""
    logger.info("Creating fact table indexes (this may take a few minutes)...")
    
    indexes = [
        ("idx_fact_fk", 'fact_order_items ("Product_ID", "User_ID", "Delivery_Date_ID", "Total_Revenue")'),
        ("idx_date_revenue", 'fact_order_items ("Delivery_Date_ID", "Total_Revenue")'),
        ("idx_rider_revenue", 'fact_order_items ("Delivery_Rider_ID", "Total_Revenue")')
    ]
    
    for index_name, columns in indexes:
        try:
            stmt = f"CREATE INDEX IF NOT EXISTS {index_name} ON {columns}"
            session.execute(text(stmt))
            logger.info(f"  Created index: {index_name}")
        except Exception as e:
            logger.error(f"  Error creating {index_name}: {e}")
            raise
    
    session.commit()
    logger.info("Indexes created successfully")





def transform_and_load_order_items():
    """
    Transform and load order items using PostgreSQL COPY for maximum speed.
    COPY bypasses query planner and writes directly to table pages.
    Truncates table first for full reload.
    """
    wh_session = Session_db_warehouse()
    source_session = Session_db_source()
    commit_successful = False  # Track if commit succeeded

    try:
        # OPTIONAL: Drop indexes for faster bulk insert
        if OPTIMIZE_INDEXES:
            logger.info("OPTIMIZE_INDEXES=true: Dropping indexes for faster bulk insert...")
            drop_fact_indexes(wh_session)
        else:
            logger.info("OPTIMIZE_INDEXES=false: Keeping indexes")
        
        logger.info("Starting order items ETL with PostgreSQL COPY...")
        
        # Load date lookup once
        logger.info("Loading date dimension lookup...")
        date_lookup = dict(
            wh_session.execute(select(Dim_Date.Date, Dim_Date.Date_ID))
            .tuples()
            .all()
        )
        logger.info(f"Loaded {len(date_lookup)} dates")
        
        # Fetch ALL joined data at once from MySQL (faster than streaming)
        logger.info("Fetching all orders+items from source database...")
        stmt = select(
            orders.c.id.label("Order_ID"),
            orders.c.orderNumber.label("Order_Num"),
            orders.c.userId.label("User_ID"),
            orders.c.deliveryRiderId.label("Delivery_Rider_ID"),
            orders.c.deliveryDate.label("Delivery_Date_Raw"),
            orderitems.c.ProductId.label("Product_ID"),
            orderitems.c.quantity.label("Quantity"),
            func.trim(func.coalesce(orderitems.c.notes, '')).label("Notes"),
            (orderitems.c.quantity * products.c.price).label("Total_Revenue"),
        ).join(orderitems, orderitems.c.OrderId == orders.c.id
        ).join(products, products.c.id == orderitems.c.ProductId)
        
        # Bulk fetch - much faster than streaming for 1.9M rows
        result = source_session.execute(stmt).fetchall()
        logger.info(f"Fetched {len(result)} order items from source")

        # Use Core connection
        conn = db_warehouse_engine.connect()
        
        try:
            # Single transaction for all operations
            with conn.begin():
                # Truncate for full reload
                logger.info("Truncating fact table for full reload...")
                conn.execute(text(f"TRUNCATE TABLE {Fact_Order_Items.__tablename__} CASCADE"))
                logger.info("Table truncated")
                
                # Transform ALL records in Python
                logger.info("Transforming all records in Python...")
                
                # OPTIMIZATION: Pre-parse unique delivery dates (much faster than parsing per row)
                logger.info("Pre-parsing unique delivery dates...")
                unique_dates = set(row.Delivery_Date_Raw for row in result if row.Delivery_Date_Raw)
                date_cache = {}
                for date_str in unique_dates:
                    parsed_date = parse_date(date_str)
                    if pd.notna(parsed_date):
                        date_key = parsed_date.date()
                        date_cache[date_str] = date_lookup.get(date_key)
                    else:
                        date_cache[date_str] = None
                logger.info(f"Pre-parsed {len(date_cache)} unique dates")
                
                all_records = []
                skipped_total = 0
                
                for row in result:
                    # Lookup pre-parsed delivery date (instant!)
                    delivery_date_raw = row.Delivery_Date_Raw
                    delivery_date_id = date_cache.get(delivery_date_raw) if delivery_date_raw else None
                    
                    # Skip rows with NULL Delivery_Date_ID
                    if delivery_date_id is None:
                        skipped_total += 1
                        continue

                    # Create composite Order_Item_ID
                    order_id = row.Order_ID
                    product_id = row.Product_ID
                    order_item_id = order_id * 1000000 + product_id

                    all_records.append({
                        "Order_Item_ID": order_item_id,
                        "Product_ID": product_id,
                        "Quantity": row.Quantity,
                        "Notes": row.Notes,
                        "Delivery_Date_ID": delivery_date_id,
                        "Delivery_Rider_ID": row.Delivery_Rider_ID,
                        "User_ID": row.User_ID,
                        "Order_Num": row.Order_Num,
                        "Total_Revenue": row.Total_Revenue,
                    })
                
                logger.info(f"Transformed {len(all_records)} records (skipped {skipped_total} with NULL dates)")
                
                # Use PostgreSQL COPY for maximum speed
                logger.info(f"Using PostgreSQL COPY for {len(all_records)} rows...")
                
                # Create CSV in memory
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                
                # Write rows in the correct column order
                for record in all_records:
                    writer.writerow([
                        record['Order_Item_ID'],
                        record['Product_ID'],
                        record['Quantity'],
                        record['Notes'],
                        record['Delivery_Date_ID'],
                        record['Delivery_Rider_ID'],
                        record['User_ID'],
                        record['Order_Num'],
                        record['Total_Revenue'],
                    ])
                
                # Reset buffer to start
                csv_buffer.seek(0)
                
                # Use raw connection for COPY
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                
                # PostgreSQL COPY - fastest bulk load method
                cursor.copy_expert(
                    f"""
                    COPY {Fact_Order_Items.__tablename__} (
                        "Order_Item_ID", "Product_ID", "Quantity", "Notes",
                        "Delivery_Date_ID", "Delivery_Rider_ID", "User_ID",
                        "Order_Num", "Total_Revenue"
                    ) FROM STDIN WITH CSV
                    """,
                    csv_buffer
                )
                
                total_inserted = len(all_records)
                logger.info("COPY completed")
                logger.info("Committing transaction...")
        
        finally:
            conn.close()
        
        commit_successful = True
        logger.info(f"✅ COPY insert completed! Total rows inserted: {total_inserted}")

    except Exception as e:
        logger.error(f"Error loading order items: {e}", exc_info=True)
        raise
    finally:
        # Ensure indexes exist after successful insert
        if commit_successful:
            try:
                # If we dropped indexes, recreate them (faster on complete data)
                # If we didn't drop them, this will just ensure they exist
                logger.info("Ensuring custom indexes exist...")
                create_fact_indexes(wh_session)
            except Exception as e:
                logger.error(f"Error ensuring indexes exist: {e}", exc_info=True)
                try:
                    wh_session.rollback()
                except Exception:
                    pass  # Ignore rollback errors in cleanup
        else:
            logger.warning("Insert was not successful, skipping index creation")
        
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
