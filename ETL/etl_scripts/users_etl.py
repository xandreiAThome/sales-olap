from util.db_source import users
from models.Dim_Users import Dim_Users
import pandas as pd
from util.db_source import Session_db_source
from contextlib import contextmanager
from util.db_warehouse import db_warehouse_engine
from util.logging_config import get_logger
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func, case, text
import itertools
import os
import gc
import io
import csv

BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 50000)  # Larger batches for PostgreSQL

logger = get_logger(__name__)


# @contextmanager
# def extract_users_stream():
#     """Context-managed stream of users rows from source DB as mappings."""
#     session = Session_db_source()
#     result = None
#     try:
#         # PostgreSQL/SQLAlchemy title case: CONCAT(UPPER(SUBSTRING(col, 1, 1)), LOWER(SUBSTRING(col, 2)))
#         def title_case(column):
#             return func.concat(
#                 func.upper(func.substring(column, 1, 1)),
#                 func.lower(func.substring(column, 2))
#             )
        
#         stmt = select(
#             users.c.id.label("Users_ID"),
#             func.trim(title_case(users.c.firstName)).label("First_Name"),
#             func.trim(title_case(users.c.lastName)).label("Last_Name"),
#             func.trim(users.c.username).label("Username"),
#             func.trim(title_case(users.c.city)).label("City"),
#             func.trim(title_case(users.c.country)).label("Country"),
#             func.regexp_replace(func.trim(users.c.zipCode), r'[^0-9]', '').label("Zipcode"),
#             case(
#                 (func.lower(func.substring(func.trim(users.c.gender), 1, 1)) == "m", "male"),
#                 (func.lower(func.substring(func.trim(users.c.gender), 1, 1)) == "f", "female"),
#                 else_=None
#             ).label("Gender"),
#         )
#         result = session.execute(
#             stmt.execution_options(stream_results=True, yield_per=BATCH_SIZE)
#         ).mappings()
#         yield result
#     except Exception as e:
#         logger.error(f"Error streaming users: {e}")
#         raise
#     finally:
#         # Ensure result is properly closed before session
#         if result is not None:
#             try:
#                 result.close()
#             except Exception as e:
#                 logger.warning(f"Error closing result: {e}")
#         # Close session after result
#         try:
#             session.close()
#         except Exception as e:
#             logger.warning(f"Error closing session: {e}")





def transform_and_load_users():
    """
    Load users using PostgreSQL COPY for maximum speed.
    Truncates table first for full reload.
    """
    source_session = Session_db_source()
    conn = db_warehouse_engine.connect()
    
    try:
        logger.info(f"Extracting users from source database...")
        
        # Simple fast query - no complex SQL transformations
        stmt = select(
            users.c.id,
            users.c.firstName,
            users.c.lastName,
            users.c.username,
            users.c.city,
            users.c.country,
            users.c.zipCode,
            users.c.gender,
        )
        
        # Fetch ALL rows at once - much faster than streaming for 100K rows
        result = source_session.execute(stmt).fetchall()
        logger.info(f"Fetched {len(result)} users from source")
        
        # Transform ALL rows in Python (faster than complex SQL)
        logger.info("Transforming data in Python...")
        records = []
        for row in result:
            # Title case helper
            def tc(s):
                return s.strip().title() if s else None
            
            # Gender normalization
            gender = None
            if row.gender:
                first_char = row.gender.strip().lower()[0] if row.gender.strip() else None
                if first_char == 'm':
                    gender = 'male'
                elif first_char == 'f':
                    gender = 'female'
            
            # Zipcode - keep only digits
            zipcode = ''.join(c for c in (row.zipCode or '') if c.isdigit())
            
            records.append({
                "Users_ID": row.id,
                "First_Name": tc(row.firstName),
                "Last_Name": tc(row.lastName),
                "Username": row.username.strip() if row.username else None,
                "City": tc(row.city),
                "Country": tc(row.country),
                "Zipcode": zipcode if zipcode else None,
                "Gender": gender,
            })
        
        logger.info(f"Transformed {len(records)} records")
        
        # Single transaction
        with conn.begin():
            # Truncate for full reload
            logger.info("Truncating table for full reload...")
            conn.execute(text(f"TRUNCATE TABLE {Dim_Users.__tablename__} CASCADE"))
            logger.info("Table truncated")
            
            # Use PostgreSQL COPY for maximum speed
            if records:
                logger.info(f"Using PostgreSQL COPY for {len(records)} users...")
                
                # Create CSV in memory
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                
                # Write rows in the correct column order
                for record in records:
                    writer.writerow([
                        record['Users_ID'],
                        record['Username'],
                        record['First_Name'],
                        record['Last_Name'],
                        record['City'],
                        record['Country'],
                        record['Zipcode'],
                        record['Gender'],
                    ])
                
                # Reset buffer to start
                csv_buffer.seek(0)
                
                # Use raw connection for COPY
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                
                # PostgreSQL COPY - fastest bulk load method
                cursor.copy_expert(
                    f"""
                    COPY {Dim_Users.__tablename__} (
                        "Users_ID", "Username", "First_Name", "Last_Name",
                        "City", "Country", "Zipcode", "Gender"
                    ) FROM STDIN WITH CSV
                    """,
                    csv_buffer
                )
                
                logger.info("COPY completed")
                logger.info("Committing transaction...")

        logger.info(f"✅ Core insert completed! Upserted {len(records)} users")

    except Exception as e:
        logger.error(f"Error during transform/load users: {e}", exc_info=True)
        raise
    finally:
        source_session.close()
        conn.close()
