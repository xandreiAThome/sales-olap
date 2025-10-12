from util.db_source import users
from models.Dim_Users import Dim_Users
import pandas as pd
from util.db_source import Session_db_source
from contextlib import contextmanager
from util.db_warehouse import Session_db_warehouse
from util.logging_config import get_logger
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import select, func, case
import itertools
import os
import gc

BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 2000)

logger = get_logger(__name__)


@contextmanager
def extract_users_stream():
    """Context-managed stream of users rows from source DB as mappings."""
    session = Session_db_source()
    result = None
    try:
        # MySQL title case: CONCAT(UPPER(SUBSTRING(col, 1, 1)), LOWER(SUBSTRING(col, 2)))
        def title_case(column):
            return func.concat(
                func.upper(func.substring(column, 1, 1)),
                func.lower(func.substring(column, 2))
            )
        
        stmt = select(
            users.c.id.label("Users_ID"),
            func.trim(title_case(users.c.firstName)).label("First_Name"),
            func.trim(title_case(users.c.lastName)).label("Last_Name"),
            func.trim(users.c.username).label("Username"),
            func.trim(title_case(users.c.city)).label("City"),
            func.trim(title_case(users.c.country)).label("Country"),
            func.regexp_replace(func.trim(users.c.zipCode), r'[^0-9]', '').label("Zipcode"),
            case(
                (func.lower(func.trim(users.c.gender)) == "m", "male"),
                (func.lower(func.trim(users.c.gender)) == "f", "female"),
                else_="other"
            ).label("Gender"),
        )
        result = session.execute(
            stmt.execution_options(stream_results=True, yield_per=BATCH_SIZE)
        ).mappings()
        yield result
    except Exception as e:
        logger.error(f"Error streaming users: {e}")
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





def transform_and_load_users():
    db_session = Session_db_warehouse()
    total_inserted = 0
    try:
        with extract_users_stream() as user_iter:
            while True:
                chunk_rows = list(itertools.islice(user_iter, BATCH_SIZE))
                if not chunk_rows:
                    break

                # Convert to dicts (all cleaning done in SQL)
                records = [dict(row) for row in chunk_rows]
                
                if records:
                    # Use INSERT IGNORE to skip duplicates (faster than ON DUPLICATE KEY UPDATE)
                    stmt = insert(Dim_Users).prefix_with("IGNORE").values(records)
                    db_session.execute(stmt)
                    db_session.commit()
                    total_inserted += len(records)

                del records, chunk_rows
                gc.collect()

        logger.info(f"Upserted {total_inserted} users in batches of {BATCH_SIZE}")

    except Exception as e:
        logger.error(f"Error during transform/load users: {e}", exc_info=True)
        db_session.rollback()
        raise
    finally:
        db_session.close()
