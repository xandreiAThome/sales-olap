from util.db_source import orderitems, orders
from sqlalchemy import select
from models.Dim_Date import Dim_Date
from models.Fact_Order_Items import Fact_Order_Items
from models.Dim_Products import Dim_Products
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

BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 5000)  # decrease if low ram

logger = get_logger(__name__)


@contextmanager
def extract_orders_with_items_stream():
    """Stream joined Orders + OrderItems from source DB as mappings."""
    session = Session_db_source()
    result = None
    try:
        stmt = select(
            orders.c.id.label("OrderId"),
            orders.c.orderNumber,
            orders.c.userId,
            orders.c.deliveryRiderId,
            orders.c.deliveryDate,
            orderitems.c.ProductId,
            orderitems.c.quantity,
            orderitems.c.notes,
        ).join(orderitems, orderitems.c.OrderId == orders.c.id)

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


def clean_orders_with_items(data):
    """Clean extracted joined Orders + OrderItems data.

    Accepts a pandas DataFrame or an iterable of row mappings.
    Returns a list of cleaned record dicts.
    """
    try:
        if isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            df = pd.DataFrame([dict(r) for r in data])

        # Parse dates
        if "deliveryDate" in df.columns:
            df["deliveryDate"] = df["deliveryDate"].apply(parse_date)

        # Clean notes (handle None, strip spaces)
        if "notes" in df.columns:
            df["notes"] = df["notes"].fillna("").astype(str).str.strip()

        cleaned_data = df.to_dict(orient="records")
        # logger.info(f"Cleaned {len(cleaned_data)} joined order+item rows")
        return cleaned_data
    except Exception as e:
        logger.error(f"Error cleaning joined orders+items: {e}", exc_info=True)
        return []


def load_transform_date_and_order_items():
    """Stream-extract -> chunk -> clean -> upsert dates and fact rows per chunk."""
    wh_session = Session_db_warehouse()
    total_upserted = 0

    try:
        with extract_orders_with_items_stream() as order_iter:
            # Load product price lookup once (may be large depending on product dimension)
            product_lookup = dict(
                wh_session.execute(select(Dim_Products.Product_ID, Dim_Products.Price))
                .tuples()
                .all()
            )

            # Keep an in-memory cache of existing dates to avoid repeated full-table scans
            date_lookup = dict(
                wh_session.execute(select(Dim_Date.Date, Dim_Date.Date_ID))
                .tuples()
                .all()
            )

            while True:
                chunk_rows = list(itertools.islice(order_iter, BATCH_SIZE))
                if not chunk_rows:
                    break

                # Clean chunk
                cleaned = clean_orders_with_items(
                    pd.DataFrame([dict(r) for r in chunk_rows])
                )

                # Identify new dates in this chunk and insert into Dim_Date
                unique_dates = (
                    pd.Series([o.get("deliveryDate") for o in cleaned])
                    .dropna()
                    .drop_duplicates()
                )
                new_date_records = []
                for d in pd.to_datetime(unique_dates):
                    date_key = d.date()
                    if date_key not in date_lookup:
                        new_date_records.append(
                            {
                                "Date_ID": int(d.strftime("%Y%m%d")),
                                "Date": date_key,
                                "Year": d.year,
                                "Month": d.month,
                                "Day": d.day,
                                "Quarter": d.quarter,
                            }
                        )

                if new_date_records:
                    stmt = (
                        insert(Dim_Date).prefix_with("IGNORE").values(new_date_records)
                    )
                    wh_session.execute(stmt)
                    wh_session.commit()

                    # refresh date_lookup for the newly inserted dates
                    refreshed = (
                        wh_session.execute(
                            select(Dim_Date.Date, Dim_Date.Date_ID).where(
                                Dim_Date.Date.in_([r["Date"] for r in new_date_records])
                            )
                        )
                        .tuples()
                        .all()
                    )
                    for d, did in refreshed:
                        date_lookup[d] = did

                # Build fact records for this chunk
                fact_records = []
                for row in cleaned:
                    delivery_date = row.get("deliveryDate")
                    delivery_date_id = None
                    if pd.notna(delivery_date):
                        delivery_date_id = date_lookup.get(
                            pd.to_datetime(delivery_date).date()
                        )

                    price = float(product_lookup.get(row.get("ProductId"), 0) or 0)
                    total_revenue = (row.get("quantity") or 0) * price

                    fact_records.append(
                        {
                            "Product_ID": row.get("ProductId"),
                            "Quantity": row.get("quantity"),
                            "Notes": row.get("notes"),
                            "Delivery_Date_ID": delivery_date_id,
                            "Delivery_Rider_ID": row.get("deliveryRiderId"),
                            "User_ID": row.get("userId"),
                            "Order_Num": row.get("orderNumber"),
                            "Total_Revenue": total_revenue,
                        }
                    )

                # Upsert facts in a single statement per chunk
                if fact_records:
                    stmt = insert(Fact_Order_Items).values(fact_records)
                    stmt = stmt.on_duplicate_key_update(
                        Quantity=stmt.inserted.Quantity,
                        Total_Revenue=stmt.inserted.Total_Revenue,
                        Notes=stmt.inserted.Notes,
                    )
                    wh_session.execute(stmt)
                    wh_session.commit()
                    total_upserted += len(fact_records)

                # cleanup
                del chunk_rows, cleaned, fact_records, unique_dates, new_date_records
                gc.collect()

        logger.info(f"Upserted {total_upserted} fact rows in batches of {BATCH_SIZE}")

    except Exception as e:
        wh_session.rollback()
        logger.error(f"Error in load/transform: {e}", exc_info=True)
        raise
    finally:
        wh_session.close()
