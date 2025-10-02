from util.db_source import orderitems, orders
from sqlalchemy import select
from models.Dim_Date import Dim_Date
from models.Fact_Order_Items import Fact_Order_Items
from models.Dim_Products import Dim_Products
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
import logging
from sqlalchemy.dialects.mysql import insert

from util.utils import parse_date

BATCH_SIZE = 50_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)


def extract_orders_with_items():
    """Extract raw joined Orders + OrderItems from source DB"""
    session = Session_db_source()
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

        result = session.execute(stmt).mappings().all()
        logging.info(f"Extracted {len(result)} raw joined order+item rows")
        return result
    except Exception as e:
        logging.error(f"Error extracting joined orders+items: {e}", exc_info=True)
        return []
    finally:
        session.close()


def clean_orders_with_items(data):
    """Clean extracted joined Orders + OrderItems data"""
    try:
        df = pd.DataFrame(data)

        # Parse dates
        df["deliveryDate"] = df["deliveryDate"].apply(parse_date)

        # Clean notes (handle None, strip spaces)
        if "notes" in df.columns:
            df["notes"] = df["notes"].fillna("").astype(str).str.strip()

        cleaned_data = df.to_dict(orient="records")
        logging.info(f"Cleaned {len(cleaned_data)} joined order+item rows")
        return cleaned_data
    except Exception as e:
        logging.error(f"Error cleaning joined orders+items: {e}", exc_info=True)
        return []


def load_transform_date_and_order_items():
    order_with_items = extract_orders_with_items()
    cleaned_order_with_items = clean_orders_with_items(order_with_items)

    session = Session_db_warehouse()

    try:
        unique_dates = (
            pd.Series([o["deliveryDate"] for o in cleaned_order_with_items])
            .dropna()
            .drop_duplicates()
        )
        date_records = [
            {
                "Date_ID": int(d.strftime("%Y%m%d")),
                "Date": d.date(),
                "Year": d.year,
                "Month": d.month,
                "Day": d.day,
                "Quarter": d.quarter,
            }
            for d in pd.to_datetime(unique_dates)
        ]

        # Insert into Dim_Date (ignore duplicates)
        if date_records:
            stmt = insert(Dim_Date).prefix_with("IGNORE").values(date_records)
            session.execute(stmt)
            session.commit()

        date_lookup = dict(
            session.execute(select(Dim_Date.Date, Dim_Date.Date_ID)).tuples().all()
        )
        product_lookup = dict(
            session.execute(select(Dim_Products.Product_ID, Dim_Products.Price))
            .tuples()
            .all()
        )

        fact_buffer = []
        for row in cleaned_order_with_items:
            delivery_date_id = date_lookup.get(
                pd.to_datetime(row["deliveryDate"]).date()
            )
            price = float(product_lookup.get(row["ProductId"], 0))
            total_revenue = row["quantity"] * price

            fact_buffer.append(
                {
                    "Product_ID": row["ProductId"],
                    "Quantity": row["quantity"],
                    "Notes": row["notes"],
                    "Delivery_Date_ID": delivery_date_id,
                    "Delivery_Rider_ID": row["deliveryRiderId"],
                    "User_ID": row["userId"],
                    "Order_Num": row["orderNumber"],
                    "Total_Revenue": total_revenue,
                }
            )

            if len(fact_buffer) >= BATCH_SIZE:
                stmt = insert(Fact_Order_Items).values(fact_buffer)
                stmt = stmt.on_duplicate_key_update(
                    Quantity=stmt.inserted.Quantity,
                    Total_Revenue=stmt.inserted.Total_Revenue,
                    Notes=stmt.inserted.Notes,
                )
                session.execute(stmt)
                fact_buffer.clear()

        # final flush
        if fact_buffer:
            stmt = insert(Fact_Order_Items).values(fact_buffer)
            stmt = stmt.on_duplicate_key_update(
                Quantity=stmt.inserted.Quantity,
                Total_Revenue=stmt.inserted.Total_Revenue,
                Notes=stmt.inserted.Notes,
            )
            session.execute(stmt)

        session.commit()
        logging.info("Finished upserting Fact_Order_Items")

    except Exception as e:
        session.rollback()
        logging.error(f"Error in load/transform: {e}", exc_info=True)
    finally:
        session.close()
