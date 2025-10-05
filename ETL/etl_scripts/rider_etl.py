from util.db_source import riders, couriers
from sqlalchemy import select
from models.Dim_Riders import Dim_Rider
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
from util.logging_config import get_logger
from sqlalchemy.dialects.mysql import insert

logger = get_logger(__name__)


def extract_riders_with_couriers():
    """Extract riders joined with couriers from the source DB (server-side join)."""
    session = Session_db_source()
    try:
        stmt = select(
            riders.c.id.label("id"),
            riders.c.firstName,
            riders.c.lastName,
            riders.c.vehicleType,
            riders.c.age,
            riders.c.gender,
            riders.c.courierId,
            couriers.c.name.label("courier_name"),
        ).select_from(riders.outerjoin(couriers, riders.c.courierId == couriers.c.id))

        result = session.execute(stmt).mappings().all()
        logger.info(f"Extracted {len(result)} joined riders rows from source DB.")
        return result
    except Exception as e:
        logger.error(f"Error extracting joined riders+couriers: {e}")
        return []
    finally:
        session.close()


def clean_joined_riders_data(data):
    """Clean rows returned by the joined extract (rider + courier_name).

    Keeps required rider fields and normalizes strings. Uses a conservative
    dropna that only drops rows missing essential rider identifiers.
    """
    try:
        # data is a list of mapping objects from .mappings()
        df = pd.DataFrame([dict(r) for r in data])

        # Drop rows missing essential identifiers only
        required = ["id", "firstName", "lastName"]
        df.dropna(subset=required, inplace=True)

        df["vehicleType"] = df["vehicleType"].astype(str).str.lower().str.strip()
        df["gender"] = (
            df["gender"].astype(str).str.lower().str.strip().replace({"m": "male", "f": "female"})
        )
        df["firstName"] = df["firstName"].astype(str).str.title().str.strip()
        df["lastName"] = df["lastName"].astype(str).str.title().str.strip()

        # Normalize courier name if present
        if "courier_name" in df.columns:
            df["courier_name"] = df["courier_name"].fillna("").astype(str).str.upper().str.strip()

        cleaned_data = df.to_dict(orient="records")
        # logger.info(f"Cleaned joined riders data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logger.error(f"Error cleaning joined riders data: {e}")
        return []


def transform_and_load_riders():
    # Extract joined rows from source DB (server-side join)
    joined_rows = extract_riders_with_couriers()
    cleaned = clean_joined_riders_data(joined_rows)

    session = Session_db_warehouse()
    try:
        rider_records = [
            {
                "Rider_ID": r["id"],
                "First_Name": r["firstName"],
                "Last_Name": r["lastName"],
                "Vehicle_Type": r.get("vehicleType"),
                "Age": r.get("age"),
                "Gender": r.get("gender"),
                "Courier_Name": r.get("courier_name") or None,
            }
            for r in cleaned
        ]

        if rider_records:
            stmt = insert(Dim_Rider).values(rider_records)

            # Define how to update existing Rider_ID rows
            stmt = stmt.on_duplicate_key_update(
                First_Name=stmt.inserted.First_Name,
                Last_Name=stmt.inserted.Last_Name,
                Vehicle_Type=stmt.inserted.Vehicle_Type,
                Age=stmt.inserted.Age,
                Gender=stmt.inserted.Gender,
                Courier_Name=stmt.inserted.Courier_Name,
            )

            session.execute(stmt)
            session.commit()
            logger.info(f"Upserted {len(rider_records)} riders")

    except Exception as e:
        logger.error(f"Error during transform/load riders: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
