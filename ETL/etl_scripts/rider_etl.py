from util.db_source import riders, couriers
from models.Dim_Riders import Dim_Rider
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
import logging
from sqlalchemy import select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)


def extract_riders():
    session = Session_db_source()
    try:
        result = session.execute(riders.select())
        riders_data = result.fetchall()
        logging.info(f"Extracted {len(riders_data)} riders from source DB.")
        return riders_data
    except Exception as e:
        logging.error(f"Error extracting riders: {e}")
        return []
    finally:
        session.close()


def extract_couriers():
    session = Session_db_source()
    try:
        result = session.execute(couriers.select())
        couriers_data = result.fetchall()
        logging.info(f"Extracted {len(couriers_data)} couriers from source DB.")
        return couriers_data
    except Exception as e:
        logging.error(f"Error extracting couriers: {e}")
        return []
    finally:
        session.close()


def clean_riders_data(data):
    try:
        df = pd.DataFrame([dict(row._mapping) for row in data])
        df.dropna(inplace=True)

        df["vehicleType"] = df["vehicleType"].str.lower().str.strip()
        df["gender"] = (
            df["gender"].str.lower().str.strip().replace({"m": "male", "f": "female"})
        )
        df["firstName"] = df["firstName"].str.title().str.strip()
        df["lastName"] = df["lastName"].str.title().str.strip()

        cleaned_data = df.to_dict(orient="records")
        logging.info(f"Cleaned riders data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logging.error(f"Error cleaning riders data: {e}")
        return []


def clean_couriers_data(data):
    try:
        df = pd.DataFrame([dict(row._mapping) for row in data])
        df.dropna(inplace=True)

        df["name"] = df["name"].str.upper().str.strip()
        cleaned_data = df.to_dict(orient="records")
        logging.info(f"Cleaned couriers data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logging.error(f"Error cleaning couriers data: {e}")
        return []


def transform_and_load_riders():
    riders_data = extract_riders()
    couriers_data = extract_couriers()
    cleaned_riders = clean_riders_data(riders_data)
    cleaned_couriers = clean_couriers_data(couriers_data)

    session = Session_db_warehouse()
    try:
        # join riders and couriers to denormalize the 2 tables
        courier_dict = {courier["id"]: courier["name"] for courier in cleaned_couriers}
        rider_records = [
            {
                "Rider_ID": rider["id"],
                "First_Name": rider["firstName"],
                "Last_Name": rider["lastName"],
                "Vehicle_Type": rider["vehicleType"],
                "Age": rider["age"],
                "Gender": rider["gender"],
                "Courier_Name": courier_dict.get(rider["courierId"]),
            }
            for rider in cleaned_riders
        ]

        # --- Hybrid UPSERT logic ---
        existing_ids = set(
            row[0] for row in session.execute(select(Dim_Rider.Rider_ID)).all()
        )

        new_records = [r for r in rider_records if r["Rider_ID"] not in existing_ids]
        update_records = [r for r in rider_records if r["Rider_ID"] in existing_ids]

        if new_records:
            session.bulk_insert_mappings(Dim_Rider, new_records)
            logging.info(f"Inserted {len(new_records)} new riders.")

        if update_records:
            session.bulk_update_mappings(Dim_Rider, update_records)
            logging.info(f"Updated {len(update_records)} existing riders.")

        session.commit()

    except Exception as e:
        logging.error(f"Error during transform/load: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
