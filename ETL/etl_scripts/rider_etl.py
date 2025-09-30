from util.db_source import riders, couriers
from models.Dim_Riders import dim_riders
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
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
    inserted = 0
    try:
        # join riders and couriers to denormalize the 2 tables
        courier_dict = {courier["id"]: courier["name"] for courier in cleaned_couriers}
        for rider in cleaned_riders:
            rider_record = {
                "First_Name": rider["firstName"],
                "Last_Name": rider["lastName"],
                "Vehicle_Type": rider["vehicleType"],
                "Age": rider["age"],
                "Gender": rider["gender"],
                "Courier_Name": courier_dict.get(rider["courierId"]),
                "Rider_ID": rider["id"],
            }
            try:
                insert_stmt = dim_riders.insert().values(**rider_record)
                session.execute(insert_stmt)
                inserted += 1
            except Exception as e:
                logging.error(f"Error inserting rider {rider_record['Rider_ID']}: {e}")
        session.commit()
        logging.info(f"Inserted {inserted} riders into warehouse DB.")
    except Exception as e:
        logging.error(f"Error during transform/load: {e}")
        session.rollback()
    finally:
        session.close()
