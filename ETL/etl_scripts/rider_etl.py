from util.db_source import riders, couriers
from models.Dim_Riders import dim_riders
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse


def extract_riders():
    session = Session_db_source()
    result = session.execute(riders.select())
    riders_data = result.fetchall()
    session.close()
    return riders_data


def extract_couriers():
    session = Session_db_source()
    result = session.execute(couriers.select())
    couriers_data = result.fetchall()
    session.close()
    return couriers_data


def clean_riders_data(data):
    # Implement your data cleaning logic here
    df = pd.DataFrame([dict(row._mapping) for row in data])
    df.dropna(inplace=True)

    df["vehicleType"] = df["vehicleType"].str.lower().str.strip()
    df["gender"] = (
        df["gender"].str.lower().str.strip().replace({"m": "male", "f": "female"})
    )
    df["firstName"] = df["firstName"].str.title().str.strip()
    df["lastName"] = df["lastName"].str.title().str.strip()

    cleaned_data = df.to_dict(orient="records")

    return cleaned_data


def clean_couriers_data(data):
    # Implement your data cleaning logic here
    df = pd.DataFrame([dict(row._mapping) for row in data])
    df.dropna(inplace=True)

    df["name"] = df["name"].str.upper().str.strip()
    cleaned_data = df.to_dict(orient="records")

    return cleaned_data


def transform_and_load_riders():
    riders_data = extract_riders()
    couriers_data = extract_couriers()
    cleaned_riders = clean_riders_data(riders_data)
    cleaned_couriers = clean_couriers_data(couriers_data)

    session = Session_db_warehouse()

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

        insert_stmt = dim_riders.insert().values(**rider_record)
        session.execute(insert_stmt)

    session.commit()
    session.close()
