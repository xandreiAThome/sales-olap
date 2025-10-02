from util.db_source import users
from models.Dim_Users import Dim_Users
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
from util.logging_config import get_logger
from util.utils import parse_date
from util.utils import clean_phone_number
from sqlalchemy.dialects.mysql import insert

logger = get_logger(__name__)


def extract_users():
    session = Session_db_source()
    try:
        result = session.execute(users.select())
        users_data = result.fetchall()
        logger.info(f"Extracted {len(users_data)} users from source DB.")
        return users_data
    except Exception as e:
        logger.error(f"Error extracting users: {e}")
        return []
    finally:
        session.close()


def clean_users_data(data):
    try:
        df = pd.DataFrame([dict(row._mapping) for row in data])
        df.dropna(inplace=True)

        df["username"] = df["username"].str.strip()
        df["firstName"] = df["firstName"].str.title().str.strip()
        df["lastName"] = df["lastName"].str.title().str.strip()
        df["address1"] = df["address1"].str.strip().str.title()
        df["address2"] = df["address2"].str.strip().str.title()
        df["city"] = df["city"].str.strip().str.title()
        df["country"] = df["country"].str.strip().str.title()
        df["zipCode"] = df["zipCode"].str.strip().str.extract(r"(\d+)").astype(str)
        df["dateOfBirthClean"] = (
            df["dateOfBirth"]
            .astype(str)
            .str.strip()  # remove spaces
            .str.replace(r"[^\d/-]", "", regex=True)  # keep only digits, /, -
        )
        df["dateOfBirth"] = df["dateOfBirthClean"].apply(parse_date)
        df["gender"] = (
            df["gender"].str.strip().str.lower().replace({"m": "male", "f": "female"})
        )
        df["phoneNumber"] = df["phoneNumber"].apply(clean_phone_number)

        cleaned_data = df.to_dict(orient="records")
        logger.info(f"Cleaned users data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logger.error(f"Error cleaning users data: {e}")
        return []


def transform_and_load_users():
    users_data = extract_users()
    cleaned_users = clean_users_data(users_data)

    session = Session_db_warehouse()
    try:
        user_records = [
            {
                "Users_ID": user["id"],
                "Username": user["username"],
                "First_Name": user["firstName"],
                "Last_Name": user["lastName"],
                "Birth_Date": user["dateOfBirth"],
                "Address_1": user["address1"],
                "Address_2": user["address2"],
                "City": user["city"],
                "Country": user["country"],
                "Zipcode": user["zipCode"],
                "Phone_Number": user["phoneNumber"],
                "Gender": user["gender"],
            }
            for user in cleaned_users
        ]

        if user_records:
            stmt = insert(Dim_Users).values(user_records)

            # Define how to update if Users_ID already exists
            stmt = stmt.on_duplicate_key_update(
                Username=stmt.inserted.Username,
                First_Name=stmt.inserted.First_Name,
                Last_Name=stmt.inserted.Last_Name,
                Birth_Date=stmt.inserted.Birth_Date,
                Address_1=stmt.inserted.Address_1,
                Address_2=stmt.inserted.Address_2,
                City=stmt.inserted.City,
                Country=stmt.inserted.Country,
                Zipcode=stmt.inserted.Zipcode,
                Phone_Number=stmt.inserted.Phone_Number,
                Gender=stmt.inserted.Gender,
            )

            session.execute(stmt)
            session.commit()
            logger.info(f"Upserted {len(user_records)} users")

    except Exception as e:
        logger.error(f"Error during transform/load users: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
