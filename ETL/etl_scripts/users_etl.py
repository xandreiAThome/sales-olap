from util.db_source import users
from models.Dim_Users import Dim_Users
import pandas as pd
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
import re
import logging
from sqlalchemy import select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)


def extract_users():
    session = Session_db_source()
    try:
        result = session.execute(users.select())
        users_data = result.fetchall()
        logging.info(f"Extracted {len(users_data)} users from source DB.")
        return users_data
    except Exception as e:
        logging.error(f"Error extracting users: {e}")
        return []
    finally:
        session.close()


def clean_phone_number(phone: str):
    phone = phone.strip()
    num = re.sub(r"x.*$", "", phone)  # Remove extension
    digits = re.sub(r"\D", "", num)  # Keep only digits
    if len(digits) == 11:
        digits = digits[1:]

    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return digits  # Return as is if not 10 digits/malformed


def parse_date(x):
    x = str(x).strip()
    try:
        # Try ISO format first (YYYY-MM-DD)
        return pd.to_datetime(x, format="%Y-%m-%d", errors="raise")
    except Exception:
        try:
            # Try US format (MM/DD/YYYY)
            return pd.to_datetime(x, format="%m/%d/%Y", errors="raise")
        except Exception:
            return pd.NaT


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
        logging.info(f"Cleaned users data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logging.error(f"Error cleaning users data: {e}")
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

        # --- Hybrid UPSERT logic ---
        existing_ids = set(
            row[0] for row in session.execute(select(Dim_Users.Users_ID)).all()
        )

        new_records = [r for r in user_records if r["Users_ID"] not in existing_ids]
        update_records = [r for r in user_records if r["Users_ID"] in existing_ids]

        if new_records:
            session.bulk_insert_mappings(Dim_Users, new_records)
            logging.info(f"Inserted {len(new_records)} new users.")

        if update_records:
            session.bulk_update_mappings(Dim_Users, update_records)
            logging.info(f"Updated {len(update_records)} existing users.")

        session.commit()

    except Exception as e:
        logging.error(f"Error during transform/load: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
