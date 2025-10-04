from util.db_source import users
from models.Dim_Users import Dim_Users
import pandas as pd
from util.db_source import Session_db_source
from contextlib import contextmanager
from util.db_warehouse import Session_db_warehouse
from util.logging_config import get_logger
from util.utils import parse_date
from util.utils import clean_phone_number
from sqlalchemy.dialects.mysql import insert
import itertools
import os

BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 2000)
import gc

logger = get_logger(__name__)


@contextmanager
def extract_users_stream():
    """Context-managed stream of users rows from source DB as mappings."""
    session = Session_db_source()
    result = None
    try:
        result = session.execute(
            users.select().execution_options(stream_results=True)
        ).mappings()
        yield result
    except Exception as e:
        logger.error(f"Error streaming users: {e}")
        return
    finally:
        try:
            if result is not None:
                result.close()
        except Exception:
            pass
        session.close()


def clean_users_data(data):
    try:
        # Accept either a pandas DataFrame or an iterable of mappings
        if isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            df = pd.DataFrame([dict(r) for r in data])

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

        df = df.rename(
            columns={
                "id": "Users_ID",
                "username": "Username",
                "firstName": "First_Name",
                "lastName": "Last_Name",
                "dateOfBirth": "Birth_Date",
                "address1": "Address_1",
                "address2": "Address_2",
                "city": "City",
                "country": "Country",
                "zipCode": "Zipcode",
                "phoneNumber": "Phone_Number",
                "gender": "Gender",
            }
        )
        df.drop(columns=["dateOfBirthClean", "createdAt", "updatedAt"], inplace=True)

        cleaned_data = df.to_dict(orient="records")
        # logger.info(f"Cleaned users data, {len(cleaned_data)} records ready.")
        return cleaned_data
    except Exception as e:
        logger.error(f"Error cleaning users data: {e}")
        return []


def transform_and_load_users():
    db_session = Session_db_warehouse()
    total_inserted = 0
    try:
        with extract_users_stream() as user_iter:
            while True:
                chunk_rows = list(itertools.islice(user_iter, BATCH_SIZE))
                if not chunk_rows:
                    break

                # Use centralized cleaning function on the DataFrame chunk
                records = clean_users_data(chunk_rows)
                if records:
                    stmt = insert(Dim_Users).values(records)
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
