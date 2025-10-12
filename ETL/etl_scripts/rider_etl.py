from util.db_source import riders, couriers
from sqlalchemy import select
from models.Dim_Riders import Dim_Rider
from sqlalchemy import select, func, case
from util.db_source import Session_db_source
from util.db_warehouse import Session_db_warehouse
from util.logging_config import get_logger
from sqlalchemy.dialects.mysql import insert

logger = get_logger(__name__)


def extract_riders_with_couriers():
    session = Session_db_source()
    try:
        # MySQL title case: CONCAT(UPPER(SUBSTRING(col, 1, 1)), LOWER(SUBSTRING(col, 2)))
        def title_case(column):
            return func.concat(
                func.upper(func.substring(column, 1, 1)),
                func.lower(func.substring(column, 2))
            )
        
        stmt = select(
            riders.c.id.label("Rider_ID"),
            func.trim(title_case(riders.c.firstName)).label("First_Name"),
            func.trim(title_case(riders.c.lastName)).label("Last_Name"),
            func.lower(func.trim(riders.c.vehicleType)).label("Vehicle_Type"),
            riders.c.age.label("Age"),
            case(
                (func.lower(func.trim(riders.c.gender)) == "m", "male"),
                (func.lower(func.trim(riders.c.gender)) == "f", "female"),
                else_="other"
            ).label("Gender"),
            couriers.c.name.label("Courier_Name")
        ).select_from(riders.outerjoin(couriers, riders.c.courierId == couriers.c.id))

        result = session.execute(stmt).mappings().all()
        logger.info(f"Extracted {len(result)} cleaned joined riders rows from source DB.")
        return result
    except Exception as e:
        logger.error(f"Error extracting joined riders+couriers: {e}")
        return []
    finally:
        session.close()

def transform_and_load_riders():
    # Extract joined rows from source DB (server-side join)
    joined_rows = extract_riders_with_couriers()

    session = Session_db_warehouse()
    try:
        if joined_rows:
            # Convert mappings to list of dicts
            rider_records = [dict(row) for row in joined_rows]
            
            # Use INSERT IGNORE to skip duplicates (faster than ON DUPLICATE KEY UPDATE)
            stmt = insert(Dim_Rider).prefix_with("IGNORE").values(rider_records)

            session.execute(stmt)
            session.commit()
            logger.info(f"Upserted {len(rider_records)} riders")

    except Exception as e:
        logger.error(f"Error during transform/load riders: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
