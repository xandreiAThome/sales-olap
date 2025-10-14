from util.db_source import riders, couriers
from sqlalchemy import select, text
from models.Dim_Riders import Dim_Rider
from util.db_source import Session_db_source
from util.db_warehouse import db_warehouse_engine
from util.logging_config import get_logger
import io
import csv

logger = get_logger(__name__)


def normalize_vehicle_type(vehicle_type):
    """Normalize vehicle types to standard categories in Python."""
    if not vehicle_type:
        return None
    
    vehicle_lower = vehicle_type.strip().lower()
    
    if vehicle_lower in ["bicycle", "bike"]:
        return "bicycle"
    elif vehicle_lower in ["motorbike", "motorcycle"]:
        return "motorcycle"
    elif vehicle_lower == "trike":
        return "trike"
    elif vehicle_lower == "car":
        return "car"
    else:
        return vehicle_lower


def normalize_gender(gender):
    """Normalize gender to 'male' or 'female' in Python."""
    if not gender:
        return None
    
    first_char = gender.strip().lower()[0] if gender.strip() else None
    
    if first_char == "m":
        return "male"
    elif first_char == "f":
        return "female"
    else:
        return None


def transform_and_load_riders():
    """
    Transform and load riders using PostgreSQL COPY for maximum speed.
    Truncates table first for full reload.
    Performs all data cleaning in Python (faster than SQL functions).
    """
    source_session = Session_db_source()
    
    try:
        logger.info("Starting riders ETL with PostgreSQL COPY...")
        
        # Bulk fetch ALL riders with courier join (no SQL transformations)
        logger.info("Fetching all riders from source database...")
        stmt = select(
            riders.c.id,
            riders.c.firstName,
            riders.c.lastName,
            riders.c.vehicleType,
            riders.c.age,
            riders.c.gender,
            couriers.c.name.label("courier_name")
        ).select_from(
            riders.outerjoin(couriers, riders.c.courierId == couriers.c.id)
        )
        
        result = source_session.execute(stmt).fetchall()
        logger.info(f"Fetched {len(result)} riders from source")
        
        # Transform ALL records in Python (faster than SQL)
        logger.info("Transforming all records in Python...")
        all_records = []
        
        for row in result:
            # Python transformations (much faster than SQL CONCAT/SUBSTRING)
            first_name = row.firstName.strip().title() if row.firstName else ""
            last_name = row.lastName.strip().title() if row.lastName else ""
            vehicle_type = normalize_vehicle_type(row.vehicleType)
            gender = normalize_gender(row.gender)
            courier_name = row.courier_name if row.courier_name else None
            
            all_records.append({
                "Rider_ID": row.id,
                "First_Name": first_name,
                "Last_Name": last_name,
                "Vehicle_Type": vehicle_type,
                "Age": row.age,
                "Gender": gender,
                "Courier_Name": courier_name,
            })
        
        logger.info(f"Transformed {len(all_records)} records")
        
        # Use Core connection for COPY
        conn = db_warehouse_engine.connect()
        
        try:
            # Single transaction for all operations
            with conn.begin():
                # Truncate for full reload
                logger.info("Truncating riders table for full reload...")
                conn.execute(text(f"TRUNCATE TABLE {Dim_Rider.__tablename__} CASCADE"))
                logger.info("Table truncated")
                
                # Use PostgreSQL COPY for maximum speed
                logger.info(f"Using PostgreSQL COPY for {len(all_records)} rows...")
                
                # Create CSV in memory
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                
                # Write rows in the correct column order
                for record in all_records:
                    writer.writerow([
                        record['Rider_ID'],
                        record['First_Name'],
                        record['Last_Name'],
                        record['Vehicle_Type'],
                        record['Age'],
                        record['Gender'],
                        record['Courier_Name'],
                    ])
                
                # Reset buffer to start
                csv_buffer.seek(0)
                
                # Use raw connection for COPY
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                
                # PostgreSQL COPY - fastest bulk load method
                cursor.copy_expert(
                    f"""
                    COPY {Dim_Rider.__tablename__} (
                        "Rider_ID", "First_Name", "Last_Name", "Vehicle_Type",
                        "Age", "Gender", "Courier_Name"
                    ) FROM STDIN WITH CSV
                    """,
                    csv_buffer
                )
                
                logger.info("COPY completed")
                logger.info("Committing transaction...")
        
        finally:
            conn.close()
        
        logger.info(f"✅ COPY insert completed! Total rows inserted: {len(all_records)}")
    
    except Exception as e:
        logger.error(f"Error loading riders: {e}", exc_info=True)
        raise
    finally:
        source_session.close()
