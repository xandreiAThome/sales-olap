from dotenv import load_dotenv
from etl_scripts.products_etl import transform_and_load_products
from etl_scripts.users_etl import transform_and_load_users
from util.db_source import db_source_engine
from util.db_warehouse import db_warehouse_engine, Session_db_warehouse
from sqlalchemy import text
from etl_scripts.rider_etl import transform_and_load_riders
from etl_scripts.order_date_etl import load_transform_date_and_order_items
from util.logging_config import setup_logging, get_logger
import time
import sys
from contextlib import contextmanager

load_dotenv()

# Setup centralized logging
setup_logging()
logger = get_logger(__name__)


def test_database_connections():
    """Test connections to both source and warehouse databases."""
    try:
        logger.info("Testing database connections...")
        
        # Test source database
        with db_source_engine.connect() as conn:
            result = conn.execute(text("SELECT NOW()"))
            source_time = result.scalar()
            logger.info(f"Source DB connected! Server time: {source_time}")

        # Test warehouse database
        with db_warehouse_engine.connect() as conn:
            result = conn.execute(text("SELECT NOW()"))
            warehouse_time = result.scalar()
            logger.info(f"Warehouse DB connected! Server time: {warehouse_time}")

        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}", exc_info=True)
        return False


def run_etl_step(step_name, etl_function):
    """
    Execute a single ETL step with error handling and logging.
    
    Args:
        step_name (str): Name of the ETL step for logging
        etl_function (callable): The ETL function to execute
    
    Returns:
        bool: True if step succeeded, False otherwise
    """
    try:
        logger.info(f"Starting ETL step: {step_name}")
        start_time = time.time()
        
        etl_function()
        
        duration = time.time() - start_time
        logger.info(f"Completed ETL step: {step_name} in {duration:.2f} seconds")
        return True
        
    except Exception as e:
        logger.error(f"Failed ETL step: {step_name} - {e}", exc_info=True)
        return False


def display_sample_data():
    """Display sample data from each dimension and fact table."""
    try:
        logger.info("Displaying sample data from transformed tables...")
        
        session = Session_db_warehouse()
        
        tables = [
            ("dim_riders", "Riders"),
            ("dim_products", "Products"), 
            ("dim_users", "Users"),
            ("dim_date", "Date"),
            ("fact_order_items", "Order Items")
        ]
        
        for table_name, display_name in tables:
            try:
                result = session.execute(text(f"SELECT * FROM {table_name} LIMIT 5"))
                rows = result.fetchall()
                logger.info(f"Sample {display_name} data ({len(rows)} rows):")
                for row in rows:
                    logger.info(f"  {row}")
            except Exception as e:
                logger.warning(f"Could not fetch sample data from {table_name}: {e}")
                
        session.close()
        
    except Exception as e:
        logger.error(f"Error displaying sample data: {e}", exc_info=True)
def main():
    """
    Main ETL pipeline orchestration with proper error handling and rollback.
    """
    logger.info("="*60)
    logger.info("Starting ETL Pipeline")
    logger.info("="*60)
    
    start_time = time.time()
    
    try:
        # Test database connections first
        if not test_database_connections():
            logger.error("Database connection tests failed. Aborting ETL pipeline.")
            sys.exit(1)
        
        # Define ETL steps in execution order
        etl_steps = [
            ("Load Riders", transform_and_load_riders),
            ("Load Products", transform_and_load_products), 
            ("Load Users", transform_and_load_users),
            ("Load Dates and Order Items", load_transform_date_and_order_items)
        ]
        
        # Track step results
        failed_steps = []
        successful_steps = []
        
        # Execute each ETL step
        for step_name, etl_function in etl_steps:
            if run_etl_step(step_name, etl_function):
                successful_steps.append(step_name)
            else:
                failed_steps.append(step_name)
                # Decide whether to continue or stop on failure
                logger.warning(f"Step '{step_name}' failed, but continuing with remaining steps...")
                # Uncomment the next line if you want to stop on first failure:
                # break
        
        # Display results summary
        total_duration = time.time() - start_time
        
        logger.info("="*60)
        logger.info("ETL Pipeline Summary")
        logger.info("="*60)
        logger.info(f"Total execution time: {total_duration:.2f} seconds")
        logger.info(f"Successful steps ({len(successful_steps)}): {', '.join(successful_steps)}")
        
        if failed_steps:
            logger.error(f"Failed steps ({len(failed_steps)}): {', '.join(failed_steps)}")
            logger.warning("Some ETL steps failed. Please check the logs above for details.")
        else:
            logger.info("All ETL steps completed successfully!")
        
        # Display sample data if all steps succeeded
        if not failed_steps:
            display_sample_data()
        
        # Exit with appropriate code
        if failed_steps:
            logger.error("ETL pipeline completed with errors.")
            sys.exit(1)
        else:
            logger.info("ETL pipeline completed successfully.")
            
    except KeyboardInterrupt:
        logger.warning("ETL pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in ETL pipeline: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
