import logging
import sys
from datetime import datetime


def setup_logging(log_level=logging.INFO):
    """
    Centralized logging configuration for the ETL pipeline.
    Sets up logging with consistent formatting across all modules.
    """
    # Create a custom formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear any existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    return root_logger


def get_logger(name):
    """Get a logger instance for a specific module."""
    return logging.getLogger(name)