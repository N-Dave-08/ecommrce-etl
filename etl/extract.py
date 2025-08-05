import pandas as pd
import logging
import os
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_file_exists(file_path: str) -> bool:
    """
    Validate that the file exists and is readable.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False
    
    if not os.access(file_path, os.R_OK):
        logger.error(f"File not readable: {file_path}")
        return False
    
    return True

def validate_csv_structure(df: pd.DataFrame, expected_columns: list, file_path: str) -> bool:
    """
    Validate that the CSV has the expected structure.
    """
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"Missing required columns in {file_path}: {missing_columns}")
        return False
    
    if df.empty:
        logger.warning(f"File {file_path} is empty")
        return False
    
    return True

def extract_customers(file_path: str) -> pd.DataFrame:
    """
    Extract customer data from a CSV file.
    """
    try:
        # Validate file exists
        if not validate_file_exists(file_path):
            return pd.DataFrame()
        
        # Read CSV
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
        
        # Validate structure
        expected_columns = ['id', 'name', 'email', 'join_date']
        if not validate_csv_structure(df, expected_columns, file_path):
            return pd.DataFrame()
        
        return df
        
    except pd.errors.EmptyDataError:
        logger.error(f"File {file_path} is empty")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV file {file_path}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error extracting customers from {file_path}: {e}")
        return pd.DataFrame()

def extract_orders(file_path: str) -> pd.DataFrame:
    """
    Extract order data from a CSV file.
    """
    try:
        # Validate file exists
        if not validate_file_exists(file_path):
            return pd.DataFrame()
        
        # Read CSV
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
        
        # Validate structure
        expected_columns = ['id', 'customer_id', 'order_date', 'total_amount']
        if not validate_csv_structure(df, expected_columns, file_path):
            return pd.DataFrame()
        
        return df
        
    except pd.errors.EmptyDataError:
        logger.error(f"File {file_path} is empty")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV file {file_path}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error extracting orders from {file_path}: {e}")
        return pd.DataFrame()
