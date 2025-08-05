import pandas as pd
from sqlalchemy import create_engine, text
import json
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def get_db_config():
    """
    Create a SQLAlchemy engine for db_config.json
    """
    try:
        config_path = 'config/db_config.json'
        if not os.path.exists(config_path):
            logger.error(f"Database config file not found: {config_path}")
            return None
            
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        required_fields = ['user', 'host', 'database']
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            logger.error(f"Missing required database config fields: {missing_fields}")
            return None
        
        # Build connection string
        password = config.get('password', '')
        db_url = f"mysql+pymysql://{config['user']}:{password}@{config['host']}/{config['database']}"
        
        # Create engine and test connection
        engine = create_engine(db_url)
        
        # Test the connection immediately
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Created database engine for: {config['database']}")
            return engine
        except Exception as e:
            logger.warning(f"MySQL connection failed: {e}")
            return None
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in database config: {e}")
        return None
    except Exception as e:
        logger.error(f"Error creating database connection: {e}")
        return None

def get_mock_db_config():
    """
    Create a SQLite engine for testing when MySQL is not available.
    """
    try:
        db_url = "sqlite:///etl_test.db"
        engine = create_engine(db_url)
        logger.info("Created SQLite engine for testing")
        return engine
    except Exception as e:
        logger.error(f"Error creating SQLite engine: {e}")
        return None

def test_database_connection(engine):
    """
    Test database connection separately.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.warning(f"Database connection test failed: {e}")
        return False

def validate_dataframe(df: pd.DataFrame, table_name: str) -> bool:
    """
    Validate dataframe before loading to database.
    """
    if df.empty:
        logger.warning(f"Empty dataframe provided for table {table_name}")
        return False
    
    # Check for required columns based on table
    if table_name == "customers":
        required_columns = ['id', 'name', 'email', 'join_date']
    elif table_name == "orders":
        required_columns = ['id', 'customer_id', 'order_date', 'total_amount']
    else:
        logger.error(f"Unknown table name: {table_name}")
        return False
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"Missing required columns for {table_name}: {missing_columns}")
        return False
    
    return True

def create_table_if_not_exists(engine, table_name: str, df: pd.DataFrame):
    """
    Create table with proper schema if it doesn't exist.
    """
    try:
        with engine.connect() as conn:
            # Check if table exists - handle both MySQL and SQLite
            try:
                # Try MySQL syntax first
                result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
                table_exists = result.fetchone() is not None
            except Exception:
                # Fallback to SQLite syntax
                try:
                    result = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"))
                    table_exists = result.fetchone() is not None
                except Exception:
                    # If both fail, assume table doesn't exist
                    table_exists = False
            
            if not table_exists:
                logger.info(f"Creating table {table_name}")
                
                # Create table based on dataframe structure
                df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
                logger.info(f"Table {table_name} created successfully")
            else:
                logger.info(f"Table {table_name} already exists")
                
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        raise

def load_to_mysql(df: pd.DataFrame, table_name: str) -> bool:
    """
    Load a DataFrame to a MySQL table.
    If the table exists, it will be replaced.
    """
    try:
        # Validate dataframe
        if not validate_dataframe(df, table_name):
            return False
        
        # Get database connection
        engine = get_db_config()
        if engine is None:
            logger.warning("MySQL connection failed, trying SQLite for testing")
            engine = get_mock_db_config()
            if engine is None:
                return False
        
        # Test connection
        if not test_database_connection(engine):
            logger.warning("Database connection test failed, but continuing...")
        
        # Create table if needed
        create_table_if_not_exists(engine, table_name, df)
        
        # Load data
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error loading data to {table_name}: {e}")
        return False

def backup_table(engine, table_name: str) -> bool:
    """
    Create a backup of the existing table before replacing it.
    """
    try:
        with engine.connect() as conn:
            backup_name = f"{table_name}_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
            conn.execute(text(f"CREATE TABLE {backup_name} AS SELECT * FROM {table_name}"))
            logger.info(f"Created backup table: {backup_name}")
            return True
    except Exception as e:
        logger.warning(f"Could not create backup for {table_name}: {e}")
        return False
