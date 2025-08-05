import pandas as pd
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

def validate_email(email: str) -> bool:
    """
    Validate email format using regex.
    """
    # Strict pattern requiring proper domain extension
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize customer data.
    """
    if df.empty:
        logger.warning("Empty customers dataframe provided")
        return df
    
    df = df.copy()
    original_count = len(df)
    
    # Drop rows with missing critical fields
    df = df.dropna(subset=['id', 'name', 'email'])
    logger.info(f"Removed {original_count - len(df)} rows with missing critical fields")
    
    # Ensure id is numeric and unique
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(int)
    
    # Remove duplicates based on id
    df = df.drop_duplicates(subset=['id'])
    logger.info(f"Removed {original_count - len(df)} duplicate customer IDs")
    
    # Clean name field
    df['name'] = df['name'].str.strip()
    df = df[df['name'].str.len() > 0]
    
    # Clean and validate email
    df['email'] = df['email'].str.lower().str.strip()
    df['email'] = df['email'].apply(lambda x: x if validate_email(x) else None)
    df = df.dropna(subset=['email'])
    
    # Remove duplicate emails
    df = df.drop_duplicates(subset=['email'])
    
    # Convert join_date to datetime
    df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
    df = df.dropna(subset=['join_date'])
    
    logger.info(f"Final cleaned customers: {len(df)} rows (from {original_count})")
    return df

def validate_customer_ids(orders_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate that all customer_ids in orders exist in customers table.
    """
    if orders_df.empty or customers_df.empty:
        logger.warning("Empty dataframe provided for customer ID validation")
        return orders_df
    
    valid_customer_ids = customers_df['id'].unique()
    original_count = len(orders_df)
    
    # Filter orders to only include valid customer IDs
    orders_df = orders_df[orders_df['customer_id'].isin(valid_customer_ids)]
    
    filtered_count = len(orders_df)
    if original_count != filtered_count:
        logger.info(f"Removed {original_count - filtered_count} orders with invalid customer IDs")
    
    return orders_df

def validate_order_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate order data for business logic.
    """
    if df.empty:
        return df
    
    original_count = len(df)
    
    # Ensure total_amount is positive
    df = df[df['total_amount'] > 0]
    
    # Ensure order_date is not in the future
    current_date = pd.Timestamp.now()
    df = df[df['order_date'] <= current_date]
    
    # Ensure order_date is not too old (e.g., not older than 10 years)
    ten_years_ago = current_date - pd.DateOffset(years=10)
    df = df[df['order_date'] >= ten_years_ago]
    
    filtered_count = len(df)
    if original_count != filtered_count:
        logger.info(f"Removed {original_count - filtered_count} orders with invalid business logic")
    
    return df

def clean_orders(df: pd.DataFrame, customers_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Clean and standardize order data.
    """
    if df.empty:
        logger.warning("Empty orders dataframe provided")
        return df
    
    df = df.copy()
    original_count = len(df)
    
    # Drop rows with missing critical fields
    df = df.dropna(subset=['id', 'customer_id', 'total_amount'])
    logger.info(f"Removed {original_count - len(df)} rows with missing critical fields")
    
    # Ensure id is numeric and unique
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(int)
    
    # Remove duplicates based on id
    df = df.drop_duplicates(subset=['id'])
    
    # Convert customer_id to numeric and drop invalid values
    df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce')
    df = df.dropna(subset=['customer_id'])
    df['customer_id'] = df['customer_id'].astype(int)
    
    # Convert order_date to datetime
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df = df.dropna(subset=['order_date'])
    
    # Ensure total_amount is numeric and positive
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    df = df.dropna(subset=['total_amount'])
    df = df[df['total_amount'] > 0]
    
    # Validate customer IDs if customers dataframe is provided
    if customers_df is not None:
        df = validate_customer_ids(df, customers_df)
    
    # Apply business logic validation
    df = validate_order_data(df)
    
    logger.info(f"Final cleaned orders: {len(df)} rows (from {original_count})")
    return df
