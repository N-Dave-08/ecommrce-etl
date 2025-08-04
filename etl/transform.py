import pandas as pd

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize customer data.
    """
    df = df.copy()

    # Drop rows with missing email
    df = df.dropna(subset=["email"])

    # Make email lowercase
    df["email"] = df["email"].str.lower().str.strip()

    # Convert join_date to datetime
    df["join_date"] = pd.to_datetime(df["join_date"], errors='coerce')

    # Drop rows with invalid dates
    df = df.dropna(subset=["join_date"])

    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize order data.
    """
    df = df.copy()

    # Convert order_date to datetime
    df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce')

    # Drop rows with missing or invalid dates
    df = df.dropna(subset=["order_date"])

    # Drop rows with missing amount or customer_id
    df = df.dropna(subset=["total_amount", "customer_id"])

    # Ensure total_amount is float
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors='coerce')

    return df
