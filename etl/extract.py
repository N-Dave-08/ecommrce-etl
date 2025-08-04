import pandas as pd

def extract_customers(file_path: str) -> pd.DataFrame:
    """
    Load customer data from a CSV file.
    """
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Loaded {len(df)} customer records.")
        return df
    except Exception as e:
        print(f"❌ Failed to load customers: {e}")
        return pd.DataFrame()

def extract_orders(file_path: str) -> pd.DataFrame:
    """
    Load order data from a CSV file.
    """
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Loaded {len(df)} order records.")
        return df
    except Exception as e:
        print(f"❌ Failed to load orders: {e}")
        return pd.DataFrame()
