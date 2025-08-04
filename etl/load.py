import pandas as pd
from sqlalchemy import create_engine
import json
import os

def get_db_engine():
    """
    Create a SQLAlchemy engine from db_config.json
    """
    with open("config/db_config.json") as f:
        config = json.load(f)

    db_url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    engine = create_engine(db_url)
    return engine


def load_to_mysql(df: pd.DataFrame, table_name: str):
    """
    Load a DataFrame into a MySQL table.
    If the table exists, it will be replaced.
    """
    engine = get_db_engine()
    try:
        df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        print(f"✅ Loaded {len(df)} records into `{table_name}`.")
    except Exception as e:
        print(f"❌ Failed to load data into {table_name}: {e}")
