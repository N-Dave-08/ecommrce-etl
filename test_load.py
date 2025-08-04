from etl.extract import extract_customers, extract_orders
from etl.transform import clean_customers, clean_orders
from etl.load import load_to_mysql

# Extract
customers = extract_customers("data/customers.csv")
orders = extract_orders("data/orders.csv")

# Transform
customers_clean = clean_customers(customers)
orders_clean = clean_orders(orders)

# Load
load_to_mysql(customers_clean, "customers")
load_to_mysql(orders_clean, "orders")
