from etl.transform import clean_customers, clean_orders
from etl.extract import extract_customers, extract_orders

customers = extract_customers("data/customers.csv")
orders = extract_orders("data/orders.csv")

clean_customers = clean_customers(customers)
clean_orders = clean_orders(orders, clean_customers)

print("Cleaned customers:")
print(clean_customers.head())
print("\nCleaned orders:")
print(clean_orders.head())