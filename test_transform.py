from etl.extract import extract_customers, extract_orders
from etl.transform import clean_customers, clean_orders

customers = extract_customers("data/customers.csv")
orders = extract_orders("data/orders.csv")

cleaned_customers = clean_customers(customers)
cleaned_orders = clean_orders(orders)

print("✅ Cleaned Customers:")
print(cleaned_customers)

print("\n✅ Cleaned Orders:")
print(cleaned_orders)
