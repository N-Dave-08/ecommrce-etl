from etl.extract import extract_customers, extract_orders

customers = extract_customers("data/customers.csv")
print(customers.head())

orders = extract_orders("data/orders.csv")
print(orders.head())
