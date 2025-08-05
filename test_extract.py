from etl.extract import extract_customers, extract_orders

customers = extract_customers("data/customers.csv")
print(customers)

orders = extract_orders("data/orders.csv")
print(orders)