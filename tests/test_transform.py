import unittest
import pandas as pd
import tempfile
import os
from etl.transform import clean_customers, clean_orders, validate_email, validate_customer_ids, validate_order_data

class TestTransform(unittest.TestCase):
    
    def setUp(self):
        """Set up test data."""
        # Valid customer data
        self.valid_customers = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice Borderland', 'Agustin Chan', 'John Doe'],
            'email': ['alice@gmail.com', 'chan@gmail.com', 'john@example.com'],
            'join_date': ['2024-09-08', '2024-09-07', '2024-09-06']
        })
        
        # Invalid customer data (missing email, invalid email, duplicate)
        self.invalid_customers = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'name': ['Alice Borderland', 'Agustin Chan', 'John Doe', 'Jane Smith'],
            'email': ['alice@gmail', 'chan@gmail.com', 'invalid-email', 'chan@gmail.com'],  # Invalid, valid, invalid, duplicate
            'join_date': ['2024-09-08', '2024-09-07', '2024-09-06', '2024-09-05']
        })
        
        # Valid order data
        self.valid_orders = pd.DataFrame({
            'id': [10, 12, 13],
            'customer_id': [1, 2, 3],
            'order_date': ['2024-09-09', '2024-09-09', '2024-09-10'],
            'total_amount': [123.75, 75.75, 200.00]
        })
        
        # Invalid order data (invalid customer_id, negative amount, future date)
        self.invalid_orders = pd.DataFrame({
            'id': [10, 12, 13, 14],
            'customer_id': [1, 999, 3, 't'],  # Valid, invalid, valid, invalid
            'order_date': ['2024-09-09', '2024-09-09', '2024-09-10', '2025-12-31'],  # Valid, valid, valid, future
            'total_amount': [123.75, -50.00, 200.00, 0.00]  # Valid, negative, valid, zero
        })
    
    def test_validate_email(self):
        """Test email validation function."""
        # Valid emails
        self.assertTrue(validate_email('test@example.com'))
        self.assertTrue(validate_email('user.name@domain.org'))
        self.assertTrue(validate_email('user+tag@domain.net'))
        
        # Invalid emails
        self.assertFalse(validate_email('test@example'))  # No domain extension
        self.assertFalse(validate_email('invalid-email'))
        self.assertFalse(validate_email('test@.com'))  # No domain
        self.assertFalse(validate_email('@example.com'))  # No username
    
    def test_clean_customers_valid_data(self):
        """Test customer cleaning with valid data."""
        result = clean_customers(self.valid_customers)
        
        # Should keep all valid customers
        self.assertEqual(len(result), 3)
        self.assertListEqual(list(result['id']), [1, 2, 3])
        self.assertListEqual(list(result['email']), ['alice@gmail.com', 'chan@gmail.com', 'john@example.com'])
    
    def test_clean_customers_invalid_data(self):
        """Test customer cleaning with invalid data."""
        result = clean_customers(self.invalid_customers)
        
        # Should filter out invalid emails and duplicates
        self.assertEqual(len(result), 1)  # Only chan@gmail.com should remain
        self.assertListEqual(list(result['id']), [2])
        self.assertListEqual(list(result['email']), ['chan@gmail.com'])
    
    def test_clean_customers_empty_data(self):
        """Test customer cleaning with empty dataframe."""
        empty_df = pd.DataFrame()
        result = clean_customers(empty_df)
        self.assertTrue(result.empty)
    
    def test_validate_customer_ids(self):
        """Test customer ID validation."""
        # Valid customer IDs
        valid_customers = pd.DataFrame({'id': [1, 2, 3]})
        valid_orders = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'total_amount': [100, 200, 300]
        })
        
        result = validate_customer_ids(valid_orders, valid_customers)
        self.assertEqual(len(result), 3)  # All orders should be kept
        
        # Invalid customer IDs
        invalid_orders = pd.DataFrame({
            'customer_id': [1, 999, 3, 4],
            'total_amount': [100, 200, 300, 400]
        })
        
        result = validate_customer_ids(invalid_orders, valid_customers)
        self.assertEqual(len(result), 2)  # Only orders with customer_id 1 and 3 should be kept
    
    def test_validate_order_data(self):
        """Test order data validation."""
        # Create orders with various issues
        orders = pd.DataFrame({
            'total_amount': [100.00, -50.00, 0.00, 200.00],
            'order_date': ['2024-09-09', '2024-09-09', '2024-09-09', '2025-12-31']  # Future date
        })
        orders['order_date'] = pd.to_datetime(orders['order_date'])
        
        result = validate_order_data(orders)
        
        # Should only keep orders with positive amounts and valid dates
        self.assertEqual(len(result), 1)  # Only the first order should remain
        self.assertEqual(result.iloc[0]['total_amount'], 100.00)
    
    def test_clean_orders_valid_data(self):
        """Test order cleaning with valid data."""
        result = clean_orders(self.valid_orders, self.valid_customers)
        
        # Should keep all valid orders
        self.assertEqual(len(result), 3)
        self.assertListEqual(list(result['id']), [10, 12, 13])
    
    def test_clean_orders_invalid_data(self):
        """Test order cleaning with invalid data."""
        result = clean_orders(self.invalid_orders, self.valid_customers)
        
        # Should filter out invalid customer IDs, negative amounts, future dates
        # Only orders with id 10 and 13 should remain (valid customer_id and positive amount)
        self.assertEqual(len(result), 2)  # Orders with id 10 and 13 should remain
        self.assertIn(10, result['id'].values)
        self.assertIn(13, result['id'].values)
    
    def test_clean_orders_without_customers(self):
        """Test order cleaning without customer validation."""
        result = clean_orders(self.valid_orders)
        
        # Should process orders without customer validation
        self.assertEqual(len(result), 3)
    
    def test_clean_orders_empty_data(self):
        """Test order cleaning with empty dataframe."""
        empty_df = pd.DataFrame()
        result = clean_orders(empty_df)
        self.assertTrue(result.empty)

if __name__ == '__main__':
    unittest.main() 