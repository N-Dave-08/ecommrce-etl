import unittest
import pandas as pd
import tempfile
import os
from etl.extract import extract_customers, extract_orders, validate_file_exists, validate_csv_structure

class TestExtract(unittest.TestCase):
    
    def setUp(self):
        """Set up test data."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test customers CSV
        self.customers_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice Borderland', 'Agustin Chan'],
            'email': ['alice@gmail.com', 'chan@gmail.com'],
            'join_date': ['2024-09-08', '2024-09-07']
        })
        self.customers_file = os.path.join(self.temp_dir, 'test_customers.csv')
        self.customers_data.to_csv(self.customers_file, index=False)
        
        # Create test orders CSV
        self.orders_data = pd.DataFrame({
            'id': [10, 12],
            'customer_id': [1, 2],
            'order_date': ['2024-09-09', '2024-09-09'],
            'total_amount': [123.75, 75.75]
        })
        self.orders_file = os.path.join(self.temp_dir, 'test_orders.csv')
        self.orders_data.to_csv(self.orders_file, index=False)
    
    def tearDown(self):
        """Clean up test files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_validate_file_exists(self):
        """Test file existence validation."""
        # Test existing file
        self.assertTrue(validate_file_exists(self.customers_file))
        
        # Test non-existent file
        self.assertFalse(validate_file_exists('non_existent_file.csv'))
    
    def test_validate_csv_structure(self):
        """Test CSV structure validation."""
        # Test valid structure
        df = pd.read_csv(self.customers_file)
        self.assertTrue(validate_csv_structure(df, ['id', 'name', 'email', 'join_date'], self.customers_file))
        
        # Test missing columns
        self.assertFalse(validate_csv_structure(df, ['id', 'name', 'email', 'join_date', 'missing_col'], self.customers_file))
        
        # Test empty dataframe
        empty_df = pd.DataFrame()
        self.assertFalse(validate_csv_structure(empty_df, ['id'], self.customers_file))
    
    def test_extract_customers_success(self):
        """Test successful customer extraction."""
        result = extract_customers(self.customers_file)
        self.assertFalse(result.empty)
        self.assertEqual(len(result), 2)
        self.assertListEqual(list(result.columns), ['id', 'name', 'email', 'join_date'])
    
    def test_extract_customers_file_not_found(self):
        """Test customer extraction with non-existent file."""
        result = extract_customers('non_existent_file.csv')
        self.assertTrue(result.empty)
    
    def test_extract_orders_success(self):
        """Test successful order extraction."""
        result = extract_orders(self.orders_file)
        self.assertFalse(result.empty)
        self.assertEqual(len(result), 2)
        self.assertListEqual(list(result.columns), ['id', 'customer_id', 'order_date', 'total_amount'])
    
    def test_extract_orders_file_not_found(self):
        """Test order extraction with non-existent file."""
        result = extract_orders('non_existent_file.csv')
        self.assertTrue(result.empty)

if __name__ == '__main__':
    unittest.main() 