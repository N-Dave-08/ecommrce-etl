import unittest
import pandas as pd
import tempfile
import os
import json
from unittest.mock import patch, MagicMock
from etl.load import (
    get_db_config, 
    get_mock_db_config, 
    test_database_connection,
    validate_dataframe, 
    create_table_if_not_exists,
    load_to_mysql
)

class TestLoad(unittest.TestCase):
    
    def setUp(self):
        """Set up test data."""
        self.valid_customers = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice Borderland', 'Agustin Chan'],
            'email': ['alice@gmail.com', 'chan@gmail.com'],
            'join_date': ['2024-09-08', '2024-09-07']
        })
        
        self.valid_orders = pd.DataFrame({
            'id': [10, 12],
            'customer_id': [1, 2],
            'order_date': ['2024-09-09', '2024-09-09'],
            'total_amount': [123.75, 75.75]
        })
        
        self.invalid_customers = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice Borderland', 'Agustin Chan'],
            # Missing email column
            'join_date': ['2024-09-08', '2024-09-07']
        })
        
        # Create temporary config file
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, 'db_config.json')
        
        config_data = {
            "user": "test_user",
            "password": "test_password",
            "host": "localhost",
            "database": "test_db"
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(config_data, f)
    
    def tearDown(self):
        """Clean up test files."""
        import shutil
        
        # Clean up temp directory
        shutil.rmtree(self.temp_dir)
    
    def test_validate_dataframe_customers_valid(self):
        """Test customer dataframe validation with valid data."""
        result = validate_dataframe(self.valid_customers, "customers")
        self.assertTrue(result)
    
    def test_validate_dataframe_customers_invalid(self):
        """Test customer dataframe validation with invalid data."""
        result = validate_dataframe(self.invalid_customers, "customers")
        self.assertFalse(result)
    
    def test_validate_dataframe_orders_valid(self):
        """Test order dataframe validation with valid data."""
        result = validate_dataframe(self.valid_orders, "orders")
        self.assertTrue(result)
    
    def test_validate_dataframe_empty(self):
        """Test dataframe validation with empty dataframe."""
        empty_df = pd.DataFrame()
        result = validate_dataframe(empty_df, "customers")
        self.assertFalse(result)
    
    def test_validate_dataframe_unknown_table(self):
        """Test dataframe validation with unknown table name."""
        result = validate_dataframe(self.valid_customers, "unknown_table")
        self.assertFalse(result)
    
    @patch('etl.load.os.path.exists')
    @patch('builtins.open')
    @patch('json.load')
    def test_get_db_config_success(self, mock_json_load, mock_open, mock_exists):
        """Test successful database configuration loading."""
        mock_exists.return_value = True
        mock_json_load.return_value = {
            "user": "test_user",
            "password": "test_password",
            "host": "localhost",
            "database": "test_db"
        }
        
        with patch('etl.load.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            result = get_db_config()
            
            self.assertIsNotNone(result)
            mock_create_engine.assert_called_once()
    
    @patch('etl.load.os.path.exists')
    def test_get_db_config_file_not_found(self, mock_exists):
        """Test database configuration with missing file."""
        mock_exists.return_value = False
        
        result = get_db_config()
        
        self.assertIsNone(result)
    
    @patch('builtins.open')
    def test_get_db_config_invalid_json(self, mock_open):
        """Test database configuration with invalid JSON."""
        mock_open.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        
        result = get_db_config()
        
        self.assertIsNone(result)
    
    def test_get_mock_db_config(self):
        """Test SQLite mock database configuration."""
        with patch('etl.load.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            result = get_mock_db_config()
            
            self.assertIsNotNone(result)
            mock_create_engine.assert_called_once_with("sqlite:///etl_test.db")
    
    @patch('etl.load.create_engine')
    def test_test_database_connection_success(self, mock_create_engine):
        """Test successful database connection test."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        
        result = test_database_connection(mock_engine)
        
        self.assertTrue(result)
        mock_conn.execute.assert_called_once()
    
    @patch('etl.load.create_engine')
    def test_test_database_connection_failure(self, mock_create_engine):
        """Test database connection test failure."""
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("Connection failed")
        mock_create_engine.return_value = mock_engine
        
        result = test_database_connection(mock_engine)
        
        self.assertFalse(result)
    
    @patch('etl.load.get_db_config')
    @patch('etl.load.test_database_connection')
    @patch('etl.load.create_table_if_not_exists')
    @patch('pandas.DataFrame.to_sql')
    def test_load_to_mysql_success(self, mock_to_sql, mock_create_table, mock_test_conn, mock_get_config):
        """Test successful data loading to MySQL."""
        mock_engine = MagicMock()
        mock_get_config.return_value = mock_engine
        mock_test_conn.return_value = True
        
        result = load_to_mysql(self.valid_customers, "customers")
        
        self.assertTrue(result)
        mock_create_table.assert_called_once()
        mock_to_sql.assert_called_once()
    
    @patch('etl.load.get_db_config')
    def test_load_to_mysql_no_connection(self, mock_get_config):
        """Test data loading with no database connection."""
        mock_get_config.return_value = None
        
        result = load_to_mysql(self.valid_customers, "customers")
        
        self.assertFalse(result)
    
    @patch('etl.load.get_db_config')
    @patch('etl.load.get_mock_db_config')
    @patch('etl.load.test_database_connection')
    @patch('etl.load.create_table_if_not_exists')
    @patch('pandas.DataFrame.to_sql')
    def test_load_to_mysql_fallback_to_sqlite(self, mock_to_sql, mock_create_table, mock_test_conn, mock_get_mock, mock_get_config):
        """Test data loading with fallback to SQLite."""
        mock_get_config.return_value = None
        mock_engine = MagicMock()
        mock_get_mock.return_value = mock_engine
        mock_test_conn.return_value = True
        
        result = load_to_mysql(self.valid_customers, "customers")
        
        self.assertTrue(result)
        mock_get_mock.assert_called_once()
        mock_create_table.assert_called_once()
        mock_to_sql.assert_called_once()
    
    def test_load_to_mysql_invalid_dataframe(self):
        """Test data loading with invalid dataframe."""
        result = load_to_mysql(self.invalid_customers, "customers")
        
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main() 