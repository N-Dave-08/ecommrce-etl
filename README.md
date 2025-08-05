# E-commerce ETL Pipeline

A robust, production-ready ETL (Extract, Transform, Load) pipeline for processing e-commerce data from CSV files to MySQL database with comprehensive data validation, error handling, and logging.

## 🎯 Features

- **Comprehensive Data Validation**: Validates file existence, CSV structure, and data integrity
- **Robust Error Handling**: Detailed logging and graceful error recovery
- **Data Quality Checks**: Email validation, business logic validation, and duplicate removal
- **Database Integration**: Secure MySQL connection with table creation and backup capabilities
- **Configurable**: Centralized configuration management
- **Testable**: Comprehensive unit tests included
- **Production-Ready**: Proper exit codes, logging levels, and error recovery

## 📁 Project Structure

```
e-commerce-etl/
├── config/
│   ├── db_config.json      # Database connection settings
│   └── etl_config.json     # ETL pipeline configuration
├── data/
│   ├── customers.csv       # Customer data source
│   └── orders.csv          # Order data source
├── etl/
│   ├── extract.py          # Data extraction functions
│   ├── transform.py        # Data transformation and cleaning
│   └── load.py            # Database loading functions
├── tests/
│   └── test_extract.py    # Unit tests
├── main.py                # Main ETL pipeline
├── requirements.txt        # Python dependencies
└── README.md             # This file
```

## 🚀 Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd e-commerce-etl
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure database**:
   Edit `config/db_config.json` with your MySQL connection details:
   ```json
   {
       "user": "your_username",
       "password": "your_password",
       "host": "localhost",
       "database": "ecommerce"
   }
   ```

## 🏃‍♂️ Usage

### Running the ETL Pipeline

```bash
python main.py
```

The pipeline will:
1. Extract data from CSV files with comprehensive validation
2. Transform and clean the data with strict quality checks
3. Load data to MySQL database with error handling
4. Log all operations to `etl.log` and console

### Running Tests

The project includes comprehensive unit tests organized in the `tests/` directory:

```bash
# Run all tests
python tests/run_tests.py

# Run specific test modules
python -m unittest tests.test_extract
python -m unittest tests.test_transform
python -m unittest tests.test_load

# Run simple integration tests
python tests/test_extract_simple.py
python tests/test_transform_simple.py
python tests/test_load_simple.py
```

### Test Structure

```
tests/
├── __init__.py              # Tests package
├── run_tests.py             # Test runner with summary
├── test_extract.py          # Comprehensive extract tests
├── test_transform.py        # Comprehensive transform tests
├── test_load.py            # Comprehensive load tests
├── test_extract_simple.py  # Simple extract integration test
├── test_transform_simple.py # Simple transform integration test
└── test_load_simple.py     # Simple load integration test
```

### Test Coverage

- **Extract Module**: File validation, CSV parsing, error handling
- **Transform Module**: Data cleaning, validation, business logic
- **Load Module**: Database operations, connection handling
- **Integration Tests**: End-to-end pipeline testing

### Test Features

- **Comprehensive Coverage**: Tests all major functions and edge cases
- **Mock Database**: Uses SQLite for testing when MySQL unavailable
- **Error Scenarios**: Tests file not found, invalid data, connection failures
- **Data Validation**: Tests email validation, customer ID validation, business logic
- **Detailed Logging**: Test output includes detailed logging for debugging

## 🔍 Data Validation

### Customer Data Validation
- **Email Format Validation**: Strict regex validation requiring proper domain extensions (`.com`, `.org`, `.net`, etc.)
- **Unique Constraints**: Ensures unique customer IDs and emails
- **Date Validation**: Validates join dates and removes invalid entries
- **Duplicate Removal**: Removes duplicate entries based on ID and email
- **Data Type Validation**: Ensures proper data types for all fields

### Order Data Validation
- **Customer Reference Validation**: Validates customer ID references against customers table
- **Business Logic Validation**: 
  - Ensures positive total amounts
  - Validates order dates (not in future, not too old)
  - Removes invalid customer references
- **Data Type Validation**: Ensures proper numeric and date formats
- **Cross-Reference Validation**: Only orders with valid customer IDs are processed

## ⚙️ Configuration

### ETL Configuration (`config/etl_config.json`)

```json
{
    "data_paths": {
        "customers": "data/customers.csv",
        "orders": "data/orders.csv"
    },
    "validation": {
        "max_order_age_years": 10,
        "min_total_amount": 0.01,
        "email_validation": true
    },
    "logging": {
        "level": "INFO",
        "file": "etl.log",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    },
    "database": {
        "backup_before_replace": true,
        "create_tables_if_not_exist": true
    }
}
```

## 🛡️ Error Handling

The pipeline includes comprehensive error handling:

- **File Validation**: Checks file existence and readability
- **Data Validation**: Validates CSV structure and data types
- **Database Validation**: Tests database connections and table structures
- **Business Logic Validation**: Applies domain-specific rules
- **Logging**: Detailed logging to both file and console
- **Graceful Degradation**: Continues processing even if some data is invalid
- **Fallback Options**: SQLite fallback when MySQL is unavailable

## 📊 Logging

Logs are written to:
- Console output for real-time monitoring
- `etl.log` file for persistent records

Log levels:
- `INFO`: Normal operations
- `WARNING`: Non-critical issues
- `ERROR`: Critical failures

Example log output:
```
2025-08-06 04:24:58,286 - INFO - Starting ETL pipeline
2025-08-06 04:24:58,289 - INFO - Successfully extracted 2 rows from data/customers.csv
2025-08-06 04:24:58,303 - INFO - Final cleaned customers: 1 rows (from 2)
2025-08-06 04:24:58,520 - INFO - Successfully loaded 1 rows to customers
```

## 🗄️ Database Schema

### Customers Table
```sql
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    join_date DATETIME NOT NULL
);
```

### Orders Table
```sql
CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);
```

## 🔧 Data Processing Details

### Extract Phase
- **File Validation**: Checks file existence, readability, and CSV structure
- **Error Handling**: Graceful handling of missing files, empty files, and parsing errors
- **Logging**: Detailed extraction statistics and error reporting

### Transform Phase
- **Customer Data Cleaning**:
  - Email format validation with strict regex patterns
  - Duplicate removal (ID and email uniqueness)
  - Date validation and conversion
  - Name field cleaning and validation
- **Order Data Cleaning**:
  - Customer ID cross-reference validation
  - Business logic validation (positive amounts, valid dates)
  - Data type conversion and validation
  - Invalid data filtering

### Load Phase
- **Database Connection**: Secure connection with fallback options
- **Table Management**: Automatic table creation with proper schema
- **Data Loading**: Efficient bulk loading with error handling
- **Backup Capability**: Optional table backup before replacement

## 🧪 Testing

### Running Tests

The project includes comprehensive unit tests organized in the `tests/` directory:

```bash
# Run all tests with beautiful summary
python tests/run_tests.py

# Run specific test modules
python -m unittest tests.test_extract
python -m unittest tests.test_transform
python -m unittest tests.test_load
```

### Test Structure

```
tests/
├── __init__.py              # Tests package
├── run_tests.py             # Test runner with summary
├── test_extract.py          # Comprehensive extract tests
├── test_transform.py        # Comprehensive transform tests
└── test_load.py            # Comprehensive load tests
```

### Test Coverage

- **Extract Module**: File validation, CSV parsing, error handling
- **Transform Module**: Data cleaning, validation, business logic
- **Load Module**: Database operations, connection handling

### Test Features

- **Comprehensive Coverage**: Tests all major functions and edge cases
- **Mock Database**: Uses SQLite for testing when MySQL unavailable
- **Error Scenarios**: Tests file not found, invalid data, connection failures
- **Data Validation**: Tests email validation, customer ID validation, business logic
- **Detailed Logging**: Test output includes detailed logging for debugging

## 🚨 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check `config/db_config.json` settings
   - Ensure MySQL server is running
   - Verify database exists
   - Pipeline will fallback to SQLite for testing

2. **File Not Found**
   - Check file paths in configuration
   - Ensure CSV files exist in data directory
   - Verify file permissions

3. **Data Validation Errors**
   - Check CSV file structure
   - Verify data types and formats
   - Review log files for specific errors
   - Invalid emails will be filtered out automatically

4. **Invalid Customer References**
   - Orders with invalid customer IDs are automatically filtered
   - Check customer data quality in source files

### Log Analysis

Check `etl.log` for detailed error information:
```bash
# Windows
type etl.log

# Linux/Mac
tail -f etl.log
```

## 📈 Performance Features

- **Efficient Data Processing**: Optimized pandas operations
- **Connection Pooling**: Database connection management
- **Batch Processing**: Handles large datasets efficiently
- **Memory Management**: Proper resource cleanup
- **Error Recovery**: Continues processing despite individual record failures

## 🔮 Future Enhancements

### Planned Features
- [ ] **Incremental Loading**: Delta updates instead of full replacement
- [ ] **Data Lineage**: Track data transformations
- [ ] **Monitoring Dashboard**: Real-time pipeline health metrics
- [ ] **Scheduling**: Automated pipeline execution
- [ ] **Docker Support**: Containerized deployment
- [ ] **CI/CD Integration**: Automated testing and deployment

### Technical Improvements
- [ ] **Async Processing**: Better performance for large datasets
- [ ] **Configuration Management**: Environment-specific settings
- [ ] **Advanced Logging**: Structured logging with correlation IDs
- [ ] **Metrics Collection**: Performance and quality metrics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add comprehensive docstrings
- Include unit tests for new features
- Update documentation for API changes

## 📝 License

This project is licensed under the MIT License.

---

**Note**: This ETL pipeline is production-ready with comprehensive error handling, data validation, and logging. It automatically filters out invalid data (like malformed emails or invalid customer references) while providing detailed feedback on the processing results.
