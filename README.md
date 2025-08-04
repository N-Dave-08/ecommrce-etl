# E-commerce ETL Pipeline

A robust Python ETL (Extract, Transform, Load) pipeline designed to process e-commerce customer and order data from CSV files and load it into a MySQL database. This project provides a clean, modular architecture for data processing with comprehensive error handling and logging.

## 📋 Project Overview

This ETL pipeline automates the process of:
- **Extract**: Reading customer and order data from CSV files
- **Transform**: Cleaning and standardizing data (email normalization, date parsing, validation)
- **Load**: Storing processed data into MySQL database tables

The pipeline is designed for reliability and maintainability, with separate modules for each ETL phase and comprehensive error handling.

## 🛠 Tech Stack

- **Python 3.8+**
- **Pandas**: Data manipulation and analysis
- **SQLAlchemy**: Database ORM and connection management
- **PyMySQL**: MySQL database connector
- **MySQL**: Target database system

## 📁 Project Structure

```
e-commerce-etl/
├── config/
│   └── db_config.json          # Database connection configuration
├── data/
│   ├── customers.csv           # Source customer data
│   └── orders.csv             # Source order data
├── etl/
│   ├── extract.py             # Data extraction module
│   ├── transform.py           # Data transformation module
│   └── load.py               # Data loading module
├── main.py                    # Main ETL orchestration script
├── requirements.txt           # Python dependencies
├── README.md                 # Project documentation
└── test_*.py                 # Unit tests for each module
```

## 🚀 Setup Instructions

### Prerequisites

1. **Python Environment**
   - Python 3.8 or higher
   - pip package manager

2. **MySQL Database**
   - MySQL Server 5.7+ or MySQL 8.0+
   - MySQL client tools

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd e-commerce-etl
   ```

2. **Create and activate virtual environment**
   ```bash
   # Windows
   python -m venv venv
   venv\Scripts\activate

   # macOS/Linux
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Database Setup

1. **Install MySQL Server**
   - Download and install MySQL Server from [MySQL Downloads](https://dev.mysql.com/downloads/mysql/)
   - Follow the installation wizard

2. **Create Database**
   ```sql
   CREATE DATABASE ecommerce;
   ```

3. **Configure Database Connection**
   Edit `config/db_config.json` with your MySQL credentials:
   ```json
   {
     "user": "your_username",
     "password": "your_password",
     "host": "localhost",
     "database": "ecommerce"
   }
   ```

## ⚙️ Configuration

### Database Credentials

The pipeline uses a JSON configuration file for database connection settings:

**File**: `config/db_config.json`
```json
{
  "user": "your_username",
  "password": "your_password", 
  "host": "localhost",
  "database": "ecommerce"
}
```

**Security Note**: 
- Never commit database credentials to version control
- Consider using environment variables for production deployments
- Add `config/db_config.json` to your `.gitignore` file

## 🏃‍♂️ Running the ETL Pipeline

### Quick Start

Run the complete ETL pipeline:
```bash
python main.py
```

### Step-by-Step Execution

You can also run individual ETL phases for debugging:

1. **Extract Phase**
   ```python
   from etl.extract import extract_customers, extract_orders
   
   customers = extract_customers("data/customers.csv")
   orders = extract_orders("data/orders.csv")
   ```

2. **Transform Phase**
   ```python
   from etl.transform import clean_customers, clean_orders
   
   customers_clean = clean_customers(customers)
   orders_clean = clean_orders(orders)
   ```

3. **Load Phase**
   ```python
   from etl.load import load_to_mysql
   
   load_to_mysql(customers_clean, "customers")
   load_to_mysql(orders_clean, "orders")
   ```

## 🗄️ Database Schema

### Customers Table
| Column | Type | Description |
|--------|------|-------------|
| `id` | INT | Primary key, auto-increment |
| `name` | VARCHAR(255) | Customer full name |
| `email` | VARCHAR(255) | Customer email address (lowercase) |
| `join_date` | DATETIME | Customer registration date |

### Orders Table
| Column | Type | Description |
|--------|------|-------------|
| `id` | INT | Primary key, auto-increment |
| `customer_id` | INT | Foreign key to customers.id |
| `order_date` | DATETIME | Date of order placement |
| `total_amount` | DECIMAL(10,2) | Order total amount |

### Relationships
- `orders.customer_id` → `customers.id` (Foreign Key)

## 🔧 Data Processing

### Extract Phase
- Reads CSV files from the `data/` directory
- Handles file reading errors gracefully
- Provides detailed logging of extraction results

### Transform Phase
- **Customer Data Cleaning**:
  - Removes rows with missing email addresses
  - Standardizes email addresses (lowercase, trimmed)
  - Converts join dates to proper datetime format
  - Removes invalid date entries

- **Order Data Cleaning**:
  - Converts order dates to datetime format
  - Removes orders with missing dates or amounts
  - Ensures total_amount is numeric
  - Validates customer_id references

### Load Phase
- Creates tables if they don't exist
- Replaces existing data (if_exists='replace')
- Provides detailed loading status feedback

## 🧪 Testing

Run the test suite to verify pipeline functionality:
```bash
python test_extract.py
python test_transform.py
python test_load.py
```

## 📊 Expected Data Format

### customers.csv
```csv
id,name,email,join_date
1,John Doe,john@example.com,2023-01-15
2,Jane Smith,jane@example.com,2023-02-20
```

### orders.csv
```csv
id,customer_id,order_date,total_amount
1,1,2023-03-10,150.00
2,2,2023-03-15,75.50
```

## 🔮 Future Improvements

### Planned Enhancements
- [ ] **Incremental Loading**: Support for delta updates instead of full table replacement
- [ ] **Data Validation**: Schema validation and data quality checks
- [ ] **Logging**: Comprehensive logging with configurable levels
- [ ] **Error Recovery**: Resume capability for failed pipeline runs
- [ ] **Monitoring**: Dashboard for pipeline health and performance metrics
- [ ] **Scheduling**: Integration with cron or Airflow for automated runs
- [ ] **Data Lineage**: Track data transformations and dependencies
- [ ] **Backup Strategy**: Automated database backups before ETL runs

### Technical Improvements
- [ ] **Async Processing**: Implement async/await for better performance
- [ ] **Batch Processing**: Process large datasets in chunks
- [ ] **Connection Pooling**: Optimize database connections
- [ ] **Configuration Management**: Environment-specific configurations
- [ ] **Docker Support**: Containerized deployment
- [ ] **CI/CD Integration**: Automated testing and deployment

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add comprehensive docstrings to functions
- Include unit tests for new features
- Update documentation for any API changes

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Contact

- **Project Maintainer**: Ian Dave Cruz
- **Email**: cdave,dev@gmail.com
- **GitHub**: @N-Dave-08

## 🙏 Acknowledgments

- Built with [Pandas](https://pandas.pydata.org/) for data manipulation
- Powered by [SQLAlchemy](https://www.sqlalchemy.org/) for database operations
- MySQL connectivity provided by [PyMySQL](https://pymysql.readthedocs.io/)

---

**Note**: This ETL pipeline is designed for educational and development purposes. For production use, consider implementing additional security measures, error handling, and monitoring capabilities.
