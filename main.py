import logging
import sys
import traceback
from etl.load import load_to_mysql
from etl.extract import extract_customers, extract_orders
from etl.transform import clean_customers, clean_orders

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_etl():
    """
    Run the complete ETL pipeline with comprehensive error handling.
    """
    try:
        logger.info("Starting ETL pipeline")
        
        # Extract phase
        logger.info("=== EXTRACT PHASE ===")
        customers = extract_customers("data/customers.csv")
        if customers.empty:
            logger.error("Failed to extract customers data")
            return False
            
        orders = extract_orders("data/orders.csv")
        if orders.empty:
            logger.error("Failed to extract orders data")
            return False
        
        # Transform phase
        logger.info("=== TRANSFORM PHASE ===")
        try:
            customers_clean = clean_customers(customers)
            logger.info(f"Customers cleaned: {len(customers_clean)} rows")
        except Exception as e:
            logger.error(f"Error cleaning customers: {e}")
            logger.error(traceback.format_exc())
            return False
            
        if customers_clean.empty:
            logger.error("No valid customers data after transformation")
            return False
            
        try:
            orders_clean = clean_orders(orders, customers_clean)
            logger.info(f"Orders cleaned: {len(orders_clean)} rows")
        except Exception as e:
            logger.error(f"Error cleaning orders: {e}")
            logger.error(traceback.format_exc())
            return False
            
        if orders_clean.empty:
            logger.warning("No valid orders data after transformation")
            # Continue with just customers
        
        # Load phase
        logger.info("=== LOAD PHASE ===")
        try:
            customers_loaded = load_to_mysql(customers_clean, "customers")
            if not customers_loaded:
                logger.error("Failed to load customers to database")
                return False
        except Exception as e:
            logger.error(f"Error loading customers: {e}")
            logger.error(traceback.format_exc())
            return False
            
        if not orders_clean.empty:
            try:
                orders_loaded = load_to_mysql(orders_clean, "orders")
                if not orders_loaded:
                    logger.error("Failed to load orders to database")
                    return False
            except Exception as e:
                logger.error(f"Error loading orders: {e}")
                logger.error(traceback.format_exc())
                return False
        
        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error in ETL pipeline: {e}")
        logger.error(traceback.format_exc())
        return False

def main():
    """
    Main entry point with proper exit codes.
    """
    success = run_etl()
    if success:
        logger.info("ETL pipeline completed successfully")
        sys.exit(0)
    else:
        logger.error("ETL pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()