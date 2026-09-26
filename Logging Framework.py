import logging
from datetime import datetime

class SparkLogger:
    """Custom logging framework for Spark applications"""
    
    def __init__(self, app_name):
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(logging.INFO)
        
        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        ch.setFormatter(formatter)
        
        self.logger.addHandler(ch)
    
    def log_transformation(self, transformation_name, row_count, duration):
        """Log transformation metrics"""
        self.logger.info(
            f"Transformation: {transformation_name} | "
            f"Rows: {row_count:,} | "
            f"Duration: {duration:.2f}s"
        )
    
    def log_data_quality(self, table_name, metrics):
        """Log data quality metrics"""
        self.logger.info(f"Data Quality - {table_name}: {metrics}")

# Explanation: Structured logging for monitoring and debugging
