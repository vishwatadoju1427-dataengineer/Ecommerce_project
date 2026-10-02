class EcommerceConfig:
    """Configuration manager for e-commerce platform"""
    
    def __init__(self):
        self.config = {
            "paths": {
                "raw_data": "/mnt/ecommerce/raw",
                "bronze": "/mnt/ecommerce/bronze",
                "silver": "/mnt/ecommerce/silver",
                "gold": "/mnt/ecommerce/gold",
                "checkpoints": "/mnt/ecommerce/checkpoints"
            },
            "file_formats": {
                "customers": "json",
                "orders": "json",
                "products": "csv",
                "inventory": "parquet"
            },
            "processing": {
                "batch_size": "10000",
                "watermark_delay": "10 minutes",
                "max_files_per_trigger": "100"
            },
            "quality": {
                "null_threshold": 0.1,
                "duplicate_threshold": 0.05
            }
        }
    
    def get_path(self, layer, data_type):
        return f"{self.config['paths'][layer]}/{data_type}"
    
    def get_checkpoint_path(self, stream_name):
        return f"{self.config['paths']['checkpoints']}/{stream_name}"

# Explanation: Centralized configuration management for maintainability
