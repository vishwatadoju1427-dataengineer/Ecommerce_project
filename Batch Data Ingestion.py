class BatchIngestion:
    """Handle batch data ingestion from various sources"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.utils = SparkUtils()
    
    def ingest_customers_batch(self, source_path):
        """Ingest customer data in batch"""
        schema = EcommerceSchemas.customer_schema()
        
        # Read data
        df = self.utils.read_with_schema(
            self.spark, source_path, "json", schema
        )
        
        # Add metadata
        df = (df
              .withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("source_file", input_file_name()))
        
        # Write to bronze
        bronze_path = self.config.get_path("bronze", "customers")
        self.utils.write_delta_table(
            df, bronze_path, partition_by="date(registration_date)"
        )
        
        return df.count()
    
    def ingest_orders_batch(self, source_path):
        """Ingest order data in batch"""
        schema = EcommerceSchemas.order_schema()
        
        df = self.utils.read_with_schema(
            self.spark, source_path, "json", schema
        )
        
        # Explode items array for processing
        df = df.withColumn("item", explode(col("items")))
        
        # Add metadata
        df = (df
              .withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("source_file", input_file_name()))
        
        # Write to bronze
        bronze_path = self.config.get_path("bronze", "orders")
        self.utils.write_delta_table(
            df, bronze_path, partition_by="date(order_date)"
        )
        
        return df.count()

# Explanation: Batch ingestion from various file formats with schema validation
