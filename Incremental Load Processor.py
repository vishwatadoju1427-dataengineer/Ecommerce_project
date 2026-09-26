class IncrementalLoader:
    """Process incremental data loads"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    def load_incremental_orders(self, last_processed_date):
        """Load orders incrementally based on date"""
        
        # Query to get new orders
        query = f"""
            SELECT * FROM external_orders 
            WHERE order_date > '{last_processed_date}'
            AND order_date <= CURRENT_DATE()
        """
        
        new_orders = self.spark.read.jdbc(
            url=self.config.jdbc_url,
            table=f"({query}) as new_orders",
            properties=self.config.jdbc_properties
        )
        
        # Merge with existing data
        existing_table = DeltaTable.forPath(self.spark, 
                                           self.config.get_path("bronze", "orders"))
        
        existing_table.alias("target").merge(
            new_orders.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        # Return count of new records
        return new_orders.count()

# Explanation: Handles incremental data loading efficiently
