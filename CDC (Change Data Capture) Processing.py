class CDCProcessor:
    """Process Change Data Capture from databases"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def process_cdc_updates(self, cdc_df, primary_key, table_name):
        """Process CDC data for updates and deletes"""
        
        # Read current table
        current_table = self.spark.read.format("delta").load(table_name)
        
        # Separate insert/update and delete records
        inserts_updates = cdc_df.filter(col("__op") != "d")
        deletes = cdc_df.filter(col("__op") == "d")
        
        # Apply merge for upserts
        current_table.alias("target").merge(
            inserts_updates.alias("source"),
            f"target.{primary_key} = source.{primary_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        # Handle deletes
        if deletes.count() > 0:
            delete_ids = [row[primary_key] for row in deletes.select(primary_key).collect()]
            current_table.filter(col(primary_key).isin(delete_ids)).delete()

# Explanation: Handles Change Data Capture for database synchronization
