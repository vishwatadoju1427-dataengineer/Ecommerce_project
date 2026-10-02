class DataArchiver:
    """Implement data archival strategy"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def archive_old_data(self, table_name, archive_cutoff_date, archive_path):
        """Archive data older than cutoff date"""
        
        # Read old data
        old_data = self.spark.sql(f"""
            SELECT * FROM {table_name}
            WHERE order_date < '{archive_cutoff_date}'
        """)
        
        # Write to archive location
        (old_data.write
         .format("parquet")
         .mode("overwrite")
         .partitionBy("year", "month")
         .save(archive_path))
        
        # Delete archived data from main table
        self.spark.sql(f"""
            DELETE FROM {table_name}
            WHERE order_date < '{archive_cutoff_date}'
        """)
        
        # Vacuum to clean up files
        self.spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
    
    def restore_from_archive(self, archive_path, table_name, date_range):
        """Restore data from archive"""
        
        archived_data = self.spark.read.parquet(archive_path)
        
        # Filter by date range
        restored_data = archived_data.filter(
            (col("order_date") >= date_range["start"]) &
            (col("order_date") <= date_range["end"])
        )
        
        # Insert back to main table
        restored_data.write.format("delta").mode("append").saveAsTable(table_name)

# Explanation: Implements data archival for historical data
