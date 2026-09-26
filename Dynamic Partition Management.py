class PartitionManager:
    """Manage table partitions dynamically"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def add_new_partition(self, table_name, partition_spec, data_df):
        """Add new partition to table"""
        
        partition_path = self.get_partition_path(table_name, partition_spec)
        
        # Write data to partition
        (data_df.write
         .format("delta")
         .mode("append")
         .partitionBy(list(partition_spec.keys()))
         .save(partition_path))
        
        # Refresh table metadata
        self.spark.sql(f"REFRESH TABLE {table_name}")
    
    def drop_old_partitions(self, table_name, retention_days=90):
        """Drop partitions older than retention period"""
        
        old_date = date_sub(current_date(), retention_days)
        
        # Get old partitions
        partitions = self.spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        
        for partition in partitions:
            partition_value = partition[0].split("=")[1]
            partition_date = to_date(lit(partition_value))
            
            if partition_date < old_date:
                self.spark.sql(f"""
                    ALTER TABLE {table_name} 
                    DROP IF EXISTS PARTITION ({partition[0]})
                """)
    
    def merge_small_files(self, table_name, min_file_size_mb=128):
        """Merge small files in partitions"""
        
        self.spark.sql(f"""
            OPTIMIZE {table_name} 
            WHERE file_count > 10 
            AND avg_file_size < {min_file_size_mb * 1024 * 1024}
        """)

# Explanation: Manages table partitions for optimal performance
