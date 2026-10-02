class DataCompressor:
    """Handle data compression and optimization"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def compress_table(self, table_name, compression_algorithm="zstd"):
        """Compress Delta table"""
        
        # Optimize table first
        self.spark.sql(f"OPTIMIZE {table_name}")
        
        # Set compression settings
        self.spark.sql(f"""
            ALTER TABLE {table_name} 
            SET TBLPROPERTIES (
                'delta.dataSkippingNumIndexedCols' = '32',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)
        
        # Vacuum old files
        self.spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
    
    def repartition_for_optimization(self, df, partition_size_mb=128):
        """Repartition data for optimal file sizes"""
        
        # Calculate optimal partitions based on data size
        df_size_mb = df.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).sum() / 1024
        
        optimal_partitions = max(1, int(df_size_mb / partition_size_mb))
        
        return df.repartition(optimal_partitions)

# Explanation: Optimizes storage through compression and partitioning
