class QueryOptimizer:
    """Optimize queries using caching and optimization techniques"""
    
    def __init__(self, spark):
        self.spark = spark
        self.cached_tables = {}
    
    def cache_frequently_used_table(self, table_name, storage_level="MEMORY_AND_DISK"):
        """Cache frequently used table"""
        
        from pyspark import StorageLevel
        
        if table_name not in self.cached_tables:
            df = self.spark.table(table_name)
            
            if storage_level == "MEMORY_ONLY":
                df.persist(StorageLevel.MEMORY_ONLY)
            elif storage_level == "MEMORY_AND_DISK":
                df.persist(StorageLevel.MEMORY_AND_DISK)
            elif storage_level == "DISK_ONLY":
                df.persist(StorageLevel.DISK_ONLY)
            
            self.cached_tables[table_name] = df.count()  # Force caching
        
        return self.cached_tables[table_name]
    
    def optimize_join_strategies(self, df1, df2, join_key):
        """Optimize join strategies based on data size"""
        
        # Get approximate sizes
        df1_size = df1.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).sum()
        df2_size = df2.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).sum()
        
        # Choose join strategy
        if df1_size < 1000000 and df2_size < 1000000:
            # Broadcast join for small tables
            return df1.join(broadcast(df2), join_key)
        elif df1_size / df2_size > 10:
            # Bucket join for skewed data
            df1_bucketed = df1.repartition(200, join_key)
            df2_bucketed = df2.repartition(200, join_key)
            return df1_bucketed.join(df2_bucketed, join_key)
        else:
            # Sort merge join for large tables
            return df1.join(df2, join_key)

# Explanation: Optimizes query performance using caching and join strategies
