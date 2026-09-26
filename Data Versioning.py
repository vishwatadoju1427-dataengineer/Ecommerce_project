class DataVersioning:
    """Implement data versioning using Delta Lake"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def create_versioned_table(self, table_name, primary_key):
        """Create versioned Delta table"""
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {primary_key} STRING,
                data STRUCT<...>,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                is_current BOOLEAN
            ) USING DELTA
            PARTITIONED BY (is_current)
        """)
    
    def insert_version(self, table_name, df, version_timestamp):
        """Insert new version of data"""
        
        # Set old versions as not current
        self.spark.sql(f"""
            UPDATE {table_name}
            SET is_current = false,
                valid_to = '{version_timestamp}'
            WHERE is_current = true
        """)
        
        # Insert new version
        new_version_df = (df
                          .withColumn("valid_from", lit(version_timestamp))
                          .withColumn("valid_to", lit(None))
                          .withColumn("is_current", lit(True)))
        
        new_version_df.write.format("delta").mode("append").saveAsTable(table_name)
    
    def query_as_of(self, table_name, timestamp):
        """Query data as of specific timestamp"""
        
        return self.spark.sql(f"""
            SELECT * FROM {table_name}
            WHERE '{timestamp}' BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')
        """)

# Explanation: Implements temporal data versioning
