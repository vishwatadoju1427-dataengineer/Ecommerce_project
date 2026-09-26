class LineageTracker:
    """Track data lineage and provenance"""
    
    def __init__(self, spark):
        self.spark = spark
        
        # Create lineage table if not exists
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS data_lineage (
                source_table STRING,
                target_table STRING,
                transformation_name STRING,
                execution_timestamp TIMESTAMP,
                rows_processed BIGINT,
                source_hash STRING,
                target_hash STRING,
                user_id STRING,
                job_id STRING
            ) USING DELTA
        """)
    
    def track_transformation(self, source_df, target_df, 
                            transformation_name, user_id, job_id):
        """Track transformation lineage"""
        
        source_hash = self.calculate_dataframe_hash(source_df)
        target_hash = self.calculate_dataframe_hash(target_df)
        
        lineage_data = [(source_df.spark.tableName(), 
                        target_df.spark.tableName(),
                        transformation_name,
                        datetime.now(),
                        target_df.count(),
                        source_hash,
                        target_hash,
                        user_id,
                        job_id)]
        
        lineage_schema = StructType([
            StructField("source_table", StringType()),
            StructField("target_table", StringType()),
            StructField("transformation_name", StringType()),
            StructField("execution_timestamp", TimestampType()),
            StructField("rows_processed", LongType()),
            StructField("source_hash", StringType()),
            StructField("target_hash", StringType()),
            StructField("user_id", StringType()),
            StructField("job_id", StringType())
        ])
        
        lineage_df = self.spark.createDataFrame(lineage_data, lineage_schema)
        lineage_df.write.format("delta").mode("append").saveAsTable("data_lineage")
    
    def calculate_dataframe_hash(self, df):
        """Calculate hash of DataFrame content"""
        return df.select(sha2(concat_ws("|", *df.columns), 256)).first()[0]

# Explanation: Tracks data lineage for governance and debugging
