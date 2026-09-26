class PipelineMonitor:
    """Monitor data pipeline health and performance"""
    
    def __init__(self, spark):
        self.spark = spark
        
        # Create monitoring tables
        self.create_monitoring_tables()
    
    def create_monitoring_tables(self):
        """Create monitoring tables"""
        
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                pipeline_name STRING,
                execution_id STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds DOUBLE,
                records_processed BIGINT,
                status STRING,
                error_message STRING
            ) USING DELTA
        """)
        
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS data_freshness (
                table_name STRING,
                last_update_time TIMESTAMP,
                expected_freshness_hours INT,
                is_fresh BOOLEAN,
                checked_at TIMESTAMP
            ) USING DELTA
        """)
    
    def record_pipeline_execution(self, pipeline_name, execution_id, 
                                 start_time, end_time, records_processed, 
                                 status, error_message=None):
        """Record pipeline execution metrics"""
        
        duration = (end_time - start_time).total_seconds()
        
        metrics_data = [(pipeline_name, execution_id, start_time, end_time,
                        duration, records_processed, status, error_message)]
        
        metrics_df = self.spark.createDataFrame(metrics_data, [
            "pipeline_name", "execution_id", "start_time", "end_time",
            "duration_seconds", "records_processed", "status", "error_message"
        ])
        
        metrics_df.write.format("delta").mode("append").saveAsTable("pipeline_metrics")
    
    def check_data_freshness(self, table_name, expected_freshness_hours=24):
        """Check data freshness for table"""
        
        last_update = self.spark.sql(f"""
            SELECT MAX(ingestion_timestamp) as last_update
            FROM {table_name}
        """).collect()[0]["last_update"]
        
        hours_since_update = (datetime.now() - last_update).total_seconds() / 3600
        is_fresh = hours_since_update <= expected_freshness_hours
        
        freshness_data = [(table_name, last_update, expected_freshness_hours,
                          is_fresh, datetime.now())]
        
        freshness_df = self.spark.createDataFrame(freshness_data, [
            "table_name", "last_update_time", "expected_freshness_hours",
            "is_fresh", "checked_at"
        ])
        
        freshness_df.write.format("delta").mode("append").saveAsTable("data_freshness")
        
        return is_fresh, hours_since_update

# Explanation: Monitors pipeline health and data freshness
