class AuditManager:
    """Manage audit trails for data changes"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def create_audit_trail(self, table_name, operation_type, user_id, changes):
        """Create audit trail entry"""
        
        audit_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("operation_type", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("changes", StringType(), True),
            StructField("before_values", StringType(), True),
            StructField("after_values", StringType(), True)
        ])
        
        audit_data = [(table_name, operation_type, user_id, 
                      datetime.now(), str(changes), "", "")]
        
        audit_df = self.spark.createDataFrame(audit_data, audit_schema)
        
        # Append to audit table
        audit_df.write.format("delta").mode("append").saveAsTable("audit_trail")
    
    def enable_delta_change_data_feed(self, table_name):
        """Enable Delta change data feed for table"""
        self.spark.sql(f"""
            ALTER TABLE {table_name} 
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)

# Explanation: Maintains audit trail for compliance and debugging
