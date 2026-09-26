class DataIntegrator:
    """Integrate data from multiple sources"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def integrate_customer_sources(self, source1_df, source2_df, matching_key):
        """Integrate customer data from multiple sources"""
        
        # Standardize schemas
        source1_std = self.standardize_customer_schema(source1_df)
        source2_std = self.standardize_customer_schema(source2_df)
        
        # Union and deduplicate
        integrated_df = (source1_std
                         .unionByName(source2_std, allowMissingColumns=True)
                         .groupBy(matching_key)
                         .agg(*self.get_merge_expressions()))
        
        return integrated_df
    
    def standardize_customer_schema(self, df):
        """Standardize customer schema across sources"""
        # Map columns to standard schema
        column_mapping = {
            "cust_id": "customer_id",
            "customer_name": "name",
            "email_address": "email",
            "phone_number": "phone"
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
        
        return df

# Explanation: Integrates data from multiple sources with schema standardization
