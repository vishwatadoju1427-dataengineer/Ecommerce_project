class PIIMasking:
    """Mask Personally Identifiable Information"""
    
    def mask_customer_pii(self, df, masking_level="full"):
        """Mask customer PII based on level"""
        
        if masking_level == "full":
            masked_df = (df
                         .withColumn("email", 
                                    concat(substring(col("email"), 1, 3), 
                                           lit("***@***.com")))
                         .withColumn("phone",
                                    concat(lit("***-***-"),
                                           substring(col("phone"), -4, 4)))
                         .withColumn("address", lit("CONFIDENTIAL"))
                         .withColumn("name", 
                                    concat(substring(col("name"), 1, 1), lit("***"))))
        elif masking_level == "partial":
            masked_df = (df
                         .withColumn("email", 
                                    concat(substring(col("email"), 1, 5), 
                                           lit("***@***")))
                         .withColumn("phone",
                                    concat(lit("***-***-"),
                                           substring(col("phone"), -4, 4))))
        
        return masked_df
    
    def anonymize_for_analytics(self, df):
        """Anonymize data for analytics use"""
        
        anonymized_df = (df
                         .withColumn("customer_hash", sha2(col("customer_id"), 256))
                         .drop("customer_id", "email", "phone", "address")
                         .withColumn("age_group",
                                    when(col("age") < 25, "18-24")
                                    .when(col("age") < 35, "25-34")
                                    .when(col("age") < 45, "35-44")
                                    .when(col("age") < 55, "45-54")
                                    .otherwise("55+")))
        
        return anonymized_df

# Explanation: Protects sensitive customer information
