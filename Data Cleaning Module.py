class DataCleaner:
    """Clean and standardize data in Bronze layer"""
    
    def __init__(self):
        self.cleaning_rules = {}
    
    def clean_customer_data(self, df):
        """Apply cleaning rules to customer data"""
        
        cleaned_df = (df
                      # Trim whitespace
                      .withColumn("name", trim(col("name")))
                      .withColumn("email", lower(trim(col("email"))))
                      .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))
                      
                      # Standardize state codes
                      .withColumn("state", 
                                 when(length(col("state")) > 2,
                                     initcap(col("state")))
                                 .otherwise(upper(col("state"))))
                      
                      # Fix zipcode format
                      .withColumn("zipcode",
                                 when(length(col("zipcode")) == 4,
                                     concat(lit("0"), col("zipcode")))
                                 .otherwise(col("zipcode")))
                      
                      # Remove invalid emails
                      .filter(col("email").contains("@"))
                      
                      # Deduplicate
                      .dropDuplicates(["customer_id"]))
        
        return cleaned_df

# Explanation: Applies data cleaning rules to standardize data
