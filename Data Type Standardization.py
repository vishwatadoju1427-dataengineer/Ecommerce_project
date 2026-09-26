class DataTypeStandardizer:
    """Standardize data types across sources"""
    
    def standardize_customer_data_types(self, df):
        """Standardize customer data types"""
        
        standardized_df = (df
                           .withColumn("customer_id", col("customer_id").cast(StringType()))
                           .withColumn("registration_date", 
                                      to_timestamp(col("registration_date"), "yyyy-MM-dd HH:mm:ss"))
                           .withColumn("phone", 
                                      when(col("phone").isNotNull(), 
                                           regexp_replace(col("phone"), "[^0-9]", ""))
                                      .otherwise(None))
                           .withColumn("zipcode", col("zipcode").cast(StringType()))
                           .withColumn("created_at", current_timestamp())
                           .withColumn("updated_at", current_timestamp()))
        
        return standardized_df
    
    def standardize_order_data_types(self, df):
        """Standardize order data types"""
        
        standardized_df = (df
                           .withColumn("order_id", col("order_id").cast(StringType()))
                           .withColumn("order_date", 
                                      to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"))
                           .withColumn("total_amount", col("total_amount").cast(DecimalType(10, 2)))
                           .withColumn("status", 
                                      when(col("status").isin(["COMPLETED", "PENDING", "CANCELLED"]),
                                           col("status"))
                                      .otherwise("PENDING"))
                           .withColumn("items", 
                                      from_json(to_json(col("items")), 
                                               ArrayType(StructType([
                                                   StructField("product_id", StringType()),
                                                   StructField("quantity", IntegerType()),
                                                   StructField("price", DecimalType(10, 2)),
                                                   StructField("discount", DecimalType(5, 2))
                                               ])))))

# Explanation: Ensures consistent data types across the platform
