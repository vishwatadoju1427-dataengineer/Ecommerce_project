class DataEnricher:
    """Enrich data during ingestion"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def enrich_customer_data(self, df):
        """Enrich customer data with additional information"""
        
        # Add geolocation based on zipcode
        zipcode_mapping = self.spark.table("zipcode_geolocation")
        
        enriched_df = (df
                       .join(zipcode_mapping, 
                            df.zipcode == zipcode_mapping.zipcode,
                            "left")
                       .withColumn("full_address", 
                                  concat_ws(", ", 
                                           col("address"),
                                           col("city"),
                                           col("state"),
                                           col("zipcode")))
                       .withColumn("customer_segment",
                                  when(col("registration_date") < "2023-01-01", "Early Adopter")
                                  .when(col("registration_date") < "2024-01-01", "Regular")
                                  .otherwise("New"))
                       .withColumn("name_initials",
                                  concat_ws(".",
                                           upper(substring(col("name"), 1, 1)),
                                           upper(substring(split(col("name"), " ")[1], 1, 1)))
                                  ))
        
        return enriched_df
    
    def enrich_order_data(self, df):
        """Enrich order data with customer and product info"""
        
        customers = self.spark.table("silver_customers")
        products = self.spark.table("silver_products")
        
        enriched_df = (df
                       .join(customers, "customer_id", "left")
                       .join(products, df.item.product_id == products.product_id, "left")
                       .withColumn("item_total",
                                  col("item.quantity") * col("item.price") * 
                                  (1 - col("item.discount")/100))
                       .withColumn("order_hour", hour(col("order_date")))
                       .withColumn("order_day_of_week", dayofweek(col("order_date"))))
        
        return enriched_df

# Explanation: Adds derived fields and joins reference data during ingestion
