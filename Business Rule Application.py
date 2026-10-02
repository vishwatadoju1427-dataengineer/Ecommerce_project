class BusinessRuleEngine:
    """Apply business rules to data"""
    
    def apply_customer_segmentation(self, df):
        """Apply customer segmentation rules"""
        
        segmented_df = (df
                        .withColumn("customer_tier",
                                   when(col("total_orders") > 100, "PLATINUM")
                                   .when(col("total_orders") > 50, "GOLD")
                                   .when(col("total_orders") > 10, "SILVER")
                                   .otherwise("BRONZE"))
                        .withColumn("loyalty_status",
                                   when(col("first_order_date") < "2022-01-01", "LOYAL")
                                   .when(col("days_since_first_order") > 365, "ESTABLISHED")
                                   .otherwise("NEW"))
                        .withColumn("preferred_category",
                                   when(col("electronics_purchases") > col("clothing_purchases"), "ELECTRONICS")
                                   .when(col("clothing_purchases") > col("home_purchases"), "CLOTHING")
                                   .otherwise("HOME")))
        
        return segmented_df
    
    def apply_pricing_rules(self, df):
        """Apply pricing rules to orders"""
        
        priced_df = (df
                     .withColumn("final_price",
                                when(col("customer_tier") == "PLATINUM",
                                     col("base_price") * 0.9)
                                .when(col("customer_tier") == "GOLD",
                                     col("base_price") * 0.95)
                                .otherwise(col("base_price")))
                     .withColumn("shipping_cost",
                                when(col("total_amount") > 100, 0)
                                .when(col("customer_tier").isin(["PLATINUM", "GOLD"]), 0)
                                .otherwise(5.99))
                     .withColumn("tax_amount",
                                col("final_price") * 0.08))

# Explanation: Applies business logic and rules to data
