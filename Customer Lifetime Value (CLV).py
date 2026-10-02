class CLVCalculator:
    """Calculate Customer Lifetime Value"""
    
    def calculate_clv(self, customer_aggregates, avg_customer_lifespan_months=36):
        """Calculate Customer Lifetime Value"""
        
        clv_df = (customer_aggregates
                  .withColumn("avg_monthly_spend",
                             col("total_spent") / 
                             (datediff(current_date(), col("first_order_date")) / 30.4))
                  .withColumn("expected_customer_lifespan",
                             when(col("days_since_last_order") > 180, 0)
                             .otherwise(avg_customer_lifespan_months - 
                                       (datediff(current_date(), col("first_order_date")) / 30.4)))
                  .withColumn("gross_clv",
                             col("avg_monthly_spend") * col("expected_customer_lifespan"))
                  .withColumn("net_clv",
                             col("gross_clv") * (1 - 0.4))  # Assuming 40% costs
                  .withColumn("clv_tier",
                             when(col("net_clv") > 10000, "VIP")
                             .when(col("net_clv") > 5000, "High Value")
                             .when(col("net_clv") > 1000, "Medium Value")
                             .otherwise("Low Value")))
        
        return clv_df

# Explanation: Calculates Customer Lifetime Value metrics
