class DataAggregator:
    """Aggregate data for analytical purposes"""
    
    def create_customer_aggregates(self, orders_df, customers_df):
        """Create customer-level aggregates"""
        
        customer_aggregates = (orders_df
                               .groupBy("customer_id")
                               .agg(
                                   countDistinct("order_id").alias("total_orders"),
                                   sum("total_amount").alias("total_spent"),
                                   avg("total_amount").alias("avg_order_value"),
                                   min("order_date").alias("first_order_date"),
                                   max("order_date").alias("last_order_date"),
                                   countDistinct("product_id").alias("unique_products_purchased"),
                                   sum("quantity").alias("total_items_purchased"),
                                   (sum(col("total_amount") * 
                                       when(col("status") == "COMPLETED", 1).otherwise(0)) /
                                    sum("total_amount")).alias("completion_rate")
                               )
                               .join(customers_df, "customer_id", "left")
                               .withColumn("days_since_first_order",
                                          datediff(current_date(), col("first_order_date")))
                               .withColumn("days_since_last_order",
                                          datediff(current_date(), col("last_order_date")))
                               .withColumn("avg_days_between_orders",
                                          col("days_since_first_order") / col("total_orders")))
        
        return customer_aggregates

# Explanation: Creates aggregated metrics for analytical queries
