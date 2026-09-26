class DashboardMetricsCalculator:
    """Calculate metrics for performance dashboards"""
    
    def calculate_kpis(self, orders_df, customers_df, products_df):
        """Calculate key performance indicators"""
        
        # Revenue metrics
        revenue_metrics = (orders_df
                           .agg(
                               sum(when(col("status") == "COMPLETED", 
                                       col("total_amount"))
                                   .otherwise(0)).alias("total_revenue"),
                               avg(when(col("status") == "COMPLETED", 
                                       col("total_amount"))
                                   .otherwise(0)).alias("avg_order_value"),
                               countDistinct(when(col("status") == "COMPLETED", 
                                                 col("customer_id")))
                               .alias("paying_customers")
                           ))
        
        # Customer metrics
        customer_metrics = (customers_df
                            .agg(
                                count("*").alias("total_customers"),
                                count(when(col("days_since_last_order") <= 30, 1))
                                .alias("active_customers"),
                                avg(col("total_spent")).alias("avg_customer_value")
                            ))
        
        # Product metrics
        product_metrics = (products_df
                           .agg(
                               count("*").alias("total_products"),
                               sum(when(col("current_stock") == 0, 1).otherwise(0))
                               .alias("out_of_stock_products"),
                               avg(col("sell_through_rate")).alias("avg_sell_through_rate")
                           ))
        
        # Combine all metrics
        kpis = (revenue_metrics
                .crossJoin(customer_metrics)
                .crossJoin(product_metrics)
                .withColumn("conversion_rate",
                           col("paying_customers") / col("total_customers"))
                .withColumn("avg_basket_size",
                           col("total_revenue") / col("paying_customers")))
        
        return kpis

# Explanation: Calculates KPIs for executive dashboards
