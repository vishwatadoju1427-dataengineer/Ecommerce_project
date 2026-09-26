class ProductAnalyzer:
    """Analyze product performance"""
    
    def calculate_product_metrics(self, order_items_df, products_df):
        """Calculate product-level metrics"""
        
        product_metrics = (order_items_df
                           .groupBy("product_id")
                           .agg(
                               sum("quantity").alias("total_quantity_sold"),
                               sum(col("quantity") * col("price")).alias("total_revenue"),
                               countDistinct("order_id").alias("order_count"),
                               countDistinct("customer_id").alias("unique_customers"),
                               avg("price").alias("avg_selling_price"),
                               sum(when(col("discount") > 0, "quantity")
                                   .otherwise(0)).alias("discounted_units")
                           )
                           .join(products_df, "product_id", "left")
                           .withColumn("sell_through_rate",
                                      col("total_quantity_sold") / col("initial_stock"))
                           .withColumn("avg_units_per_order",
                                      col("total_quantity_sold") / col("order_count"))
                           .withColumn("discount_rate",
                                      col("discounted_units") / col("total_quantity_sold"))
                           .withColumn("popularity_rank",
                                      rank().over(Window.orderBy(desc("total_quantity_sold")))))
        
        return product_metrics
    
    def identify_slow_movers(self, product_metrics, threshold_days=90):
        """Identify slow-moving products"""
        
        slow_movers = (product_metrics
                       .filter(col("last_sale_date") < 
                              date_sub(current_date(), threshold_days))
                       .withColumn("days_since_last_sale",
                                  datediff(current_date(), col("last_sale_date")))
                       .orderBy(desc("days_since_last_sale")))
        
        return slow_movers

# Explanation: Analyzes product sales and performance metrics
