class InventoryAnalyzer:
    """Analyze inventory metrics"""
    
    def calculate_inventory_turnover(self, inventory_df, sales_df):
        """Calculate inventory turnover ratio"""
        
        turnover_df = (sales_df
                       .groupBy("product_id", date_trunc("month", "sale_date").alias("month"))
                       .agg(sum("quantity").alias("monthly_sales"))
                       .join(inventory_df, ["product_id", "month"], "left")
                       .withColumn("inventory_turnover",
                                  col("monthly_sales") / col("average_inventory"))
                       .withColumn("days_in_inventory",
                                  30 / col("inventory_turnover")))
        
        return turnover_df
    
    def identify_stockout_risks(self, inventory_df, sales_forecast_df):
        """Identify potential stockout risks"""
        
        risk_df = (inventory_df
                   .join(sales_forecast_df, "product_id", "left")
                   .withColumn("days_of_supply",
                              col("current_stock") / col("daily_forecast"))
                   .withColumn("stockout_risk",
                              when(col("days_of_supply") < 7, "HIGH")
                              .when(col("days_of_supply") < 14, "MEDIUM")
                              .when(col("days_of_supply") < 30, "LOW")
                              .otherwise("SAFE")))
        
        return risk_df

# Explanation: Analyzes inventory metrics and identifies risks
