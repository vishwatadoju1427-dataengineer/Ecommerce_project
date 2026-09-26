class MarketBasketAnalyzer:
    """Perform market basket analysis"""
    
    def find_frequent_itemsets(self, order_items_df, min_support=0.01):
        """Find frequent itemsets using Apriori-like algorithm"""
        
        # Get all item pairs in each order
        item_pairs = (order_items_df
                      .groupBy("order_id")
                      .agg(collect_list("product_id").alias("items"))
                      .select("order_id", 
                             explode(arrays_zip(
                                 posexplode(col("items")).alias("pos1", "item1"),
                                 posexplode(col("items")).alias("pos2", "item2")
                             )).alias("pair"))
                      .filter(col("pair.pos1") < col("pair.pos2"))
                      .select(col("pair.item1").alias("item1"),
                             col("pair.item2").alias("item2")))
        
        # Calculate support
        total_orders = order_items_df.select("order_id").distinct().count()
        frequent_pairs = (item_pairs
                          .groupBy("item1", "item2")
                          .agg(count("*").alias("pair_count"))
                          .withColumn("support", col("pair_count") / total_orders)
                          .filter(col("support") >= min_support))
        
        return frequent_pairs
    
    def calculate_association_rules(self, frequent_itemsets, min_confidence=0.5):
        """Calculate association rules"""
        
        # Calculate confidence for rules
        item_counts = frequent_itemsets.groupBy("item1").agg(
            sum("pair_count").alias("item1_count"))
        
        association_rules = (frequent_itemsets
                             .join(item_counts, "item1")
                             .withColumn("confidence",
                                        col("pair_count") / col("item1_count"))
                             .filter(col("confidence") >= min_confidence)
                             .withColumn("lift",
                                        col("confidence") / 
                                        (col("item2_count") / total_orders)))
        
        return association_rules

# Explanation: Performs market basket analysis for product recommendations
