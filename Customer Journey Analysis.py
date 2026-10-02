class CustomerJourneyAnalyzer:
    """Analyze customer journey paths"""
    
    def analyze_journey_paths(self, clickstream_df, orders_df):
        """Analyze customer journey paths to purchase"""
        
        # Join clickstream with orders
        journey_df = (clickstream_df
                      .join(orders_df, 
                           ["customer_id", "session_id"], 
                           "left")
                      .withColumn("converted",
                                 when(col("order_id").isNotNull(), 1).otherwise(0)))
        
        # Analyze paths for converted vs non-converted sessions
        conversion_paths = (journey_df
                            .groupBy("session_id", "converted")
                            .agg(collect_list("page_url").alias("journey_path"),
                                 min("event_timestamp").alias("session_start"),
                                 max("event_timestamp").alias("session_end"))
                            .withColumn("session_duration",
                                       unix_timestamp(col("session_end")) - 
                                       unix_timestamp(col("session_start")))
                            .withColumn("path_length", size(col("journey_path"))))
        
        return conversion_paths
    
    def identify_common_paths(self, conversion_paths, min_frequency=10):
        """Identify common successful paths"""
        
        successful_paths = (conversion_paths
                            .filter(col("converted") == 1)
                            .groupBy("journey_path")
                            .agg(count("*").alias("frequency"),
                                 avg("session_duration").alias("avg_duration"))
                            .filter(col("frequency") >= min_frequency)
                            .orderBy(desc("frequency")))
        
        return successful_paths

# Explanation: Analyzes customer journey paths and conversion funnels
