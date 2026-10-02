class CustomerSegmenter:
    """Segment customers based on behavior"""
    
    def segment_by_rfm(self, customer_aggregates):
        """Segment customers using RFM analysis"""
        
        # Calculate recency, frequency, monetary scores
        rfm_df = (customer_aggregates
                  .withColumn("recency_score",
                             when(col("days_since_last_order") <= 30, 5)
                             .when(col("days_since_last_order") <= 60, 4)
                             .when(col("days_since_last_order") <= 90, 3)
                             .when(col("days_since_last_order") <= 180, 2)
                             .otherwise(1))
                  .withColumn("frequency_score",
                             when(col("total_orders") >= 50, 5)
                             .when(col("total_orders") >= 20, 4)
                             .when(col("total_orders") >= 10, 3)
                             .when(col("total_orders") >= 5, 2)
                             .otherwise(1))
                  .withColumn("monetary_score",
                             when(col("total_spent") >= 10000, 5)
                             .when(col("total_spent") >= 5000, 4)
                             .when(col("total_spent") >= 1000, 3)
                             .when(col("total_spent") >= 500, 2)
                             .otherwise(1)))
        
        # Calculate RFM score and segment
        segmented_df = (rfm_df
                        .withColumn("rfm_score",
                                   col("recency_score") + 
                                   col("frequency_score") + 
                                   col("monetary_score"))
                        .withColumn("segment",
                                   when(col("rfm_score") >= 13, "Champions")
                                   .when(col("rfm_score") >= 11, "Loyal Customers")
                                   .when(col("rfm_score") >= 9, "Potential Loyalists")
                                   .when(col("rfm_score") >= 7, "Recent Customers")
                                   .when(col("rfm_score") >= 5, "Promising")
                                   .when(col("rfm_score") >= 3, "Need Attention")
                                   .otherwise("At Risk")))
        
        return segmented_df

# Explanation: Segments customers using RFM analysis
