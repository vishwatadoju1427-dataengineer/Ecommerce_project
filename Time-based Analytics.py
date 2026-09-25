class TimeSeriesAnalyzer:
    """Perform time-based analytics"""

    def calculate_daily_metrics(self, orders_df):
        """Calculate daily order metrics"""

        daily_metrics = (
            orders_df
            .groupBy(
                date_trunc("day", "order_date").alias("order_day")
            )
            .agg(
                count("order_id").alias("order_count"),
                sum("total_amount").alias("daily_revenue"),
                avg("total_amount").alias("avg_order_value"),
                countDistinct("customer_id").alias("unique_customers"),
                sum("quantity").alias("total_items_sold"),
                sum(
                    when(col("status") == "CANCELLED", 1)
                    .otherwise(0)
                ).alias("cancelled_orders")
            )
            .withColumn(
                "revenue_per_customer",
                col("daily_revenue") / col("unique_customers")
            )
            .withColumn(
                "cancellation_rate",
                col("cancelled_orders") / col("order_count")
            )
            .orderBy("order_day")
        )

        return daily_metrics

    def calculate_moving_averages(
        self,
        daily_metrics,
        window_sizes=[7, 30]
    ):
        """Calculate moving averages"""

        for window_size in window_sizes:
            window_spec = (
                Window
                .orderBy("order_day")
                .rowsBetween(-window_size, 0)
            )

            daily_metrics = (
                daily_metrics
                .withColumn(
                    f"ma_{window_size}d_revenue",
                    avg("daily_revenue").over(window_spec)
                )
                .withColumn(
                    f"ma_{window_size}d_orders",
                    avg("order_count").over(window_spec)
                )
            )

        return daily_metrics


# Explanation:
# Performs time-series analysis and calculates moving averages.
