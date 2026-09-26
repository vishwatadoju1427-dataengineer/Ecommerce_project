class SalesForecaster:
    """Generate sales forecasts"""

    def calculate_seasonal_patterns(self, sales_df):
        """Calculate seasonal sales patterns"""

        seasonal_patterns = (
            sales_df
            .groupBy(
                dayofweek("sale_date").alias("day_of_week"),
                month("sale_date").alias("month")
            )
            .agg(
                avg("daily_sales").alias("avg_daily_sales"),
                stddev("daily_sales").alias("sales_stddev"),
                count("*").alias("sample_size")
            )
            .withColumn(
                "seasonal_index",
                col("avg_daily_sales")
                / avg("avg_daily_sales").over(Window.partitionBy())
            )
        )

        return seasonal_patterns

    def forecast_next_period(self, historical_df, periods=30):
        """Forecast sales for next period"""

        # Simple moving average forecast
        window_spec = Window.orderBy("date").rowsBetween(-30, 0)

        forecast_df = (
            historical_df
            .withColumn(
                "forecast",
                avg("sales").over(window_spec)
            )
            .withColumn(
                "forecast_lower",
                col("forecast")
                - 1.96 * stddev("sales").over(window_spec)
            )
            .withColumn(
                "forecast_upper",
                col("forecast")
                + 1.96 * stddev("sales").over(window_spec)
            )
        )

        return forecast_df


# Explanation: Generates sales forecasts using statistical methods
