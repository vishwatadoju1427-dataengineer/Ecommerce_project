class StreamingIngestion:
    """Handle streaming data ingestion"""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def stream_orders_from_kafka(self, bootstrap_servers, topic):
        """Stream orders from Kafka"""

        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
        )

        # Parse JSON data
        schema = EcommerceSchemas.order_schema()

        orders_df = (
            df
            .select(
                from_json(
                    col("value").cast("string"),
                    schema
                ).alias("data")
            )
            .select("data.*")
        )

        # Add metadata
        orders_df = orders_df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )

        # Write stream to bronze
        bronze_path = self.config.get_path(
            "bronze",
            "orders_stream"
        )

        checkpoint_path = self.config.get_checkpoint_path(
            "orders_stream"
        )

        query = (
            orders_df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option(
                "checkpointLocation",
                checkpoint_path
            )
            .option(
                "path",
                bronze_path
            )
            .partitionBy("date(order_date)")
            .trigger(processingTime="1 minute")
            .start()
        )

        return query

    def stream_customer_clicks(self, source_path):
        """Stream customer clickstream data"""

        click_schema = StructType([
            StructField("session_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True)
        ])

        df = (
            self.spark
            .readStream
            .schema(click_schema)
            .json(source_path)
        )

        # Apply watermark for late data
        df = df.withWatermark(
            "event_timestamp",
            "10 minutes"
        )

        return df


# Explanation:
# Real-time data ingestion from streaming sources.
