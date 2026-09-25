class SparkUtils:
    """Utility functions for Spark operations"""

    @staticmethod
    def read_with_schema(spark, path, file_format, schema=None):
        """Read data with schema validation"""
        reader = spark.read.format(file_format)

        if schema:
            reader = reader.schema(schema)

        return reader.load(path)

    @staticmethod
    def write_delta_table(df, path, mode="append", partition_by=None):
        """Write DataFrame to Delta table"""
        writer = df.write.format("delta").mode(mode)

        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.save(path)

    @staticmethod
    def optimize_table(spark, table_name, zorder_cols=None):
        """Optimize Delta table"""
        optimize_sql = f"OPTIMIZE {table_name}"

        if zorder_cols:
            zorder_str = ", ".join(zorder_cols)
            optimize_sql += f" ZORDER BY ({zorder_str})"

        spark.sql(optimize_sql)

    @staticmethod
    def vacuum_table(spark, table_name, retention_hours=168):
        """Vacuum Delta table"""
        spark.sql(
            f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
        )
     # Explanation: Common Spark operations utility functions
