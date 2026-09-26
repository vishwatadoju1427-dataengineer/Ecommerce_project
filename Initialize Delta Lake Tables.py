def initialize_delta_tables(spark, database_name="ecommerce_db"):
    """Create database and initialize Delta tables"""
    
    # Create database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    spark.sql(f"USE {database_name}")
    
    # Create bronze layer tables
    bronze_tables = {
        "raw_customers": """
            CREATE TABLE IF NOT EXISTS raw_customers (
                customer_id STRING,
                name STRING,
                email STRING,
                phone STRING,
                address STRING,
                city STRING,
                state STRING,
                country STRING,
                zipcode STRING,
                registration_date TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                source_file STRING
            ) USING DELTA
            PARTITIONED BY (date(registration_date))
        """,
        
        "raw_orders": """
            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id STRING,
                customer_id STRING,
                order_date TIMESTAMP,
                total_amount DECIMAL(10,2),
                status STRING,
                payment_method STRING,
                shipping_address STRING,
                billing_address STRING,
                items ARRAY<STRUCT<
                    product_id: STRING,
                    quantity: INT,
                    price: DECIMAL(10,2),
                    discount: DECIMAL(5,2)
                >>,
                ingestion_timestamp TIMESTAMP,
                source_file STRING
            ) USING DELTA
            PARTITIONED BY (date(order_date))
        """
    }
    
    for table_name, ddl in bronze_tables.items():
        spark.sql(ddl)
    
    return len(bronze_tables)

# Explanation: Sets up Delta tables with proper partitioning for time-based data
