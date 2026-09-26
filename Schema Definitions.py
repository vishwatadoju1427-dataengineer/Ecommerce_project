class EcommerceSchemas:
    """Schema definitions for e-commerce data"""

    @staticmethod
    def customer_schema():
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("zipcode", StringType(), True),
            StructField("registration_date", TimestampType(), True),
            StructField(
                "preferences",
                MapType(StringType(), StringType()),
                True
            )
        ])

    @staticmethod
    def order_schema():
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_date", TimestampType(), True),
            StructField("total_amount", DecimalType(10, 2), True),
            StructField("status", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("billing_address", StringType(), True),
            StructField(
                "items",
                ArrayType(
                    StructType([
                        StructField("product_id", StringType(), True),
                        StructField("quantity", IntegerType(), True),
                        StructField("price", DecimalType(10, 2), True),
                        StructField("discount", DecimalType(5, 2), True)
                    ])
                ),
                True
            )
        ])


# Explanation: Centralized schema definitions for consistency
