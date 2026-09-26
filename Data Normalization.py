class DataNormalizer:
    """Normalize data to third normal form"""
    
    def normalize_customer_data(self, df):
        """Normalize customer data"""
        
        # Extract address to separate table
        address_df = (df
                      .select("customer_id", "address", "city", "state", 
                             "country", "zipcode", "latitude", "longitude")
                      .distinct()
                      .withColumn("address_id", 
                                 sha2(concat_ws("_", "address", "city", "state"), 256)))
        
        # Extract contact information
        contact_df = (df
                      .select("customer_id", "email", "phone")
                      .distinct())
        
        # Base customer info
        customer_df = (df
                       .select("customer_id", "name", "registration_date",
                              "customer_tier", "loyalty_status")
                       .distinct())
        
        return {
            "customers": customer_df,
            "addresses": address_df,
            "contacts": contact_df
        }
    
    def normalize_order_data(self, df):
        """Normalize order data"""
        
        # Order header
        order_header_df = (df
                           .select("order_id", "customer_id", "order_date",
                                  "total_amount", "status", "payment_method",
                                  "shipping_address_id", "billing_address_id")
                           .distinct())
        
        # Order items
        order_items_df = (df
                          .select("order_id", "product_id", "quantity",
                                 "price", "discount", "item_total")
                          .distinct())
        
        return {
            "order_headers": order_header_df,
            "order_items": order_items_df
        }

# Explanation: Normalizes data to reduce redundancy
