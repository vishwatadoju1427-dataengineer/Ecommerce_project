class GDPRCompliance:
    """Handle GDPR compliance requirements"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def process_right_to_be_forgotten(self, customer_id):
        """Process right to be forgotten request"""
        
        # Anonymize customer data
        tables_to_anonymize = ["customers", "orders", "clickstream"]
        
        for table in tables_to_anonymize:
            self.spark.sql(f"""
                UPDATE {table}
                SET email = 'ANONYMIZED',
                    phone = 'ANONYMIZED',
                    address = 'ANONYMIZED',
                    name = 'ANONYMIZED'
                WHERE customer_id = '{customer_id}'
            """)
        
        # Log the request
        self.log_gdpr_request(customer_id, "right_to_be_forgotten")
    
    def process_data_portability(self, customer_id):
        """Process data portability request"""
        
        # Collect all customer data
        customer_data = {}
        
        tables_to_export = ["customers", "orders", "order_items", "reviews"]
        
        for table in tables_to_export:
            df = self.spark.sql(f"""
                SELECT * FROM {table}
                WHERE customer_id = '{customer_id}'
            """)
            
            customer_data[table] = df.toPandas().to_dict(orient="records")
        
        # Export as JSON
        import json
        export_json = json.dumps(customer_data, indent=2)
        
        # Save to secure location
        export_path = f"/mnt/gdpr_exports/{customer_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(export_path, 'w') as f:
            f.write(export_json)
        
        return export_path

# Explanation: Handles GDPR compliance requirements
