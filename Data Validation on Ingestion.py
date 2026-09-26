class IngestionValidator:
    """Validate data during ingestion"""
    
    def __init__(self, spark):
        self.spark = spark
        self.dq_framework = DataQualityFramework(spark)
    
    def validate_customer_ingestion(self, df):
        """Validate customer data during ingestion"""
        issues = []
        
        # Check required fields
        required_fields = ["customer_id", "email"]
        for field in required_fields:
            null_pct = self.dq_framework.check_null_percentage(df, field)
            if null_pct > 0:
                issues.append(f"High null percentage in {field}: {null_pct}")
        
        # Check email format
        email_count = df.filter(col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$')).count()
        invalid_email_pct = 1 - (email_count / df.count())
        
        if invalid_email_pct > 0.1:
            issues.append(f"High invalid email percentage: {invalid_email_pct}")
        
        # Check duplicate customer_ids
        duplicate_pct = self.dq_framework.check_duplicates(df, ["customer_id"])
        if duplicate_pct > 0:
            issues.append(f"Duplicate customer_ids found: {duplicate_pct}")
        
        return issues

# Explanation: Validates data quality during ingestion phase
