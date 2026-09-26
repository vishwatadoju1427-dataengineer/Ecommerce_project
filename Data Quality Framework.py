class DataQualityFramework:
    """Framework for data quality checks"""
    
    def __init__(self, spark):
        self.spark = spark
        self.quality_metrics = {}
    
    def check_null_percentage(self, df, column_name):
        """Calculate null percentage in a column"""
        total_count = df.count()
        null_count = df.filter(col(column_name).isNull()).count()
        null_percentage = (null_count / total_count) if total_count > 0 else 0
        
        self.quality_metrics[f"{column_name}_null_percentage"] = null_percentage
        return null_percentage
    
    def check_duplicates(self, df, key_columns):
        """Check for duplicate records"""
        total_count = df.count()
        distinct_count = df.select(key_columns).distinct().count()
        duplicate_percentage = 1 - (distinct_count / total_count) if total_count > 0 else 0
        
        self.quality_metrics["duplicate_percentage"] = duplicate_percentage
        return duplicate_percentage
    
    def validate_schema(self, df, expected_schema):
        """Validate DataFrame schema against expected schema"""
        actual_schema = df.schema
        mismatches = []
        
        for field in expected_schema:
            if field.name not in [f.name for f in actual_schema]:
                mismatches.append(f"Missing field: {field.name}")
        
        return mismatches

# Explanation: Reusable data quality checks for validation
