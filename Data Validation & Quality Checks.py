class SilverDataValidator:
    """Validate data before moving to Silver layer"""
    
    def __init__(self):
        self.validation_rules = {
            "customers": [
                ("customer_id", "not_null"),
                ("email", "valid_email"),
                ("registration_date", "not_future_date")
            ],
            "orders": [
                ("order_id", "not_null"),
                ("total_amount", "positive_value"),
                ("order_date", "not_future_date")
            ]
        }
    
    def validate_table(self, df, table_name):
        """Validate DataFrame against rules"""
        
        validation_results = []
        
        if table_name in self.validation_rules:
            for column, rule in self.validation_rules[table_name]:
                result = self.apply_validation_rule(df, column, rule)
                validation_results.append({
                    "table": table_name,
                    "column": column,
                    "rule": rule,
                    "result": result
                })
        
        return validation_results
    
    def apply_validation_rule(self, df, column, rule):
        """Apply specific validation rule"""
        
        if rule == "not_null":
            null_count = df.filter(col(column).isNull()).count()
            return {"passed": null_count == 0, "null_count": null_count}
        
        elif rule == "valid_email":
            valid_count = df.filter(col(column).rlike(r'^[^@]+@[^@]+\.[^@]+$')).count()
            return {"passed": valid_count == df.count(), "invalid_count": df.count() - valid_count}
        
        elif rule == "not_future_date":
            future_count = df.filter(col(column) > current_date()).count()
            return {"passed": future_count == 0, "future_count": future_count}

# Explanation: Comprehensive validation before promoting to Silver layer
