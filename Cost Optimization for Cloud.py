class CostOptimizer:
    """Optimize costs for cloud deployments"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def estimate_query_cost(self, query_plan):
        """Estimate cost of query execution"""
        
        # Analyze query plan
        analyzed_plan = self.spark.sql(query_plan)._jdf.queryExecution().analyzed()
        
        # Estimate data scanned
        data_scanned_mb = self.estimate_data_scanned(analyzed_plan)
        
        # Calculate cost (example: $5 per TB scanned)
        cost_per_tb = 5.0
        estimated_cost = (data_scanned_mb / (1024 * 1024)) * cost_per_tb
        
        return {
            "data_scanned_mb": data_scanned_mb,
            "estimated_cost": estimated_cost,
            "suggestions": self.generate_cost_suggestions(analyzed_plan)
        }
    
    def generate_cost_suggestions(self, query_plan):
        """Generate cost optimization suggestions"""
        
        suggestions = []
        
        # Check for full table scans
        if "Scan" in str(query_plan) and "Filter" not in str(query_plan):
            suggestions.append("Add filters to avoid full table scans")
        
        # Check for large shuffles
        if "Exchange" in str(query_plan):
            suggestions.append("Consider increasing partition count to reduce shuffle size")
        
        # Check for cartesian joins
        if "CartesianProduct" in str(query_plan):
            suggestions.append("Avoid cartesian joins, add join conditions")
        
        return suggestions

# Explanation: Optimizes costs for cloud-based deployments
