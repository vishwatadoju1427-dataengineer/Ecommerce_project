class DataPipelineTester:
    """Automated testing framework for data pipelines"""
    
    def __init__(self, spark):
        self.spark = spark
        self.test_results = {}
    
    def run_data_quality_tests(self, table_name, test_suite):
        """Run data quality tests on table"""
        
        test_results = []
        
        for test in test_suite:
            if test["type"] == "not_null":
                result = self.test_not_null(table_name, test["column"])
            elif test["type"] == "unique":
                result = self.test_unique(table_name, test["column"])
            elif test["type"] == "value_range":
                result = self.test_value_range(table_name, test["column"], 
                                              test["min"], test["max"])
            elif test["type"] == "referential_integrity":
                result = self.test_referential_integrity(table_name, 
                                                        test["column"],
                                                        test["reference_table"],
                                                        test["reference_column"])
            
            test_results.append({
                "test_name": test["name"],
                "table": table_name,
                "result": result["passed"],
                "details": result
            })
        
        self.test_results[table_name] = test_results
        return test_results
    
    def test_not_null(self, table_name, column_name):
        """Test that column has no null values"""
        
        null_count = self.spark.sql(f"""
            SELECT COUNT(*) as null_count
            FROM {table_name}
            WHERE {column_name} IS NULL
        """).collect()[0]["null_count"]
        
        total_count = self.spark.table(table_name).count()
        
        return {
            "passed": null_count == 0,
            "null_count": null_count,
            "total_count": total_count,
            "null_percentage": null_count / total_count if total_count > 0 else 0
        }
    
    def test_unique(self, table_name, column_name):
        """Test that column values are unique"""
        
        distinct_count = self.spark.sql(f"""
            SELECT COUNT(DISTINCT {column_name}) as distinct_count
            FROM {table_name}
        """).collect()[0]["distinct_count"]
        
        total_count = self.spark.table(table_name).count()
        
        return {
            "passed": distinct_count == total_count,
            "distinct_count": distinct_count,
            "total_count": total_count,
            "duplicate_count": total_count - distinct_count
        }
    
    def generate_test_report(self):
        """Generate comprehensive test report"""
        
        report = {
            "summary": {
                "total_tables_tested": len(self.test_results),
                "total_tests_passed": sum(
                    1 for table_results in self.test_results.values()
                    for test in table_results if test["result"]
                ),
                "total_tests_failed": sum(
                    1 for table_results in self.test_results.values()
                    for test in table_results if not test["result"]
                )
            },
            "detailed_results": self.test_results
        }
        
        return report

# Explanation: Automated testing framework for data quality
