class ConnectionManager:
    """Manage connections to external systems"""
    
    def __init__(self, spark):
        self.spark = spark
        self.connections = {}
    
    def create_jdbc_connection(self, name, url, properties):
        """Create JDBC connection"""
        self.connections[name] = {
            "type": "jdbc",
            "url": url,
            "properties": properties
        }
    
    def read_from_jdbc(self, connection_name, table_name):
        """Read data from JDBC source"""
        connection = self.connections[connection_name]
        return (self.spark.read
                .format("jdbc")
                .option("url", connection["url"])
                .options(**connection["properties"])
                .option("dbtable", table_name)
                .load())

# Explanation: Manages connections to external databases and systems
