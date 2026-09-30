class ExternalDataEnricher:
    """Enrich data with external APIs"""
    
    def __init__(self, spark):
        self.spark = spark
        
    def enrich_with_geolocation(self, df):
        """Enrich with geolocation data"""
        
        # Define UDF for geocoding API call
        @udf(returnType=StructType([
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("timezone", StringType())
        ]))
        def geocode_address(address, city, state):
            # Mock API call - replace with actual geocoding service
            import requests
            try:
                response = requests.get(
                    f"https://geocode.example.com/?address={address},{city},{state}"
                )
                return (response.json()["lat"], 
                       response.json()["lng"],
                       response.json()["timezone"])
            except:
                return (None, None, None)
        
        enriched_df = (df
                       .withColumn("geolocation",
                                  geocode_address(col("address"),
                                                 col("city"),
                                                 col("state")))
                       .withColumn("latitude", col("geolocation.latitude"))
                       .withColumn("longitude", col("geolocation.longitude"))
                       .withColumn("timezone", col("geolocation.timezone")))
        
        return enriched_df

# Explanation: Enriches data by calling external APIs
