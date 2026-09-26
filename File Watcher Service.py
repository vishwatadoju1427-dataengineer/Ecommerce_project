class FileWatcher:
    """Monitor and process new files in directories"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    def watch_and_process(self, input_dir, file_pattern, process_function):
        """Watch directory and process new files"""
        
        df = (self.spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", 
                     self.config.get_path("checkpoints", "schema"))
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
              .load(input_dir))
        
        # Process each micro-batch
        def process_batch(batch_df, batch_id):
            process_function(batch_df, batch_id)
        
        query = (df.writeStream
                 .foreachBatch(process_batch)
                 .option("checkpointLocation",
                        self.config.get_checkpoint_path("file_watcher"))
                 .start())
        
        return query

# Explanation: Watches directories for new files and processes them automatically
