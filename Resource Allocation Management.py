class ResourceManager:
    """Manage Spark resource allocation"""

    def __init__(self, spark_context):
        self.sc = spark_context
        self.executor_metrics = {}

    def monitor_resource_usage(self):
        """Monitor current resource usage"""

        # Get executor metrics
        status_tracker = self.sc.statusTracker()
        executor_info = status_tracker.getExecutorInfos()

        for executor in executor_info:
            executor_id = executor.executorId()

            self.executor_metrics[executor_id] = {
                "total_cores": executor.totalCores(),
                "memory_used": executor.memoryUsed(),
                "disk_used": executor.diskUsed(),
                "active_tasks": executor.activeTasks()
            }

        return self.executor_metrics

    def optimize_resource_allocation(self, df, operation_type):
        """Optimize resource allocation based on operation type"""

        if operation_type == "join":
            # Increase shuffle partitions for joins
            df.sparkSession.conf.set(
                "spark.sql.shuffle.partitions",
                "400"
            )

        elif operation_type == "aggregation":
            # Adjust memory fraction for aggregations
            df.sparkSession.conf.set(
                "spark.memory.fraction",
                "0.8"
            )

            df.sparkSession.conf.set(
                "spark.memory.storageFraction",
                "0.5"
            )

        elif operation_type == "sort":
            # Increase sort buffer for sorting operations
            df.sparkSession.conf.set(
                "spark.sql.sort.spill.numElementsForceSpillThreshold",
                "1000000"
            )

        return df


# Explanation: Manages and optimizes Spark resource allocation
