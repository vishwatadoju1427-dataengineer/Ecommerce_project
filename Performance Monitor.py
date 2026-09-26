class PerformanceMonitor:
    """Monitor Spark job performance"""
    
    def __init__(self):
        self.metrics = {}
    
    def start_stage(self, stage_name):
        """Start timing a stage"""
        self.metrics[stage_name] = {
            "start_time": datetime.now(),
            "end_time": None,
            "duration": None
        }
    
    def end_stage(self, stage_name):
        """End timing a stage"""
        if stage_name in self.metrics:
            self.metrics[stage_name]["end_time"] = datetime.now()
            duration = (self.metrics[stage_name]["end_time"] - 
                       self.metrics[stage_name]["start_time"])
            self.metrics[stage_name]["duration"] = duration.total_seconds()
    
    def get_report(self):
        """Generate performance report"""
        report = "Performance Report:\n"
        for stage, metrics in self.metrics.items():
            report += f"{stage}: {metrics['duration']:.2f}s\n"
        return report

# Explanation: Tracks performance metrics for optimization
