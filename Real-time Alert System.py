class AlertSystem:
    """Generate real-time alerts for anomalies"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def monitor_sales_anomalies(self, sales_stream_df, threshold_stddev=3):
        """Monitor sales for anomalies"""
        
        # Calculate moving statistics
        window_spec = Window.orderBy("timestamp").rowsBetween(-100, 0)
        
        monitored_stream = (sales_stream_df
                            .withColumn("moving_avg", 
                                       avg("sales_amount").over(window_spec))
                            .withColumn("moving_stddev",
                                       stddev("sales_amount").over(window_spec))
                            .withColumn("z_score",
                                       (col("sales_amount") - col("moving_avg")) / 
                                       col("moving_stddev"))
                            .withColumn("is_anomaly",
                                       abs(col("z_score")) > threshold_stddev))
        
        # Generate alerts for anomalies
        alerts = monitored_stream.filter(col("is_anomaly") == True)
        
        return alerts
    
    def send_alert(self, alert_df, alert_type="sales_anomaly"):
        """Send alerts to monitoring system"""
        
        # Convert to Pandas for alert generation
        alert_pandas = alert_df.toPandas()
        
        for _, alert in alert_pandas.iterrows():
            alert_message = {
                "type": alert_type,
                "timestamp": alert["timestamp"],
                "metric": "sales_amount",
                "value": alert["sales_amount"],
                "expected_range": f"{alert['moving_avg'] - 2*alert['moving_stddev']:.2f} - "
                                f"{alert['moving_avg'] + 2*alert['moving_stddev']:.2f}",
                "severity": "HIGH" if abs(alert['z_score']) > 5 else "MEDIUM"
            }
            
            # Send alert (implement actual alerting mechanism)
            self.log_alert(alert_message)

# Explanation: Generates real-time alerts for anomalies
