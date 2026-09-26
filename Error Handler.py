class ErrorHandler:
    """Handle errors and exceptions in Spark jobs"""
    
    def __init__(self, logger):
        self.logger = logger
        self.error_counts = {}
    
    def handle_exception(self, exception, context=None):
        """Handle exceptions with context"""
        error_type = type(exception).__name__
        
        # Increment error count
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
        
        # Log error
        error_msg = f"Error in {context}: {str(exception)}"
        self.logger.error(error_msg)
        
        # Return error response
        return {
            "success": False,
            "error": error_type,
            "message": str(exception),
            "context": context
        }
    
    def get_error_summary(self):
        """Get error summary report"""
        return self.error_counts

# Explanation: Centralized error handling for reliability
