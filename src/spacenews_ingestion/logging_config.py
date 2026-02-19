"""
Structured JSON Logging Configuration
Provides correlation_id/run_id tracking across the pipeline
"""

import logging
import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional
import uuid


class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def __init__(self, correlation_id: Optional[str] = None):
        super().__init__()
        self.correlation_id = correlation_id or str(uuid.uuid4())
    
    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "correlation_id": self.correlation_id,
            "message": record.getMessage(),
        }
        
        # Add extra fields if present
        if hasattr(record, "run_id"):
            log_data["run_id"] = record.run_id
        if hasattr(record, "endpoint"):
            log_data["endpoint"] = record.endpoint
        if hasattr(record, "page_number"):
            log_data["page_number"] = record.page_number
        if hasattr(record, "record_count"):
            log_data["record_count"] = record.record_count
        if hasattr(record, "s3_path"):
            log_data["s3_path"] = record.s3_path
        if hasattr(record, "error_type"):
            log_data["error_type"] = record.error_type
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


def get_logger(
    name: str,
    level: int = logging.INFO,
    correlation_id: Optional[str] = None,
    json_format: bool = True
) -> logging.Logger:
    """
    Get a configured logger instance
    
    Args:
        name: Logger name (usually __name__)
        level: Logging level
        correlation_id: Optional correlation ID for tracking
        json_format: Whether to use JSON format (default: True)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplication
    logger.handlers = []
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    
    # Set formatter
    if json_format:
        formatter = JsonFormatter(correlation_id=correlation_id)
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


class LogContext:
    """Context manager for adding extra fields to log records"""
    
    def __init__(self, logger: logging.Logger, **kwargs):
        self.logger = logger
        self.extra_fields = kwargs
        self.old_factory = logging.getLogRecordFactory()
    
    def __enter__(self):
        def record_factory(*args, **kwargs):
            record = self.old_factory(*args, **kwargs)
            for key, value in self.extra_fields.items():
                setattr(record, key, value)
            return record
        
        logging.setLogRecordFactory(record_factory)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.setLogRecordFactory(self.old_factory)


# Example usage:
if __name__ == "__main__":
    logger = get_logger(__name__, correlation_id="test_run_123")
    
    logger.info("Starting extraction")
    
    with LogContext(logger, endpoint="articles", page_number=1):
        logger.info("Fetching page 1")
    
    with LogContext(logger, record_count=100, s3_path="s3://bucket/data"):
        logger.info("Uploaded to S3")
    
    try:
        1 / 0
    except Exception:
        logger.exception("Error occurred", extra={"error_type": "ZeroDivisionError"})
