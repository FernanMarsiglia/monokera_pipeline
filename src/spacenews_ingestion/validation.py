"""
Data Validation Module
Schema validation and required fields checking
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from .logging_config import get_logger

logger = get_logger(__name__)


class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass


class Validator:
    """
    Validator for SpaceNews API data
    
    Validates:
    - Required fields presence
    - Field types
    - Timestamp formats
    - URL formats
    """
    
    # Schema definitions for each endpoint
    SCHEMAS = {
        "articles": {
            "required_fields": ["id", "title", "url", "published_at"],
            "optional_fields": ["image_url", "summary", "news_site", "updated_at", 
                              "featured", "launches", "events", "authors"],
            "timestamp_fields": ["published_at", "updated_at"],
            "url_fields": ["url", "image_url"],
            "array_fields": ["launches", "events", "authors"],
        },
        "blogs": {
            "required_fields": ["id", "title", "url", "published_at"],
            "optional_fields": ["image_url", "summary", "news_site", "updated_at",
                              "featured", "launches", "events"],
            "timestamp_fields": ["published_at", "updated_at"],
            "url_fields": ["url", "image_url"],
            "array_fields": ["launches", "events"],
        },
        "reports": {
            "required_fields": ["id", "title", "url", "published_at"],
            "optional_fields": ["image_url", "summary", "news_site", "updated_at"],
            "timestamp_fields": ["published_at", "updated_at"],
            "url_fields": ["url", "image_url"],
            "array_fields": [],
        },
        "info": {
            "required_fields": ["version"],
            "optional_fields": ["news_sites"],
            "timestamp_fields": [],
            "url_fields": [],
            "array_fields": ["news_sites"],
        }
    }
    
    def __init__(self, endpoint: str):
        """
        Initialize validator for specific endpoint
        
        Args:
            endpoint: API endpoint name (articles, blogs, reports, info)
        """
        if endpoint not in self.SCHEMAS:
            raise ValueError(f"Unknown endpoint: {endpoint}")
        
        self.endpoint = endpoint
        self.schema = self.SCHEMAS[endpoint]
        
        logger.debug(f"Initialized validator for endpoint: {endpoint}")
    
    def validate(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate a single record
        
        Args:
            record: Record dictionary to validate
        
        Returns:
            Tuple of (is_valid, error_reason)
        """
        # Check required fields
        for field in self.schema["required_fields"]:
            if field not in record or record[field] is None:
                error_msg = f"Missing required field: {field}"
                logger.warning(
                    "Validation failed",
                    extra={
                        "endpoint": self.endpoint,
                        "error_reason": error_msg,
                        "record_id": record.get("id")
                    }
                )
                return False, error_msg
        
        # Validate timestamps
        for field in self.schema["timestamp_fields"]:
            if field in record and record[field] is not None:
                if not self._is_valid_timestamp(record[field]):
                    error_msg = f"Invalid timestamp format for field: {field}"
                    logger.warning(
                        "Validation failed",
                        extra={
                            "endpoint": self.endpoint,
                            "error_reason": error_msg,
                            "record_id": record.get("id"),
                            "invalid_value": record[field]
                        }
                    )
                    return False, error_msg
        
        # Validate URLs
        for field in self.schema["url_fields"]:
            if field in record and record[field] is not None:
                if not self._is_valid_url(record[field]):
                    error_msg = f"Invalid URL format for field: {field}"
                    logger.warning(
                        "Validation failed",
                        extra={
                            "endpoint": self.endpoint,
                            "error_reason": error_msg,
                            "record_id": record.get("id"),
                            "invalid_value": record[field]
                        }
                    )
                    return False, error_msg
        
        # Validate array fields
        for field in self.schema["array_fields"]:
            if field in record and record[field] is not None:
                if not isinstance(record[field], list):
                    error_msg = f"Field should be array: {field}"
                    logger.warning(
                        "Validation failed",
                        extra={
                            "endpoint": self.endpoint,
                            "error_reason": error_msg,
                            "record_id": record.get("id"),
                            "invalid_type": type(record[field]).__name__
                        }
                    )
                    return False, error_msg
        
        # All validations passed
        return True, None
    
    def validate_batch(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Validate a batch of records
        
        Args:
            records: List of record  dictionaries
        
        Returns:
            Tuple of (valid_records, invalid_records_with_errors)
        """
        valid_records = []
        invalid_records = []
        
        for record in records:
            is_valid, error_reason = self.validate(record)
            
            if is_valid:
                valid_records.append(record)
            else:
                # Add error reason to record
                invalid_record = record.copy()
                invalid_record["_error_reason"] = error_reason
                invalid_record["_validation_timestamp"] = datetime.utcnow().isoformat()
                invalid_records.append(invalid_record)
        
        logger.info(
            "Batch validation complete",
            extra={
                "endpoint": self.endpoint,
                "total_records": len(records),
                "valid_count": len(valid_records),
                "invalid_count": len(invalid_records)
            }
        )
        
        return valid_records, invalid_records
    
    @staticmethod
    def _is_valid_timestamp(value: str) -> bool:
        """Check if value is a valid ISO 8601 timestamp"""
        if not isinstance(value, str):
            return False
        
        # Try common formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",  # 2024-01-01T12:00:00.000Z
            "%Y-%m-%dT%H:%M:%SZ",      # 2024-01-01T12:00:00Z
            "%Y-%m-%dT%H:%M:%S",       # 2024-01-01T12:00:00
            "%Y-%m-%d %H:%M:%S",       # 2024-01-01 12:00:00
        ]
        
        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        
        return False
    
    @staticmethod
    def _is_valid_url(value: str) -> bool:
        """Check if value is a valid URL"""
        if not isinstance(value, str):
            return False
        
        # Basic URL validation
        return value.startswith(("http://", "https://")) and len(value) > 10
    
    @staticmethod
    def parse_timestamp(value: str) -> Optional[datetime]:
        """
        Safely parse timestamp string
        
        Args:
            value: Timestamp string
        
        Returns:
            datetime object or None if parsing fails
        """
        if not value:
            return None
        
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        
        logger.warning(f"Failed to parse timestamp: {value}")
        return None


# Example usage
if __name__ == "__main__":
    validator = Validator("articles")
    
    # Valid record
    valid_record = {
        "id": 123,
        "title": "SpaceX launches Falcon 9",
        "url": "https://example.com/article",
        "published_at": "2024-01-01T12:00:00Z",
        "summary": "SpaceX successfully launched...",
        "news_site": "Space.com",
        "authors": ["John Doe"],
        "launches": [],
        "events": []
    }
    
    is_valid, error = validator.validate(valid_record)
    print(f"Valid record: {is_valid}, Error: {error}")
    
    # Invalid record (missing required field)
    invalid_record = {
        "id": 124,
        "title": "Another article",
        # Missing 'url'
        "published_at": "2024-01-01T12:00:00Z",
    }
    
    is_valid, error = validator.validate(invalid_record)
    print(f"Invalid record: {is_valid}, Error: {error}")
    
    # Batch validation
    records = [valid_record, invalid_record]
    valid, invalid = validator.validate_batch(records)
    print(f"\nBatch validation: {len(valid)} valid, {len(invalid)} invalid")
    
    if invalid:
        print(f"Invalid record error: {invalid[0]['_error_reason']}")
