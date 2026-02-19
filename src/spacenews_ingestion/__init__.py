"""
SpaceNews Ingestion Package
Production-grade ingestion system for Spaceflight News API v4
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from .api_client import SpaceNewsApiClient
from .paginator import Paginator
from .validation import Validator, ValidationError
from .logging_config import get_logger
from .alerts import SNSAlerter

__all__ = [
    "SpaceNewsApiClient",
    "Paginator",
    "Validator",
    "ValidationError",
    "get_logger",
    "SNSAlerter",
]
