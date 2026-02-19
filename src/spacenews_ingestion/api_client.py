"""
API client for Spaceflight News API v4 with pagination, retries, and rate limit handling.
"""
import time
import logging
from typing import Any, Dict, Iterator, Optional
import requests
from .logging_config import get_logger

class SpaceNewsApiClient:
    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 5, backoff_base: float = 1.5, logger: Optional[logging.Logger] = None):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.logger = logger or get_logger(__name__)

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a single GET request to the API.
        
        Args:
            endpoint: API endpoint path or full URL
            params: Query parameters
        
        Returns:
            JSON response as dictionary
        """
        # If endpoint is already a full URL, use it directly
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = f"{self.base_url}{endpoint}"
        
        response = self._request_with_retries(url, params or {})
        return response.json()

    def get_paginated(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        offset = params.get("offset", 0)
        retries = 0
        while url:
            start_time = time.time()
            try:
                resp = self._request_with_retries(url, params)
                data = resp.json()
                elapsed_ms = int((time.time() - start_time) * 1000)
                self.logger.info(
                    f"Fetched page from {endpoint}, offset={offset}, records={len(data.get('results', []))}, elapsed={elapsed_ms}ms"
                )
                yield data
                url = data.get("next")
                if url:
                    params = {}  # next is a full URL, params not needed
                    offset = int(data.get("offset", 0))
            except Exception as e:
                self.logger.error(f"Failed to fetch page from {endpoint}, offset={offset}: {str(e)}")
                raise

    def _request_with_retries(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        retries = 0
        while True:
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    raise requests.HTTPError(f"{resp.status_code} {resp.reason}", response=resp)
                resp.raise_for_status()
                return resp
            except requests.HTTPError as e:
                if retries >= self.max_retries:
                    self.logger.error(f"Max retries exceeded for {url}: {str(e)}")
                    raise
                wait = self.backoff_base ** retries
                self.logger.warning(f"Retrying {url} after error (attempt {retries+1}/{self.max_retries}), waiting {wait}s: {str(e)}")
                time.sleep(wait)
                retries += 1

    def close(self):
        """Close the HTTP session."""
        if hasattr(self, 'session'):
            self.session.close()
