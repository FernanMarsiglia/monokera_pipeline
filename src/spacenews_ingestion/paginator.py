"""
Pagination Handler for SpaceNews API
Handles efficient pagination using the 'next' field
"""

from typing import Generator, Optional, Dict, Any, Callable
from .logging_config import get_logger

logger = get_logger(__name__)


class Paginator:
    """
    Efficient paginator for APIs that use 'next' link pattern
    
    Supports:
    - Streaming results (memory efficient)
    - Custom page size limits
    - Stop condition based on 'next' field
    """
    
    def __init__(
        self,
        fetch_func: Callable[[str, Optional[Dict[str, Any]]], Dict[str, Any]],
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        max_results: Optional[int] = None
    ):
        """
        Initialize paginator
        
        Args:
            fetch_func: Function to fetch data (e.g., api_client.get)
            endpoint: API endpoint path
            params: Initial query parameters
            page_size: Number of results per page
            max_pages: Maximum number of pages to fetch (None = unlimited)
            max_results: Maximum total results to fetch (None = unlimited)
        """
        self.fetch_func = fetch_func
        self.endpoint = endpoint
        self.params = params or {}
        self.page_size = page_size
        self.max_pages = max_pages
        self.max_results = max_results
        
        # Set limit parameter
        self.params["limit"] = page_size
        
        # Tracking
        self.current_page = 0
        self.total_results = 0
        self.next_url = None
        
        logger.info(
            "Initialized paginator",
            extra={
                "endpoint": endpoint,
                "page_size": page_size,
                "max_pages": max_pages,
                "max_results": max_results
            }
        )
    
    def __iter__(self) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate through all pages and yield individual records
        
        Yields:
            Individual record dictionaries
        """
        return self.stream_results()
    
    def stream_results(self) -> Generator[Dict[str, Any], None, None]:
        """
        Stream results page by page
        
        Yields:
            Individual record dictionaries
        
        Stops when:
        - 'next' field is null
        - max_pages reached
        - max_results reached
        """
        # Start with initial endpoint
        current_url = self.endpoint
        
        while current_url:
            # Check page limit
            if self.max_pages and self.current_page >= self.max_pages:
                logger.info(
                    "Reached maximum page limit",
                    extra={"max_pages": self.max_pages, "total_results": self.total_results}
                )
                break
            
            # Fetch page
            try:
                if self.current_page == 0:
                    # First page - use params
                    data = self.fetch_func(current_url, self.params)
                else:
                    # Subsequent pages - 'next' URL already has params
                    data = self.fetch_func(current_url, None)
                
            except Exception as e:
                logger.error(
                    "Failed to fetch page",
                    extra={
                        "endpoint": self.endpoint,
                        "page_number": self.current_page + 1,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    }
                )
                raise
            
            self.current_page += 1
            
            # Extract results
            # Special case: if no 'results' field and no 'next' field, treat entire response as single record
            if "results" not in data and "next" not in data:
                logger.info(
                    "Non-paginated endpoint detected - treating response as single record",
                    extra={
                        "endpoint": self.endpoint,
                        "page_number": self.current_page
                    }
                )
                yield data
                self.total_results += 1
                return
            
            results = data.get("results", [])
            page_count = len(results)
            
            logger.info(
                "Fetched page",
                extra={
                    "endpoint": self.endpoint,
                    "page_number": self.current_page,
                    "record_count": page_count,
                    "total_results": self.total_results + page_count
                }
            )
            
            # Yield results
            for record in results:
                yield record
                self.total_results += 1
                
                # Check result limit
                if self.max_results and self.total_results >= self.max_results:
                    logger.info(
                        "Reached maximum result limit",
                        extra={"max_results": self.max_results, "total_results": self.total_results}
                    )
                    return
            
            # Get next URL
            current_url = data.get("next")
            self.next_url = current_url
            
            # Stop if no more pages
            if not current_url:
                logger.info(
                    "Pagination complete - no more pages",
                    extra={
                        "endpoint": self.endpoint,
                        "total_pages": self.current_page,
                        "total_results": self.total_results
                    }
                )
                break
            
            # If page is empty and there's no next, stop
            if page_count == 0:
                logger.info(
                    "Pagination complete - empty page",
                    extra={
                        "endpoint": self.endpoint,
                        "total_pages": self.current_page,
                        "total_results": self.total_results
                    }
                )
                break
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get pagination statistics
        
        Returns:
            Dictionary with pagination stats
        """
        return {
            "endpoint": self.endpoint,
            "total_pages": self.current_page,
            "total_results": self.total_results,
            "page_size": self.page_size,
            "has_more": self.next_url is not None
        }


# Example usage
if __name__ == "__main__":
    from .api_client import SpaceNewsApiClient
    
    client = SpaceNewsApiClient()
    
    try:
        # Test pagination with limit
        print("Testing pagination with max_results=25:")
        paginator = Paginator(
            fetch_func=client.get,
            endpoint="/articles",
            page_size=10,
            max_results=25
        )
        
        for idx, article in enumerate(paginator, 1):
            print(f"{idx}. {article.get('title', 'No title')[:50]}...")
        
        stats = paginator.get_stats()
        print(f"\nStats: {stats}")
        
        # Test pagination with max_pages
        print("\n\nTesting pagination with max_pages=2:")
        paginator2 = Paginator(
            fetch_func=client.get,
            endpoint="/blogs",
            page_size=10,
            max_pages=2
        )
        
        count = sum(1 for _ in paginator2)
        print(f"Total blogs fetched: {count}")
        print(f"Stats: {paginator2.get_stats()}")
    
    finally:
        client.close()
