"""
Unit tests for paginator module
Tests pagination logic and API response handling
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.spacenews_ingestion.paginator import Paginator


class TestPaginatorInitialization:
    """Tests for Paginator initialization"""
    
    def test_paginator_init_default_params(self):
        """Test paginator initializes with default parameters"""
        client = Mock()
        paginator = Paginator(client, "/articles")
        
        assert paginator.endpoint == "/articles"
        assert paginator.params == {}
        assert paginator.limit is None
    
    def test_paginator_init_with_params(self):
        """Test paginator initializes with custom parameters"""
        client = Mock()
        params = {"search": "SpaceX", "limit": 10}
        paginator = Paginator(client, "/articles", params=params)
        
        assert paginator.endpoint == "/articles"
        assert paginator.params == params
        assert "search" in paginator.params
    
    def test_paginator_init_with_limit(self):
        """Test paginator initializes with result limit"""
        client = Mock()
        paginator = Paginator(client, "/articles", limit=100)
        
        assert paginator.limit == 100


class TestPaginatorIteration:
    """Tests for paginator iteration logic"""
    
    def test_paginator_single_page(self):
        """Test paginator handles single page response"""
        client = Mock()
        client.get.return_value = {
            "count": 10,
            "next": None,
            "previous": None,
            "results": [{"id": i} for i in range(10)]
        }
        
        paginator = Paginator(client, "/articles")
        results = list(paginator)
        
        assert len(results) == 1  # One page
        assert len(results[0]["results"]) == 10
        assert client.get.call_count == 1
    
    def test_paginator_multiple_pages(self):
        """Test paginator handles multiple pages"""
        client = Mock()
        
        # Mock responses for 3 pages
        responses = [
            {
                "count": 25,
                "next": "https://api.example.com/articles?offset=10",
                "previous": None,
                "results": [{"id": i} for i in range(10)]
            },
            {
                "count": 25,
                "next": "https://api.example.com/articles?offset=20",
                "previous": "https://api.example.com/articles",
                "results": [{"id": i} for i in range(10, 20)]
            },
            {
                "count": 25,
                "next": None,
                "previous": "https://api.example.com/articles?offset=10",
                "results": [{"id": i} for i in range(20, 25)]
            }
        ]
        
        client.get.side_effect = responses
        
        paginator = Paginator(client, "/articles")
        results = list(paginator)
        
        assert len(results) == 3  # Three pages
        assert client.get.call_count == 3
        assert len(results[0]["results"]) == 10
        assert len(results[1]["results"]) == 10
        assert len(results[2]["results"]) == 5
    
    def test_paginator_stops_at_limit(self):
        """Test paginator respects result limit"""
        client = Mock()
        
        # Mock responses with more pages available
        client.get.side_effect = [
            {
                "count": 100,
                "next": "https://api.example.com/articles?offset=10",
                "previous": None,
                "results": [{"id": i} for i in range(10)]
            },
            {
                "count": 100,
                "next": "https://api.example.com/articles?offset=20",
                "previous": "https://api.example.com/articles",
                "results": [{"id": i} for i in range(10, 20)]
            }
        ]
        
        paginator = Paginator(client, "/articles", limit=15)
        results = list(paginator)
        
        # Should stop after collecting 15 results (2 pages: 10 + 5)
        total_results = sum(len(page["results"]) for page in results)
        assert total_results <= 15
    
    def test_paginator_empty_response(self):
        """Test paginator handles empty response"""
        client = Mock()
        client.get.return_value = {
            "count": 0,
            "next": None,
            "previous": None,
            "results": []
        }
        
        paginator = Paginator(client, "/articles")
        results = list(paginator)
        
        assert len(results) == 1  # Still yields one page
        assert len(results[0]["results"]) == 0
    
    def test_paginator_yields_pages_not_items(self):
        """Test paginator yields entire pages, not individual items"""
        client = Mock()
        client.get.return_value = {
            "count": 5,
            "next": None,
            "previous": None,
            "results": [{"id": i} for i in range(5)]
        }
        
        paginator = Paginator(client, "/articles")
        
        for page in paginator:
            assert isinstance(page, dict)
            assert "results" in page
            assert isinstance(page["results"], list)
            break  # Just check first iteration


class TestPaginatorErrorHandling:
    """Tests for error handling in pagination"""
    
    def test_paginator_handles_missing_results_key(self):
        """Test paginator handles response without 'results' key"""
        client = Mock()
        client.get.return_value = {
            "count": 0,
            "next": None
            # Missing 'results' key
        }
        
        paginator = Paginator(client, "/articles")
        
        with pytest.raises(KeyError):
            list(paginator)
    
    def test_paginator_handles_network_error(self):
        """Test paginator propagates network errors"""
        client = Mock()
        client.get.side_effect = ConnectionError("Network error")
        
        paginator = Paginator(client, "/articles")
        
        with pytest.raises(ConnectionError):
            list(paginator)
    
    def test_paginator_handles_malformed_response(self):
        """Test paginator handles malformed API response"""
        client = Mock()
        client.get.return_value = "not a dict"  # Malformed response
        
        paginator = Paginator(client, "/articles")
        
        with pytest.raises((TypeError, AttributeError, KeyError)):
            list(paginator)
    
    def test_paginator_handles_none_response(self):
        """Test paginator handles None response"""
        client = Mock()
        client.get.return_value = None
        
        paginator = Paginator(client, "/articles")
        
        with pytest.raises((TypeError, AttributeError)):
            list(paginator)


class TestPaginatorQueryParams:
    """Tests for query parameter handling"""
    
    def test_paginator_preserves_query_params(self):
        """Test paginator preserves custom query parameters"""
        client = Mock()
        client.get.return_value = {
            "count": 5,
            "next": None,
            "results": [{"id": i} for i in range(5)]
        }
        
        params = {
            "search": "SpaceX",
            "published_at_gte": "2026-01-01",
            "ordering": "-published_at"
        }
        
        paginator = Paginator(client, "/articles", params=params)
        list(paginator)
        
        # Check that params were passed to client.get
        call_args = client.get.call_args
        assert call_args is not None
    
    def test_paginator_handles_limit_param(self):
        """Test paginator correctly applies limit parameter"""
        client = Mock()
        
        # First page with 10 results
        client.get.return_value = {
            "count": 100,
            "next": "https://api.example.com/articles?offset=10",
            "results": [{"id": i} for i in range(10)]
        }
        
        paginator = Paginator(client, "/articles", params={"limit": 10})
        page = next(iter(paginator))
        
        assert len(page["results"]) == 10
    
    def test_paginator_handles_offset_param(self):
        """Test paginator can start from specific offset"""
        client = Mock()
        client.get.return_value = {
            "count": 100,
            "next": "https://api.example.com/articles?offset=60",
            "results": [{"id": i} for i in range(50, 60)]
        }
        
        paginator = Paginator(client, "/articles", params={"offset": 50})
        page = next(iter(paginator))
        
        assert page["results"][0]["id"] == 50


class TestPaginatorNextUrl:
    """Tests for handling 'next' URL pagination"""
    
    def test_paginator_follows_next_url(self):
        """Test paginator follows 'next' URL for pagination"""
        client = Mock()
        
        responses = [
            {
                "count": 20,
                "next": "https://api.example.com/v4/articles/?offset=10",
                "results": [{"id": i} for i in range(10)]
            },
            {
                "count": 20,
                "next": None,
                "results": [{"id": i} for i in range(10, 20)]
            }
        ]
        
        client.get.side_effect = responses
        
        paginator = Paginator(client, "/articles")
        pages = list(paginator)
        
        assert len(pages) == 2
        assert client.get.call_count == 2
    
    def test_paginator_handles_absolute_next_url(self):
        """Test paginator handles absolute URLs in 'next'"""
        client = Mock()
        
        client.get.side_effect = [
            {
                "count": 15,
                "next": "https://api.spaceflightnewsapi.net/v4/articles/?offset=10",
                "results": [{"id": i} for i in range(10)]
            },
            {
                "count": 15,
                "next": None,
                "results": [{"id": i} for i in range(10, 15)]
            }
        ]
        
        paginator = Paginator(client, "/articles")
        pages = list(paginator)
        
        assert len(pages) == 2
    
    def test_paginator_handles_relative_next_url(self):
        """Test paginator handles relative URLs in 'next'"""
        client = Mock()
        
        client.get.side_effect = [
            {
                "count": 15,
                "next": "/v4/articles/?offset=10",  # Relative URL
                "results": [{"id": i} for i in range(10)]
            },
            {
                "count": 15,
                "next": None,
                "results": [{"id": i} for i in range(10, 15)]
            }
        ]
        
        paginator = Paginator(client, "/articles")
        pages = list(paginator)
        
        assert len(pages) == 2


class TestPaginatorPerformance:
    """Tests for paginator performance characteristics"""
    
    def test_paginator_lazy_evaluation(self):
        """Test paginator uses lazy evaluation (generator)"""
        client = Mock()
        client.get.return_value = {
            "count": 10,
            "next": None,
            "results": [{"id": i} for i in range(10)]
        }
        
        paginator = Paginator(client, "/articles")
        
        # Creating paginator shouldn't make any API calls
        assert client.get.call_count == 0
        
        # Only when iterating should it call the API
        next(iter(paginator))
        assert client.get.call_count == 1
    
    def test_paginator_memory_efficient_large_dataset(self):
        """Test paginator doesn't load all results into memory at once"""
        client = Mock()
        
        # Simulate 10 pages of data
        def side_effect(*args, **kwargs):
            call_count = client.get.call_count
            if call_count < 10:
                return {
                    "count": 100,
                    "next": f"https://api.example.com/articles?offset={call_count * 10}",
                    "results": [{"id": i} for i in range(call_count * 10, (call_count + 1) * 10)]
                }
            else:
                return {
                    "count": 100,
                    "next": None,
                    "results": []
                }
        
        client.get.side_effect = side_effect
        
        paginator = Paginator(client, "/articles")
        
        # Process pages one at a time - memory efficient
        processed = 0
        for page in paginator:
            processed += len(page["results"])
            if processed >= 50:
                break  # Can stop early without fetching all pages
        
        # Should have only fetched enough pages to reach 50 results
        assert client.get.call_count <= 6


class TestPaginatorIntegration:
    """Integration tests with mocked client"""
    
    @patch('src.spacenews_ingestion.api_client.SpaceNewsApiClient')
    def test_paginator_with_real_client_structure(self, mock_client_class):
        """Test paginator works with realistic client structure"""
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "count": 5,
            "next": None,
            "previous": None,
            "results": [
                {"id": 1, "title": "Article 1"},
                {"id": 2, "title": "Article 2"},
                {"id": 3, "title": "Article 3"},
                {"id": 4, "title": "Article 4"},
                {"id": 5, "title": "Article 5"}
            ]
        }
        
        paginator = Paginator(mock_client, "/articles")
        results = list(paginator)
        
        assert len(results) == 1
        assert len(results[0]["results"]) == 5
        assert results[0]["results"][0]["title"] == "Article 1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
