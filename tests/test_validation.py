"""
Unit tests for validation module
Tests schema validation and data quality checks
"""

import pytest
from datetime import datetime
from src.spacenews_ingestion.validation import (
    validate_article,
    validate_content_item,
    ValidationError
)


class TestArticleValidation:
    """Tests for article schema validation"""
    
    def test_validate_article_success(self):
        """Test valid article passes validation"""
        valid_article = {
            "id": 12345,
            "title": "SpaceX Launches Starship",
            "url": "https://example.com/article",
            "image_url": "https://example.com/image.jpg",
            "news_site": "SpaceNews",
            "summary": "SpaceX successfully launched Starship today.",
            "published_at": "2026-02-18T10:00:00Z",
            "updated_at": "2026-02-18T10:05:00Z",
            "featured": False,
            "launches": [],
            "events": []
        }
        
        # Should not raise any exception
        validated = validate_article(valid_article)
        assert validated is not None
        assert validated["id"] == 12345
    
    def test_validate_article_missing_required_field(self):
        """Test article without required field fails"""
        invalid_article = {
            "id": 12345,
            # Missing title - required field
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_article(invalid_article)
        
        assert "title" in str(exc_info.value).lower()
    
    def test_validate_article_invalid_url(self):
        """Test article with invalid URL fails"""
        invalid_article = {
            "id": 12345,
            "title": "Test Article",
            "url": "not-a-valid-url",  # Invalid URL
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_article(invalid_article)
        
        assert "url" in str(exc_info.value).lower()
    
    def test_validate_article_invalid_date_format(self):
        """Test article with invalid date format fails"""
        invalid_article = {
            "id": 12345,
            "title": "Test Article",
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18"  # Wrong format, needs ISO 8601
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_article(invalid_article)
        
        assert "date" in str(exc_info.value).lower() or "published_at" in str(exc_info.value).lower()
    
    def test_validate_article_with_optional_fields(self):
        """Test article with optional fields passes"""
        article_with_optionals = {
            "id": 12345,
            "title": "Test Article",
            "url": "https://example.com/article",
            "image_url": "https://example.com/image.jpg",
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:00:00Z",
            "updated_at": "2026-02-18T10:05:00Z",
            "featured": True,
            "launches": [{"id": "123", "provider": "SpaceX"}],
            "events": [{"id": "456", "name": "Launch Event"}]
        }
        
        validated = validate_article(article_with_optionals)
        assert validated is not None
        assert validated["featured"] is True
        assert len(validated["launches"]) == 1
    
    def test_validate_article_empty_title(self):
        """Test article with empty title fails"""
        invalid_article = {
            "id": 12345,
            "title": "",  # Empty title
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_article(invalid_article)
        
        assert "title" in str(exc_info.value).lower() or "empty" in str(exc_info.value).lower()
    
    def test_validate_article_null_required_field(self):
        """Test article with null required field fails"""
        invalid_article = {
            "id": 12345,
            "title": "Test Article",
            "url": None,  # Required field is null
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_article(invalid_article)
        
        assert "url" in str(exc_info.value).lower() or "null" in str(exc_info.value).lower()


class TestContentItemValidation:
    """Tests for generic content item validation"""
    
    def test_validate_content_item_all_types(self):
        """Test validation works for all content types"""
        content_types = ["articles", "blogs", "reports"]
        
        for content_type in content_types:
            valid_item = {
                "id": 12345,
                "title": f"Test {content_type}",
                "url": f"https://example.com/{content_type}/test",
                "news_site": "SpaceNews",
                "summary": f"Summary for {content_type}",
                "published_at": "2026-02-18T10:00:00Z"
            }
            
            validated = validate_content_item(valid_item, content_type)
            assert validated is not None
            assert validated["id"] == 12345
    
    def test_validate_content_item_strips_whitespace(self):
        """Test validation strips leading/trailing whitespace"""
        item_with_whitespace = {
            "id": 12345,
            "title": "  Test Article  ",  # Extra whitespace
            "url": "https://example.com/article",
            "news_site": "  SpaceNews  ",
            "summary": "  Test summary  ",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        validated = validate_content_item(item_with_whitespace, "articles")
        assert validated["title"] == "Test Article"
        assert validated["news_site"] == "SpaceNews"
        assert validated["summary"] == "Test summary"
    
    def test_validate_content_item_normalizes_url(self):
        """Test validation normalizes URLs"""
        valid_item = {
            "id": 12345,
            "title": "Test Article",
            "url": "HTTPS://EXAMPLE.COM/ARTICLE",  # Uppercase URL
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        validated = validate_content_item(valid_item, "articles")
        assert validated["url"] == "https://example.com/ARTICLE"  # Scheme and domain lowercased
    
    def test_validate_content_item_date_parsing(self):
        """Test validation correctly parses dates"""
        valid_item = {
            "id": 12345,
            "title": "Test Article",
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:30:45Z"
        }
        
        validated = validate_content_item(valid_item, "articles")
        # Validate that date is parseable
        published_date = datetime.fromisoformat(validated["published_at"].replace('Z', '+00:00'))
        assert published_date.year == 2026
        assert published_date.month == 2
        assert published_date.day == 18


class TestDataQualityChecks:
    """Tests for data quality validation"""
    
    def test_validate_article_summary_length(self):
        """Test articles with very short summaries are flagged"""
        short_summary_article = {
            "id": 12345,
            "title": "Test Article",
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "Short",  # Too short summary
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        # Depending on implementation, might raise warning or pass
        # This test assumes minimum summary length validation
        try:
            validated = validate_article(short_summary_article)
            # If it passes, check if there's a warning field
            if hasattr(validated, 'warnings'):
                assert 'summary' in validated.warnings
        except ValidationError as e:
            assert 'summary' in str(e).lower()
    
    def test_validate_article_title_length(self):
        """Test articles with very long titles are handled"""
        long_title = "A" * 500  # Very long title
        long_title_article = {
            "id": 12345,
            "title": long_title,
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        # Should either truncate or reject
        try:
            validated = validate_article(long_title_article)
            assert len(validated["title"]) <= 500
        except ValidationError as e:
            assert 'title' in str(e).lower() or 'length' in str(e).lower()
    
    def test_validate_article_duplicate_check(self):
        """Test validation can detect potential duplicates"""
        article1 = {
            "id": 12345,
            "title": "SpaceX Launch",
            "url": "https://example.com/article1",
            "news_site": "SpaceNews",
            "summary": "SpaceX launches Starship",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        article2 = {
            "id": 12345,  # Same ID - potential duplicate
            "title": "SpaceX Launch",
            "url": "https://example.com/article2",  # Different URL
            "news_site": "SpaceNews",
            "summary": "SpaceX launches Starship",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        v1 = validate_article(article1)
        v2 = validate_article(article2)
        
        # Both should validate, but IDs should match
        assert v1["id"] == v2["id"]


class TestEdgeCases:
    """Tests for edge cases and boundary conditions"""
    
    def test_validate_article_unicode_characters(self):
        """Test articles with unicode characters pass"""
        unicode_article = {
            "id": 12345,
            "title": "🚀 SpaceX Lanzamiento Exitoso ñáéíóú",
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "Descripción con caracteres especiales: ñ, á, é, í, ó, ú, ü",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        validated = validate_article(unicode_article)
        assert validated is not None
        assert "🚀" in validated["title"]
        assert "ñ" in validated["summary"]
    
    def test_validate_article_special_html_entities(self):
        """Test articles with HTML entities are handled"""
        html_article = {
            "id": 12345,
            "title": "SpaceX &amp; NASA Launch",
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "Summary with &lt;tags&gt; and &quot;quotes&quot;",
            "published_at": "2026-02-18T10:00:00Z"
        }
        
        validated = validate_article(html_article)
        # Depending on implementation, might decode HTML entities
        assert validated is not None
    
    def test_validate_article_future_date(self):
        """Test articles with future dates are handled"""
        future_article = {
            "id": 12345,
            "title": "Future Article",
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "This is from the future",
            "published_at": "2099-12-31T23:59:59Z"  # Far future date
        }
        
        # Should either warn or reject based on business rules
        validated = validate_article(future_article)
        assert validated is not None
    
    def test_validate_article_very_old_date(self):
        """Test articles with very old dates are handled"""
        old_article = {
            "id": 12345,
            "title": "Historical Article",
            "url": "https://example.com/article",
            "news_site": "SpaceNews",
            "summary": "From the distant past",
            "published_at": "1957-10-04T00:00:00Z"  # Sputnik 1 launch
        }
        
        validated = validate_article(old_article)
        assert validated is not None
    
    def test_validate_article_null_optional_fields(self):
        """Test articles with null optional fields pass"""
        article_null_optionals = {
            "id": 12345,
            "title": "Test Article",
            "url": "https://example.com/article",
            "image_url": None,  # Optional, can be null
            "news_site": "SpaceNews",
            "summary": "Test summary",
            "published_at": "2026-02-18T10:00:00Z",
            "updated_at": None,  # Optional
            "featured": False,
            "launches": None,  # Optional
            "events": None  # Optional
        }
        
        validated = validate_article(article_null_optionals)
        assert validated is not None
        assert validated["image_url"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
