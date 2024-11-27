import pytest
from unittest.mock import patch, Mock
from src.webscraper.search import CompanySearcher


@pytest.fixture
def searcher():
    with patch('src.webscraper.search.load_dotenv'):  # Mock load_dotenv
        searcher = CompanySearcher()
        searcher.api_key = "fake_api_key"
        return searcher


@pytest.fixture
def mock_response():
    def _create_response(status_code, json_data):
        mock_resp = Mock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = json_data
        return mock_resp

    return _create_response


def test_valid_domain_check(searcher):
    """Test the domain validation logic"""
    assert searcher._is_valid_domain("https://company.com") == True
    assert searcher._is_valid_domain("https://linkedin.com/company") == False
    assert searcher._is_valid_domain("https://sub.facebook.com") == False
    assert searcher._is_valid_domain("https://example.twitter.com/path") == False


def test_successful_search(searcher, mock_response):
    """Test successful company search with valid domain"""
    mock_data = {
        "organic": [
            {"link": "https://company.com"},
            {"link": "https://linkedin.com/company"}
        ]
    }

    with patch('requests.post') as mock_post:
        mock_post.return_value = mock_response(200, mock_data)
        result = searcher.search_company("Valid Company")

        # Should return first valid domain
        assert result == "https://company.com"

        # Verify API call
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert kwargs['headers']['X-API-KEY'] == "fake_api_key"
        assert "Valid Company" in kwargs['json']['q']


def test_only_invalid_domains(searcher, mock_response):
    """Test when all search results are from excluded domains"""
    mock_data = {
        "organic": [
            {"link": "https://linkedin.com/company"},
            {"link": "https://facebook.com/business"},
            {"link": "https://twitter.com/company"}
        ]
    }

    with patch('requests.post') as mock_post:
        mock_post.return_value = mock_response(200, mock_data)
        result = searcher.search_company("Social Media Company")

        assert result is None


def test_failed_api_request(searcher, mock_response):
    """Test handling of failed API request"""
    with patch('requests.post') as mock_post:
        mock_post.return_value = mock_response(404, {})
        result = searcher.search_company("Failed Company")

        assert result is None


def test_empty_response(searcher, mock_response):
    """Test handling of empty API response"""
    mock_data = {
        "organic": []
    }

    with patch('requests.post') as mock_post:
        mock_post.return_value = mock_response(200, mock_data)
        result = searcher.search_company("Empty Company")

        assert result is None


def test_malformed_response(searcher, mock_response):
    """Test handling of malformed API response"""
    mock_data = {
        "organic": [
            {"no_link": "https://example.com"},
            {}
        ]
    }

    with patch('requests.post') as mock_post:
        mock_post.return_value = mock_response(200, mock_data)
        result = searcher.search_company("Malformed Company")

        assert result is None


def test_empty_company_name(searcher):
    """Test search with empty company name"""
    result = searcher.search_company("")
    assert result is None