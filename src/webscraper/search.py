from typing import Optional, Set
import requests
import os
from dotenv import load_dotenv
from urllib.parse import urlparse


class CompanySearcher:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("SERPER_API_KEY")
        self.headers = {
            'X-API-KEY': self.api_key
        }
        self.excluded_domains = {
            'linkedin.com',
            'facebook.com',
            'twitter.com',
            'instagram.com',
            'bloomberg.com'
        }

    def _is_valid_domain(self, url: str) -> bool:
        """Check if the URL's domain is not in excluded domains."""
        try:
            domain = urlparse(url).netloc.lower()
            return not any(excluded in domain for excluded in self.excluded_domains)
        except:
            return False

    def search_company(self, company_name: str) -> Optional[str]:
        """Search for company website using Serper API."""
        url = "https://api.serper.dev/search"
        payload = {
            "q": f"{company_name} company official website",
            "num": 5  # Increased to have more results to filter through
        }

        response = requests.post(url, json=payload, headers=self.headers)
        if response.status_code != 200:
            return None

        data = response.json()
        organic = data.get("organic", [])

        # Filter and return the first valid domain
        for result in organic:
            link = result.get("link")
            if link and self._is_valid_domain(link):
                return link

        return None