"""Faire API client - handles auth, session, and base requests.

Environment variables:
    FAIRE_ACCESS_TOKEN: API access token (from Faire brand settings or integrations.support@faire.com)
    FAIRE_BASE_URL: Optional, defaults to https://www.faire.com

API Docs: https://faire.github.io/external-api-v2-docs/
"""
import os
from typing import Any
import requests


class FaireAPI:
    """Faire External API v2 client."""

    def __init__(self):
        self.base_url = os.environ.get("FAIRE_BASE_URL", "https://www.faire.com")
        self.access_token = os.environ["FAIRE_ACCESS_TOKEN"]

        self.session = requests.Session()
        self.session.headers.update({
            "X-FAIRE-ACCESS-TOKEN": self.access_token,
            "Content-Type": "application/json",
        })

    def get(self, endpoint: str, params: dict = None) -> dict:
        """GET request to Faire API endpoint.

        Returns the full JSON response (not just data list) since Faire
        returns pagination info at the top level alongside the data array.

        Args:
            endpoint: API endpoint path (e.g., "external-api/v2/orders")
            params: Query parameters

        Returns:
            Full JSON response dict containing pagination + data
        """
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_orders(
        self,
        updated_at_min: str = None,
        page: int = 1,
        limit: int = 50,
    ) -> dict:
        """Get orders with optional filtering.

        Args:
            updated_at_min: ISO 8601 timestamp for incremental sync (e.g., "2024-01-01T00:00:00.000Z")
            page: Page number for offset pagination (default 1)
            limit: Results per page (default 50, range 10-50)

        Returns:
            Dict with 'orders' array and pagination info ('page', 'limit', 'cursor')
        """
        params = {
            "page": page,
            "limit": limit,
        }
        if updated_at_min:
            params["updated_at_min"] = updated_at_min

        return self.get("external-api/v2/orders", params=params)

    def test_connection(self) -> dict:
        """Test API connection by fetching brand profile.

        Returns:
            Brand profile dict if successful
        """
        return self.get("external-api/v2/brands/profile")
