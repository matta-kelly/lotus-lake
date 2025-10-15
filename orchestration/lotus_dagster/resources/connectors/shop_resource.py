import logging
import requests
import time
import json
import shopify
from dagster import resource
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential, retry_if_exception_type, before_sleep_log
from shared.config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ShopifyClient:
    def __init__(self):
        self.shop_url = settings.SHOP_URL
        self.token = settings.SHOP_TOKEN
        self.api_version = "2025-01"

        if not all([self.shop_url, self.token]):
            raise ValueError("Missing required Shopify credentials in configuration")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        before_sleep=before_sleep_log(logger, logging.INFO),
    )
    def fetch(self, endpoint, params=None):
        """Fetch raw Shopify REST API data with rate limiting and error handling."""
        url = f"https://{self.shop_url}/admin/api/{self.api_version}/{endpoint}.json"
        response = requests.get(url, headers=self.get_headers(), params=params, timeout=30)
        response.raise_for_status()

        # Rate limit protection
        call_limit = response.headers.get('X-Shopify-Shop-Api-Call-Limit', '0/40').split('/')
        calls_made, calls_max = int(call_limit[0]), int(call_limit[1])
        if calls_made > calls_max * 0.8:
            logger.warning("Approaching REST rate limit: %d/%d, sleeping 2s", calls_made, calls_max)
            time.sleep(2)

        return response.json()

    def get_headers(self):
        return {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.token,
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, json.JSONDecodeError, RuntimeError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def execute(self, query, variables=None):
        """Execute a Shopify GraphQL query and return parsed dict with retry logic."""
        session = shopify.Session(self.shop_url, self.api_version, self.token)
        shopify.ShopifyResource.activate_session(session)
        response = shopify.GraphQL().execute(query, variables or {})
        shopify.ShopifyResource.clear_session()

        parsed = response if isinstance(response, dict) else json.loads(response)
        
        # Fail fast on errors so retry logic kicks in
        if "errors" in parsed:
            error_msg = f"GraphQL errors: {parsed['errors']}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        return parsed


def safe_get(data, *keys, default=None):
    """Safely extract nested values from dicts."""
    current = data
    for key in keys:
        try:
            if current is None or key not in current:
                return default
            current = current[key]
        except (TypeError, KeyError, AttributeError):
            return default
    return current if current is not None else default


@resource
def shopify_client_resource(_):
    """
    Dagster resource wrapping ShopifyClient.
    Assets use this via `context.resources.shopify`.
    """
    client = ShopifyClient()
    return client