from dagster import resource, InitResourceContext
from klaviyo_api import KlaviyoAPI
import logging
import requests

logger = logging.getLogger(__name__)

class KlaviyoClient:
    """
    Wrapper around Klaviyo official SDK with retry logic.
    """
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("KLAVIYO_API_KEY is required")
        
        self.client = KlaviyoAPI(
            api_key=api_key,
            max_retries=3,
            max_delay=60,
        )
        logger.info("Initialized Klaviyo client with official SDK")

    def get_events(self, **params):
        """
        Fetch events via Klaviyo Events endpoint.
        """
        return self.client.Events.get_events(**params)

    def get_campaigns(self, **params):
        """
        Fetch campaigns directly from the new Klaviyo API (/api/campaigns),
        bypassing the SDK entirely while keeping the same return shape.
        Handles pagination URLs correctly.
        """
        base_url = "https://a.klaviyo.com/api/campaigns"
        api_key = self.client.api_key
        headers = {
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "Accept": "application/json",
            "revision": "2025-07-15",
        }

        try:
            # Detect if this is a pagination call (URL passed directly)
            page_cursor = params.pop("page_cursor", None)
            if page_cursor and page_cursor.startswith("http"):
                url = page_cursor
                response = requests.get(url, headers=headers, timeout=30)
            else:
                response = requests.get(base_url, headers=headers, params=params, timeout=30)

            response.raise_for_status()
            payload = response.json()

            class _Shim:
                def __init__(self, data):
                    self.data = data.get("data", [])
                    self.links = type("Links", (), data.get("links", {}))()

            return _Shim(payload)

        except Exception as e:
            logger.error(f"Failed to fetch campaigns from new API: {e}")
            raise


@resource(config_schema={"api_key": str})
def klaviyo_client_resource(context: InitResourceContext) -> KlaviyoClient:
    """
    Dagster resource for Klaviyo API client.
    """
    api_key = context.resource_config["api_key"]
    return KlaviyoClient(api_key)