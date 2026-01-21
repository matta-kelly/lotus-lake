"""Odoo API client - handles auth, session, and base requests."""
import os
import requests


class OdooAPI:
    def __init__(self):
        self.base_url = os.environ["ODOO_URL"]
        self.db = os.environ["ODOO_DB"]
        self.username = os.environ["ODOO_USER"]
        self.password = os.environ["ODOO_PASSWORD"]

        self.session = requests.Session()
        self.uid = None
        self._authenticate()

    def _authenticate(self):
        url = f"{self.base_url}/web/session/authenticate"
        payload = {
            "jsonrpc": "2.0",
            "params": {
                "db": self.db,
                "login": self.username,
                "password": self.password,
            },
        }
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        result = response.json().get("result")
        if not result or not result.get("uid"):
            raise ValueError("Odoo authentication failed")
        self.uid = result["uid"]

    def get(self, endpoint: str, params: dict = None) -> list:
        """GET request to Odoo API endpoint. Returns data list."""
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != "success":
            raise ValueError(f"Odoo API error: {data.get('message')}")
        return data["data"]
