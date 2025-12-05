import logging

import requests

from .models import PaginatedResponse, StorageResource

logger = logging.getLogger(__name__)


class StorageProxyClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Token {token}",
            "User-Agent": "CSCS-Infrastructure-Agent/1.0",
        }

    def fetch_all_resources(self, storage_system: str = None) -> list[StorageResource]:
        """Iterates through all pages to fetch full state."""
        results = []
        page = 1

        while True:
            params = {"page": page, "page_size": 100}
            if storage_system:
                params["storage_system"] = storage_system

            try:
                logger.debug(f"Fetching page {page}...")
                resp = requests.get(self.base_url, headers=self.headers, params=params)
                resp.raise_for_status()

                # Unwrap the "result" envelope specific to the Proxy's response format
                data = resp.json()
                if "result" in data:
                    data = data["result"]

                parsed = PaginatedResponse(**data)
                results.extend(parsed.resources)

                if len(results) >= parsed.paginate["total"]:
                    break
                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch resources: {e}")
                break

        return results

    def send_callback(self, url: str):
        """Triggers a lifecycle callback (e.g. set_state_done)."""
        if not url:
            return

        try:
            logger.info(f"Sending callback to Waldur: {url}")
            # Usually these are POST requests. The body might vary based on Waldur version,
            # but often empty POST is sufficient for state transitions.
            resp = requests.post(url, headers=self.headers)
            resp.raise_for_status()
            logger.info("Callback successful.")
        except Exception as e:
            logger.error(f"Callback failed: {e}")
            # Depending on policy, we might want to re-raise to retry later
