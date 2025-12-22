import logging
from typing import List

import requests

from .models import PaginatedResponse, StorageResource

logger = logging.getLogger(__name__)


class StorageProxyClient:
    def __init__(self, base_url: str, proxy_token: str, waldur_token: str):
        self.base_url = base_url
        self.proxy_headers = {
            "Authorization": f"Bearer {proxy_token}",
            "User-Agent": "CSCS-Storage-Sync/0.1.0",
            "Content-Type": "application/json",
        }
        self.waldur_headers = {
            "Authorization": f"Token {waldur_token}",
            "User-Agent": "CSCS-Storage-Sync/0.1.0",
            "Content-Type": "application/json",
        }

    def fetch_all_resources(self, storage_system: str = None) -> List[StorageResource]:
        all_resources = []
        page = 1
        page_size = 100

        while True:
            params = {"page": page, "page_size": page_size}
            if storage_system:
                params["storage_system"] = storage_system

            try:
                logger.debug(f"Fetching page {page}...")
                resp = requests.get(self.base_url, headers=self.proxy_headers, params=params)
                resp.raise_for_status()

                data = resp.json()
                parsed = PaginatedResponse(**data)

                all_resources.extend(parsed.resources)

                # Check pagination
                if not parsed.pagination:
                    break

                total_items = parsed.pagination.total
                if len(all_resources) >= total_items:
                    break

                if parsed.resources:
                    page += 1
                else:
                    break

            except requests.RequestException as e:
                logger.error(f"API request failed: {e}")
                break
            except Exception as e:
                logger.error(f"Parsing error: {e}")
                break

        return all_resources

    def send_callback(self, url: str, data: dict = None):
        if not url:
            return
        try:
            logger.info(f"Callback: {url} | Data: {data}")
            resp = requests.post(url, headers=self.waldur_headers, json=data)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send callback: {e}")
