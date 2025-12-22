import logging
import os
import time

import yaml

from .api_client import StorageProxyClient
from .filesystem import FilesystemDriver
from .processors import ResourceProcessor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CSCS-Sync")


def load_config(path="config.yaml"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def run_sync_loop():
    config = load_config()

    # Initialize components
    client = StorageProxyClient(
        base_url=config["proxy_url"],
        proxy_token=config["api_token"],
        waldur_token=config["waldur_api_token"],
    )
    fs = FilesystemDriver(
        config["storage_root"],
        dry_run=config.get("dry_run", False),
        debug_mode=config.get("debug_mode", False),
    )
    processor = ResourceProcessor(fs, client, config)

    interval = config.get("sync_interval_seconds", 60)

    logger.info("Starting CSCS Storage Sync Agent")
    logger.info(f"Dry Run: {config.get('dry_run', False)}")
    logger.info(f"Debug Mode: {config.get('debug_mode', False)}")

    while True:
        try:
            logger.info("Polling proxy...")
            resources = client.fetch_all_resources()
            logger.info(f"Fetched {len(resources)} resources.")

            for res in resources:
                processor.process(res)

        except KeyboardInterrupt:
            logger.info("Stopping...")
            break
        except Exception as e:
            logger.error(f"Sync loop error: {e}", exc_info=True)

        time.sleep(interval)


if __name__ == "__main__":
    run_sync_loop()
