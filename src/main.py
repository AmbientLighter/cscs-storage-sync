import logging
import time

import yaml

from .api_client import StorageProxyClient
from .filesystem import FilesystemDriver
from .processors import ResourceProcessor

# Setup Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CSCS-Sync")


def load_config(path="config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def run_sync_loop():
    config = load_config()

    client = StorageProxyClient(config["proxy_url"], config["api_token"])
    fs = FilesystemDriver(config["storage_root"], dry_run=config["dry_run"])
    processor = ResourceProcessor(fs, client, config)

    interval = config.get("sync_interval_seconds", 60)

    logger.info("Starting Sync Agent...")

    while True:
        try:
            logger.info("Starting polling cycle...")

            resources = client.fetch_all_resources()

            logger.info(f"Retrieved {len(resources)} resources from Proxy.")

            for res in resources:
                processor.process(res)

            logger.info("Cycle complete.")

        except Exception as e:
            logger.error(f"Critical error in sync loop: {e}", exc_info=True)

        time.sleep(interval)


if __name__ == "__main__":
    run_sync_loop()
