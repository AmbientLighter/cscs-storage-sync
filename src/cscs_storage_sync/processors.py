import logging

from .api_client import StorageProxyClient
from .filesystem import FilesystemDriver
from .models import StorageResource

logger = logging.getLogger(__name__)


class ResourceProcessor:
    def __init__(self, fs: FilesystemDriver, client: StorageProxyClient, config: dict):
        self.fs = fs
        self.client = client
        self.min_gid = config.get("min_gid_allowed", 1000)
        self.archive_dir = config.get("archive_dir", "/tmp/archive")

    def process(self, resource: StorageResource):
        try:
            if resource.status == "pending":
                self._handle_pending(resource)
            elif resource.status == "active":
                self._handle_active(resource)
            elif resource.status == "removing":
                self._handle_removing(resource)
        except Exception as e:
            logger.error(f"Error processing resource {resource.itemId}: {e}")
            if resource.set_state_erred_url:
                self.client.send_callback(resource.set_state_erred_url)

    def _validate_gid(self, resource: StorageResource) -> int:
        if resource.target.targetType == "project":
            gid = resource.target.targetItem.unixGid
            if gid is None or gid < self.min_gid:
                raise ValueError(f"Invalid GID: {gid}")
            return gid
        return 0

    def _handle_pending(self, res: StorageResource):
        """Creation Flow"""
        logger.info(f"PROVISIONING: {res.mountPoint['default']}")

        path = res.mountPoint["default"]

        # 1. Validate Target
        gid = self._validate_gid(res)

        # 2. Create FS structures
        self.fs.ensure_directory(path, gid)

        # 3. Apply Quotas
        if res.quotas:
            self.fs.set_lustre_quota(path, gid, res.quotas)

        # 4. Notify Waldur
        # First approve by provider (if strictly following site agent flow)
        if res.approve_by_provider_url:
            self.client.send_callback(res.approve_by_provider_url)

        # Then set state done
        if res.set_state_done_url:
            self.client.send_callback(res.set_state_done_url)

    def _handle_active(self, res: StorageResource):
        """Update/Drift Check Flow"""
        # Logic here can be optimized to only run if 'UPDATING' or periodically
        path = res.mountPoint["default"]
        gid = self._validate_gid(res)

        # Re-apply quotas (Idempotent operation)
        if res.quotas:
            self.fs.set_lustre_quota(path, gid, res.quotas)

    def _handle_removing(self, res: StorageResource):
        """Deletion Flow"""
        logger.info(f"DEPROVISIONING: {res.mountPoint['default']}")

        path = res.mountPoint["default"]

        # 1. Archive Data
        self.fs.archive_directory(path, self.archive_dir)

        # 2. Notify Waldur (using set_state_done to confirm termination)
        if res.set_state_done_url:
            self.client.send_callback(res.set_state_done_url)
