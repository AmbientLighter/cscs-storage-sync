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
            elif resource.status == "updating":
                self._handle_updating(resource)
            elif resource.status == "removed":
                # Already handled, just log
                pass
        except Exception as e:
            logger.error(f"Error processing {resource.itemId}: {e}")
            if resource.set_state_erred_url:
                self.client.send_callback(resource.set_state_erred_url)

    def _get_gid_and_mode(self, res: StorageResource):
        """Determines valid GID and Mode for the resource."""
        # Default mode from JSON or fallback
        mode = res.permission.value if res.permission else "775"

        # 1. Project Level (Has specific GID)
        if res.target.targetType == "project":
            gid = res.target.targetItem.unixGid
            if gid is None:
                logger.warning(f"Project resource {res.itemId} missing unixGid. Using 0.")
                return 0, mode
            if gid < self.min_gid:
                logger.warning(f"GID {gid} below min_gid {self.min_gid}. Using 0.")
                return 0, mode
            return gid, mode

        # 2. Tenant/Customer Level (Usually owned by root or system group)
        # Based on example JSON, these have "775" but no unixGid.
        return 0, mode

    def _map_quotas_to_waldur(self, quotas: list) -> dict:
        mapping = {}
        for q in quotas:
            key = None
            if q.type == "space":
                if q.enforcementType == "soft":
                    key = "soft_quota_space"
                elif q.enforcementType == "hard":
                    key = "hard_quota_space"
            elif q.type == "inodes":
                if q.enforcementType == "soft":
                    key = "soft_quota_inodes"
                elif q.enforcementType == "hard":
                    key = "hard_quota_inodes"

            if key:
                mapping[key] = q.quota
        return mapping

    def _handle_pending(self, res: StorageResource):
        path = res.mountPoint.get("default")
        if not path:
            logger.error(f"No mount point for {res.itemId}")
            return

        gid, mode = self._get_gid_and_mode(res)

        logger.info(f"Provisioning {res.target.targetType}: {path} (gid: {gid})")

        # 1. Ensure Directory
        self.fs.ensure_directory(path, gid, mode)

        # 2. Apply Quota (Only if present and valid GID)
        if res.quotas and gid > 0:
            self.fs.set_lustre_quota(path, gid, res.quotas)

        # 3. Callbacks
        if res.approve_by_provider_url:
            self.client.send_callback(res.approve_by_provider_url)
        if res.set_state_done_url:
            # 1. Report quotas via special endpoint
            if res.quotas and res.update_resource_options_url:
                payload = {"options": self._map_quotas_to_waldur(res.quotas)}
                self.client.send_callback(res.update_resource_options_url, data=payload)

            # 2. Set state to done
            self.client.send_callback(res.set_state_done_url)

    def _handle_active(self, res: StorageResource):
        path = res.mountPoint.get("default")
        gid, mode = self._get_gid_and_mode(res)

        # Periodic enforcement
        self.fs.ensure_directory(path, gid, mode)
        if res.quotas and gid > 0:
            self.fs.set_lustre_quota(path, gid, res.quotas)

    def _handle_removing(self, res: StorageResource):
        path = res.mountPoint.get("default")
        if not path:
            return

        logger.info(f"Deprovisioning: {path}")
        self.fs.archive_directory(path, self.archive_dir)

        if res.set_state_done_url:
            self.client.send_callback(res.set_state_done_url)

    def _handle_updating(self, res: StorageResource):
        # Similar to pending, we re-apply state and notify
        path = res.mountPoint.get("default")
        if not path:
            logger.error(f"No mount point for {res.itemId}")
            return

        gid, mode = self._get_gid_and_mode(res)
        logger.info(f"Updating {res.target.targetType}: {path} (gid: {gid})")

        # 1. Ensure Directory (Updates permissions/ownership if needed)
        self.fs.ensure_directory(path, gid, mode)

        # 2. Apply Quota
        if res.quotas and gid > 0:
            self.fs.set_lustre_quota(path, gid, res.quotas)

        # 3. Callbacks
        if res.set_state_done_url:
            # 1. Report quotas via special endpoint
            if res.quotas and res.update_resource_options_url:
                payload = {"options": self._map_quotas_to_waldur(res.quotas)}
                self.client.send_callback(res.update_resource_options_url, data=payload)

            # 2. Set state to done
            self.client.send_callback(res.set_state_done_url)
