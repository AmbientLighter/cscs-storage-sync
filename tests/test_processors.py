import unittest
from unittest.mock import MagicMock, call

from cscs_storage_sync.models import StorageResource, Target, TargetItem, QuotaItem
from cscs_storage_sync.processors import ResourceProcessor


class TestResourceProcessor(unittest.TestCase):
    def setUp(self):
        self.mock_fs = MagicMock()
        self.mock_client = MagicMock()
        self.config = {"min_gid_allowed": 1000}
        self.processor = ResourceProcessor(self.mock_fs, self.mock_client, self.config)

    def test_handle_pending_success(self):
        resource = StorageResource(
            itemId="res-1",
            status="pending",
            mountPoint={"default": "/mnt/test"},
            target=Target(
                targetType="project", targetItem=TargetItem(itemId="t-1", name="proj", unixGid=2000)
            ),
            quotas=[
                QuotaItem(type="space", quota=100, unit="GB", enforcementType="hard"),
                QuotaItem(type="space", quota=90, unit="GB", enforcementType="soft"),
            ],
            approve_by_provider_url="http://approve",
            update_resource_options_url="http://update_opts",
            set_state_done_url="http://done",
            set_backend_id_url="http://backend_id",
            storageSystem={"itemId": "s-1", "key": "sys", "name": "Sys", "active": True},
            storageFileSystem={"itemId": "fs-1", "key": "fs", "name": "FS", "active": True},
            storageDataType={"itemId": "dt-1", "key": "dt", "name": "DT", "active": True},
        )

        self.processor.process(resource)

        # check approve called first
        self.mock_client.send_callback.assert_any_call("http://approve")

        # check fs ops
        self.mock_fs.ensure_directory.assert_called_with("/mnt/test", 2000, "775")
        self.mock_fs.set_lustre_quota.assert_called()

        # check quota mapping
        expected_quota_payload = {"options": {"hard_quota_space": 100}}
        self.mock_client.send_callback.assert_any_call(
            "http://update_opts", data=expected_quota_payload
        )

        # check backend id set
        self.mock_client.send_callback.assert_any_call(
            "http://backend_id", data={"backend_id": "/mnt/test"}
        )

        # check done called last (implicit order in calls needed? assert_has_calls checks order)
        # Verify order: Approve -> [FS] -> Report Options/BackendID -> Done
        calls = self.mock_client.send_callback.call_args_list
        self.assertEqual(calls[0], call("http://approve"))
        # intermediate calls...
        self.assertEqual(calls[-1], call("http://done"))

    def test_handle_updating(self):
        resource = StorageResource(
            itemId="res-1",
            status="updating",
            mountPoint={"default": "/mnt/test"},
            target=Target(
                targetType="project", targetItem=TargetItem(itemId="t-1", name="proj", unixGid=2000)
            ),
            quotas=[
                QuotaItem(type="space", quota=200, unit="GB", enforcementType="hard"),
            ],
            update_resource_options_url="http://update_opts",
            set_state_done_url="http://done",
            storageSystem={"itemId": "s-1", "key": "sys", "name": "Sys", "active": True},
            storageFileSystem={"itemId": "fs-1", "key": "fs", "name": "FS", "active": True},
            storageDataType={"itemId": "dt-1", "key": "dt", "name": "DT", "active": True},
        )

        self.processor.process(resource)

        self.mock_fs.ensure_directory.assert_called()
        self.mock_client.send_callback.assert_any_call(
            "http://update_opts", data={"options": {"hard_quota_space": 200}}
        )
        self.mock_client.send_callback.assert_any_call("http://done")
