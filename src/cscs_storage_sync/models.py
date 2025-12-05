from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class QuotaItem(BaseModel):
    type: str
    quota: float
    unit: str
    enforcementType: str


class TargetItem(BaseModel):
    itemId: str
    name: str
    # 'key' is present in Tenant/Customer items
    key: Optional[str] = None
    # 'unixGid' is specific to Project items
    unixGid: Optional[int] = None
    # 'unixUid' is specific to User items
    unixUid: Optional[int] = None


class Target(BaseModel):
    targetType: str  # 'tenant', 'customer', 'project', 'user'
    targetItem: TargetItem


class Permission(BaseModel):
    permissionType: str
    value: str


class StorageEntity(BaseModel):
    """Generic model for storageSystem, storageFileSystem, etc."""

    itemId: str
    key: str
    name: str
    active: bool


class StorageResource(BaseModel):
    itemId: str
    status: str  # 'pending', 'active', 'removing'

    # Maps e.g. {"default": "/path/to/dir"}
    mountPoint: Dict[str, str]

    permission: Optional[Permission] = None
    quotas: Optional[List[QuotaItem]] = None

    target: Target

    storageSystem: StorageEntity
    storageFileSystem: StorageEntity
    storageDataType: StorageEntity

    parentItemId: Optional[str] = None

    # Callback URLs (Optional, usually present on Project items)
    approve_by_provider_url: Optional[str] = None
    set_state_done_url: Optional[str] = None
    set_state_erred_url: Optional[str] = None


class PaginationInfo(BaseModel):
    current: int
    limit: int
    offset: int
    pages: int
    total: int
    api_total: Optional[int] = None


class PaginatedResponse(BaseModel):
    status: str
    # matches "resources": [...]
    resources: List[StorageResource] = Field(default_factory=list)
    pagination: Optional[PaginationInfo] = None
