from typing import Dict, List, Optional

from pydantic import BaseModel


class QuotaItem(BaseModel):
    type: str  # 'space' or 'inodes'
    quota: float
    unit: str
    enforcementType: str  # 'soft' or 'hard'


class TargetItem(BaseModel):
    # Depending on targetType, fields vary.
    # Projects use unixGid, Users use unixUid
    unixGid: Optional[int] = None
    unixUid: Optional[int] = None
    name: str


class Target(BaseModel):
    targetType: str  # 'project', 'user', 'tenant', 'customer'
    targetItem: TargetItem


class StorageResource(BaseModel):
    itemId: str
    status: str  # 'pending', 'active', 'removing', 'removed'
    mountPoint: Dict[str, str]  # e.g. {'default': '/capstor/store/...'}
    quotas: Optional[List[QuotaItem]] = []
    target: Target
    storageSystem: Dict[str, str]

    # Callback URLs provided by Waldur for state transitions
    approve_by_provider_url: Optional[str] = None
    set_state_executing_url: Optional[str] = None
    set_state_done_url: Optional[str] = None
    set_state_erred_url: Optional[str] = None


class PaginatedResponse(BaseModel):
    storageResources: List[StorageResource]
    paginate: Dict[str, int]
