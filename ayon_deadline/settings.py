from ayon_core.lib import EnumDef
from ayon_core.settings import BaseSettingsModel
from pydantic import Field
from typing import Optional, List

class CollectJobInfoProfile(BaseSettingsModel):
    _isGroup = True
    _layout = "expanded"

    # Existing fields...
    job_type: str = Field(
        default="render",
        title="Job Type",
        enum_resolver=lambda: ["render", "publish", "cache"]
    )
    # Other fields like priority, etc. (unchanged)
    priority: Optional[int] = Field(default=50, title="Priority")
    # ... other fields ...

class CollectJobInfoSettings(BaseSettingsModel):
    profiles: List[CollectJobInfoProfile] = Field(default_factory=list, title="Profiles")

# If there's a plugin settings model, include the above.
class DeadlineSettings(BaseSettingsModel):
    collect_job_info: CollectJobInfoSettings = Field(default_factory=CollectJobInfoSettings)
    # ... other settings ...
