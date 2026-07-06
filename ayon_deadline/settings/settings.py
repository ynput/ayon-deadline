from ayon_core.settings import BaseSettingsModel
from ayon_core.settings.fields import (
    ConfigField,
    DictField,
    EnumField,
    ListField,
    NumberField,
    StringField,
)


class DeadlineJobOverrideModel(BaseSettingsModel):
    """Model for job-specific overrides."""
    priority = NumberField(
        label="Priority",
        default=None
    )
    pool = StringField(
        label="Pool",
        default=""
    )
    pool_secondary = StringField(
        label="Secondary Pool",
        default=""
    )
    # Add other common fields as needed
    chunk_size = NumberField(
        label="Chunk Size",
        default=None
    )
    concurrent_tasks = NumberField(
        label="Concurrent Tasks",
        default=None
    )
    # Machine limits, etc.


class DeadlineProfileModel(BaseSettingsModel):
    """Profile for Deadline job info."""
    family = StringField(
        label="Family",
        description="Family to match (use * for all)",
        default="*"
    )
    job_type = EnumField(
        label="Job Type",
        enum_items=[
            ("render", "Render"),
            ("publish", "Publish"),
            ("cache", "Cache")
        ],
        default="render",
        description="Type of Deadline job"
    )
    overrides = DictField(
        label="Overrides",
        model=DeadlineJobOverrideModel,
        default={}
    )


class DeadlineSettingsModel(BaseSettingsModel):
    """Main settings model for Deadline submitter."""
    deadlines_profiles = ListField(
        DeadlineProfileModel,
        label="Deadline Profiles",
        default=[]
    )


# Note: This settings model would be integrated into the larger AYON settings.
# The actual registration and default values configuration is assumed elsewhere.
