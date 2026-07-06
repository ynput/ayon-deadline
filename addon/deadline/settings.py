from ayon_core.settings import BaseSettingsModel
from ayon_core.lib import EnumDef


class JobTypeEnum(BaseSettingsModel):
    _enum_name = "job_type"
    _enum_items = [
        ("render", "Render"),
        ("publish", "Publish"),
        ("cache", "Cache")
    ]


class DeadlineSettings(BaseSettingsModel):
    job_types: list[JobTypeEnum] = Field(
        default_factory=lambda: [
            "render",
            "publish",
            "cache"
        ],
        title="Job Types"
    )
