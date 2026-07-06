from ayon_core.settings import BaseSettingsModel
from ayon_core.lib import EnumDef


class JobInfoProfile(BaseSettingsModel):
    _layout = "compact"
    name = StringField("Name", default="")
    job_type = EnumDef(
        {
            "render": "Render",
            "publish": "Publish",
            "cache": "Cache"
        },
        default="render",
        label="Job Type",
        description="Type of Deadline job this profile applies to."
    )
    # Common fields (prefixed later)
    priority = IntegerField("Priority", default=50)
    pool = StringField("Pool", default="")
    group = StringField("Group", default="")
    department = StringField("Department", default="")
    # Add more fields as needed


class DeadlineSettings(BaseSettingsModel):
    collect_job_info_profiles = ListField(
        JobInfoProfile,
        default=[],
        label="Collect Job Info Profiles"
    )
