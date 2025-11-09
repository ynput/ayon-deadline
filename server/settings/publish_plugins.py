from pydantic import validator

from ayon_server.settings import (
    BaseSettingsModel,
    SettingsField,
    ensure_unique_names,
    task_types_enum,
)


class LimitGroupsSubmodel(BaseSettingsModel):
    _layout = "compact"
    name: str = SettingsField(title="Group Name")
    value: list[str] = SettingsField(
        default_factory=list,
        title="Node Classes"
    )


class EnvSearchReplaceSubmodel(BaseSettingsModel):
    _layout = "compact"
    name: str = SettingsField(title="Name")
    value: str = SettingsField(title="Value")


class CollectAYONServerToFarmJobModel(BaseSettingsModel):
    enabled: bool = SettingsField(False, title="Enabled")


def extract_jobinfo_overrides_enum():
    """Enum of fields that could be overridden by artist in Publisher UI"""
    return [
        {"value": "department", "label": "Department"},
        {"value": "job_delay", "label": "Delay job (timecode dd:hh:mm:ss)"},
        {"value": "chunk_size", "label": "Frames per Task"},
        {"value": "group", "label": "Group"},
        {"value": "priority", "label": "Priority"},
        {"value": "limit_groups", "label": "Limit groups"},
        {"value": "primary_pool", "label": "Primary pool"},
        {"value": "secondary_pool", "label": "Secondary pool"},
        {"value": "machine_list", "label": "Machine List"},
        {"value": "machine_list_deny", "label": "Machine List is a Deny"},
        {"value": "concurrent_tasks", "label": "Number of Concurrent Tasks"},
        {"value": "publish_job_state", "label": "Publish Job State"},
    ]


def publish_job_state_enum():
    """Enum for initial state of publish job"""
    return [
        {"value": "active", "label": "Active"},
        {"value": "suspended", "label": "Suspended"},
    ]


class CollectJobInfoItem(BaseSettingsModel):
    _layout = "expanded"
    host_names: list[str] = SettingsField(
        default_factory=list,
        title="Host names"
    )
    task_types: list[str] = SettingsField(
        default_factory=list,
        title="Task types",
        enum_resolver=task_types_enum
    )
    task_names: list[str] = SettingsField(
        default_factory=list,
        title="Task names"
    )

    #########################################

    chunk_size: int = SettingsField(999, title="Frames per Task")
    priority: int = SettingsField(50, title="Priority")
    group: str = SettingsField("", title="Group")
    limit_groups: list[str] = SettingsField(
        default_factory=list,
        title="Limit Groups"
    )
    primary_pool: str = SettingsField("", title="Primary Pool")
    secondary_pool: str = SettingsField("", title="Secondary Pool")
    machine_limit: int = SettingsField(
        0,
        title="Machine Limit",
        description=(
            "Specifies the maximum number of machines this job can be"
            " rendered on at the same time (default = 0, which means"
            " unlimited)."
        )
    )
    machine_list: list[str] = SettingsField(
        default_factory=list,
        title="Machine List",
        description=(
            "List of workers where submission can/cannot run "
            "based on Machine Allow/Deny toggle."
        )
    )
    machine_list_deny: bool = SettingsField(
        False, title="Machine List is a Deny",
        description=(
            "Explicitly DENY list of machines to render. Without it "
            "it will ONLY ALLOW machines from list."
        )
    )
    concurrent_tasks: int = SettingsField(
        1,
        title="Number of concurrent tasks",
        description="Concurrent tasks on single render node"
    )
    department: str = SettingsField("", title="Department")
    job_delay: str = SettingsField(
        "", title="Delay job",
        placeholder="dd:hh:mm:ss"
    )
    publish_job_state : str = SettingsField(
        "active",
        enum_resolver=publish_job_state_enum,
        title="Publish Job State",
        description="Publish job could wait to be manually enabled from "
                    "Suspended state after quality check"
    )
    use_published: bool = SettingsField(True, title="Use Published scene")
    use_asset_dependencies: bool = SettingsField(
        True, title="Use Asset dependencies")
    use_workfile_dependency: bool = SettingsField(
        True, title="Workfile Dependency")

    additional_job_info: str = SettingsField(
        "",
        title="Additional JobInfo data",
        widget="textarea",
        description=
            "Dictionary (JSON parsable) to paste unto JobInfo of submission"
    )
    additional_plugin_info: str = SettingsField(
        "",
        title="Additional PluginInfo data",
        widget="textarea",
        description=
            "Dictionary (JSON parsable) to paste unto PluginInfo "
            "of submission"
    )
    overrides: list[str] = SettingsField(
        "",
        enum_resolver=extract_jobinfo_overrides_enum,
        title="Exposed Overrides",
        description=(
            "Expose the attribute in this list to the user when publishing."
        )
    )


class CollectJobInfoModel(BaseSettingsModel):
    _isGroup = True
    profiles: list[CollectJobInfoItem] = SettingsField(default_factory=list)


class ValidateExpectedFilesModel(BaseSettingsModel):
    """Validate render frames match the job's expected outputs."""
    enabled: bool = SettingsField(True, title="Enabled")
    active: bool = SettingsField(True, title="Active")
    allow_user_override: bool = SettingsField(
        True, title="Allow user change frame range",
        description=(
            "Allow user to override the frame range of the job in Deadline "
            "Monitor and use this as the new expected files. "
            "This is useful when artist should be allowed control on the "
            "render frame range."
        )
    )
    families: list[str] = SettingsField(
        default_factory=list, title="Trigger on families"
    )
    targets: list[str] = SettingsField(
        default_factory=list, title="Trigger for plugins"
    )


def tile_assembler_enum():
    """Return a list of value/label dicts for the enumerator.

    Returning a list of dicts is used to allow for a custom label to be
    displayed in the UI.
    """
    return [
        {
            "value": "DraftTileAssembler",
            "label": "Draft Tile Assembler"
        }
    ]


class ScenePatchesSubmodel(BaseSettingsModel):
    _layout = "expanded"
    name: str = SettingsField(title="Patch name")
    regex: str = SettingsField(title="Patch regex")
    line: str = SettingsField(title="Patch line")


class MayaSubmitDeadlineModel(BaseSettingsModel):
    """Maya-specific settings"""

    import_reference: bool = SettingsField(
        title="Use Scene with Imported Reference"
    )
    tile_priority: int = SettingsField(title="Tile Priority")

    tile_assembler_plugin: str = SettingsField(
        title="Tile Assembler Plugin",
        enum_resolver=tile_assembler_enum,
    )

    scene_patches: list[ScenePatchesSubmodel] = SettingsField(
        default_factory=list,
        title="Scene patches",
    )
    strict_error_checking: bool = SettingsField(
        title="Disable Strict Error Check profiles"
    )

    @validator("scene_patches")
    def validate_unique_names(cls, value):
        ensure_unique_names(value)
        return value


def fusion_deadline_plugin_enum():
    """Return a list of value/label dicts for the enumerator.

    Returning a list of dicts is used to allow for a custom label to be
    displayed in the UI.
    """
    return [
        {
            "value": "Fusion",
            "label": "Fusion"
        },
        {
            "value": "FusionCmd",
            "label": "FusionCmd"
        }
    ]


class FusionSubmitDeadlineModel(BaseSettingsModel):
    """Fusion-specific settings"""
    plugin: str = SettingsField("Fusion",
                                enum_resolver=fusion_deadline_plugin_enum,
                                title="Deadline Plugin")


class NukeSubmitDeadlineModel(BaseSettingsModel):
    """Nuke-specific settings"""

    use_gpu: bool = SettingsField(True, title="Use GPU")
    node_class_limit_groups: list[LimitGroupsSubmodel] = SettingsField(
        default_factory=list,
        title="Node based Limit Groups",
        description=
            "Provide list of Nuke node classes to get particular limit group. "
            "Example: 'OFX.absoft.neatvideo5_v5'"
    )


class HoudiniSubmitDeadlineModel(BaseSettingsModel):
    """Houdini Export Job settings

    Submitting from Houdini can be configured to first export a renderable
    scene file (e.g. `usd`, `ifd`, `ass`) instead of rendering directly from
    the Houdini file. These settings apply to this Houdini **Export Job**.
    """

    export_priority: int = SettingsField(title="Export Priority")
    export_chunk_size: int = SettingsField(title="Export Frames Per Task")
    export_group: str = SettingsField(title="Export Group")
    export_limits: str = SettingsField(
        title="Export Limit Groups",
        description=(
            "Enter a comma separated list of limits.\n"
            "Specifies the limit groups that this job is a member"
            " of (default = blank)."
        )
    )
    export_machine_limit: int = SettingsField(
        title="Export Machine Limit",
        description=(
            "Specifies the maximum number of machines this job can be"
            " rendered on at the same time (default = 0, which means"
            " unlimited)."
        )
    )


class ProcessCacheJobFarmModel(BaseSettingsModel):
    """Houdini cache submission settings

    These settings apply only to Houdini cache publish jobs. Those are the
    **publish jobs** for any farm submitted caching, like for Alembic
    or VDB products.
    """

    deadline_priority: int = SettingsField(title="Priority")
    deadline_group: str = SettingsField(title="Group")
    deadline_pool: str = SettingsField(title="Pool")
    deadline_department: str = SettingsField(title="Department")


class AOVFilterSubmodel(BaseSettingsModel):
    _layout = "expanded"
    name: str = SettingsField(title="Host")
    value: list[str] = SettingsField(
        default_factory=list,
        title="AOV regex"
    )


class ProcessSubmittedJobOnFarmModel(BaseSettingsModel):
    """Publish job settings"""

    deadline_priority: int = SettingsField(title="Priority")
    deadline_group: str = SettingsField(title="Group")
    deadline_pool: str = SettingsField(title="Pool")
    deadline_department: str = SettingsField(title="Department")
    skip_integration_repre_list: list[str] = SettingsField(
        default_factory=list,
        title="Skip integration of representation with ext"
    )
    families_transfer: list[str] = SettingsField(
        default_factory=list,
        title=(
            "List of family names to transfer\n"
            "to generated instances (AOVs for example)."
        )
    )
    aov_filter: list[AOVFilterSubmodel] = SettingsField(
        default_factory=list,
        title="Reviewable products filter",
    )

    add_rendered_dependencies: bool = SettingsField(
        False,
        title="Add rendered files as Dependencies",
        description="Add all expected rendered files as job Dependencies."
                    "Publish job won't trigger until all files are present."
    )

    @validator("aov_filter")
    def validate_unique_names(cls, value):
        ensure_unique_names(value)
        return value


class PublishPluginsModel(BaseSettingsModel):
    # Generic submission settings applying to all hosts
    CollectJobInfo: CollectJobInfoModel = SettingsField(
        default_factory=CollectJobInfoModel,
        title="Render Job Settings",
        description=(
            "Define defaults for Deadline job properties like Pools, Groups "
            "etc. It allows context-aware control based on Profiles (eg. "
            "different task types might use different Pools etc.)"
        )
    )
    ProcessSubmittedJobOnFarm: ProcessSubmittedJobOnFarmModel = SettingsField(
        default_factory=ProcessSubmittedJobOnFarmModel,
        title="Publish Job Settings")

    # Host-specific
    FusionSubmitDeadline: FusionSubmitDeadlineModel = SettingsField(
        default_factory=FusionSubmitDeadlineModel,
        title="Fusion",
        section="Host specific")
    HoudiniSubmitDeadline: HoudiniSubmitDeadlineModel = SettingsField(
        default_factory=HoudiniSubmitDeadlineModel,
        title="Houdini Export Job Settings")
    ProcessSubmittedCacheJobOnFarm: ProcessCacheJobFarmModel = SettingsField(
        default_factory=ProcessCacheJobFarmModel,
        title="Houdini Cache Publish Job Settings")
    MayaSubmitDeadline: MayaSubmitDeadlineModel = SettingsField(
        default_factory=MayaSubmitDeadlineModel,
        title="Maya")
    NukeSubmitDeadline: NukeSubmitDeadlineModel = SettingsField(
        default_factory=NukeSubmitDeadlineModel,
        title="Nuke")

    # Others
    CollectAYONServerToFarmJob: CollectAYONServerToFarmJobModel = SettingsField(  # noqa
        default_factory=CollectAYONServerToFarmJobModel,
        title="Add AYON server to farm job",
        description=(
            "When enabled submit along your `AYON_SERVER_URL` to the farm job."
            " On the Deadline AYON Plug-in on the Deadline Repository settings"
            " you can specify a custom API key for those server URLs."
        ),
        section="Others"
    )
    ValidateExpectedFiles: ValidateExpectedFilesModel = SettingsField(
        default_factory=ValidateExpectedFilesModel,
        title="Validate Expected Files"
    )


DEFAULT_DEADLINE_PLUGINS_SETTINGS = {
    "CollectJobInfo": {
      "profiles": [
        {
          "group": "",
          "priority": 50,
          "job_delay": "",
          "publish_job_state": "active",
          "overrides": [
            "department",
            "chunk_size",
            "group",
            "priority",
            "primary_pool",
            "secondary_pool",
            "publish_job_state"
          ],
          "chunk_size": 1,
          "department": "",
          "host_names": [],
          "task_names": [],
          "task_types": [],
          "limit_groups": [],
          "machine_list": [],
          "primary_pool": "",
          "machine_limit": 0,
          "use_published": True,
          "secondary_pool": "",
          "concurrent_tasks": 1,
          "machine_list_deny": False,
          "additional_job_info": "",
          "additional_plugin_info": "",
          "use_asset_dependencies": False,
          "use_workfile_dependency": True
        },
        {
          "group": "",
          "priority": 50,
          "job_delay": "",
          "publish_job_state": "active",
          "overrides": [
            "department",
            "chunk_size",
            "group",
            "priority",
            "primary_pool",
            "secondary_pool",
            "publish_job_state"
          ],
          "chunk_size": 10,
          "department": "",
          "host_names": [
            "nuke",
            "fusion",
            "aftereffects"
          ],
          "task_names": [],
          "task_types": [],
          "limit_groups": [],
          "machine_list": [],
          "primary_pool": "",
          "machine_limit": 0,
          "use_published": True,
          "secondary_pool": "",
          "concurrent_tasks": 1,
          "machine_list_deny": False,
          "additional_job_info": "",
          "additional_plugin_info": "",
          "use_asset_dependencies": False,
          "use_workfile_dependency": True
        }
      ]
    },
    "CollectAYONServerToFarmJob": {
        "enabled": False
    },
    "ValidateExpectedFiles": {
        "enabled": True,
        "active": True,
        "allow_user_override": True,
        "families": [
            "render"
        ],
        "targets": [
            "deadline"
        ]
    },
    "FusionSubmitDeadline": {
        "plugin": "Fusion"
    },
    "HoudiniSubmitDeadline": {
        "export_priority": 50,
        "export_chunk_size": 10,
        "export_group": "",
        "export_limits": "",
        "export_machine_limit": 0
    },
    "MayaSubmitDeadline": {
        "tile_assembler_plugin": "DraftTileAssembler",
        "import_reference": False,
        "strict_error_checking": True,
        "tile_priority": 50,
        "scene_patches": []
    },
    "NukeSubmitDeadline": {
        "use_gpu": True
    },
    "ProcessSubmittedCacheJobOnFarm": {
        "deadline_priority": 50,
        "deadline_group": "",
        "deadline_pool": "",
        "deadline_department": "",
    },
    "ProcessSubmittedJobOnFarm": {
        "deadline_priority": 50,
        "deadline_group": "",
        "deadline_pool": "",
        "deadline_department": "",
        "skip_integration_repre_list": [],
        "families_transfer": ["render3d", "render2d", "slate"],
        "aov_filter": [
            {
                "name": "maya",
                "value": [
                    ".*([Bb]eauty).*"
                ]
            },
            {
                "name": "blender",
                "value": [
                    ".*([Bb]eauty).*"
                ]
            },
            {
                "name": "aftereffects",
                "value": [
                    ".*"
                ]
            },
            {
                "name": "celaction",
                "value": [
                    ".*"
                ]
            },
            {
                "name": "harmony",
                "value": [
                    ".*"
                ]
            },
            {
                "name": "max",
                "value": [
                    ".*"
                ]
            },
            {
                "name": "fusion",
                "value": [
                    ".*"
                ]
            }
        ],
        "add_rendered_dependencies": False
    }
}
