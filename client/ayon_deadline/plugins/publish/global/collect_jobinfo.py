# -*- coding: utf-8 -*-
import json
from datetime import datetime
from typing import Optional

import pyblish.api
from ayon_core.lib import (
    BoolDef,
    NumberDef,
    EnumDef,
    TextDef,
    UISeparatorDef
)
from ayon_core.pipeline import KnownPublishError
from ayon_core.pipeline.publish import (
    AYONPyblishPluginMixin,
    PublishError
)
from ayon_core.lib.profiles_filtering import filter_profiles
from ayon_core.addon import AddonsManager

from ayon_deadline.lib import (
    FARM_FAMILIES,
    PublishDeadlineJobInfo,
    DeadlineWebserviceError,
)


class CollectJobInfo(pyblish.api.InstancePlugin, AYONPyblishPluginMixin):
    """Collect variables that belong to Deadline's JobInfo.

    Variables like:
    - department
    - priority
    - chunk size

    """

    order = pyblish.api.CollectorOrder + 0.420
    label = "Collect Deadline JobInfo"

    families = FARM_FAMILIES
    targets = ["local"]

    profiles = []
    pool_enum_values = []
    group_enum_values = []
    limit_group_enum_values = []
    machines_enum_values = []

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Should not be processed on farm, skipping.")
            return

        attr_values = self._get_jobinfo_defaults(instance)
        if not attr_values:
            raise PublishError(
                "No profile selected for defaults. Ask Admin to "
                "fill generic profiles at "
                "ayon+settings://deadline/publish/CollectJobInfo/profiles"
            )

        attr_values.update(self.get_attr_values_from_data(instance.data))
        job_info = PublishDeadlineJobInfo.from_attribute_values(attr_values)

        self._handle_machine_list(attr_values, job_info)
        self._handle_job_delay(attr_values, job_info)
        self._handle_custom_frames(attr_values, job_info)

        self._handle_additional_jobinfo(attr_values, job_info)

        # pass through explicitly key and values for PluginInfo
        plugin_info_data = None
        if attr_values["additional_plugin_info"]:
            plugin_info_data = json.loads(
                attr_values["additional_plugin_info"]
            )

        deadline_info = instance.data["deadline"]
        deadline_info["job_info"] = job_info
        deadline_info["plugin_info_data"] = plugin_info_data

        self._add_deadline_families(instance)

    def _add_deadline_families(self, instance):
        """Add deadline specific families to instance.

        Add 'deadline' to all instances and 'deadline.submit.publish.job'
            to instances that should create publish job.

        """
        instance_families = instance.data.setdefault("families", [])
        all_families = set(instance_families)
        all_families.add(instance.data["family"])

        # Add deadline family
        if "deadline" not in instance_families:
            instance_families.append("deadline")

        # 'publish.hou' has different submit job plugin
        # TODO find out if we need separate submit publish job plugin
        if (
            "publish.hou" in all_families
            or "remote_publish_on_farm" in all_families
        ):
            return

        # Add submit publish job family
        if "deadline.submit.publish.job" not in instance_families:
            instance_families.append("deadline.submit.publish.job")

    def _handle_additional_jobinfo(self,attr_values, job_info):
        """Adds not explicitly implemented fields by values from Settings."""
        additional_job_info = attr_values["additional_job_info"]
        if not additional_job_info:
            return
        for key, value in json.loads(additional_job_info).items():
            setattr(job_info, key, value)

    def _handle_machine_list(self, attr_values, job_info):
        machine_list = attr_values["machine_list"]
        if machine_list:
            if attr_values["machine_list_deny"]:
                job_info.Blacklist = machine_list
                job_info.Whitelist = None
            else:
                job_info.Whitelist = machine_list
                job_info.Blacklist = None

    def _handle_job_delay(self, attr_values, job_info):
        job_delay = attr_values["job_delay"]
        if not job_delay:
            return
        try:
            parts = job_delay.split(':')
            if len(parts) != 4:
                raise ValueError("Invalid format: requires dd:hh:mm:ss")

            _days = int(parts[0])
            hours = int(parts[1])
            minutes = int(parts[2])
            seconds = int(parts[3])

            formatted_time_string = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            _ = datetime.strptime(formatted_time_string, "%H:%M:%S").time()
            job_info.JobDelay = job_delay
            job_info.ScheduledType = "Once"
        except ValueError:
            self.log.warning(
                f"Job delay '{job_delay}' doesn't match to "
                "'dd:hh:mm:ss' format"
            )
            job_info.JobDelay = None

    def _handle_custom_frames(self, attr_values, job_info):
        """Fill JobInfo.Frames only if dropdown says so."""
        job_info.Frames = None
        job_info.reuse_last_version = False
        use_custom_frames = self._is_custom_frames_used(
            attr_values.get("use_custom_frames")
        )
        if use_custom_frames:
            if not attr_values["frames"]:
                raise KnownPublishError("Please fill `Custom Frames` value")
            job_info.Frames = attr_values["frames"]
            if attr_values["use_custom_frames"] == "reuse_last_version":
                job_info.reuse_last_version = True

    @classmethod
    def apply_settings(cls, project_settings):
        settings = project_settings["deadline"]
        profiles = settings["publish"][cls.__name__]["profiles"]

        cls.profiles = profiles or []

        addons_manager = AddonsManager(project_settings)
        deadline_addon = addons_manager["deadline"]
        deadline_server_name = settings["deadline_server"]
        pools = []
        groups = []
        limit_groups = []
        machines = []
        try:
            server_info = deadline_addon.get_server_info_by_name(
                deadline_server_name
            )
            pools = [
                {"value": pool, "label": pool}
                for pool in server_info.pools
            ]
            # Groups always includes the default 'none' group
            groups = [
                {"value": group, "label": group}
                for group in server_info.groups
            ]
            limit_groups = [
                {"value": limit_group, "label": limit_group}
                for limit_group in server_info.limit_groups
            ]
            machines = [
                {"value": machine, "label": machine}
                for machine in server_info.machines
            ]
        except DeadlineWebserviceError:
            cls.log.warning(f"Unable to connect to {deadline_server_name}")

        for items in [
            pools, groups, limit_groups, machines
        ]:
            if not items:
                items.append({"value": None, "label": "< none >"})

        cls.pool_enum_values = pools
        cls.group_enum_values = groups
        cls.limit_group_enum_values = limit_groups
        cls.machines_enum_values = machines

    @classmethod
    def get_attr_defs_for_instance(cls, create_context, instance):
        """Get list of attr defs that are set in Settings as artist overridable

        Args:
            create_context (ayon_core.pipeline.create.CreateContext)
            instance (ayon_core.pipeline.create.CreatedInstance):

        Returns:
            (list)
        """
        if not cls.instance_matches_plugin_families(instance):
            return []

        host_name = create_context.host_name

        task_name = instance["task"]
        folder_path = instance["folderPath"]
        task_entity = create_context.get_task_entity(folder_path, task_name)

        task_name = task_type = None
        if task_entity:
            task_name = task_entity["name"]
            task_type = task_entity["taskType"]
        profile = filter_profiles(
            cls.profiles,
            {
                "host_names": host_name,
                "task_types": task_type,
                "task_names": task_name,
            }
        )
        if not profile:
            return []
        overrides = set(profile["overrides"])
        if not overrides:
            return []

        defs = [
            UISeparatorDef("deadline_defs_starts"),
        ]

        defs.extend(cls._get_artist_overrides(overrides, profile))

        use_custom_frames = (
            cls._get_publish_use_custom_frames_value(instance.data) or "none"
        )

        # explicit frames to render - for test renders
        use_custom_frames_enum_values = [
            {"value": "none", "label": "Disabled"},
            {"value": "custom_only", "label": "Custom Frames Only"},
            {"value": "reuse_last_version", "label": "Reuse from Last Version"}
        ]
        defs.append(
            EnumDef(
                "use_custom_frames",
                label="Use Custom Frames",
                default=use_custom_frames,
                items=use_custom_frames_enum_values,
            )
        )
        custom_frames_visible = cls._is_custom_frames_used(use_custom_frames)
        defs.append(
            TextDef(
                "frames",
                label="Custom Frames",
                default="",
                tooltip="Explicit frames to be rendered. (1001,1003-1004)(2x)",
                visible=custom_frames_visible
            )
        )

        defs.append(
            UISeparatorDef("deadline_defs_end")
        )

        return defs

    @classmethod
    def _get_artist_overrides(cls, overrides, profile):
        """Provide list of all possible Defs that could be filled by artist"""
        # should be matching to extract_jobinfo_overrides_enum
        default_values = {}
        for key in overrides:
            default_value = profile[key]
            if key == "machine_limit":
                available_values = {
                    item["value"]
                    for item in cls.machines_enum_values
                }
                default_value = [
                    value
                    for value in default_value
                    if value in available_values
                ]
            elif key == "limit_groups":
                available_values = {
                    item["value"]
                    for item in cls.limit_group_enum_values
                }
                default_value = [
                    value
                    for value in default_value
                    if value in available_values
                ]
            elif key == "group":
                available_values = [
                    item["value"]
                    for item in cls.group_enum_values
                ]
                if not available_values:
                    default_value = None
                elif default_value not in available_values:
                    default_value = available_values[0]
            default_values[key] = default_value

        attr_defs = [
            NumberDef(
                "chunk_size",
                label="Frames Per Task",
                default=default_values.get("chunk_size"),
                decimals=0,
                minimum=1,
                maximum=1000
            ),
            NumberDef(
                "concurrent_tasks",
                label="Concurrent Tasks",
                tooltip="Number of concurrent tasks to run per render node",
                default=default_values.get("concurrent_tasks"),
                decimals=0,
                minimum=1,
                maximum=1000
            ),
            NumberDef(
                "priority",
                label="Priority",
                default=default_values.get("priority"),
                decimals=0
            ),
            TextDef(
                "department",
                label="Department",
                default=default_values.get("department")
            ),
            EnumDef(
                "group",
                label="Group",
                default=default_values.get("group"),
                items=cls.group_enum_values,
            ),
            EnumDef(
                "limit_groups",
                label="Limit Groups",
                multiselection=True,
                default=default_values.get("limit_groups"),
                items=cls.limit_group_enum_values,
            ),
            EnumDef(
                "primary_pool",
                label="Primary pool",
                default=default_values.get("primary_pool", "none"),
                items=cls.pool_enum_values,
            ),
            EnumDef(
                "secondary_pool",
                label="Secondary pool",
                default=default_values.get("secondary_pool", "none"),
                items=cls.pool_enum_values,
            ),
            EnumDef(
                "machine_list",
                label="Machine list",
                multiselection=True,
                default=default_values.get("machine_list"),
                items=cls.machines_enum_values,
            ),
            BoolDef(
                "machine_list_deny",
                label="Machine List is a Deny",
                default=default_values.get("machine_list_deny")
            ),
            TextDef(
                "job_delay",
                label="Delay job",
                default=default_values.get("job_delay"),
                tooltip=(
                    "Delay job by specified timecode. Format: dd:hh:mm:ss"
                ),
                placeholder="00:00:00:00"
            ),
            EnumDef(
                "publish_job_state",
                label="Publish Job State",
                default=default_values.get("publish_job_state"),
                items=[
                    {"value": "active", "label": "Active"},
                    {"value": "suspended", "label": "Suspended"}
                ]
            )
        ]

        return [
            attr_def
            for attr_def in attr_defs
            if attr_def.key in overrides
        ]

    @classmethod
    def register_create_context_callbacks(cls, create_context):
        create_context.add_value_changed_callback(cls.on_values_changed)

    @classmethod
    def on_values_changed(cls, event):
        for instance_change in event["changes"]:
            custom_frame_change = cls._get_publish_use_custom_frames_value(
                instance_change["changes"]
            )

            instance = instance_change["instance"]
            #recalculate only if context changes
            if (
                "task" not in instance_change
                and "folderPath" not in instance_change
                and not custom_frame_change
            ):
                continue

            if not cls.instance_matches_plugin_families(instance):
                continue

            new_attrs = cls.get_attr_defs_for_instance(
                event["create_context"], instance
            )
            instance.set_publish_plugin_attr_defs(cls.__name__, new_attrs)

    @classmethod
    def _is_custom_frames_used(cls, value) -> bool:
        return value in ["custom_only", "reuse_last_version"]

    @classmethod
    def _get_publish_use_custom_frames_value(
        cls,
        instance_data
    ) -> Optional[str]:
        return (
            instance_data.get("publish_attributes", {})
                         .get("CollectJobInfo", {})
                         .get("use_custom_frames")
        )

    def _get_jobinfo_defaults(self, instance):
        """Queries project setting for profile with default values

        Args:
            instance (pyblish.api.Instance): Source instance.

        Returns:
            (dict)
        """
        context_data = instance.context.data
        host_name = context_data["hostName"]
        task_entity = context_data["taskEntity"]

        task_name = task_type = None
        if task_entity:
            task_name = task_entity["name"]
            task_type = task_entity["taskType"]

        profile = filter_profiles(
            self.profiles,
            {
                "host_names": host_name,
                "task_types": task_type,
                "task_names": task_name,
                # "product_type": product_type
            }
        )
        return profile or {}
